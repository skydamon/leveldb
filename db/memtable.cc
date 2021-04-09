// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/memtable.h"

#include "db/dbformat.h"

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

#include "util/coding.h"

namespace leveldb {

//从data里面解析出数据域，并存到Slice中返回。（数据域前面是size域）
static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;
  // 32bit的值，最多用5个byte存储（含标记位），这里是读取data前面的size域
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
  //创建并返回一个size为len的Slice，p此时是data的数据域起点。
  return Slice(p, len);
}

MemTable::MemTable(const InternalKeyComparator& comparator)
    : comparator_(comparator), refs_(0), table_(comparator_, &arena_) {}

MemTable::~MemTable() { assert(refs_ == 0); }

size_t MemTable::ApproximateMemoryUsage() { return arena_.MemoryUsage(); }

//比较两个数据的大小，先转成Slice，再比较
int MemTable::KeyComparator::operator()(const char* aptr,
                                        const char* bptr) const {
  //这里就是把aptr和bptr包装成一个Slice，然后比较大小。Slice直接拷贝了指针，不会复制整个字符串。
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);
  return comparator.Compare(a, b);
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
//从Slice组装一个编码后的key，包含size+value。这里复制了字符串。
static const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  //设置scratch的size域名（至多前面5个byte）
  PutVarint32(scratch, target.size());
  //把data放到scratch后面。
  scratch->append(target.data(), target.size());
  return scratch->data();
}

//这个主要是把SkipList的Iterator封装了一下
class MemTableIterator : public Iterator {
 public:
  explicit MemTableIterator(MemTable::Table* table) : iter_(table) {}

  MemTableIterator(const MemTableIterator&) = delete;
  MemTableIterator& operator=(const MemTableIterator&) = delete;

  ~MemTableIterator() override = default;

  bool Valid() const override { return iter_.Valid(); }
  void Seek(const Slice& k) override { iter_.Seek(EncodeKey(&tmp_, k)); }
  void SeekToFirst() override { iter_.SeekToFirst(); }
  void SeekToLast() override { iter_.SeekToLast(); }
  void Next() override { iter_.Next(); }
  void Prev() override { iter_.Prev(); }

  //返回key的数据域，以Slice的形式保存。
  Slice key() const override { return GetLengthPrefixedSlice(iter_.key()); }

  //返回当前的value的数据域，以Slice的形式保存。
  Slice value() const override {
    Slice key_slice = GetLengthPrefixedSlice(iter_.key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  Status status() const override { return Status::OK(); }

 private:
  MemTable::Table::Iterator iter_;
  std::string tmp_;  // For passing to EncodeKey
};

Iterator* MemTable::NewIterator() { return new MemTableIterator(&table_); }

//添加k,v，
// 从Arena获取内存，然后把编码后的字符串存到内存中，然后把指针存入到skiplist中。
// 编码格式如下：
// var_key_size;key_data;fix_seq_type;var_value_size;value_data
// 简单来说就是key's size-data + fix_seq_type + values' size-data
// seq是序列号，type是类型，有删除和添加的元素两种。leveldb中删除元素也是添加一条记录。
void MemTable::Add(SequenceNumber s, ValueType type, const Slice& key,
                   const Slice& value) {
  // Format of an entry is concatenation of:
  //  key_size     : varint32 of internal_key.size()
  //  key bytes    : char[internal_key.size()]
  //  value_size   : varint32 of value.size()
  //  value bytes  : char[value.size()]
  size_t key_size = key.size();
  size_t val_size = value.size();
  size_t internal_key_size = key_size + 8;  //还有8个字节给了fix位，存取seq和type
  const size_t encoded_len = VarintLength(internal_key_size) +
                             internal_key_size + VarintLength(val_size) +
                             val_size;
  char* buf = arena_.Allocate(encoded_len);
  char* p = EncodeVarint32(buf, internal_key_size);
  std::memcpy(p, key.data(), key_size);
  p += key_size;
  // s使用前7个字节，type使用后1个字节，然后小端编码到8字节存储中。固定长度编码。
  EncodeFixed64(p, (s << 8) | type);
  p += 8;
  p = EncodeVarint32(p, val_size);
  std::memcpy(p, value.data(), val_size);
  assert(p + val_size == buf + encoded_len);

  //插入到skiplist中。
  table_.Insert(buf);
}

//这个就是从skiplist中查找key，然后解码，把value的data赋值给value。如果没有找到返回false。如果找到了返回true，但是如果是delete的话，要把status设置为notfound。
bool MemTable::Get(const LookupKey& key, std::string* value, Status* s) {
  Slice memkey = key.memtable_key();
  Table::Iterator iter(&table_);
  iter.Seek(memkey.data());
  if (iter.Valid()) {
    // entry format is:
    //    klength  varint32
    //    userkey  char[klength]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    const char* entry = iter.key();
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
    if (comparator_.comparator.user_comparator()->Compare(
            Slice(key_ptr, key_length - 8), key.user_key()) == 0) {
      // Correct user key
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      switch (static_cast<ValueType>(tag & 0xff)) {
        case kTypeValue: {
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
          value->assign(v.data(), v.size());
          return true;
        }
        case kTypeDeletion:
          *s = Status::NotFound(Slice());
          return true;
      }
    }
  }
  return false;
}

}  // namespace leveldb
