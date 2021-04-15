// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch::rep_ :=
//    sequence: fixed64
//    count: fixed32
//    data: record[count]
// record :=
//    kTypeValue varstring varstring         |
//    kTypeDeletion varstring
// varstring :=
//    len: varint32
//    data: uint8[len]

#include "leveldb/write_batch.h"

#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"

#include "leveldb/db.h"

#include "util/coding.h"

namespace leveldb
{

  // WriteBatch header has an 8-byte sequence number followed by a 4-byte count.
  static const size_t kHeader = 12;

  WriteBatch::WriteBatch(){ Clear(); }

  WriteBatch::~WriteBatch() = default;

  WriteBatch::Handler::~Handler() = default;

  void WriteBatch::Clear()
  {
    rep_.clear();
    rep_.resize(kHeader);
  }

  size_t WriteBatch::ApproximateSize() const{ return rep_.size(); }

  Status WriteBatch::Iterate(Handler* handler) const
  {
    Slice input(rep_);
    if( input.size() < kHeader )
    {
      return Status::Corruption("malformed WriteBatch (too small)");
    }

    input.remove_prefix(kHeader);
    Slice key, value;
    int found = 0;
    while( !input.empty())
    {
      found++;
      char tag = input[0];
      input.remove_prefix(1);
      switch( tag )
      {
        case kTypeValue:
          if( GetLengthPrefixedSlice(&input, &key) && GetLengthPrefixedSlice(&input, &value))
          {
            handler->Put(key, value);
          }else
          {
            return Status::Corruption("bad WriteBatch Put");
          }
          break;
        case kTypeDeletion:
          if( GetLengthPrefixedSlice(&input, &key))
          {
            handler->Delete(key);
          }else
          {
            return Status::Corruption("bad WriteBatch Delete");
          }
          break;
        default:
          return Status::Corruption("unknown WriteBatch tag");
      }
    }
    if( found != WriteBatchInternal::Count(this))
    {
      return Status::Corruption("WriteBatch has wrong count");
    }else
    {
      return Status::OK();
    }
  }

  int WriteBatchInternal::Count(const WriteBatch* b)
  {
    return DecodeFixed32(b->rep_.data() + 8);
  }

  //把n以
  void WriteBatchInternal::SetCount(WriteBatch* b, int n)
  {
    // 看起来rep_[8-12)是count字段。
    EncodeFixed32(&b->rep_[8], n);
  }

  SequenceNumber WriteBatchInternal::Sequence(const WriteBatch* b)
  {
    //看起来rep_[0,8)是Sequence字段。
    return SequenceNumber(DecodeFixed64(b->rep_.data()));
  }

  void WriteBatchInternal::SetSequence(WriteBatch* b, SequenceNumber seq)
  {
    EncodeFixed64(&b->rep_[0], seq);
  }

  //把key和value写入rep_,每个key-value的钱8个字节是count，1个字节是type，后面分别是k和v，k和v均由size+value组成。
  void WriteBatch::Put(const Slice& key, const Slice& value)
  {
    // rep_的前8个字节是存储的是size，Count解码读到size，SetCount把size+1写入前8个字节，即
    // size++
    WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);

    // type只有两类，Delete和Value，Value的意思应该就是写入新值
    rep_.push_back(static_cast<char>(kTypeValue));

    //这个函数在req_后面加上变长size(key),和key的value。
    PutLengthPrefixedSlice(&rep_, key);
    //同上，等于把key，value分别按照size value的格式填入了req_
    PutLengthPrefixedSlice(&rep_, value);
  }

  //删除操作也是通过写入来实现的，size+1，和Put的区别是两点：
  // 1. 只写key，不写Value
  // 2. 写key的type前缀是Deletion，标记为删除
  void WriteBatch::Delete(const Slice& key)
  {
    WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
    rep_.push_back(static_cast<char>(kTypeDeletion));
    PutLengthPrefixedSlice(&rep_, key);
  }

  void WriteBatch::Append(const WriteBatch& source)
  {
    WriteBatchInternal::Append(this, &source);
  }

  namespace
  {  //这个单独一个namespace是干嘛的???
    // 匿名namespace，这个和static的作用域有点像，匿名namespace是为了让namespace里面的函数/类/对象等只能在当前文件访问。
    // handler是个抽象类
    // It's called an unnamed namespace / anonymous namespace. It's use is to make functions/objects/etc accessible only within that file. It's almost the same as static in C.
    class MemTableInserter : public WriteBatch::Handler
    {
    public:
      SequenceNumber sequence_;
      MemTable* mem_;

      void Put(const Slice& key, const Slice& value) override
      {
        mem_->Add(sequence_, kTypeValue, key, value);
        sequence_++;
      }

      void Delete(const Slice& key) override
      {
        mem_->Add(sequence_, kTypeDeletion, key, Slice());
        sequence_++;
      }
    };
  }  // namespace

  Status WriteBatchInternal::InsertInto(const WriteBatch* b, MemTable* memtable)
  {
    MemTableInserter inserter;
    inserter.sequence_ = WriteBatchInternal::Sequence(b);
    inserter.mem_ = memtable;
    return b->Iterate(&inserter);
  }

  //这个事直接把contents写入WriteBatch的rep_,contents包含header
  void WriteBatchInternal::SetContents(WriteBatch* b, const Slice& contents)
  {
    assert(contents.size() >= kHeader);
    b->rep_.assign(contents.data(), contents.size());
  }

  //把src的数据段append到dst的后面。
  //更新dst的count
  // dst的Sequence不变。
  void WriteBatchInternal::Append(WriteBatch* dst, const WriteBatch* src)
  {
    // Count相加
    SetCount(dst, Count(dst) + Count(src));
    // src必须包含header
    assert(src->rep_.size() >= kHeader);
    // append数据段到dst的rep_
    dst->rep_.append(src->rep_.data() + kHeader, src->rep_.size() - kHeader);
  }

}  // namespace leveldb
