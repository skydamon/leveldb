// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_writer.h"

#include <cstdint>

#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

static void InitTypeCrc(uint32_t* type_crc) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc[i] = crc32c::Value(&t, 1);
  }
}

Writer::Writer(WritableFile* dest) : dest_(dest), block_offset_(0) {
  InitTypeCrc(type_crc_);
}

Writer::Writer(WritableFile* dest, uint64_t dest_length)
    : dest_(dest), block_offset_(dest_length % kBlockSize) {
  InitTypeCrc(type_crc_);
}

Writer::~Writer() = default;


//所以这里就是把slick的数据分成1~n个block写入到文件中。
//每个block最前面是7个字节的header，存储了这个block内部数据的长度，block的位置、和数据的crc校验码
//每个block后面就是实际的数据
Status Writer::AddRecord(const Slice& slice) {
  const char* ptr = slice.data();
  size_t left = slice.size();

  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  Status s;
  bool begin = true;
  do {
    const int leftover = kBlockSize - block_offset_;
    assert(leftover >= 0);
    //所剩空间不够写一个Header的，开启一个新的block
    if (leftover < kHeaderSize) {
      // Switch to a new block
      if (leftover > 0) {
        // Fill the trailer (literal below relies on kHeaderSize being 7)
        static_assert(kHeaderSize == 7, "");
        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
      }
      block_offset_ = 0;
    }

    // Invariant: we never leave < kHeaderSize bytes in a block.
    assert(kBlockSize - block_offset_ - kHeaderSize >= 0);

    //每个Block由header和data组成，下面的逻辑是当要写入的数据长度（left）大于BLock可用长度(avail)时，对数据data进行分片。
    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
    //如果left小于avail，那么段长度就是left，只需要一个block，如果left大于avial，就需要分段，段长度等于avail
    const size_t fragment_length = (left < avail) ? left : avail;

    //这里的RecordType是表征当前block在data中的相对位置
    RecordType type;
    //left这里表示剩下的还没有写入的数据，当下一个分段长度==left长度的时候，说明到底了。
    const bool end = (left == fragment_length);  

    if (begin && end) {
      //begin为True，即第一个block，end为True，说明是最后一个block，所以只有一个block
      type = kFullType; //一个block存下，所以是fullType
    } else if (begin) {
      //begin为True，那么end为FALSE,说明是第一个block，但不是最后一个
      type = kFirstType; //第一个block
    } else if (end) {
      //begin为False，end为True，至少有两个block，当前block为最后一个block
      type = kLastType;
    } else {
      //既不是第一个block也不是最后一个block，是中间的block
      type = kMiddleType;
    }

    //ptr是实际要写入的数据
    s = EmitPhysicalRecord(type, ptr, fragment_length);
    ptr += fragment_length;
    left -= fragment_length;
    begin = false;
  } while (s.ok() && left > 0);
  return s;
}


//把当前block的数据写入，block的开头是header，7个字节。这里实际上等于是block顺序存入文件里面。
Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr,
                                  size_t length) {
  assert(length <= 0xffff);  // Must fit in two bytes（这里的意思是length本身的size可以用两个字节存储）
  assert(block_offset_ + kHeaderSize + length <= kBlockSize);

  // Format the header
  char buf[kHeaderSize];
  buf[4] = static_cast<char>(length & 0xff);
  buf[5] = static_cast<char>(length >> 8);
  buf[6] = static_cast<char>(t);

  // Compute the crc of the record type and the payload.
  uint32_t crc = crc32c::Extend(type_crc_[t], ptr, length);
  crc = crc32c::Mask(crc);  // Adjust for storage
  EncodeFixed32(buf, crc);
  //到这里buf[0-3]存储了crc，buf[4-5]存除了length，buf[6]存储了ReocrdType（表示当前block的位置）

  // Write the header and the payload
  //Writer写Header的内容
  Status s = dest_->Append(Slice(buf, kHeaderSize));
  if (s.ok()) {
    //写入数据内容
    //初步看了一下env_posix的实现，这里貌似并不会保证每次Append写入磁盘
    s = dest_->Append(Slice(ptr, length));
    if (s.ok()) {
      //这里Flush是想刷到磁盘上应该
      s = dest_->Flush();
    }
  }
  //这里的offset_ = header+ data的长度。
  block_offset_ += kHeaderSize + length;
  return s;
}

}  // namespace log
}  // namespace leveldb
