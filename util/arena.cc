// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/arena.h"

namespace leveldb
{

  static const int kBlockSize = 4096;

  Arena::Arena() : alloc_ptr_(nullptr), alloc_bytes_remaining_(0), memory_usage_(0){}

  Arena::~Arena()
  {
    for( size_t i = 0; i < blocks_.size(); i++ )
    {
      delete[] blocks_[i];
    }
  }

  char* Arena::AllocateFallback(size_t bytes)
  {
    if( bytes > kBlockSize / 4 )
    {

      // 注意，这里是直接分配了调用者申请的bytes大小的内存，不是标准block的大小，这样就可以继续用上一个Block剩下的内存了，节省内存。
      // Object is more than a quarter of our block size.  Allocate it separately
      // to avoid wasting too much space in leftover bytes.
      char* result = AllocateNewBlock(bytes);
      return result;
    }

    // We waste the remaining space in the current block.
    alloc_ptr_ = AllocateNewBlock(kBlockSize);
    alloc_bytes_remaining_ = kBlockSize;

    char* result = alloc_ptr_;
    alloc_ptr_ += bytes;
    alloc_bytes_remaining_ -= bytes;
    return result;
  }

  char* Arena::AllocateAligned(size_t bytes)
  {
    const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
    static_assert((align & (align - 1)) == 0, "Pointer size should be a power of 2");
    size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align - 1);

    //这里slop是求余操作 A % (2^x) = A & (2^x -1);，只有第二个操作数是2^x时才能这样执行。
    size_t slop = (current_mod == 0 ? 0 : align - current_mod);
    size_t needed = bytes + slop;
    char* result;
    if( needed <= alloc_bytes_remaining_ )
    {
      result = alloc_ptr_ + slop;
      alloc_ptr_ += needed;
      alloc_bytes_remaining_ -= needed;
    }else
    {
      // AllocateFallback always returned aligned memory
      result = AllocateFallback(bytes);
    }
    assert((reinterpret_cast<uintptr_t>(result) & (align - 1)) == 0);
    return result;
  }


  //新创建一个指定size的block，指针存到block_中，memory_usage_累加新的size
  char* Arena::AllocateNewBlock(size_t block_bytes)
  {
    char* result = new char[block_bytes];
    blocks_.push_back(result);
    memory_usage_.fetch_add(block_bytes + sizeof(char*), std::memory_order_relaxed);
    return result;
  }

}  // namespace leveldb


