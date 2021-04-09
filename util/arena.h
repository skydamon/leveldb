// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_ARENA_H_
#define STORAGE_LEVELDB_UTIL_ARENA_H_

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace leveldb
{

  //根据网上看到的资料，Arena是个内存分配器，用来为memtable管理内存的。
  class Arena
  {
  public:
    Arena();

    Arena(const Arena&) = delete;

    Arena& operator=(const Arena&) = delete;

    ~Arena();

    // Return a pointer to a newly allocated memory block of "bytes" bytes.
    char* Allocate(size_t bytes);

    // Allocate memory with the normal alignment guarantees provided by malloc.
    char* AllocateAligned(size_t bytes);

    // Returns an estimate of the total memory usage of data allocated
    // by the arena.
    //这个的值其实是分配的内存块的大小。
    size_t MemoryUsage() const
    {
      return memory_usage_.load(std::memory_order_relaxed);
    }

  private:
    char* AllocateFallback(size_t bytes);

    char* AllocateNewBlock(size_t block_bytes);

    // Allocation state
    char* alloc_ptr_;
    size_t alloc_bytes_remaining_;

    // Array of new[] allocated memory blocks
    std::vector<char*> blocks_;

    // Total memory usage of the arena.
    //
    // TODO(costan): This member is accessed via atomics, but the others are
    //               accessed without any locking. Is this OK?
    std::atomic<size_t> memory_usage_;
  };

  inline char* Arena::Allocate(size_t bytes)
  {
    // The semantics of what to return are a bit messy if we allow
    // 0-byte allocations, so we disallow them here (we don't need
    // them for our internal use).
    assert(bytes > 0);
    //如果当前block还有空间，就直接从当前block拿内存，这里可以看出alloc_ptr指向的是可以分配的内存的首地址，alloc_bytes_remaining_是当前block剩余的size
    if( bytes <= alloc_bytes_remaining_ )
    {
      char* result = alloc_ptr_;
      alloc_ptr_ += bytes;
      alloc_bytes_remaining_ -= bytes;
      return result;
    }
    return AllocateFallback(bytes);
  }

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_ARENA_H_

