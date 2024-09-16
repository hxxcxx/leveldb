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
/*
这个构造函数用于初始化一个 Writer 对象，但它还考虑了文件的当前长度（dest_length）。
block_offset_ 是通过计算 dest_length % kBlockSize 来确定的，这意味着你会从文件当前写入位置的偏移量继续写入。
就是log当前写到哪个block的offset
*/
Writer::Writer(WritableFile* dest, uint64_t dest_length)
    : dest_(dest), block_offset_(dest_length % kBlockSize) {
  InitTypeCrc(type_crc_);
}

Writer::~Writer() = default;

Status Writer::AddRecord(const Slice& slice) {
  const char* ptr = slice.data();
  size_t left = slice.size();

  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  Status s;
  bool begin = true;
  do {
    /* 计算当前 Block 剩余写入 */
    const int leftover = kBlockSize - block_offset_;
    assert(leftover >= 0);
    /* 如果当前 Block 不足以写入 Record Header 的话，需要使用新的 Block */
    if (leftover < kHeaderSize) {
      // Switch to a new block
      if (leftover > 0) {
        // Fill the trailer (literal below relies on kHeaderSize being 7)
        static_assert(kHeaderSize == 7, "");
        /* 如果还有剩余空间的话，用 0 进行补齐 */
        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
      }
      /* Block 写入偏移量清零，向新的 Block 中写入 */
      block_offset_ = 0;
    }
    // Invariant: we never leave < kHeaderSize bytes in a block.
    
    assert(kBlockSize - block_offset_ - kHeaderSize >= 0);
    /* 计算剩余写入容量 */
    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
    /* 计算片段长度 */
    const size_t fragment_length = (left < avail) ? left : avail;

    RecordType type;
    const bool end = (left == fragment_length);
    if (begin && end) { /* 刚好装下 */
      type = kFullType;
    } else if (begin) {/* 一个 Block 无法装下 */
      type = kFirstType;
    } else if (end) { /* 能装下上一份数据 */
      type = kLastType;
    } else {/* 其它的中间状态 */
      type = kMiddleType;
    }
    /*这是预写日志真正被写入的地方，并更新数据指针、剩余长度以及 begin 标志位:
    计算数据的 CRC32、数据长度等信息，并调用 WritableFile 接口的 
    Append() 和 Flush() 方法将数据写入至内核缓冲区中，而是否进行 fsync() 取决于用户的配置，也就是 Options.sync 字段。
    */
    s = EmitPhysicalRecord(type, ptr, fragment_length);
    ptr += fragment_length;
    left -= fragment_length;
    begin = false;
  } while (s.ok() && left > 0);
  return s;
}

Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr,
                                  size_t length) {
  assert(length <= 0xffff);  // Must fit in two bytes
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

  // Write the header and the payload
  Status s = dest_->Append(Slice(buf, kHeaderSize));
  if (s.ok()) {
    s = dest_->Append(Slice(ptr, length));
    if (s.ok()) {
      s = dest_->Flush();
    }
  }
  block_offset_ += kHeaderSize + length;
  return s;
}

}  // namespace log
}  // namespace leveldb
