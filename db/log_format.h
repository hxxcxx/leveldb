// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Log format information shared by reader and writer.
// See ../doc/log_format.md for more detail.

#ifndef STORAGE_LEVELDB_DB_LOG_FORMAT_H_
#define STORAGE_LEVELDB_DB_LOG_FORMAT_H_

namespace leveldb {
namespace log {

enum RecordType {
  kZeroType = 0,        /* 预留给预分配文件 */
  kFullType = 1,        /* 当前 Record 包含完整的日志记录 */
  kFirstType = 2,       /* 当前 Record 包含日志记录的起始部分 */
  kMiddleType = 3,      /* 当前 Record 包含日志记录的中间部分 */
  kLastType = 4         /* 当前 Record 包含日志记录的结束部分 */
};
static const int kMaxRecordType = kLastType;

static const int kBlockSize = 32768;

// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
static const int kHeaderSize = 4 + 2 + 1;

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_FORMAT_H_
