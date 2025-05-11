/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL
v2. You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "log_manager.h"
#include <cstring>

/**
 * @description: 添加日志记录到日志缓冲区中，并返回日志记录号
 * @param {LogRecord*} log_record 要写入缓冲区的日志记录
 * @return {lsn_t} 返回该日志的日志记录号
 */
lsn_t LogManager::add_log_to_buffer(LogRecord *log_record) {
  std::unique_lock<std::mutex> lock(latch_);

  // 1. 为日志记录分配LSN
  log_record->lsn_ = ++global_lsn_;

  // 2. 检查日志缓冲区是否有足够空间
  if (log_buffer_.is_full(log_record->log_tot_len_)) {
    // 如果缓冲区满了，先将其刷新到磁盘
    flush_log_to_disk();
    log_buffer_.offset_ = 0; // 重置偏移量
  }

  // 3. 将日志记录序列化到缓冲区
  log_record->serialize(log_buffer_.buffer_ + log_buffer_.offset_);
  log_buffer_.offset_ += log_record->log_tot_len_;

  return log_record->lsn_;
}

/**
 * @description:
 * 把日志缓冲区的内容刷到磁盘中，由于目前只设置了一个缓冲区，因此需要阻塞其他日志操作
 */
void LogManager::flush_log_to_disk() {
  std::unique_lock<std::mutex> lock(latch_);
  if (log_buffer_.offset_ == 0) {
    return; // 缓冲区为空，无需刷新
  }

  // 将日志缓冲区的内容写入磁盘
  disk_manager_->write_log(log_buffer_.buffer_, log_buffer_.offset_);

  // 更新持久化的LSN
  persist_lsn_ = global_lsn_;

  // 清空缓冲区
  memset(log_buffer_.buffer_, 0, LOG_BUFFER_SIZE);
  log_buffer_.offset_ = 0;
}
