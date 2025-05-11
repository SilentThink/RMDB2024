/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "transaction_manager.h"
#include "record/rm_file_handle.h"
#include "system/sm_manager.h"

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
Transaction * TransactionManager::begin(Transaction* txn, LogManager* log_manager) {
    // Todo:
    // 1. 判断传入事务参数是否为空指针
    // 2. 如果为空指针，创建新事务
    // 3. 把开始事务加入到全局事务表中
    // 4. 返回当前事务指针
    
    // 1. 判断传入事务参数是否为空指针
    if (txn == nullptr) {
        // 2. 如果为空指针，创建新事务
        txn_id_t txn_id = next_txn_id_++;
        txn = new Transaction(txn_id);
        // 设置事务状态和时间戳
        txn->set_state(TransactionState::GROWING);
        txn->set_start_ts(next_timestamp_++);

        // 写入BEGIN日志
        if (log_manager != nullptr) {
            auto begin_log = std::make_unique<BeginLogRecord>(txn_id);
            begin_log->prev_lsn_ = INVALID_LSN;
            lsn_t lsn = log_manager->add_log_to_buffer(begin_log.get());
            txn->set_prev_lsn(lsn);
        }
    }

    // 3. 把开始事务加入到全局事务表中
    {
        std::unique_lock<std::mutex> lock(latch_);
        txn_map[txn->get_transaction_id()] = txn;
    }

    // 4. 返回当前事务指针
    return txn;
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::commit(Transaction* txn, LogManager* log_manager) {
    // Todo:
    // 1. 如果存在未提交的写操作，提交所有的写操作
    // 2. 释放所有锁
    // 3. 释放事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态

    assert(txn->get_state() != TransactionState::COMMITTED);
    
    // 1. 提交所有的写操作(写操作已经在执行时完成)
    auto write_set = txn->get_write_set();
    for (auto &write_record : *write_set) {
        // 写操作已经在执行时完成，这里不需要额外处理
    }

    // 2. 释放所有锁
    auto lock_set = txn->get_lock_set();
    for (auto &lock_data_id : *lock_set) {
        lock_manager_->unlock(txn, lock_data_id);
    }

    // 3. 释放事务相关资源
    write_set->clear();
    lock_set->clear();
    txn->get_index_latch_page_set()->clear();
    txn->get_index_deleted_page_set()->clear();

    // 4. 写入COMMIT日志并刷入磁盘
    if (log_manager != nullptr) {
        auto commit_log = std::make_unique<CommitLogRecord>(txn->get_transaction_id());
        commit_log->prev_lsn_ = txn->get_prev_lsn();
        lsn_t lsn = log_manager->add_log_to_buffer(commit_log.get());
        txn->set_prev_lsn(lsn);
        log_manager->flush_log_to_disk();
    }

    // 5. 更新事务状态
    txn->set_state(TransactionState::COMMITTED);
}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::abort(Transaction * txn, LogManager *log_manager) {
    // Todo:
    // 1. 回滚所有写操作
    // 2. 释放所有锁
    // 3. 清空事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态
    
    // 1. 回滚所有写操作
    auto write_set = txn->get_write_set();
    for (auto it = write_set->rbegin(); it != write_set->rend(); ++it) {
        auto write_record = *it;
        // 获取表的文件句柄
        RmFileHandle* fh = sm_manager_->fhs_[write_record->GetTableName()].get();
        
        // 根据写操作类型执行回滚
        switch(write_record->GetWriteType()) {
            case WType::INSERT_TUPLE:
                fh->delete_record(write_record->GetRid(), nullptr);
                break;
            case WType::DELETE_TUPLE:
                // 需要从RmRecord中获取data
                fh->insert_record(write_record->GetRecord().data, nullptr);
                break;
            case WType::UPDATE_TUPLE:
                // 需要从RmRecord中获取data
                fh->update_record(write_record->GetRid(), 
                                write_record->GetRecord().data, 
                                nullptr);
                break;
        }
    }

    // 2. 释放所有锁
    auto lock_set = txn->get_lock_set();
    for (auto &lock_data_id : *lock_set) {
        lock_manager_->unlock(txn, lock_data_id);
    }

    // 3. 清空事务相关资源
    write_set->clear();
    lock_set->clear();
    txn->get_index_latch_page_set()->clear();
    txn->get_index_deleted_page_set()->clear();

    // 4. 写入ABORT日志并刷入磁盘
    if (log_manager != nullptr) {
        auto abort_log = std::make_unique<AbortLogRecord>(txn->get_transaction_id());
        abort_log->prev_lsn_ = txn->get_prev_lsn();
        lsn_t lsn = log_manager->add_log_to_buffer(abort_log.get());
        txn->set_prev_lsn(lsn);
        log_manager->flush_log_to_disk();
    }

    // 5. 更新事务状态
    txn->set_state(TransactionState::ABORTED);
}