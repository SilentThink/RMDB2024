/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class UpdateExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager *sm_manager_;

   public:
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }
    std::unique_ptr<RmRecord> Next() override {
        // 如果没有需要更新的记录，直接返回nullptr
        if (rids_.empty()) {
            return nullptr;
        }

        // 根据所有需要更新的记录rid进行更新
        for (auto& rid : rids_) {
            // 获取原记录
            auto rec = fh_->get_record(rid, context_);
            
            // 创建新记录，初始为原记录的拷贝
            auto new_rec = std::make_unique<RmRecord>(*rec);
            
            // 根据每个set子句更新对应的字段
            for (auto &set_clause : set_clauses_) {
                // 找到要更新的列
                auto col = tab_.get_col(set_clause.lhs.col_name);
                
                // 检查类型是否匹配
                if (col->type != set_clause.rhs.type) {
                    throw IncompatibleTypeError(coltype2str(col->type), coltype2str(set_clause.rhs.type));
                }
                
                // 初始化要设置的值
                set_clause.rhs.init_raw(col->len);
                
                // 将新值拷贝到记录中
                memcpy(new_rec->data + col->offset, set_clause.rhs.raw->data, col->len);
            }
            
            // 更新索引
            for (auto &index : tab_.indexes) {
                // 构造旧索引键值
                char* old_key = new char[index.col_tot_len];
                int old_offset = 0;
                for (auto &col : index.cols) {
                    memcpy(old_key + old_offset, rec->data + col.offset, col.len);
                    old_offset += col.len;
                }
                
                // 构造新索引键值
                char* new_key = new char[index.col_tot_len];
                int new_offset = 0;
                for (auto &col : index.cols) {
                    memcpy(new_key + new_offset, new_rec->data + col.offset, col.len);
                    new_offset += col.len;
                }
                
                // 获取索引处理器
                auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                
                // 删除旧索引
                ih->delete_entry(old_key, context_->txn_);
                
                // 插入新索引
                ih->insert_entry(new_key, rid, context_->txn_);
                
                delete[] old_key;
                delete[] new_key;
            }
            
            // 更新记录
            fh_->update_record(rid, new_rec->data, context_);
        }
        
        // 清空rids_列表
        rids_.clear();
        
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
};