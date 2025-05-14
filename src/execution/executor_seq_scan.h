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

#include "defs.h"
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"
#include <memory>

class SeqScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;              // 表的名称
    std::vector<Condition> conds_;      // scan的条件
    RmFileHandle *fh_;                  // 表的数据文件句柄
    std::vector<ColMeta> cols_;         // scan后生成的记录的字段
    size_t len_;                        // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_;  // 同conds_，两个字段相同

    Rid rid_;
    std::unique_ptr<RecScan> scan_;     // table_iterator

    SmManager *sm_manager_;

   public:
    SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab.cols;
        len_ = cols_.back().offset + cols_.back().len;

        context_ = context;

        fed_conds_ = conds_;
    }

    void beginTuple() override {
        // 初始化表扫描迭代器
        scan_ = std::make_unique<RmScan>(fh_);
        nextTuple();
    }

    void nextTuple() override {
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            auto rec = fh_->get_record(rid_, context_);
            bool satisfies = true;

            // 检查所有条件
            for (auto &cond: conds_) {
                auto lhs_col = get_col(cols_, cond.lhs_col);
                char *lhs_val = rec->data + lhs_col->offset;
                char *rhs_val = cond.rhs_val.raw->data;

                // 根据条件类型和值类型进行比较
                bool res = false;
                if (lhs_col->type == TYPE_INT) {
                    int lhs = *(int *)lhs_val;
                    int rhs = *(int *)rhs_val;
                    res = compare(lhs, rhs, cond.op);
                } else if (lhs_col->type == TYPE_FLOAT) {
                    float lhs = *(float *)lhs_val;
                    float rhs = *(float *)rhs_val;
                    res = compare(lhs, rhs, cond.op);
                } else if (lhs_col->type == TYPE_STRING) {
                    std::string lhs(lhs_val, lhs_col->len);
                    std::string rhs(rhs_val, cond.rhs_val.raw->size);
                    res = compare(lhs, rhs, cond.op);
                }

                if (!res) {
                    satisfies = false;
                    break;
                }
            }

            if (satisfies) {
                // 找到满足条件的记录
                return;
            }
            // 继续查找下一条记录
            scan_->next();
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        if (scan_->is_end()) {
            return nullptr;
        }

        // 获取当前记录
        auto rec = fh_->get_record(rid_, context_);
        // 移动到下一条满足条件的记录
        scan_->next();
        
        return rec;
    }

    Rid &rid() override { return rid_; }

    const std::vector<ColMeta> &cols() const override {
        return cols_;
    };

    bool is_end() const override { return scan_->is_end();};
};