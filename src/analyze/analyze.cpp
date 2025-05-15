/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "analyze.h"

/**
 * @description: 分析器，进行语义分析和查询重写，需要检查不符合语义规定的部分
 * @param {shared_ptr<ast::TreeNode>} parse parser生成的结果集
 * @return {shared_ptr<Query>} Query 
 */
std::shared_ptr<Query> Analyze::do_analyze(std::shared_ptr<ast::TreeNode> parse)
{
    std::shared_ptr<Query> query = std::make_shared<Query>();
    if (auto x = std::dynamic_pointer_cast<ast::SelectStmt>(parse))
    {
        // 处理表名
        query->tables = std::move(x->tabs);
        /** TODO: 检查表是否存在 */
        for (auto &tab_name : query->tables) {
            if (!sm_manager_->db_.is_table(tab_name)) {
                throw TableNotFoundError(tab_name);
            }
        }

        // 处理target list，再target list中添加上表名，例如 a.id
        for (auto &sv_sel_col : x->cols) {
            TabCol sel_col = {.tab_name = sv_sel_col->tab_name, .col_name = sv_sel_col->col_name};
            query->cols.push_back(sel_col);
        }
        
        std::vector<ColMeta> all_cols;
        get_all_cols(query->tables, all_cols);
        if (query->cols.empty()) {
            // select all columns
            for (auto &col : all_cols) {
                TabCol sel_col = {.tab_name = col.tab_name, .col_name = col.name};
                query->cols.push_back(sel_col);
            }
        } else {
            // infer table name from column name
            for (auto &sel_col : query->cols) {
                sel_col = check_column(all_cols, sel_col);  // 列元数据校验
            }
        }
        //处理where条件
        get_clause(x->conds, query->conds);
        check_clause(query->tables, query->conds);
    } else if (auto x = std::dynamic_pointer_cast<ast::UpdateStmt>(parse)) {
        /** TODO: */
        // 需要处理update语句,将set子句填充到query->set_clauses中
        query->tables.push_back(x->tab_name);
        get_clause(x->conds, query->conds);
        check_clause(query->tables, query->conds);
        
        // 处理set子句
        std::vector<ColMeta> all_cols;
        get_all_cols(query->tables, all_cols);
        
        // 遍历所有set子句
        for (auto &sv_set : x->set_clauses) {
            SetClause set_clause;
            // 设置要更新的列
            set_clause.lhs = {.tab_name = x->tab_name, .col_name = sv_set->col_name};
            // 检查列是否存在
            set_clause.lhs = check_column(all_cols, set_clause.lhs);
            
            // 获取右值并进行类型转换
            if (auto rhs_val = std::dynamic_pointer_cast<ast::Value>(sv_set->val)) {
                // 如果右值是常量
                set_clause.rhs = convert_sv_value(rhs_val);
                
                // 获取列的类型信息用于类型检查和转换
                TabMeta &tab = sm_manager_->db_.get_table(set_clause.lhs.tab_name);
                auto col = tab.get_col(set_clause.lhs.col_name);
                
                // 进行类型转换
                if (col->type == TYPE_FLOAT && set_clause.rhs.type == TYPE_INT) {
                    // 整数转浮点数
                    int int_val = set_clause.rhs.int_val;
                    set_clause.rhs.set_float(static_cast<float>(int_val));
                } else if (col->type == TYPE_INT && set_clause.rhs.type == TYPE_FLOAT) {
                    // 浮点数转整数
                    float float_val = set_clause.rhs.float_val;
                    set_clause.rhs.set_int(static_cast<int>(float_val));
                } else if (col->type != set_clause.rhs.type) {
                    // 其他类型不匹配的情况
                    throw IncompatibleTypeError(coltype2str(col->type), 
                                            coltype2str(set_clause.rhs.type));
                }

                // 初始化要设置的值
                set_clause.rhs.init_raw(col->len);
                
            } else {
                throw InternalError("Unexpected value type in set clause");
            }
            
            // 将处理好的set子句添加到查询中
            query->set_clauses.push_back(set_clause);
        }
        

    } else if (auto x = std::dynamic_pointer_cast<ast::DeleteStmt>(parse)) {
        //处理where条件
        get_clause(x->conds, query->conds);
        check_clause({x->tab_name}, query->conds);        
    } else if (auto x = std::dynamic_pointer_cast<ast::InsertStmt>(parse)) {
        // 处理insert 的values值
        for (auto &sv_val : x->vals) {
            query->values.push_back(convert_sv_value(sv_val));
        }
    } else {
        // do nothing
    }
    query->parse = std::move(parse);
    return query;
}


TabCol Analyze::check_column(const std::vector<ColMeta> &all_cols, TabCol target) {
    if (target.tab_name.empty()) {
        // Table name not specified, infer table name from column name
        std::string tab_name;
        for (auto &col : all_cols) {
            if (col.name == target.col_name) {
                if (!tab_name.empty()) {
                    throw AmbiguousColumnError(target.col_name);
                }
                tab_name = col.tab_name;
            }
        }
        if (tab_name.empty()) {
            throw ColumnNotFoundError(target.col_name);
        }
        target.tab_name = tab_name;
    } else {
        /** TODO: Make sure target column exists */
        
    }
    return target;
}

void Analyze::get_all_cols(const std::vector<std::string> &tab_names, std::vector<ColMeta> &all_cols) {
    for (auto &sel_tab_name : tab_names) {
        // 这里db_不能写成get_db(), 注意要传指针
        const auto &sel_tab_cols = sm_manager_->db_.get_table(sel_tab_name).cols;
        all_cols.insert(all_cols.end(), sel_tab_cols.begin(), sel_tab_cols.end());
    }
}

void Analyze::get_clause(const std::vector<std::shared_ptr<ast::BinaryExpr>> &sv_conds, std::vector<Condition> &conds) {
    conds.clear();
    for (auto &expr : sv_conds) {
        Condition cond;
        cond.lhs_col = {.tab_name = expr->lhs->tab_name, .col_name = expr->lhs->col_name};
        cond.op = convert_sv_comp_op(expr->op);
        if (auto rhs_val = std::dynamic_pointer_cast<ast::Value>(expr->rhs)) {
            cond.is_rhs_val = true;
            cond.rhs_val = convert_sv_value(rhs_val);
        } else if (auto rhs_col = std::dynamic_pointer_cast<ast::Col>(expr->rhs)) {
            cond.is_rhs_val = false;
            cond.rhs_col = {.tab_name = rhs_col->tab_name, .col_name = rhs_col->col_name};
        }
        conds.push_back(cond);
    }
}

void Analyze::check_clause(const std::vector<std::string> &tab_names, std::vector<Condition> &conds) {
    // auto all_cols = get_all_cols(tab_names);
    std::vector<ColMeta> all_cols;
    get_all_cols(tab_names, all_cols);
    // Get raw values in where clause
    for (auto &cond : conds) {
        // Infer table name from column name
        cond.lhs_col = check_column(all_cols, cond.lhs_col);
        if (!cond.is_rhs_val) {
            cond.rhs_col = check_column(all_cols, cond.rhs_col);
        }
        TabMeta &lhs_tab = sm_manager_->db_.get_table(cond.lhs_col.tab_name);
        auto lhs_col = lhs_tab.get_col(cond.lhs_col.col_name);
        ColType lhs_type = lhs_col->type;
        ColType rhs_type;
        if (cond.is_rhs_val) {
            // 对常量值进行类型转换
            if (lhs_type == TYPE_FLOAT && cond.rhs_val.type == TYPE_INT) {
                // 如果列是float类型而常量是int类型，将常量转换为float
                int int_val = cond.rhs_val.int_val;
                cond.rhs_val.set_float(static_cast<float>(int_val));
            } else if (lhs_type == TYPE_INT && cond.rhs_val.type == TYPE_FLOAT) {
                // 如果列是int类型而常量是float类型，将常量转换为int
                float float_val = cond.rhs_val.float_val;
                cond.rhs_val.set_int(static_cast<int>(float_val));
            }
            cond.rhs_val.init_raw(lhs_col->len);
            rhs_type = cond.rhs_val.type;
        } else {
            TabMeta &rhs_tab = sm_manager_->db_.get_table(cond.rhs_col.tab_name);
            auto rhs_col = rhs_tab.get_col(cond.rhs_col.col_name);
            rhs_type = rhs_col->type;
        }
        
        // 检查类型是否兼容（现在允许int和float之间的比较）
        if (lhs_type != rhs_type && 
            !((lhs_type == TYPE_INT && rhs_type == TYPE_FLOAT) || 
              (lhs_type == TYPE_FLOAT && rhs_type == TYPE_INT))) {
            throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
        }
    }
}


Value Analyze::convert_sv_value(const std::shared_ptr<ast::Value> &sv_val) {
    Value val;
    if (auto int_lit = std::dynamic_pointer_cast<ast::IntLit>(sv_val)) {
        val.set_int(int_lit->val);
    } else if (auto float_lit = std::dynamic_pointer_cast<ast::FloatLit>(sv_val)) {
        val.set_float(float_lit->val);
    } else if (auto str_lit = std::dynamic_pointer_cast<ast::StringLit>(sv_val)) {
        val.set_str(str_lit->val);
    } else {
        throw InternalError("Unexpected sv value type");
    }
    return val;
}

CompOp Analyze::convert_sv_comp_op(ast::SvCompOp op) {
    std::map<ast::SvCompOp, CompOp> m = {
        {ast::SV_OP_EQ, OP_EQ}, {ast::SV_OP_NE, OP_NE}, {ast::SV_OP_LT, OP_LT},
        {ast::SV_OP_GT, OP_GT}, {ast::SV_OP_LE, OP_LE}, {ast::SV_OP_GE, OP_GE},
    };
    return m.at(op);
}
