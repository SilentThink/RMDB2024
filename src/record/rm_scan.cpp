/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_scan.h"
#include "rm_file_handle.h"

/**
 * @brief 初始化file_handle和rid
 * @param file_handle
 */
RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle) {
    // Todo:
    // 初始化file_handle和rid（指向第一个存放了记录的位置）
    rid_.page_no = 1; // 从1开始，因为0是文件头页面
    rid_.slot_no = -1; // 从-1开始，这样第一次next()会从0开始查找

    // 移动到第一个有效记录
    next();
}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
void RmScan::next() {
    // Todo:
    // 找到文件中下一个存放了记录的非空闲位置，用rid_来指向这个位置

    while (rid_.page_no < file_handle_->file_hdr_.num_pages) {
        // 获取当前页面
        RmPageHandle page_handle = file_handle_->fetch_page_handle(rid_.page_no);
        
        // 在当前页面中查找下一个有效记录
        int next_slot = rid_.slot_no + 1;
        while (next_slot < file_handle_->file_hdr_.num_records_per_page) {
            if (Bitmap::is_set(page_handle.bitmap, next_slot)) {
                // 找到有效记录
                rid_.slot_no = next_slot;
                return;
            }
            next_slot++;
        }
        
        // 当前页面已经遍历完，移动到下一页
        rid_.page_no++;
        rid_.slot_no = -1;
    }
    
    // 所有页面都遍历完了，设置为结束状态
    rid_.page_no = file_handle_->file_hdr_.num_pages;
    rid_.slot_no = 0;
}

/**
 * @brief ​ 判断是否到达文件末尾
 */
bool RmScan::is_end() const {
    // Todo: 修改返回值
    return rid_.page_no >= file_handle_->file_hdr_.num_pages;
}

/**
 * @brief RmScan内部存放的rid
 */
Rid RmScan::rid() const {
    return rid_;
}