#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MapReduce时段热搜分布 - Mapper
功能：统计不同时段的热搜词分布
输入：历史数据文件 (timestamp,rank,title)
输出：(hour,word)\t1
"""
import sys
import time


def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        # 跳过注释行
        if line.startswith("#"):
            continue
        
        try:
            # 解析历史数据格式：timestamp,rank,title
            parts = line.split(",", 2)
            if len(parts) < 3:
                continue
            
            timestamp = int(parts[0].strip())
            title = parts[2].strip()
            
            # 获取小时时段
            hour = time.strftime("%H", time.localtime(timestamp))
            
            # 简单分词（保留主要关键词）
            # 这里可以根据需要替换为更复杂的分词逻辑
            words = title.split()
            for word in words:
                if len(word) >= 2:  # 过滤短词
                    print(f"{hour},{word}\t1")
                    
        except Exception as e:
            continue


if __name__ == "__main__":
    main()
