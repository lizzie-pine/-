#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MapReduce时段热搜分布 - Reducer
功能：聚合相同时段相同词的计数
输入：(hour,word)\t1
输出：(hour,word)\tcount
"""
import sys


def main():
    current_key = None
    current_count = 0
    
    for line in sys.stdin:
        line = line.strip()
        if not line or "\t" not in line:
            continue
        
        try:
            key, count_str = line.split("\t", 1)
            count = int(count_str)
        except Exception:
            continue
        
        if current_key == key:
            current_count += count
        else:
            if current_key is not None:
                print(f"{current_key}\t{current_count}")
            current_key = key
            current_count = count
    
    # 输出最后一个键值对
    if current_key is not None:
        print(f"{current_key}\t{current_count}")


if __name__ == "__main__":
    main()
