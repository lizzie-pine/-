#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MapReduce共现关系计算 - Reducer
功能：聚合相同共现对的计数，生成最终共现关系
输入：(word1,word2)\t1
输出：(word1,word2)\tcount
"""
import sys


def main():
    current_pair = None
    current_count = 0
    
    for line in sys.stdin:
        line = line.strip()
        if not line or "\t" not in line:
            continue
        
        try:
            pair_str, count_str = line.split("\t", 1)
            count = int(count_str)
        except Exception:
            continue
        
        if current_pair == pair_str:
            current_count += count
        else:
            if current_pair is not None:
                print(f"{current_pair}\t{current_count}")
            current_pair = pair_str
            current_count = count
    
    # 输出最后一个共现对
    if current_pair is not None:
        print(f"{current_pair}\t{current_count}")


if __name__ == "__main__":
    main()
