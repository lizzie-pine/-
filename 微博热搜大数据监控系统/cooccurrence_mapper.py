#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MapReduce共现关系计算 - Mapper
功能：将每条热搜记录中的关键词两两组合，生成共现对
输入：news_seg.txt (rank,token1 token2 ...)
输出：(word1,word2)\t1
"""
import sys
from itertools import combinations


def main():
    for line in sys.stdin:
        line = line.strip()
        if not line or "," not in line:
            continue
        
        try:
            parts = line.split(",", 1)
            tokens_str = parts[1].strip() if len(parts) > 1 else ""
            if not tokens_str:
                continue
            
            tokens = tokens_str.split()
            # 过滤掉短词和无意义词
            valid_tokens = [t for t in tokens if len(t) >= 2]
            
            # 生成所有两两组合（无序对）
            for pair in combinations(sorted(valid_tokens), 2):
                print(f"{pair[0]},{pair[1]}\t1")
                
        except Exception as e:
            continue


if __name__ == "__main__":
    main()
