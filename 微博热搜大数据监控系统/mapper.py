#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys

SEMANTIC_GROUPS = {
    "火灾事故": ["火灾", "事故", "起火", "失火", "着火"],
    "调查报告": ["报告", "调查", "通报", "结果", "结论"],
    "展品": ["文物", "展品", "藏品", "古董", "艺术品"],
    "真假": ["真伪", "真假", "伪造", "赝品"],
    "怀孕": ["怀孕", "妊娠", "有孕", "有喜", "准妈妈"],
    "发声": ["发声", "回应", "表态", "说话"],
    "质疑": ["质疑", "怀疑", "争议"],
    "驾车": ["开车", "驾车", "驾驶"],
    "身亡": ["死亡", "身亡", "去世", "逝世", "丧命"],
    "手术": ["手术", "开刀", "治疗", "救治"],
    "利用漏洞": ["漏洞", "利用", "系统漏洞", "黑客"],
    "智力障碍": ["智力", "障碍", "智障", "残疾"],
    "博物馆": ["博物馆", "展馆", "美术馆", "博物院"],
    "格莱美": ["格莱美", "grammy", "颁奖礼"],
    "旗舰": ["旗舰", "高端", "顶级", "豪华"],
    "镜头": ["镜头", "画面", "影像"],
    "舞台": ["舞台", "表演", "演出现场"],
}

EXCLUDE_PREFIXES = ("原", "级", "化", "体", "性")


def normalize_semantic(word):
    for standard, variants in SEMANTIC_GROUPS.items():
        if word in variants or word == standard:
            return standard
    return word


def should_exclude(word, base_word):
    if len(word) <= len(base_word):
        return False
    extra = word[len(base_word):]
    if len(extra) <= 2 and any(extra.startswith(p) for p in EXCLUDE_PREFIXES):
        return True
    return False


def normalize_word_mapper(word):
    word = normalize_semantic(word)
    return word


def main():
    seen = set()
    for line in sys.stdin:
        line = line.strip()
        if not line or "," not in line:
            continue

        try:
            parts = line.split(",", 1)
            rank_str = parts[0].strip()
            tokens_str = parts[1].strip() if len(parts) > 1 else ""
            rank = int(rank_str)
            weight = 51 - rank
            if weight < 1:
                weight = 1
            
            if not tokens_str:
                continue
                
            tokens = tokens_str.split()
        except Exception:
            continue

        for token in tokens:
            normalized = normalize_word_mapper(token)
            if not normalized or len(normalized) < 2:
                continue

            key = normalized
            if key in seen:
                continue
            seen.add(key)

            sys.stdout.write("%s\t%d\n" % (key, weight))


if __name__ == "__main__":
    main()
