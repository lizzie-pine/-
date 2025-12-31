#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys


def deduplicate_by_inclusion(words_with_counts):
    """基于包含关系的去重逻辑"""
    if not words_with_counts:
        return []

    sorted_words = sorted(words_with_counts, key=lambda x: x[1], reverse=True)

    result = []
    for word, count in sorted_words:
        should_add = True

        for existing_word, existing_count in result:
            if word in existing_word:
                should_add = False
                break
            if existing_word in word:
                if count > existing_count * 0.7:
                    result.remove((existing_word, existing_count))
                else:
                    should_add = False
                    break

        if should_add:
            result.append((word, count))

    result.sort(key=lambda x: x[1], reverse=True)
    return result


current_word = None
current_sum = 0
aggregated = []

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t", 1)
    if len(parts) != 2:
        continue

    word = parts[0]
    try:
        cnt = int(parts[1])
    except Exception:
        continue

    if current_word == word:
        current_sum += cnt
    else:
        if current_word is not None:
            aggregated.append((current_word, current_sum))
        current_word = word
        current_sum = cnt

if current_word is not None:
    aggregated.append((current_word, current_sum))

if aggregated:
    final_results = deduplicate_by_inclusion(aggregated)
    for word, count in final_results:
        sys.stdout.write("%s\t%d\n" % (word, count))
