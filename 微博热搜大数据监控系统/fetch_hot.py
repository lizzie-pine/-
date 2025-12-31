# fetch_hot.py
import sys
import time
from pathlib import Path
import json

import requests

HOT_API = "https://weibo.com/ajax/side/hotSearch"
OUT_FILE = Path(__file__).resolve().parent / "news_raw.txt"
HISTORY_DIR = Path(__file__).resolve().parent / "history"

# 创建历史数据目录
HISTORY_DIR.mkdir(exist_ok=True)


def save_hot(titles: list[str]) -> None:
    # 获取当前时间戳
    timestamp = int(time.time())
    current_time = time.strftime("%Y-%m-%d %H:%M:%S")
    date_str = time.strftime("%Y-%m-%d")
    hour_str = time.strftime("%H")
    
    # 1. 覆盖写入当前数据（保持原有接口兼容）
    with OUT_FILE.open("w", encoding="utf-8") as f:
        for i, title in enumerate(titles, start=1):
            f.write(f"{i},{title}\n")
    print(f"抓取成功：已保存 {len(titles)} 条热搜 -> {OUT_FILE}")
    
    # 2. 保存带时间戳的历史数据到本地
    history_file = HISTORY_DIR / f"news_raw_{date_str}_{hour_str}.txt"
    with history_file.open("w", encoding="utf-8") as f:
        f.write(f"# 采集时间: {current_time}\n")
        f.write(f"# 时间戳: {timestamp}\n")
        for i, title in enumerate(titles, start=1):
            f.write(f"{timestamp},{i},{title}\n")
    print(f"历史数据已保存 -> {history_file}")
    
    # 3. 保存为JSON格式便于后续处理
    json_data = {
        "timestamp": timestamp,
        "datetime": current_time,
        "count": len(titles),
        "hotwords": [
            {"rank": i+1, "word": title}
            for i, title in enumerate(titles)
        ]
    }
    json_file = HISTORY_DIR / f"news_raw_{date_str}_{hour_str}.json"
    with json_file.open("w", encoding="utf-8") as f:
        json.dump(json_data, f, ensure_ascii=False, indent=2)
    print(f"JSON数据已保存 -> {json_file}")


def main() -> None:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Referer": "https://weibo.com/hot/search",
    }

    try:
        print("正在抓取微博实时热搜...")
        resp = requests.get(HOT_API, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        realtime = data.get("data", {}).get("realtime", [])
        titles: list[str] = []
        for item in realtime:
            if item.get("is_ad"):
                continue
            w = (item.get("word") or "").strip()
            if w:
                titles.append(w)

        if not titles:
            raise RuntimeError("接口返回为空：titles 为空（可能被限流/结构变化）")

        save_hot(titles)
        print("更新时间：", time.strftime("%Y-%m-%d %H:%M:%S"))

    except Exception as e:
        print(f"[fetch_hot.py] 抓取失败：{e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
