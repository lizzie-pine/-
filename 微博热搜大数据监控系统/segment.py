# segment.py
# -*- coding: utf-8 -*-
"""
将 news_raw.txt (rank,title) -> news_seg.txt (rank,token1 token2 ...)
优化点：
1) 关闭 HMM 新词识别，减少"妹小"等怪词
2) 归一化（爆光->曝光 等）并扩展微博常见变体
3) 泛词/黑名单过滤 + 亲属称谓过滤 + 微博特有噪声过滤
4) 轻量短语增强（相邻名词/专名合并），同时严格防止拼出垃圾词
5) 扩展停用词库 + 动态词性过滤
6) 添加正则过滤模式，移除微博特有噪声（话题符号、艾特等）
"""

import re
import sys
from pathlib import Path

import jieba

try:
    import jieba.posseg as pseg
except Exception:
    pseg = None


BASE = Path(__file__).resolve().parent
RAW = BASE / "news_raw.txt"
OUT = BASE / "news_seg.txt"
STOP = BASE / "stopwords.txt"
USER_DICT = BASE / "user_dict.txt"


# 1) 常见写法归一化（扩展微博常见变体）
NORMALIZE_MAP = {
    "爆光": "曝光",
    "官宣了": "官宣",
    "官宣啦": "官宣",
    "预告": "预告片",
    "微博": "微博",
    "热搜": "热搜",
    "回应": "回应",
    "曝光": "曝光",
    "官方": "官方",
    "声明": "声明",
    "最新": "最新",
    "终于": "终于",
    "竟然": "竟然",
    "惊呆": "惊呆",
    "炸了": "炸裂",
    "崩溃": "崩溃",
    "无语": "无语",
    "哭了": "哭泣",
    "笑死": "笑死",
    "太好哭": "太好哭",
    "破防": "破防",
    "上头": "上头",
    "绝绝子": "绝绝子",
    "无语子": "无语子",
    "yyds": "永远的神",
    "zfb": "支付宝",
    "xswl": "笑死我了",
    "nsdd": "你说得对",
    "nbcs": "没人在乎",
    "gkd": "搞快点",
    "ssfd": "瑟瑟发抖",
    "plgg": "漂亮哥哥",
    "wpp": "网友",
    "bp": "闭麦",
    "blx": "玻璃心",
    "cjb": "成绩表",
    "dbq": "对不起",
    "doge": "doge",
    "emmm": "嗯",
    "srds": "虽然但是",
    "xjj": "小姐姐",
    "yysy": "有一说一",
    "物料": "物料",
    "杀我": "杀我",
    "上热搜": "上热搜",
}

# 2) 明确的垃圾词/泛词（可按你需要增删）
BAD_WORDS = set([
    "粉丝", "感动", "预告片", "回收", "产业链", "摔倒", "房型",
    "妹妹", "妹", "妹子", "小妹", "儿子", "女儿", "老公", "老婆",
    "姐姐", "姐", "哥哥", "哥", "妈妈", "妈", "爸爸", "爸",
    "小", "大", "太", "很", "真的", "觉得", "好像", "一个",
    "现场", "视频", "照片", "网友", "热搜", "回应", "最新",
    "爆料", "真相", "原因", "结果", "进展", "后续", "瓜",
    "妹小", "这个", "那个", "什么", "怎么", "如何", "为什么",
    "已经", "开始", "结束", "终于", "可能", "应该", "需要",
    "大家", "有人", "没有", "不是", "就是",
    "但是", "而且", "虽然", "因为", "所以", "如果", "只有",
    "自己", "别人", "事情", "问题", "东西", "时候", "地方",
    "一下", "一点", "一些", "这种", "那样", "怎样",
    "确实", "简直", "居然", "竟然", "总算",
    "看看", "说说", "听听", "问问", "想想", "念念",
    "今天", "昨天", "明天", "今年", "去年", "明年", "以前",
    "以后", "现在", "马上", "立刻", "正在", "曾经",
    "还有", "只有", "都是", "总是", "经常", "偶尔",
    "也许", "大概", "似乎", "仿佛", "反正",
    "实在", "果然", "难怪", "怪不得",
    "千万", "一定", "必须",
    "快来", "快去", "快看", "快听", "快说", "快打",
    "来了", "去了", "看了", "听了", "说了", "打了",
    "热搜榜", "热榜", "话题", "超话", "广场",
    "微博", "微博之夜", "微博电影",
    "工作室", "公司", "方面", "消息",
    "第一时间", "最新消息", "刚刚", "官方回应",
    "强烈推荐", "推荐", "来看", "这部", "太好",
    "太好哭", "太好笑", "一起来", "一起来看",
])

# 3) 亲属/称谓相关：只要 token 含这些字（且不是正规专名），直接过滤
KINSHIP_RE = re.compile(r"(妹|姐|哥|弟|嫂|姨|叔|舅|爸|妈|爹|娘|儿子|女儿|老公|老婆)")

# 4) 微博噪声模式（需要被过滤的模式）
WEIBO_NOISE_RE = re.compile(
    r"(#\w+#|@\w+|http[s]?://\S+|&\w+;|转发微博|\[.*?\]|\(.*?\)|（.*?）)"
)

# 5) 允许保留的词性（偏"热点实体/事件"）
# nr:人名 ns:地名 nt:机构名 nz:其他专名 n*:名词 v*:动词 a:形容词 eng:英语词
ALLOW_POS_PREFIX = ("n", "v", "a", "eng")
ALLOW_POS_EXACT = set(["nr", "ns", "nt", "nz", "eng", "an", "i", "j"])

# 6) 用于短语合并时禁止出现的"组件词"（防止拼出"妹小/小X"这种）
BAD_COMPONENT = set(["妹", "小", "姐", "哥", "儿子", "女儿", "粉丝", "微博", "热搜"])


def load_stopwords(path):
    if not path.exists():
        return set()
    txt = path.read_text(encoding="utf-8", errors="ignore")
    return set([w.strip() for w in txt.splitlines() if w.strip()])


def load_user_dict(path):
    if path.exists():
        try:
            jieba.load_userdict(str(path))
        except Exception:
            pass


def clean_title(s):
    """
    清洗标题：保留中英文数字，其他符号统一空格，避免粘连造词
    同时移除微博特有噪声（话题、艾特、表情等）
    """
    if not s:
        return ""
    
    s = WEIBO_NOISE_RE.sub(" ", s)
    s = re.sub(r"[^\u4e00-\u9fffA-Za-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def normalize_word(w):
    w = w.strip()
    if not w:
        return ""
    w_lower = w.lower()
    if w_lower in NORMALIZE_MAP:
        w = NORMALIZE_MAP[w_lower]
    if re.fullmatch(r"[A-Za-z]+", w):
        w = w.upper()
    return w


def is_bad_token(w, stop_set):
    if not w:
        return True
    if w in stop_set:
        return True
    if w in BAD_WORDS:
        return True

    # 纯数字、太短（中文 1 字通常信息太弱）
    if w.isdigit():
        return True

    # 英文太短（如 "a"）
    if re.fullmatch(r"[A-Za-z]+", w) and len(w) < 2:
        return True

    # 中文长度限制：一般 2~8 比较合理；太长多半是句子碎片
    if re.search(r"[\u4e00-\u9fff]", w):
        if len(w) < 2:
            return True
        if len(w) > 10:
            return True

    # 含亲属/称谓的碎词直接丢（保守一点：长度<=4 更像碎片）
    if KINSHIP_RE.search(w) and len(w) <= 4:
        return True

    return False


def allow_by_pos(flag):
    if not flag:
        return False
    if flag in ALLOW_POS_EXACT:
        return True
    for p in ALLOW_POS_PREFIX:
        if flag.startswith(p):
            return True
    return False


def tokenize(title, stop_set):
    """
    分词 + 过滤 + 轻量短语增强 + 质量评分
    返回：tokens（按质量排序，去重）
    """
    title = clean_title(title)
    if not title:
        return []

    tokens = []
    pos_list = []

    if pseg is not None:
        for w, flag in pseg.cut(title, HMM=False):
            w = normalize_word(w)
            if not w:
                continue
            if not allow_by_pos(flag):
                continue
            if is_bad_token(w, stop_set):
                continue
            tokens.append(w)
            pos_list.append((w, flag))
    else:
        for w in jieba.lcut(title, HMM=False):
            w = normalize_word(w)
            if is_bad_token(w, stop_set):
                continue
            tokens.append(w)
            pos_list.append((w, ""))

    if not pos_list:
        return []

    phrases = []
    for i in range(len(pos_list) - 1):
        w1, f1 = pos_list[i]
        w2, f2 = pos_list[i + 1]

        if w1 in BAD_COMPONENT or w2 in BAD_COMPONENT:
            continue
        if KINSHIP_RE.search(w1) or KINSHIP_RE.search(w2):
            continue

        ok1 = (f1 in ALLOW_POS_EXACT) or (f1.startswith("n")) or (f1.startswith("a")) or (f1 == "")
        ok2 = (f2 in ALLOW_POS_EXACT) or (f2.startswith("n")) or (f2.startswith("a")) or (f2 == "")
        if not (ok1 and ok2):
            continue

        phrase = w1 + w2
        phrase = normalize_word(phrase)

        if is_bad_token(phrase, stop_set):
            continue
        if len(phrase) < 2 or len(phrase) > 8:
            continue

        phrases.append((phrase, calculate_word_score(phrase, f1, f2, pos_list)))

    phrases.sort(key=lambda x: x[1], reverse=True)

    seen = set()
    final_tokens = []
    for phrase, score in phrases:
        if phrase not in seen:
            seen.add(phrase)
            final_tokens.append(phrase)

    for w in tokens:
        if w not in seen:
            seen.add(w)
            final_tokens.append(w)

    return final_tokens[:40]


def calculate_word_score(word, flag1, flag2, pos_list):
    """
    计算词语质量分数（用于排序）
    分数越高表示越可能是热点词汇
    """
    score = 0
    
    score += len(word) * 2
    
    if flag1 in ALLOW_POS_EXACT or flag2 in ALLOW_POS_EXACT:
        score += 10
    if flag1.startswith("n") or flag2.startswith("n"):
        score += 5
    if flag1.startswith("a") or flag2.startswith("a"):
        score += 3
    
    high_value_flags = ("nr", "ns", "nt", "nz")
    if flag1 in high_value_flags or flag2 in high_value_flags:
        score += 15
    
    for w, f in pos_list:
        if w == word:
            continue
        if word in w:
            score += 2
    
    return score


def deduplicate_by_inclusion(words_with_counts):
    """
    基于包含关系的去重逻辑
    如果短词和长词频数接近（差异<=30%），保留更长的那个
    如果短词频数明显更高（差异>30%），说明短词更核心，保留短的并过滤长的
    """
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


SEMANTIC_GROUPS = {
    "火灾事故": ["火灾", "事故", "起火", "失火", "着火"],
    "调查报告": ["报告", "调查", "通报", "结果", "结论"],
    "展品": ["文物", "展品", "藏品", "古董", "艺术品"],
    "真假": ["真伪", "真假", "真假", "伪造", "赝品"],
    "怀孕": ["怀孕", "妊娠", "有孕", "有喜", "准妈妈"],
    "发声": ["发声", "回应", "回应", "表态", "说话"],
    "质疑": ["质疑", "质疑", "怀疑", "争议", "争议"],
    "驾车": ["开车", "驾车", "驾驶", "开车", "开远"],
    "身亡": ["死亡", "身亡", "去世", "逝世", "丧命"],
    "手术": ["手术", "手术", "开刀", "治疗", "救治"],
    "利用漏洞": ["漏洞", "利用", "系统漏洞", "黑客"],
    "智力障碍": ["智力", "障碍", "智障", "残疾"],
    "博物馆": ["博物馆", "展馆", "美术馆", "博物院"],
    "格莱美": ["格莱美", "grammy", "颁奖礼"],
    "旗舰": ["旗舰", "高端", "顶级", "豪华"],
    "镜头": ["镜头", "画面", "影像", "画面"],
    "舞台": ["舞台", "表演", "演出现场"],
    "发声": ["发声", "表态", "发言", "回应"],
}


def normalize_semantic(word):
    """将词语归一化到标准形式（处理语义等价词）"""
    for standard, variants in SEMANTIC_GROUPS.items():
        if word in variants or word == standard:
            return standard
    return word


def merge_semantic_duplicates(words_with_counts):
    """合并语义重复的词汇"""
    if not words_with_counts:
        return []
    
    merged = {}
    for word, count in words_with_counts:
        normalized = normalize_semantic(word)
        if normalized in merged:
            merged[normalized] += count
        else:
            merged[normalized] = merged.get(normalized, 0) + count
    
    return [(k, v) for k, v in sorted(merged.items(), key=lambda x: x[1], reverse=True)]


def main():
    if not RAW.exists():
        sys.stderr.write("[segment.py] 找不到输入文件：%s\n" % str(RAW))
        sys.exit(1)

    load_user_dict(USER_DICT)
    stop_set = load_stopwords(STOP)

    lines = RAW.read_text(encoding="utf-8", errors="ignore").splitlines()
    out_lines = []

    for line in lines:
        line = line.strip()
        if not line or "," not in line:
            continue

        rank_str, title = line.split(",", 1)
        rank_str = rank_str.strip()
        title = title.strip()
        if not rank_str.isdigit():
            continue

        toks = tokenize(title, stop_set)
        if not toks:
            continue

        out_lines.append("%s,%s" % (rank_str, " ".join(toks)))

    if not out_lines:
        sys.stderr.write("[segment.py] 分词输出为空：请检查 news_raw.txt 格式/内容\n")
        sys.exit(1)

    OUT.write_text("\n".join(out_lines), encoding="utf-8", errors="ignore")
    sys.stdout.write("分词完成：输出 %d 行 -> %s\n" % (len(out_lines), str(OUT)))


if __name__ == "__main__":
    main()
