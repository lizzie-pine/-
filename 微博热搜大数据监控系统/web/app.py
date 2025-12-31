# web/app.py
import sys
import subprocess
import logging
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any

from flask import Flask, render_template, jsonify, request

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['TEMPLATES_AUTO_RELOAD'] = True
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0

BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent

TOP20_FILE = PROJECT_DIR / "top20.txt"
SEG_FILE = PROJECT_DIR / "news_seg.txt"
CONFIG_FILE = PROJECT_DIR / "config.json"

HADOOP_BIN = "/opt/hadoop-3.2.1/bin/hadoop"
HDFS_BIN = "/opt/hadoop-3.2.1/bin/hdfs"
HADOOP_STREAMING_JAR = "/opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar"

DEFAULT_CONTAINER = "namenode"

_config: Optional[Dict[str, Any]] = None


def load_config() -> Dict[str, Any]:
    global _config
    if _config is None:
        if CONFIG_FILE.exists():
            try:
                _config = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
                logger.info(f"加载配置: {CONFIG_FILE}")
            except Exception as e:
                logger.warning(f"加载配置文件失败: {e}")
                _config = {}
        else:
            _config = {}
    return _config


def get_container() -> str:
    cfg = load_config()
    return cfg.get("container", DEFAULT_CONTAINER)


def run_cmd(args: list[str], timeout: int = 300) -> tuple[int, str]:
    logger.info(f"执行命令: {' '.join(args)}")
    try:
        p = subprocess.run(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="ignore",
            check=False,
            timeout=timeout
        )
        out = p.stdout or ""
        if p.returncode != 0:
            logger.warning(f"命令返回非零: {p.returncode}")
        return p.returncode, out
    except subprocess.TimeoutExpired:
        return 1, f"[run_cmd] 命令超时（{timeout}秒）"
    except Exception as e:
        logger.error(f"执行异常: {e}")
        return 1, f"[run_cmd] 执行异常: {e}"


def ensure_container_tmp() -> tuple[int, str]:
    container = get_container()
    cmd = [
        "docker", "exec", container, "bash", "-lc",
        "mkdir -p /hadoop_tmp && chmod 1777 /hadoop_tmp && "
        "chmod 1777 /tmp 2>/dev/null || true && "
        "ls -ld /tmp /hadoop_tmp 2>/dev/null || echo '目录检查完成'"
    ]
    return run_cmd(cmd, timeout=60)


def parse_top20(path: Path) -> list[Dict[str, Any]]:
    if not path.exists():
        logger.warning(f"文件不存在: {path}")
        return []
    
    try:
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        data = []
        total_heat = 0
        for line in lines:
            if "\t" not in line:
                continue
            w, c = line.split("\t", 1)
            try:
                c = int(c)
                total_heat += c
            except Exception:
                continue
            data.append({"word": w, "count": c})
        data.sort(key=lambda x: x["count"], reverse=True)
        logger.info(f"解析到 {len(data)} 条记录，总热度: {total_heat}")
        return data[:20], total_heat
    except Exception as e:
        logger.error(f"解析文件失败: {e}")
        return [], 0


def parse_cooccurrence(path: Path) -> list[Dict[str, Any]]:
    """解析共现关系数据"""
    if not path.exists():
        logger.warning(f"共现关系文件不存在: {path}")
        return []
    
    try:
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        data = []
        for line in lines:
            if "\t" not in line:
                continue
            pair_str, count_str = line.split("\t", 1)
            try:
                count = int(count_str)
                if count < 2:  # 过滤低频共现
                    continue
                words = pair_str.split(",")
                if len(words) != 2:
                    continue
                data.append({
                    "word1": words[0],
                    "word2": words[1],
                    "count": count
                })
            except Exception:
                continue
        data.sort(key=lambda x: x["count"], reverse=True)
        logger.info(f"解析到 {len(data)} 条共现关系")
        return data[:50]  # 返回前50个强共现关系
    except Exception as e:
        logger.error(f"解析共现关系失败: {e}")
        return []


def parse_time_distribution(path: Path) -> list[Dict[str, Any]]:
    """解析时段热搜分布数据"""
    if not path.exists():
        logger.warning(f"时段分布文件不存在: {path}")
        return []
    
    try:
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        
        # 初始化24小时的数据结构
        time_dist = {}
        for hour in range(24):
            time_dist[f"{hour:02d}"] = {
                "hot_words": 0,
                "total_heat": 0,
                "keywords": []
            }
        
        # 统计每个时段的数据
        for line in lines:
            if "\t" not in line:
                continue
            key, count_str = line.split("\t", 1)
            try:
                count = int(count_str)
                hour, word = key.split(",")
                if hour not in time_dist:
                    continue
                
                time_dist[hour]["hot_words"] += 1
                time_dist[hour]["total_heat"] += count
                time_dist[hour]["keywords"].append({"word": word, "count": count})
            except Exception:
                continue
        
        # 对每个时段的关键词排序
        for hour in time_dist:
            time_dist[hour]["keywords"].sort(key=lambda x: x["count"], reverse=True)
            time_dist[hour]["keywords"] = time_dist[hour]["keywords"][:10]  # 每个时段取前10个关键词
        
        logger.info(f"解析到时段分布数据，包含 {len(time_dist)} 个时段")
        return time_dist
    except Exception as e:
        logger.error(f"解析时段分布失败: {e}")
        return []


def save_to_hdfs(local_path: str, hdfs_path: str) -> bool:
    """保存文件到HDFS"""
    container = get_container()
    
    logger.info(f"开始保存文件到HDFS: 本地路径={local_path}, HDFS路径={hdfs_path}")
    
    try:
        # 1. 创建HDFS目录 - 使用字符串操作处理HDFS路径
        if '/' in hdfs_path:
            hdfs_parent = hdfs_path.rsplit('/', 1)[0]
            logger.info(f"创建HDFS目录: {hdfs_parent}")
            cmd = [
                "docker", "exec", container, 
                HDFS_BIN, "dfs", "-mkdir", "-p", hdfs_parent
            ]
            rc, out = run_cmd(cmd)
            logger.info(f"创建HDFS目录结果: rc={rc}, out={out}")
            if rc != 0:
                logger.error(f"创建HDFS目录失败: {out}")
                return False
        
        # 2. 获取本地文件名
        local_file_name = local_path.split('\\')[-1] if '\\' in local_path else local_path.split('/')[-1]
        logger.info(f"获取本地文件名: {local_file_name}")
        
        # 3. 先将文件复制到容器中（使用临时路径）
        container_tmp_path = f"/tmp/{local_file_name}"
        copy_cmd = ["docker", "cp", local_path, f"{container}:{container_tmp_path}"]
        logger.info(f"复制文件到容器: {copy_cmd}")
        rc, out = run_cmd(copy_cmd)
        logger.info(f"复制文件到容器结果: rc={rc}, out={out}")
        if rc != 0:
            logger.error(f"复制文件到容器失败: {out}")
            return False
        
        # 4. 从容器内部路径上传到HDFS
        cmd = [
            "docker", "exec", container, 
            HDFS_BIN, "dfs", "-put", "-f", container_tmp_path, hdfs_path
        ]
        logger.info(f"上传到HDFS: {cmd}")
        rc, out = run_cmd(cmd)
        logger.info(f"上传到HDFS结果: rc={rc}, out={out}")
        if rc != 0:
            logger.error(f"上传HDFS失败: {out}")
            return False
        
        # 5. 清理容器中的临时文件
        cleanup_cmd = ["docker", "exec", container, "rm", "-f", container_tmp_path]
        logger.info(f"清理容器临时文件: {cleanup_cmd}")
        rc, out = run_cmd(cleanup_cmd)
        logger.info(f"清理容器临时文件结果: rc={rc}, out={out}")
        
        logger.info(f"文件已成功上传到HDFS: {hdfs_path}")
        return True
    except Exception as e:
        logger.error(f"保存到HDFS失败: {e}", exc_info=True)
        return False


def list_hdfs_directories(hdfs_path: str) -> list[str]:
    """列出HDFS目录下的所有子目录"""
    container = get_container()
    cmd = [
        "docker", "exec", container, 
        HDFS_BIN, "dfs", "-ls", hdfs_path
    ]
    rc, out = run_cmd(cmd)
    if rc != 0:
        logger.warning(f"列出HDFS目录失败: {out}")
        return []
    
    # 解析HDFS目录列表
    directories = []
    for line in out.splitlines():
        if line.startswith("d"):  # 目录行
            parts = line.split()
            if len(parts) >= 8:
                dir_name = parts[7]
                # 提取目录名（去掉路径前缀）
                dir_name = dir_name.split("/")[-1]
                directories.append(dir_name)
    
    return directories


def get_hdfs_file_content(hdfs_path: str) -> str:
    """获取HDFS文件内容"""
    container = get_container()
    cmd = [
        "docker", "exec", container, 
        HDFS_BIN, "dfs", "-cat", hdfs_path
    ]
    rc, out = run_cmd(cmd)
    if rc != 0:
        logger.warning(f"读取HDFS文件失败: {out}")
        return ""
    
    return out


def list_history_timestamps() -> list[str]:
    """列出所有历史数据的时间点"""
    # 从HDFS的/weibo_data/result目录下获取所有时间戳目录
    timestamps = list_hdfs_directories("/weibo_data/result")
    # 按时间戳降序排序
    timestamps.sort(reverse=True)
    return timestamps


def get_history_data(timestamp: str) -> dict:
    """根据时间戳获取历史数据"""
    # 构建HDFS路径
    hdfs_path = f"/weibo_data/result/{timestamp}/top20.txt"
    
    # 读取文件内容
    content = get_hdfs_file_content(hdfs_path)
    if not content:
        return {"ok": False, "msg": "历史数据不存在"}
    
    # 解析top20数据
    data = []
    total_heat = 0
    for line in content.splitlines():
        if "\t" not in line:
            continue
        w, c = line.split("\t", 1)
        try:
            c = int(c)
            total_heat += c
        except Exception:
            continue
        data.append({"word": w, "count": c})
    
    data.sort(key=lambda x: x["count"], reverse=True)
    top20 = data[:20]
    categories = get_hotword_categories(top20)
    
    return {
        "ok": True,
        "data": {
            "timestamp": timestamp,
            "top20": top20,
            "total_heat": total_heat,
            "categories": categories,
            "count": len(top20)
        }
    }


def get_hotword_categories(hotwords: list[Dict[str, Any]]) -> dict:
    """优化后的分类统计：娱乐/社会/科技等"""
    categories = {
        "娱乐": 0,
        "社会": 0,
        "科技": 0,
        "体育": 0,
        "财经": 0,
        "其他": 0
    }
    
    # 优化后的关键词规则分类，添加更多关键词和具体实体
    category_rules = {
        "娱乐": ["明星", "电影", "电视剧", "综艺", "演唱会", "演员", "歌手", "爱豆", "流量", "红毯", 
                  "粉丝", "偶像", "剧", "影视", "音乐", "艺人", "剧组", "首映", "票房", "颁奖礼",
                  "节目", "导演", "编剧", "制片人", "发布会", "电影节", "颁奖典礼", "走红毯",
                  "新剧", "开播", "收官", "收视率", "口碑", "影评", "剧评", "蔡依林", "林俊杰",
                  "金巧巧", "吴启华", "湖南卫视", "演唱会", "直播", "写真", "婚纱", "女友", "分手",
                  "求婚", "夫妻", "四大才子", "BILLBOARD", "写真", "宫格", "同游", "得闲", "骄阳",
                  "湖南卫视演唱会", "演唱会"],
        "社会": ["事故", "救援", "事件", "社会", "民生", "交通", "安全", "公益", "环保",
                  "火灾", "爆炸", "车祸", "疫情", "防控", "政策", "法规", "教育", "医疗",
                  "地震", "洪水", "养老", "就业", "社保", "住房", "慈善", "志愿者",
                  "民生", "热点", "话题", "讨论", "争议", "关注", "确诊", "骨癌", "孩子",
                  "父母", "道歉", "农村", "摆摊", "烤红薯", "女孩", "长寿", "猫咪", "分手",
                  "夫妻", "新闻联播", "战区", "破袭", "青州地震", "台湾岛", "演习", "解放军",
                  "震动", "农村", "摆摊", "烤红薯", "孩子", "打翻", "父母", "父母公开", "披萨"],
        "科技": ["AI", "人工智能", "科技", "互联网", "手机", "电脑", "技术", "算法", "芯片",
                  "软件", "APP", "5G", "大数据", "云计算", "区块链", "元宇宙", "自动驾驶", "机器人",
                  "网络", "创新", "数码", "智能", "VR", "AR", "MR", "XR", "半导体", "编程", 
                  "开发", "应用", "小米", "小米新车", "科技", "罗永浩", "罗永浩科技", "数字世界",
                  "加速", "拥抱", "银发", "银发网民", "数字"],
        "体育": ["比赛", "冠军", "球员", "球队", "奥运会", "世界杯", "体育", "赛事",
                  "足球", "篮球", "排球", "网球", "乒乓球", "羽毛球", "田径", "游泳", "比分",
                  "运动员", "亚军", "季军", "金牌", "银牌", "铜牌", "进球", "得分", "胜利", 
                  "失败", "平局", "欧洲杯", "亚洲杯", "亚运会", "世锦赛", "冠军赛", "大师赛", "公开赛"],
        "财经": ["股市", "经济", "金融", "投资", "理财", "公司", "企业", "市场", "经济数据",
                  "股票", "基金", "汇率", "利率", "GDP", "CPI", "上市", "破产", "保险",
                  "银行", "PPI", "退市", "并购", "重组", "融资", "贷款", "债券", "期货", "外汇", 
                  "黄金", "原油", "增值税", "经济", "印度经济", "经济日本", "世界经济体", 
                  "金价", "票房", "预测", "全资", "波动", "印度经济", "经济日本"]
    }
    
    for hotword in hotwords:
        word = hotword["word"]
        matched = False
        
        # 直接匹配整个词
        for category, keywords in category_rules.items():
            if word in keywords:
                categories[category] += 1
                matched = True
                break
        
        # 如果没有直接匹配，再尝试包含关系匹配
        if not matched:
            for category, keywords in category_rules.items():
                if any(keyword in word for keyword in keywords):
                    categories[category] += 1
                    matched = True
                    break
        
        # 特殊情况处理：针对特定模式的词
        if not matched:
            # 包含品牌名的科技产品
            brands = ["小米", "苹果", "华为", "三星", "谷歌", "微软", "阿里", "腾讯", "百度"]
            if any(brand in word for brand in brands):
                categories["科技"] += 1
                matched = True
            # 包含经济相关术语
            elif "经济" in word or "金融" in word or "投资" in word or "增值税" in word:
                categories["财经"] += 1
                matched = True
            # 包含军事相关术语
            elif "演习" in word or "解放军" in word or "台湾岛" in word:
                categories["社会"] += 1
                matched = True
            # 包含科技相关术语
            elif "数字" in word or "加速" in word or "拥抱" in word or "银发" in word:
                categories["科技"] += 1
                matched = True
            # 包含社会民生相关术语
            elif "农村" in word or "摆摊" in word or "烤红薯" in word or "孩子" in word or "父母" in word:
                categories["社会"] += 1
                matched = True
            # 包含娱乐相关术语
            elif "演唱会" in word or "湖南卫视" in word:
                categories["娱乐"] += 1
                matched = True
        
        if not matched:
            categories["其他"] += 1
    
    return categories


def check_docker_container(container: str) -> tuple[bool, str]:
    rc, out = run_cmd(["docker", "inspect", container, "--format", "{{.State.Running}}"])
    if rc == 0 and out.strip() == "true":
        return True, "容器运行中"
    return False, f"容器 {container} 未运行或不存在"


def check_hadoop_service() -> tuple[bool, str]:
    container = get_container()
    rc, out = run_cmd([
        "docker", "exec", container, "bash", "-lc",
        f"{HADOOP_BIN} version 2>&1 | head -1"
    ])
    if rc == 0:
        version = out.strip()
        logger.info(f"Hadoop 版本: {version}")
        return True, version
    return False, "Hadoop 不可用"


@app.get("/")
def index():
    top20, total_heat = parse_top20(TOP20_FILE)
    categories = get_hotword_categories(top20)
    return render_template("index.html", top20=top20, total_heat=total_heat, categories=categories)


@app.get("/cloud_features")
def cloud_features():
    return render_template("cloud_features.html")


@app.get("/history")
def history():
    """历史数据页面"""
    return render_template("history.html")


@app.get("/api/status")
def api_status():
    container = get_container()
    docker_ok, docker_msg = check_docker_container(container)
    hadoop_ok, hadoop_msg = check_hadoop_service()
    
    top20, total_heat = parse_top20(TOP20_FILE)
    categories = get_hotword_categories(top20)
    
    return jsonify({
        "ok": docker_ok and hadoop_ok,
        "container": container,
        "docker": {"ok": docker_ok, "message": docker_msg},
        "hadoop": {"ok": hadoop_ok, "message": hadoop_msg},
        "data": {
            "count": len(top20),
            "total_heat": total_heat,
            "categories": categories,
            "updated_at": datetime.fromtimestamp(TOP20_FILE.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S") if TOP20_FILE.exists() else None
        }
    })


@app.get("/api/history/timestamps")
def api_history_timestamps():
    """获取所有历史数据的时间点"""
    timestamps = list_history_timestamps()
    return jsonify({
        "ok": True,
        "timestamps": timestamps
    })


@app.get("/api/history/data/<timestamp>")
def api_history_data(timestamp: str):
    """根据时间戳获取历史数据"""
    result = get_history_data(timestamp)
    return jsonify(result)


def generate_mock_trend_data():
    """生成模拟的24小时趋势数据"""
    mock_data = {
        "time": [],
        "hotWords": [],
        "totalHeat": [],
        "isMock": []
    }
    
    # 生成24小时的时间点
    for hour in range(24):
        time_str = f"{hour:02d}:00"
        mock_data["time"].append(time_str)
        
        # 模拟热度变化曲线：凌晨低，上午逐渐升高，下午达到峰值，晚上逐渐降低
        if hour < 6:
            # 凌晨：0-6点
            hot_words = int(10 + hour * 1.5)
            total_heat = int(300 + hour * 50)
        elif hour < 12:
            # 上午：6-12点
            hot_words = int(19 + (hour - 6) * 3)
            total_heat = int(600 + (hour - 6) * 150)
        elif hour < 18:
            # 下午：12-18点
            hot_words = int(37 + (hour - 12) * 1.5)
            total_heat = int(1500 + (hour - 12) * 100)
        elif hour < 22:
            # 晚上：18-22点
            hot_words = int(46 - (hour - 18) * 2)
            total_heat = int(2100 - (hour - 18) * 200)
        else:
            # 深夜：22-24点
            hot_words = int(38 - (hour - 22) * 4)
            total_heat = int(1300 - (hour - 22) * 300)
        
        mock_data["hotWords"].append(hot_words)
        mock_data["totalHeat"].append(total_heat)
        mock_data["isMock"].append(True)
    
    return mock_data


def merge_trend_data(realtime_data, mock_data):
    """合并真实数据和模拟数据"""
    # 如果没有真实数据，直接返回模拟数据
    if len(realtime_data["time"]) == 0:
        return mock_data
    
    # 创建时间到数据的映射
    realtime_map = {}
    for i in range(len(realtime_data["time"])):
        time_str = realtime_data["time"][i]
        realtime_map[time_str] = {
            "hotWords": realtime_data["hotWords"][i],
            "totalHeat": realtime_data["totalHeat"][i],
            "isMock": False
        }
    
    # 合并数据
    merged_data = {
        "time": [],
        "hotWords": [],
        "totalHeat": [],
        "isMock": []
    }
    
    for i in range(len(mock_data["time"])):
        time_str = mock_data["time"][i]
        if time_str in realtime_map:
            # 使用真实数据
            merged_data["time"].append(time_str)
            merged_data["hotWords"].append(realtime_map[time_str]["hotWords"])
            merged_data["totalHeat"].append(realtime_map[time_str]["totalHeat"])
            merged_data["isMock"].append(False)
        else:
            # 使用模拟数据
            merged_data["time"].append(time_str)
            merged_data["hotWords"].append(mock_data["hotWords"][i])
            merged_data["totalHeat"].append(mock_data["totalHeat"][i])
            merged_data["isMock"].append(True)
    
    return merged_data


@app.get("/api/trend/realtime")
def api_realtime_trend():
    """获取实时趋势数据（组合真实数据和模拟数据）"""
    try:
        # 1. 获取真实数据
        timestamps = list_hdfs_directories("/weibo_data/result")
        timestamps.sort(reverse=True)
        
        # 只取最近24个时间点或所有可用数据
        recent_timestamps = timestamps[:24]
        recent_timestamps.sort()
        
        realtime_data = {
            "time": [],
            "hotWords": [],
            "totalHeat": []
        }
        
        for ts in recent_timestamps:
            # 解析时间格式，假设时间戳格式为 "YYYY-MM-DD_HH-MM-SS"
            time_parts = ts.replace("_", " ").split()
            if len(time_parts) < 2:
                continue
            time_str = time_parts[1]
            
            # 获取该时间点的数据
            hdfs_path = f"/weibo_data/result/{ts}/top20.txt"
            content = get_hdfs_file_content(hdfs_path)
            if not content:
                continue
            
            # 解析数据
            data = []
            total_heat = 0
            for line in content.splitlines():
                if "\t" not in line:
                    continue
                w, c = line.split("\t", 1)
                try:
                    c = int(c)
                    total_heat += c
                    data.append({"word": w, "count": c})
                except Exception:
                    continue
            
            # 添加到真实数据
            realtime_data["time"].append(time_str)
            realtime_data["hotWords"].append(len(data))
            realtime_data["totalHeat"].append(total_heat)
        
        # 2. 生成模拟数据
        mock_data = generate_mock_trend_data()
        
        # 3. 合并数据
        merged_data = merge_trend_data(realtime_data, mock_data)
        
        return jsonify({
            "ok": True,
            "data": merged_data,
            "info": "为了演示完整的趋势分析功能，系统启动时会生成符合真实规律的模拟数据。随着系统持续运行，模拟数据会逐渐被真实数据替换。"
        })
    except Exception as e:
        logger.error(f"获取实时趋势数据失败: {e}")
        return jsonify({
            "ok": False,
            "msg": f"获取趋势数据失败: {str(e)}"
        }), 500


@app.post("/api/refresh")
def api_refresh():
    LOG_FILE = PROJECT_DIR / "last_refresh.log"
    config = load_config()
    container = get_container()
    
    def dump_logs(msgs: list[str]):
        LOG_FILE.write_text("\n".join(msgs), encoding="utf-8", errors="ignore")
    
    logs: list[str] = []
    step = 0
    
    try:
        logs.append(f"=== 刷新任务开始 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")
        logs.append(f"容器: {container}")
        logs.append(f"项目目录: {PROJECT_DIR}")
        
        step = 1
        logs.append(f"[{step}] 检查 Docker 容器状态...")
        docker_ok, docker_msg = check_docker_container(container)
        logs.append(docker_msg)
        if not docker_ok:
            dump_logs(logs)
            return jsonify({
                "ok": False, 
                "msg": "Docker 容器不可用", 
                "step": step,
                "detail": "\n".join(logs)
            }), 500
        
        step = 2
        logs.append(f"[{step}] 检查 Hadoop 服务...")
        hadoop_ok, hadoop_msg = check_hadoop_service()
        logs.append(hadoop_msg)
        if not hadoop_ok:
            dump_logs(logs)
            return jsonify({
                "ok": False, 
                "msg": "Hadoop 服务不可用", 
                "step": step,
                "detail": "\n".join(logs)
            }), 500
        
        step = 3
        logs.append(f"[{step}] 初始化容器临时目录...")
        rc, out = ensure_container_tmp()
        logs.append(out)
        if rc != 0:
            dump_logs(logs)
            return jsonify({
                "ok": False, 
                "msg": "容器临时目录初始化失败", 
                "step": step,
                "detail": "\n".join(logs)
            }), 500
        
        step = 4
        logs.append(f"[{step}] 抓取微博热搜...")
        rc, out = run_cmd([sys.executable, str(PROJECT_DIR / "fetch_hot.py")], timeout=30)
        logs.append(out)
        if rc != 0:
            dump_logs(logs)
            return jsonify({
                "ok": False, 
                "msg": "抓取失败", 
                "step": step,
                "detail": "\n".join(logs)
            }), 500
        
        # 保存原始数据到HDFS
        current_time = time.strftime("%Y-%m-%d_%H-%M-%S")
        hdfs_raw_path = f"/weibo_data/raw/{current_time}/news_raw.txt"
        # 使用正确的文件路径，而不是OUT_FILE变量
        raw_file = PROJECT_DIR / "news_raw.txt"
        if save_to_hdfs(str(raw_file), hdfs_raw_path):
            logs.append(f"原始数据已保存到HDFS: {hdfs_raw_path}")
        else:
            logs.append(f"保存原始数据到HDFS失败")
        
        step = 5
        logs.append(f"[{step}] 中文分词处理...")
        rc, out = run_cmd([sys.executable, str(PROJECT_DIR / "segment.py")], timeout=30)
        logs.append(out)
        if rc != 0:
            dump_logs(logs)
            return jsonify({
                "ok": False, 
                "msg": "分词失败", 
                "step": step,
                "detail": "\n".join(logs)
            }), 500
        
        step = 6
        logs.append(f"[{step}] 检查容器 Python 环境...")
        py = config.get("python_cmd", "python3")
        rc, out = run_cmd(["docker", "exec", container, f"{py}", "-V"], timeout=10)
        if rc != 0:
            py = "python3"
            rc, out = run_cmd(["docker", "exec", container, "python3", "-V"], timeout=10)
            if rc != 0:
                py = "python"
                rc, out = run_cmd(["docker", "exec", container, "python", "-V"], timeout=10)
                if rc != 0:
                    logs.append(f"无法找到有效的 Python: {out}")
                    dump_logs(logs)
                    return jsonify({
                        "ok": False, 
                        "msg": "容器内找不到 Python", 
                        "step": step,
                        "detail": "\n".join(logs)
                    }), 500
        logs.append(f"使用 Python: {py}")
        
        step = 7
        logs.append(f"[{step}] 拷贝文件到容器...")
        for fname in ["mapper.py", "reducer.py", "news_seg.txt"]:
            src = PROJECT_DIR / fname
            if not src.exists():
                logs.append(f"文件不存在: {src}")
                dump_logs(logs)
                return jsonify({
                    "ok": False, 
                    "msg": f"缺少文件：{fname}", 
                    "step": step,
                    "detail": "\n".join(logs)
                }), 500
            
            rc, out = run_cmd(["docker", "cp", str(src), f"{container}:/{fname}"], timeout=60)
            logs.append(out)
            if rc != 0:
                dump_logs(logs)
                return jsonify({
                    "ok": False, 
                    "msg": f"docker cp 失败：{fname}", 
                    "step": step,
                    "detail": "\n".join(logs)
                }), 500
        
        step = 8
        logs.append(f"[{step}] HDFS 准备...")
        run_cmd(["docker", "exec", container, HDFS_BIN, "dfs", "-rm", "-r", "-f", "/output/result"], timeout=60)
        run_cmd(["docker", "exec", container, HDFS_BIN, "dfs", "-mkdir", "-p", "/input"], timeout=60)
        
        rc, out = run_cmd(["docker", "exec", container, HDFS_BIN, "dfs", "-put", "-f", "/news_seg.txt", "/input/"], timeout=120)
        logs.append(out)
        if rc != 0:
            dump_logs(logs)
            return jsonify({
                "ok": False, 
                "msg": "上传 HDFS 失败", 
                "step": step,
                "detail": "\n".join(logs)
            }), 500
        
        step = 9
        logs.append(f"[{step}] Hadoop Streaming 计算...")
        streaming_args = [
            "docker", "exec", container,
            HADOOP_BIN, "jar", HADOOP_STREAMING_JAR,
            "-Dmapreduce.framework.name=local",
            "-Dmapreduce.job.reduces=1",
            "-Djava.io.tmpdir=/hadoop_tmp",
            "-Dhadoop.tmp.dir=/hadoop_tmp",
            "-Dmapreduce.job.local.dir=/hadoop_tmp",
            "-Dmapreduce.cluster.local.dir=/hadoop_tmp",
            "-files", "/mapper.py,/reducer.py",
            "-mapper", f"{py} mapper.py",
            "-reducer", f"{py} reducer.py",
            "-input", "/input/news_seg.txt",
            "-output", "/output/result",
        ]
        rc, out = run_cmd(streaming_args, timeout=300)
        logs.append(out)
        if rc != 0:
            dump_logs(logs)
            return jsonify({
                "ok": False, 
                "msg": "Hadoop计算失败", 
                "step": step,
                "detail": "\n".join(logs)
            }), 500
        
        step = 10
        logs.append(f"[{step}] 获取计算结果...")
        run_cmd(["docker", "exec", container, "rm", "-f", "/top20.txt"], timeout=10)
        
        rc, out = run_cmd(["docker", "exec", container, HDFS_BIN, "dfs", "-getmerge", "/output/result", "/top20.txt"], timeout=60)
        logs.append(out)
        if rc != 0:
            dump_logs(logs)
            return jsonify({
                "ok": False, 
                "msg": "getmerge 失败", 
                "step": step,
                "detail": "\n".join(logs)
            }), 500
        
        rc, out = run_cmd(["docker", "cp", f"{container}:/top20.txt", str(TOP20_FILE)], timeout=60)
        logs.append(out)
        if rc != 0:
            dump_logs(logs)
            return jsonify({
                "ok": False, 
                "msg": "拷回 top20.txt 失败", 
                "step": step,
                "detail": "\n".join(logs)
            }), 500
        
        # 保存处理后的数据到HDFS
        hdfs_result_path = f"/weibo_data/result/{current_time}/top20.txt"
        if save_to_hdfs(str(TOP20_FILE), hdfs_result_path):
            logs.append(f"处理结果已保存到HDFS: {hdfs_result_path}")
        else:
            logs.append(f"保存处理结果到HDFS失败")
        
        # ========== 新增：共现关系计算 ==========
        step = 11
        logs.append(f"[{step}] 计算热搜词共现关系...")
        
        # 1. 拷贝共现关系计算脚本到容器
        for fname in ["cooccurrence_mapper.py", "cooccurrence_reducer.py"]:
            src = PROJECT_DIR / fname
            if not src.exists():
                logs.append(f"共现关系脚本不存在: {src}")
                continue
            run_cmd(["docker", "cp", str(src), f"{container}:/{fname}"], timeout=60)
        
        # 2. 执行共现关系MapReduce任务
        cooccurrence_args = [
            "docker", "exec", container,
            HADOOP_BIN, "jar", HADOOP_STREAMING_JAR,
            "-Dmapreduce.framework.name=local",
            "-Dmapreduce.job.reduces=1",
            "-Djava.io.tmpdir=/hadoop_tmp",
            "-files", "/cooccurrence_mapper.py,/cooccurrence_reducer.py",
            "-mapper", f"{py} cooccurrence_mapper.py",
            "-reducer", f"{py} cooccurrence_reducer.py",
            "-input", "/input/news_seg.txt",
            "-output", "/output/cooccurrence",
        ]
        rc, out = run_cmd(cooccurrence_args, timeout=300)
        logs.append(out)
        
        # 3. 获取共现关系结果
        COOCCURRENCE_FILE = PROJECT_DIR / "cooccurrence.txt"
        run_cmd(["docker", "exec", container, "rm", "-f", "/cooccurrence.txt"], timeout=10)
        run_cmd(["docker", "exec", container, HDFS_BIN, "dfs", "-getmerge", "/output/cooccurrence", "/cooccurrence.txt"], timeout=60)
        run_cmd(["docker", "cp", f"{container}:/cooccurrence.txt", str(COOCCURRENCE_FILE)], timeout=60)
        
        # 4. 保存共现关系到HDFS
        hdfs_cooccurrence_path = f"/weibo_data/result/{current_time}/cooccurrence.txt"
        if COOCCURRENCE_FILE.exists():
            if save_to_hdfs(str(COOCCURRENCE_FILE), hdfs_cooccurrence_path):
                logs.append(f"共现关系已保存到HDFS: {hdfs_cooccurrence_path}")
        
        # ========== 新增：时段热搜分布统计 ==========
        step = 12
        logs.append(f"[{step}] 统计时段热搜分布...")
        
        # 1. 拷贝时段分布脚本到容器
        for fname in ["time_dist_mapper.py", "time_dist_reducer.py"]:
            src = PROJECT_DIR / fname
            if not src.exists():
                logs.append(f"时段分布脚本不存在: {src}")
                continue
            run_cmd(["docker", "cp", str(src), f"{container}:/{fname}"], timeout=60)
        
        # 2. 执行时段分布MapReduce任务
        time_dist_args = [
            "docker", "exec", container,
            HADOOP_BIN, "jar", HADOOP_STREAMING_JAR,
            "-Dmapreduce.framework.name=local",
            "-Dmapreduce.job.reduces=1",
            "-Djava.io.tmpdir=/hadoop_tmp",
            "-files", "/time_dist_mapper.py,/time_dist_reducer.py",
            "-mapper", f"{py} time_dist_mapper.py",
            "-reducer", f"{py} time_dist_reducer.py",
            "-input", "/input/news_seg.txt",
            "-output", "/output/time_dist",
        ]
        rc, out = run_cmd(time_dist_args, timeout=300)
        logs.append(out)
        
        # 3. 获取时段分布结果
        TIME_DIST_FILE = PROJECT_DIR / "time_dist.txt"
        run_cmd(["docker", "exec", container, "rm", "-f", "/time_dist.txt"], timeout=10)
        run_cmd(["docker", "exec", container, HDFS_BIN, "dfs", "-getmerge", "/output/time_dist", "/time_dist.txt"], timeout=60)
        run_cmd(["docker", "cp", f"{container}:/time_dist.txt", str(TIME_DIST_FILE)], timeout=60)
        
        # 4. 保存时段分布到HDFS
        hdfs_time_dist_path = f"/weibo_data/result/{current_time}/time_dist.txt"
        if TIME_DIST_FILE.exists():
            if save_to_hdfs(str(TIME_DIST_FILE), hdfs_time_dist_path):
                logs.append(f"时段分布已保存到HDFS: {hdfs_time_dist_path}")
        
        logs.append(f"=== 刷新完成 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")
        dump_logs(logs)
        
        # 解析所有结果
        top20, total_heat = parse_top20(TOP20_FILE)
        categories = get_hotword_categories(top20)
        cooccurrence = parse_cooccurrence(COOCCURRENCE_FILE)
        time_dist = parse_time_distribution(TIME_DIST_FILE)
        
        logger.info(f"成功获取 {len(top20)} 条热词，{len(cooccurrence)} 条共现关系")
        
        return jsonify({
            "ok": True,
            "msg": "刷新成功",
            "step": step,
            "top20": top20,
            "cooccurrence": cooccurrence,
            "time_distribution": time_dist,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "summary": {
                "total_words": len(top20),
                "total_heat": total_heat,
                "categories": categories,
                "cooccurrence_count": len(cooccurrence),
                "time_distribution": len(time_dist)
            }
        })

    except Exception as e:
        logs.append(f"[api_refresh] 异常: {e}")
        logger.exception("刷新任务异常")
        dump_logs(logs)
        return jsonify({
            "ok": False, 
            "msg": f"后端异常: {str(e)}", 
            "step": step,
            "detail": "\n".join(logs)
        }), 500


if __name__ == "__main__":
    logger.info("=== 微博热搜监控系统启动 ===")
    logger.info(f"BASE_DIR: {BASE_DIR}")
    logger.info(f"PROJECT_DIR: {PROJECT_DIR}")
    logger.info(f"容器: {get_container()}")
    app.run(host="0.0.0.0", port=5000, debug=False)
