"""
电商模拟数据生成器 —— 基于统计分布的真实感数据合成
=====================================================
输出文件:
  - UserBehavior.csv       (行为流水: user_id, item_id, category_id, type, ts)
  - items_simulated.csv    (商品维表: item_id, category_id, price)
  - users_simulated.csv    (用户维表: user_id, age_group, channel)

核心业务规律:
  1. 二八法则（Zipf 分布）:        20% 商品贡献 80% 流量/销量
  2. 陡峭的漏斗转化:               PV→Cart 10%, Cart→Buy 20%, 总计 PV→Buy ≈ 2%
  3. 双11 流量爆发:                11-08 ~ 11-14 流量倍增, 11-11 当天 5x
  4. 日内潮汐:                     晚 19-23 时高峰, 凌晨 02-06 低谷
  5. 周末效应:                     周末流量比工作日高 ~20%
  6. 价格负相关:                   高价商品转化率显著低于低价
  7. 用户留存指数衰减:              次日留存 ~35%, 7日留存 <10%
"""

import csv
import os
import time
import datetime
import numpy as np
from collections import defaultdict

# ════════════════════════════════════════════════════════════════════
# 全局配置
# ════════════════════════════════════════════════════════════════════

SEED = 42
np.random.seed(SEED)

# 数据规模
NUM_USERS = 50_000       # 用户数
NUM_ITEMS = 10_000       # 商品数
NUM_CATEGORIES = 500     # 品类数
TARGET_ROWS = 2_000_000  # 目标行为流水行数

# 时间范围（北京时间 UTC+8）
START_DATE = datetime.datetime(2017, 11, 1, 0, 0, 0)
END_DATE = datetime.datetime(2017, 12, 10, 23, 59, 59)
TOTAL_DAYS = (END_DATE.date() - START_DATE.date()).days + 1  # 40 天

# 输出路径
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
BEHAVIOR_FILE = os.path.join(OUTPUT_DIR, "UserBehavior.csv")
ITEMS_FILE = os.path.join(OUTPUT_DIR, "items_simulated.csv")
USERS_FILE = os.path.join(OUTPUT_DIR, "users_simulated.csv")


# ════════════════════════════════════════════════════════════════════
# [1] 生成商品维表 (items_simulated.csv)
# ════════════════════════════════════════════════════════════════════

def generate_items():
    """
    商品价格分布:
      - 低价 (9.9~50):   占 40%  → 浏览量巨大, 高转化
      - 中低 (50~200):   占 30%
      - 中高 (200~800):  占 20%
      - 高价 (800~5000): 占 10%  → 浏览适中, 极低转化
    使用 log-normal 分布 + 分段映射实现自然连续的价格曲线
    """
    print("📦 [1/4] 生成商品维表...")

    # 品类 ID 池
    category_ids = np.arange(1, NUM_CATEGORIES + 1)

    # 商品-品类映射: 品类热度服从 Zipf 分布
    # Zipf(a=1.5) 产生长尾: 少数品类拥有大量商品
    category_weights = np.random.zipf(a=1.5, size=NUM_CATEGORIES).astype(float)
    category_weights /= category_weights.sum()
    item_categories = np.random.choice(category_ids, size=NUM_ITEMS, p=category_weights)

    # 价格: log-normal 分布 (中位数 ~80 元, 长尾到数千元)
    raw_prices = np.random.lognormal(mean=4.0, sigma=1.2, size=NUM_ITEMS)
    # 裁剪到合理范围 [5, 9999]
    prices = np.clip(raw_prices, 5, 9999)
    prices = np.round(prices, 2)

    items = []
    for i in range(NUM_ITEMS):
        items.append({
            "item_id": i + 1,
            "category_id": int(item_categories[i]),
            "price": float(prices[i]),
        })

    # 写入 CSV
    with open(ITEMS_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["item_id", "category_id", "price"])
        writer.writeheader()
        writer.writerows(items)

    print(f"  ✅ 商品维表: {NUM_ITEMS:,} 条 → {ITEMS_FILE}")
    return items


# ════════════════════════════════════════════════════════════════════
# [2] 生成用户维表 (users_simulated.csv)
# ════════════════════════════════════════════════════════════════════

def generate_users():
    """
    用户画像分布:
      - 年龄段: 18-24 (25%), 25-34 (35%), 35-45 (22%), 46+ (15%), 未知 (3%)
      - 渠道:   App Store (34%), 官网 (33%), 小程序 (33%)
    """
    print("👤 [2/4] 生成用户维表...")

    age_groups = ["18-24", "25-34", "35-45", "46+", "未知"]
    age_probs = [0.25, 0.35, 0.22, 0.15, 0.03]

    channels = ["App Store", "官网", "小程序"]
    channel_probs = [0.34, 0.33, 0.33]

    users = []
    user_ages = np.random.choice(age_groups, size=NUM_USERS, p=age_probs)
    user_channels = np.random.choice(channels, size=NUM_USERS, p=channel_probs)

    for i in range(NUM_USERS):
        users.append({
            "user_id": i + 1,
            "age_group": user_ages[i],
            "channel": user_channels[i],
        })

    with open(USERS_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["user_id", "age_group", "channel"])
        writer.writeheader()
        writer.writerows(users)

    print(f"  ✅ 用户维表: {NUM_USERS:,} 条 → {USERS_FILE}")
    return users


# ════════════════════════════════════════════════════════════════════
# [3] 时间分布引擎
# ════════════════════════════════════════════════════════════════════

def build_daily_weights():
    """
    为每一天计算流量权重系数（融合：基础 + 双11爆发 + 周末效应）

    双11 爆发模型（高斯叠加）:
      - 11月11日: 5x 基础流量
      - 前后3天 (11-08 ~ 11-14): 按高斯衰减, σ=1.5 天
    """
    weights = []
    dates = []
    double11 = datetime.date(2017, 11, 11)

    for day_offset in range(TOTAL_DAYS):
        current = START_DATE.date() + datetime.timedelta(days=day_offset)
        dates.append(current)

        # 基础权重
        w = 1.0

        # 周末效应: 周六=5, 周日=6
        if current.weekday() in (5, 6):
            w *= 1.20

        # 双11 高斯爆发
        delta_days = abs((current - double11).days)
        if delta_days <= 5:
            # 高斯峰: 11-11 当天 5x, σ=1.5
            spike = 4.0 * np.exp(-0.5 * (delta_days / 1.5) ** 2)
            w += spike

        weights.append(w)

    weights = np.array(weights)
    weights /= weights.sum()
    return dates, weights


def build_hourly_weights():
    """
    日内潮汐分布（24 小时权重）:
      - 02:00~06:00  低谷 (权重 0.3~0.5)
      - 07:00~09:00  早高峰 (1.0~1.2)
      - 10:00~12:00  上午平稳 (1.0)
      - 12:00~14:00  午休低谷 (0.8)
      - 14:00~18:00  下午平稳 (1.0)
      - 19:00~23:00  晚高峰 (1.5~2.0)
      - 00:00~01:00  深夜中等 (0.8)
    """
    hourly = np.array([
        0.8,   # 00
        0.5,   # 01
        0.3,   # 02
        0.2,   # 03
        0.2,   # 04
        0.3,   # 05
        0.5,   # 06
        1.0,   # 07
        1.2,   # 08
        1.1,   # 09
        1.0,   # 10
        1.0,   # 11
        0.9,   # 12
        0.8,   # 13
        1.0,   # 14
        1.0,   # 15
        1.1,   # 16
        1.2,   # 17
        1.4,   # 18
        1.8,   # 19
        2.0,   # 20
        2.0,   # 21
        1.8,   # 22
        1.2,   # 23
    ], dtype=float)
    hourly /= hourly.sum()
    return hourly


# ════════════════════════════════════════════════════════════════════
# [4] 行为流水生成器 (UserBehavior.csv)
# ════════════════════════════════════════════════════════════════════

def generate_behaviors(items, users):
    """
    核心生成逻辑:
      1. 用户活跃度: Pareto 分布 → 多数低频, 少数高频
      2. 商品热度:   Zipf 分布 → 头部商品吃掉绝大多数流量
      3. 漏斗转化:   PV → Cart (10%), Cart → Buy (20%)
      4. 价格摩擦:   高价商品降低转化概率
      5. 用户留存:   指数衰减模型决定用户活跃天数分布
    """
    print("🔄 [3/4] 生成行为流水（核心引擎）...")
    t0 = time.time()

    # ── 预计算分布 ──

    # 用户活跃度: Pareto 分布 (α=1.2, 典型的长尾)
    # 大量用户只产生几条记录, 极少数 VIP 产生数百条
    user_activity = (np.random.pareto(a=1.2, size=NUM_USERS) + 1)
    user_activity /= user_activity.sum()

    # 商品热度: Zipf 分布 (a=1.8)
    # 20% 的商品获得 ~80% 的浏览量
    item_popularity = np.random.zipf(a=1.3, size=NUM_ITEMS).astype(float)
    item_popularity /= item_popularity.sum()

    # 商品价格查找表
    item_prices = {item["item_id"]: item["price"] for item in items}
    item_cats = {item["item_id"]: item["category_id"] for item in items}

    # 时间分布
    dates, daily_weights = build_daily_weights()
    hourly_weights = build_hourly_weights()

    # ── 用户留存模型 ──
    # 为每个用户分配"活跃天数分布":
    #   次日留存 ~35%, 7日留存 ~8%, 30日 ~2%
    #   使用指数分布: P(active on day d) = exp(-d / τ), τ=3.5
    tau = 3.5  # 衰减时间常数
    user_active_days = {}
    for uid in range(1, NUM_USERS + 1):
        # 每个用户有一个首次活跃日
        first_day = np.random.choice(len(dates), p=daily_weights)
        # 后续活跃日按指数衰减概率独立采样
        active_days_set = {first_day}
        for d in range(first_day + 1, TOTAL_DAYS):
            days_since = d - first_day
            retention_prob = np.exp(-days_since / tau)
            if np.random.random() < retention_prob:
                active_days_set.add(d)
        user_active_days[uid] = sorted(active_days_set)

    # ── 价格对转化率的影响函数 ──
    def price_friction(price):
        """
        价格摩擦因子 (0~1): 价格越高, 转化越难
          - ≤50:   摩擦 1.0 (不影响)
          - 50~200: 摩擦 0.7~1.0
          - 200~800: 摩擦 0.3~0.7
          - >800:  摩擦 0.1~0.3
          - >2000: 摩擦 0.05~0.1
        使用 sigmoid 衰减: friction = 1 / (1 + (price/200)^1.5)
        """
        return 1.0 / (1.0 + (price / 200.0) ** 1.5)

    # ── 预计算每个商品的转化概率 ──
    # 基础转化率: PV→Cart=10%, Cart→Buy=20%
    BASE_PV_TO_CART = 0.10
    BASE_CART_TO_BUY = 0.20
    item_cart_prob = {}
    item_buy_prob = {}
    for item in items:
        friction = price_friction(item["price"])
        item_cart_prob[item["item_id"]] = BASE_PV_TO_CART * friction
        item_buy_prob[item["item_id"]] = BASE_CART_TO_BUY * friction

    # ── 双11 转化率加成 ──
    double11_idx = (datetime.date(2017, 11, 11) - START_DATE.date()).days
    double11_range = set(range(max(0, double11_idx - 3), min(TOTAL_DAYS, double11_idx + 4)))

    # ── 主循环: 批量生成行为记录 ──
    print(f"  [*] 目标: {TARGET_ROWS:,} 行, 用户: {NUM_USERS:,}, 商品: {NUM_ITEMS:,}")

    # 预分配用户和商品的采样索引
    user_ids = np.arange(1, NUM_USERS + 1)
    item_ids = np.arange(1, NUM_ITEMS + 1)

    behaviors = []
    total_pv = 0
    total_cart = 0
    total_buy = 0

    # 按用户批量生成, 利用 Pareto 分布分配每个用户的行为数
    events_per_user = np.random.pareto(a=1.2, size=NUM_USERS) + 1
    events_per_user = events_per_user / events_per_user.sum() * TARGET_ROWS
    events_per_user = np.maximum(events_per_user.astype(int), 1)

    # 调整总数使其接近目标
    diff = TARGET_ROWS - events_per_user.sum()
    if diff > 0:
        # 随机给一些用户加事件
        bonus_users = np.random.choice(NUM_USERS, size=abs(diff), p=user_activity)
        for u in bonus_users:
            events_per_user[u] += 1

    progress_interval = NUM_USERS // 20  # 每 5% 打印一次
    for user_idx in range(NUM_USERS):
        uid = user_idx + 1
        n_events = int(events_per_user[user_idx])
        if n_events == 0:
            continue

        # 该用户的活跃天列表
        active_days = user_active_days.get(uid, [0])
        n_days = len(active_days)

        # 为该用户的所有事件分配: 日期 + 小时 + 商品
        day_indices = np.random.choice(active_days, size=n_events)
        hours = np.random.choice(24, size=n_events, p=hourly_weights)
        minutes = np.random.randint(0, 60, size=n_events)
        seconds = np.random.randint(0, 60, size=n_events)
        chosen_items = np.random.choice(item_ids, size=n_events, p=item_popularity)

        for ev in range(n_events):
            iid = int(chosen_items[ev])
            cat_id = item_cats[iid]
            day_idx = int(day_indices[ev])
            dt = dates[day_idx]

            # 组装时间戳（北京时间 → Unix 时间戳需减掉 8 小时偏移）
            event_dt = datetime.datetime(
                dt.year, dt.month, dt.day,
                int(hours[ev]), int(minutes[ev]), int(seconds[ev])
            )
            # datetime.timestamp() 在 Windows 上已按本地时区(UTC+8)转换，无需额外偏移
            ts = int(event_dt.timestamp())

            # ── 漏斗转化逻辑 ──
            # 每条记录默认为 pv
            behavior_type = "pv"
            total_pv += 1

            # 双11 期间转化率提升 50%
            is_double11 = day_idx in double11_range
            cart_boost = 1.5 if is_double11 else 1.0

            # PV → Cart?
            cart_prob = item_cart_prob[iid] * cart_boost
            if np.random.random() < cart_prob:
                behavior_type = "cart"
                total_cart += 1

                # Cart → Buy?
                buy_prob = item_buy_prob[iid] * cart_boost
                if np.random.random() < buy_prob:
                    behavior_type = "buy"
                    total_buy += 1

            behaviors.append((uid, iid, cat_id, behavior_type, ts))

        # 进度条
        if (user_idx + 1) % progress_interval == 0:
            pct = (user_idx + 1) / NUM_USERS * 100
            elapsed = time.time() - t0
            print(f"  [{'█' * int(pct // 5)}{' ' * (20 - int(pct // 5))}] "
                  f"{pct:5.1f}% | {len(behaviors):>10,} 行 | {elapsed:.1f}s")

    # ── 按时间戳排序 ──
    print(f"  [*] 排序 {len(behaviors):,} 条记录...")
    behaviors.sort(key=lambda x: x[4])

    # ── 写入 CSV (无 header, 兼容原始 UserBehavior.csv 格式) ──
    print(f"  [*] 写入 CSV...")
    with open(BEHAVIOR_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for row in behaviors:
            writer.writerow(row)

    elapsed = time.time() - t0
    actual_pv2cart = total_cart / max(total_pv, 1) * 100
    actual_cart2buy = total_buy / max(total_cart, 1) * 100
    actual_pv2buy = total_buy / max(total_pv, 1) * 100

    print(f"\n  ✅ 行为流水: {len(behaviors):,} 行 → {BEHAVIOR_FILE}")
    print(f"  ─── 漏斗验证 ───")
    print(f"  PV: {total_pv:,}  |  Cart: {total_cart:,}  |  Buy: {total_buy:,}")
    print(f"  PV→Cart: {actual_pv2cart:.2f}%  |  Cart→Buy: {actual_cart2buy:.2f}%  |  PV→Buy: {actual_pv2buy:.2f}%")
    print(f"  耗时: {elapsed:.1f}s")

    return behaviors


# ════════════════════════════════════════════════════════════════════
# [5] 数据质量报告
# ════════════════════════════════════════════════════════════════════

def print_quality_report(behaviors, items):
    """打印数据分布质量报告"""
    print("\n" + "=" * 60)
    print("📊 数据质量报告")
    print("=" * 60)

    # 日期分布
    from collections import Counter
    date_counter = Counter()
    for row in behaviors:
        ts = row[4]
        dt = datetime.datetime.fromtimestamp(ts)
        date_counter[dt.strftime("%Y-%m-%d")] += 1

    sorted_dates = sorted(date_counter.items())
    print(f"\n  📅 日期分布 (共 {len(sorted_dates)} 天):")
    print(f"    {'日期':<12} {'行为数':>10} {'柱状图'}")
    max_count = max(date_counter.values())
    for date_str, count in sorted_dates:
        bar = "█" * int(count / max_count * 40)
        print(f"    {date_str:<12} {count:>10,} {bar}")

    # 商品热度验证: Top 20% 的流量占比
    item_counter = Counter()
    for row in behaviors:
        item_counter[row[1]] += 1

    sorted_items = sorted(item_counter.values(), reverse=True)
    top_20_pct = int(len(sorted_items) * 0.2)
    top_20_traffic = sum(sorted_items[:top_20_pct])
    total_traffic = sum(sorted_items)
    print(f"\n  📊 二八法则验证:")
    print(f"    Top 20% 商品流量占比: {top_20_traffic / total_traffic * 100:.1f}%")

    # 时段分布
    hour_counter = Counter()
    for row in behaviors:
        ts = row[4]
        dt = datetime.datetime.utcfromtimestamp(ts + 8 * 3600)
        hour_counter[dt.hour] += 1

    print(f"\n  ⏰ 日内时段分布:")
    max_h = max(hour_counter.values())
    for h in range(24):
        c = hour_counter.get(h, 0)
        bar = "█" * int(c / max_h * 30)
        label = "🔥" if h in (20, 21) else ("💤" if h in (3, 4) else "  ")
        print(f"    {h:02d}:00 {label} {c:>8,} {bar}")


# ════════════════════════════════════════════════════════════════════
# 主入口
# ════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 60)
    print("🏪 电商模拟数据生成器 (统计分布驱动)")
    print(f"   时间跨度: {START_DATE.date()} ~ {END_DATE.date()} ({TOTAL_DAYS} 天)")
    print(f"   目标行数: {TARGET_ROWS:,}")
    print("=" * 60)

    items = generate_items()
    users = generate_users()
    behaviors = generate_behaviors(items, users)
    print_quality_report(behaviors, items)

    print("\n" + "=" * 60)
    print("✅ 全部完成！生成的文件:")
    print(f"   {BEHAVIOR_FILE}")
    print(f"   {ITEMS_FILE}")
    print(f"   {USERS_FILE}")
    print("=" * 60)
