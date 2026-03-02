"""
数据加载器
负责 SparkSession 初始化、CSV 数据读取、清洗和大宽表构建。
"""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark import StorageLevel


# 确保 PySpark Worker 使用当前 Python 解释器
os.environ['PYSPARK_PYTHON'] = sys.executable


class DataLoader:
    """Spark 初始化 + 数据加载 + 清洗"""

    def __init__(self, config, dq):
        """
        :param config: ConfigManager 实例
        :param dq: DataQualityReport 实例
        """
        self.config = config
        self.dq = dq
        self.spark = None
        self.df_joined = None       # 清洗后的大宽表（已缓存）
        self.max_date_str = None    # 数据集截止日期

    def init_spark(self) -> SparkSession:
        """创建 SparkSession"""
        cfg = self.config
        self.spark = SparkSession.builder \
            .appName("Strict_DS_Pipeline") \
            .config("spark.driver.memory", cfg["driver_memory"]) \
            .config("spark.ui.enabled", "false") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.default.parallelism", cfg["default_parallelism"]) \
            .config("spark.locality.wait", cfg["locality_wait"]) \
            .config("spark.sql.shuffle.partitions", cfg["default_parallelism"]) \
            .config("spark.jars.packages",
                    "com.clickhouse:clickhouse-jdbc:0.6.0,"
                    "org.apache.httpcomponents.client5:httpclient5:5.3.1") \
            .getOrCreate()
        return self.spark

    def load_and_clean(self):
        """
        加载 CSV、预处理、构建大宽表并缓存。
        返回 (df_joined, max_date_str)
        """
        print("🚀 [1/6] 加载数据与预处理...")
        cfg = self.config
        spark = self.spark

        # 读取原始数据
        schema_bhv = "user_id INT, item_id INT, category_id INT, type STRING, ts INT"
        df_bhv_raw = spark.read.csv(cfg["behavior_csv"], header=False, schema=schema_bhv)
        df_bhv = df_bhv_raw.limit(5000000) if cfg["data_limit"] else df_bhv_raw

        df_items = spark.read.csv(cfg["items_csv"], header=True, inferSchema=True)
        df_users_info = spark.read.csv(cfg["users_csv"], header=True, inferSchema=True)

        # 记录原始数据量
        raw_count = df_bhv.count()
        self.dq.add_metric("原始行为数据行数", raw_count)
        self.dq.add_metric("商品维表行数", df_items.count())
        self.dq.add_metric("用户维表行数", df_users_info.count())

        # 日期解析与过滤
        df_cleaned = df_bhv \
            .withColumn("date", F.to_date(F.from_unixtime(F.col("ts")))) \
            .filter((F.col("date") >= "2017-11-01") & (F.col("date") <= "2017-12-10")) \
            .withColumn("order_id", F.concat_ws('_', F.col('user_id'), F.col('ts'), F.col('item_id')))

        max_date_val = df_cleaned.agg(F.max("date")).collect()[0][0]
        self.max_date_str = str(max_date_val)
        print(f"  [*] 数据集业务截止日期: {self.max_date_str}")
        self.dq.add_metric("数据集截止日期", self.max_date_str)

        # 构建大宽表（JOIN 维表）
        df_with_price = df_cleaned \
            .join(df_items.drop("category_id"), on="item_id", how="left") \
            .join(df_users_info, on="user_id", how="left")

        # 数据质量校验
        missing_price_count = df_with_price.filter(F.col("price").isNull()).count()
        missing_channel = df_with_price.filter(F.col("channel").isNull()).count()
        missing_age = df_with_price.filter(F.col("age_group").isNull()).count()

        self.dq.add_metric("缺失价格记录数", missing_price_count)
        self.dq.add_metric("缺失渠道记录数", missing_channel)
        self.dq.add_metric("缺失年龄分组记录数", missing_age)

        if missing_price_count > 0:
            self.dq.add_warning(
                f"发现 {missing_price_count} 条记录缺失价格(price)字段，已被丢弃。"
                f"生产环境下应写入死信队列排查。"
            )
            print(f"  [WARN] [数据质量告警] {missing_price_count} 条缺失价格记录已丢弃")
        if missing_channel > 0:
            self.dq.add_warning(f"发现 {missing_channel} 条记录缺失渠道(channel)字段，已填充为'未知渠道'")
        if missing_age > 0:
            self.dq.add_warning(f"发现 {missing_age} 条记录缺失年龄分组(age_group)字段，已填充为'未知'")

        # 清洗并缓存（MEMORY_AND_DISK 策略：优先内存，溢出写磁盘）
        self.df_joined = df_with_price \
            .dropna(subset=["price"]) \
            .fillna({"channel": "未知渠道", "age_group": "未知"}) \
            .persist(StorageLevel.MEMORY_AND_DISK)

        clean_count = self.df_joined.count()
        self.dq.add_metric("清洗后有效记录数", clean_count)
        self.dq.add_metric("数据丢弃率", f"{(1 - clean_count / max(raw_count, 1)) * 100:.2f}%")
        print(f"  [*] 清洗后有效记录数: {clean_count:,}")

        return self.df_joined, self.max_date_str

    def unpersist(self):
        """释放缓存"""
        if self.df_joined is not None:
            self.df_joined.unpersist()

    def stop_spark(self):
        """停止 SparkSession"""
        if self.spark is not None:
            self.spark.stop()
