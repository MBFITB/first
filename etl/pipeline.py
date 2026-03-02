"""
ETL Pipeline 主入口
按流水线顺序组装并执行各模块，DataQualityReport 贯穿全链路。
"""

from etl.data_quality import DataQualityReport
from etl.config_manager import ConfigManager
from etl.data_loader import DataLoader
from etl.feature_engineer import FeatureEngineer
from etl.business_transformer import BusinessTransformer
from etl.data_writer import DataWriter


def main():
    """
    ETL 流水线主入口。
    执行顺序：
      1. 配置加载 + 预检（ConfigManager）
      2. Spark 初始化 + 数据加载清洗（DataLoader）
      3. RFM 特征工程 + KMeans 聚类（FeatureEngineer）
      4. 同期群留存 + 双口径漏斗（BusinessTransformer）
      5. 数据库写入（DataWriter）
      6. 输出 DQ 报告 + 释放资源
    """

    # ── Step 0: 初始化全链路质量报告 ──
    dq = DataQualityReport()

    # ── Step 1: 配置加载 + 预检 + Hadoop 环境 ──
    config = ConfigManager()

    # ── Step 2: Spark 初始化 + 数据加载清洗 ──
    loader = DataLoader(config, dq)
    loader.init_spark()
    df_joined, max_date_str = loader.load_and_clean()

    try:
        # ── Step 3: RFM 特征工程 + KMeans 聚类 ──
        engineer = FeatureEngineer(config, dq)
        user_rfm = engineer.run(df_joined, max_date_str)

        # ── Step 4: 业务转换 ──
        transformer = BusinessTransformer(dq)
        cohort_matrix = transformer.build_cohort_matrix(df_joined)
        user_funnel_mart, user_funnel_loose_mart = transformer.build_funnels(df_joined)
        buy_fact = transformer.extract_buy_fact(df_joined)

        # 记录各表行数
        transformer.collect_counts(
            buy_fact, user_rfm, cohort_matrix,
            user_funnel_mart, user_funnel_loose_mart
        )

        # ── Step 5: 数据库写入 ──
        write_tasks = [
            (buy_fact, "buy_fact"),
            (user_rfm, "user_rfm"),
            (cohort_matrix, "cohort_matrix"),
            (user_funnel_mart, "user_funnel_mart"),
            (user_funnel_loose_mart, "user_funnel_loose_mart"),
        ]

        writer = DataWriter(config, dq)
        writer.write_all(write_tasks)

    finally:
        # ── Step 6: 释放资源 + 输出报告 ──
        loader.unpersist()
        dq.print_report()
        print("[完成] ETL 流水线执行完毕。")
        loader.stop_spark()


if __name__ == "__main__":
    main()
