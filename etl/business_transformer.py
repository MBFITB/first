"""
业务转换模块
负责同期群留存矩阵、双口径漏斗聚合和 buy_fact 事实表抽取。
"""

from pyspark.sql import functions as F


class BusinessTransformer:
    """同期群留存 + 双口径漏斗 + 事实表抽取"""

    def __init__(self, dq):
        self.dq = dq

    def build_cohort_matrix(self, df_joined):
        """
        [3/6] 计算同期群留存矩阵（Day 0~7）。
        返回 cohort_matrix DataFrame。
        """
        print("[3/6] 计算同期群留存矩阵...")
        user_cohorts = df_joined.groupBy("user_id").agg(F.min("date").alias("cohort_date"))
        cohort_sizes = user_cohorts.groupBy("cohort_date").agg(F.count("user_id").alias("cohort_users"))

        retention_raw = df_joined.join(user_cohorts, "user_id") \
            .withColumn("day_diff", F.datediff("date", "cohort_date")) \
            .filter((F.col("day_diff") >= 0) & (F.col("day_diff") <= 7)) \
            .groupBy("cohort_date", "day_diff") \
            .agg(F.countDistinct("user_id").alias("active_users"))

        cohort_matrix = retention_raw.join(cohort_sizes, "cohort_date", "inner")
        return cohort_matrix

    def build_funnels(self, df_joined):
        """
        [4/6] 计算双口径漏斗（严格时序约束 + 宽松非时序约束）。
        返回 (user_funnel_mart, user_funnel_loose_mart)
        """
        print("[4/6] 计算转化漏斗（双口径：严格时序约束 + 宽松非时序约束）...")

        # Step 1: 按 (user_id, item_id) 聚合每种行为最早时间戳
        item_funnel = df_joined.groupBy("user_id", "item_id").agg(
            F.min(F.when(F.col("type") == "pv", F.col("ts"))).alias("first_pv"),
            F.min(F.when(F.col("type") == "cart", F.col("ts"))).alias("first_cart"),
            F.min(F.when(F.col("type") == "buy", F.col("ts"))).alias("first_buy"),
        )

        # Step 2A: 严格口径（时序约束：cart_ts >= pv_ts 且 buy_ts >= cart_ts）
        strict_funnel = item_funnel.select(
            "user_id",
            F.when(F.col("first_pv").isNotNull(), 1).otherwise(0).alias("has_pv"),
            F.when(
                (F.col("first_cart").isNotNull()) &
                (F.col("first_pv").isNotNull()) &
                (F.col("first_cart") >= F.col("first_pv")),
                1
            ).otherwise(0).alias("has_cart"),
            F.when(
                (F.col("first_buy").isNotNull()) &
                (F.col("first_cart").isNotNull()) &
                (F.col("first_pv").isNotNull()) &
                (F.col("first_buy") >= F.col("first_cart")) &
                (F.col("first_cart") >= F.col("first_pv")),
                1
            ).otherwise(0).alias("has_buy"),
            F.to_date(F.from_unixtime(
                F.coalesce(F.col("first_pv"), F.col("first_cart"), F.col("first_buy"))
            )).alias("pv_date"),
            F.to_date(F.from_unixtime(F.col("first_cart"))).alias("cart_date"),
            F.to_date(F.from_unixtime(F.col("first_buy"))).alias("buy_date"),
        )

        # Step 2B: 宽松口径（非时序约束：只看是否发生过，不要求顺序）
        loose_funnel = item_funnel.select(
            "user_id",
            F.when(F.col("first_pv").isNotNull(), 1).otherwise(0).alias("has_pv"),
            F.when(F.col("first_cart").isNotNull(), 1).otherwise(0).alias("has_cart"),
            F.when(F.col("first_buy").isNotNull(), 1).otherwise(0).alias("has_buy"),
            F.to_date(F.from_unixtime(
                F.coalesce(F.col("first_pv"), F.col("first_cart"), F.col("first_buy"))
            )).alias("pv_date"),
            F.to_date(F.from_unixtime(F.col("first_cart"))).alias("cart_date"),
            F.to_date(F.from_unixtime(F.col("first_buy"))).alias("buy_date"),
        )

        # Step 3: 展开到日期级事实表
        user_funnel_mart = self._build_funnel_mart(strict_funnel)
        user_funnel_loose_mart = self._build_funnel_mart(loose_funnel)

        print("  ✔ 严格口径漏斗 (user_funnel_mart) 计算完成")
        print("  ✔ 宽松口径漏斗 (user_funnel_loose_mart) 计算完成")
        return user_funnel_mart, user_funnel_loose_mart

    @staticmethod
    def _build_funnel_mart(funnel_df):
        """将漏斗标志 DataFrame 展开为按日期聚合的事实表"""
        events = funnel_df.select(
            "user_id",
            F.explode(
                F.filter(
                    F.array(
                        F.when(F.col("has_pv") == 1,
                            F.struct(
                                F.col("pv_date").alias("date"),
                                F.lit(1).alias("has_pv"),
                                F.lit(0).alias("has_cart"),
                                F.lit(0).alias("has_buy"),
                            )
                        ),
                        F.when(F.col("has_cart") == 1,
                            F.struct(
                                F.col("cart_date").alias("date"),
                                F.lit(0).alias("has_pv"),
                                F.lit(1).alias("has_cart"),
                                F.lit(0).alias("has_buy"),
                            )
                        ),
                        F.when(F.col("has_buy") == 1,
                            F.struct(
                                F.col("buy_date").alias("date"),
                                F.lit(0).alias("has_pv"),
                                F.lit(0).alias("has_cart"),
                                F.lit(1).alias("has_buy"),
                            )
                        ),
                    ),
                    lambda x: x.isNotNull()
                )
            ).alias("event")
        ).select(
            "user_id",
            F.col("event.date").alias("date"),
            F.col("event.has_pv").alias("has_pv"),
            F.col("event.has_cart").alias("has_cart"),
            F.col("event.has_buy").alias("has_buy"),
        )

        return events.groupBy("user_id", "date").agg(
            F.max("has_pv").alias("has_pv"),
            F.max("has_cart").alias("has_cart"),
            F.max("has_buy").alias("has_buy"),
        )

    def extract_buy_fact(self, df_joined):
        """[5/6] 抽取 buy_fact 事实表"""
        print("[5/6] 抽取底层业务事实表...")
        buy_fact = df_joined.filter(F.col("type") == "buy").select(
            "date", "user_id", "order_id", "item_id", "category_id", "price", "channel", "age_group"
        )
        return buy_fact

    def collect_counts(self, buy_fact, user_rfm, cohort_matrix, user_funnel_mart, user_funnel_loose_mart):
        """记录各表行数到 DQ 报告"""
        self.dq.add_metric("buy_fact 行数", buy_fact.count())
        self.dq.add_metric("user_rfm 行数", user_rfm.count())
        self.dq.add_metric("cohort_matrix 行数", cohort_matrix.count())
        self.dq.add_metric("user_funnel_mart 行数", user_funnel_mart.count())
        self.dq.add_metric("user_funnel_loose_mart 行数", user_funnel_loose_mart.count())
