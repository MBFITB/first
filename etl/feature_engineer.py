"""
特征工程模块
负责 RFM 指标计算、StandardScaler 标准化、KMeans 聚类寻优和智能标签映射。
"""

import functools

from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator


class FeatureEngineer:
    """RFM 特征工程 + KMeans 聚类打标"""

    def __init__(self, config, dq):
        self.config = config
        self.dq = dq

    def run(self, df_joined, max_date_str):
        """
        执行完整的 RFM 聚类流程。
        返回 user_rfm DataFrame（包含 user_id 和 rfm_label 两列）。
        """
        print("🧪 [2/6] 特征工程与 KMeans 聚类打标...")

        # Step 1: 计算 RFM 原始指标
        rfm_base = df_joined.filter(F.col("type") == "buy").groupBy("user_id").agg(
            F.datediff(F.lit(max_date_str), F.max("date")).alias("R"),
            F.countDistinct("order_id").alias("F"),
            F.sum("price").alias("M")
        )

        # Step 2: VectorAssembler 组装特征向量
        assembler = VectorAssembler(inputCols=["R", "F", "M"], outputCol="features_raw")
        rfm_vec = assembler.transform(rfm_base)

        # Step 3: StandardScaler 标准化（消除量纲差异）
        scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)
        scaler_model = scaler.fit(rfm_vec)
        rfm_scaled = scaler_model.transform(rfm_vec)

        # Step 4: KMeans 轮廓系数自动选 K
        best_k, best_score, best_model = self._auto_kmeans(rfm_scaled)
        rfm_clustered = best_model.transform(rfm_scaled)

        # Step 5: 模型持久化（可选）
        self._persist_model(best_model, scaler_model)

        # Step 6: 智能标签映射
        cluster_to_label = self._build_label_mapping(best_model)

        # Step 7: 收集聚类画像到 DQ 报告
        self._collect_cluster_profiles(rfm_clustered, cluster_to_label)

        # Step 8: 生成最终 user_rfm
        mapping_expr = functools.reduce(
            lambda acc, kv: acc.when(F.col("cluster") == kv[0], kv[1]),
            cluster_to_label.items(),
            F.when(F.lit(False), "")
        ).otherwise("未知群体")

        user_rfm = rfm_clustered.withColumn("rfm_label", mapping_expr).select("user_id", "rfm_label")
        return user_rfm

    def _auto_kmeans(self, rfm_scaled):
        """轮廓系数自动选 K（K=3~5）"""
        evaluator = ClusteringEvaluator(
            predictionCol="cluster", featuresCol="features",
            metricName="silhouette", distanceMeasure="squaredEuclidean"
        )

        best_k, best_score, best_model = 4, -1, None
        for k in range(3, 6):
            kmeans = KMeans(k=k, seed=42, featuresCol="features", predictionCol="cluster")
            model = kmeans.fit(rfm_scaled)
            predictions = model.transform(rfm_scaled)
            score = evaluator.evaluate(predictions)
            if score > best_score:
                best_k, best_score, best_model = k, score, model

        print(f"  [KMeans] 自动寻优完成，最优K: {best_k} (Silhouette: {best_score:.4f})")
        self.dq.add_metric("KMeans 最优K值", best_k)
        self.dq.add_metric("KMeans 轮廓系数", f"{best_score:.4f}")
        return best_k, best_score, best_model

    def _persist_model(self, best_model, scaler_model):
        """生产环境模型持久化"""
        model_path = self.config.get("model_save_path")
        if model_path:
            best_model.save(model_path)
            scaler_model.save(model_path + "_scaler")
            print(f"  [模型持久化] KMeans 模型已保存至: {model_path}")
            print(f"  [模型持久化] Scaler 模型已保存至: {model_path}_scaler")

    def _build_label_mapping(self, best_model) -> dict:
        """基于聚类中心特征的智能标签判定"""
        centers = best_model.clusterCenters()
        weights = self.config["rfm_weights"]
        thresholds = self.config["rfm_thresholds"]

        cluster_to_label = {}
        for i, c in enumerate(centers):
            weighted_score = weights["R"] * c[0] + weights["F"] * c[1] + weights["M"] * c[2]
            label = self._classify_cluster(c, thresholds)
            cluster_to_label[i] = label
            print(f"  [KMeans] Cluster {i}: R={c[0]:.3f}, F={c[1]:.3f}, M={c[2]:.3f} "
                  f"→ 加权分={weighted_score:.3f} → 标签='{label}'")

        print(f"  [KMeans] 最终映射: {cluster_to_label}")
        return cluster_to_label

    @staticmethod
    def _classify_cluster(center, th):
        """
        基于聚类中心标准化特征值进行智能标签判定。
        判定优先级：流失检测 > 高价值识别 > 高频识别 > 潜力识别 > 一般
        """
        r_val, f_val, m_val = center[0], center[1], center[2]

        if r_val > th["high_r"]:
            return "流失/沉睡客户"
        if m_val > th["high_m"] and r_val < 0:
            return "核心高价值客户"
        if f_val > th["high_f"] and m_val <= th["high_m"]:
            return "高频忠诚客户"
        if r_val <= 0 and (f_val > 0 or m_val > 0):
            return "潜力发展客户"
        return "一般维持客户"

    def _collect_cluster_profiles(self, rfm_clustered, cluster_to_label):
        """收集各聚类簇的原始 RFM 均值，写入 DQ 报告"""
        cluster_stats = rfm_clustered.groupBy("cluster").agg(
            F.round(F.avg("R"), 2).alias("R_mean"),
            F.round(F.avg("F"), 2).alias("F_mean"),
            F.round(F.avg("M"), 2).alias("M_mean"),
            F.count("user_id").alias("user_count"),
        ).collect()

        for row in cluster_stats:
            cid = row["cluster"]
            self.dq.add_cluster_profile(
                cluster_id=cid,
                label=cluster_to_label.get(cid, "未知"),
                r_mean=row["R_mean"],
                f_mean=row["F_mean"],
                m_mean=row["M_mean"],
                user_count=row["user_count"],
            )
