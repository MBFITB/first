"""
数据质量报告收集器
贯穿 ETL 全链路，收集数据量指标、质量告警和聚类画像。
"""

import datetime
import json


class DataQualityReport:
    """收集 ETL 过程中的数据质量指标，在管道结束时输出统一报告"""

    def __init__(self):
        self.metrics = {}
        self.warnings = []
        self.cluster_profiles = []
        self.start_time = datetime.datetime.now()

    def add_metric(self, name: str, value):
        """记录一个质量指标"""
        self.metrics[name] = value

    def add_warning(self, message: str):
        """记录一条质量告警"""
        self.warnings.append(message)

    def add_cluster_profile(self, cluster_id, label, r_mean, f_mean, m_mean, user_count):
        """记录单个聚类簇的 RFM 画像，用于业务解释"""
        self.cluster_profiles.append({
            "cluster_id": cluster_id,
            "label": label,
            "r_mean": r_mean,
            "f_mean": f_mean,
            "m_mean": m_mean,
            "user_count": user_count,
        })

    def print_report(self):
        """输出完整的数据质量报告"""
        elapsed = datetime.datetime.now() - self.start_time
        print("\n" + "=" * 70)
        print("数据质量报告 (Data Quality Report)")
        print("=" * 70)
        print(f"  执行耗时: {elapsed}")

        print("\n  数据量指标:")
        for name, value in self.metrics.items():
            print(f"    • {name}: {value:,}" if isinstance(value, (int, float)) else f"    • {name}: {value}")

        if self.warnings:
            print(f"\n  质量告警 ({len(self.warnings)} 条):")
            for i, w in enumerate(self.warnings, 1):
                print(f"    {i}. {w}")
        else:
            print("\n  无质量告警")

        if self.cluster_profiles:
            print(f"\n  聚类画像 (Cluster Profiles):")
            sorted_profiles = sorted(self.cluster_profiles, key=lambda x: x['user_count'], reverse=True)
            for p in sorted_profiles:
                print(
                    f"    Cluster {p['cluster_id']} [{p['label']}] "
                    f"({p['user_count']}人): "
                    f"R={p['r_mean']}, F={p['f_mean']}, M={p['m_mean']}"
                )

        print("=" * 70 + "\n")

    def to_json_dict(self):
        """导出为 JSON 可序列化字典（供 DQ 日志写入使用）"""
        elapsed = (datetime.datetime.now() - self.start_time).total_seconds()
        return {
            "run_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "elapsed_seconds": elapsed,
            "metrics": json.dumps(self.metrics, ensure_ascii=False, default=str),
            "warnings": json.dumps(self.warnings, ensure_ascii=False),
            "cluster_profiles": json.dumps(self.cluster_profiles, ensure_ascii=False, default=str),
        }
