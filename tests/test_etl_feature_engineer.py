"""
ETL 特征工程单元测试
测试 FeatureEngineer._classify_cluster 智能标签判定逻辑（纯函数，无需 Spark）。
"""

from etl.feature_engineer import FeatureEngineer


# ═══════════════════════════════════════
#  _classify_cluster 标签判定测试
# ═══════════════════════════════════════

class TestClassifyCluster:
    """基于聚类中心的智能标签判定逻辑"""

    # 使用默认阈值配置（与 config.json 一致）
    THRESHOLDS = {"high_r": 0.5, "high_m": 0.3, "high_f": 0.3}

    def test_high_r_churned(self):
        """R 值超过阈值 → 流失/沉睡客户（最高优先级）"""
        center = [1.0, 0.5, 0.5]  # R=1.0 > high_r=0.5
        label = FeatureEngineer._classify_cluster(center, self.THRESHOLDS)
        assert label == "流失/沉睡客户"

    def test_high_r_boundary(self):
        """R 值恰好等于阈值 → 不算流失（严格大于）"""
        center = [0.5, 0.5, 0.5]  # R=0.5 == high_r=0.5
        label = FeatureEngineer._classify_cluster(center, self.THRESHOLDS)
        assert label != "流失/沉睡客户"

    def test_high_value_customer(self):
        """M 值高 + R 值低 → 核心高价值客户"""
        center = [-0.5, 0.1, 0.8]  # M=0.8 > high_m=0.3, R=-0.5 < 0
        label = FeatureEngineer._classify_cluster(center, self.THRESHOLDS)
        assert label == "核心高价值客户"

    def test_high_frequency_loyal(self):
        """F 值高 + M 值一般 → 高频忠诚客户"""
        center = [-0.3, 0.8, 0.2]  # F=0.8 > high_f=0.3, M=0.2 <= high_m=0.3
        label = FeatureEngineer._classify_cluster(center, self.THRESHOLDS)
        assert label == "高频忠诚客户"

    def test_potential_customer(self):
        """R 低 + F 或 M 正值 → 潜力发展客户"""
        center = [-0.2, 0.1, 0.1]  # R<0, F>0, M 不够高
        label = FeatureEngineer._classify_cluster(center, self.THRESHOLDS)
        assert label == "潜力发展客户"

    def test_general_customer(self):
        """所有指标均不突出 → 一般维持客户"""
        center = [0.1, -0.1, -0.1]  # R>0 但 < high_r, F<0, M<0
        label = FeatureEngineer._classify_cluster(center, self.THRESHOLDS)
        assert label == "一般维持客户"

    def test_priority_r_over_m(self):
        """R 值高时即使 M 值也高，仍然判定为流失（R 优先级最高）"""
        center = [1.0, 0.5, 1.0]  # R>high_r，M 也很高
        label = FeatureEngineer._classify_cluster(center, self.THRESHOLDS)
        assert label == "流失/沉睡客户"

    def test_priority_m_over_f(self):
        """M 和 F 都高时，优先判定为核心高价值"""
        center = [-0.5, 0.8, 0.8]  # R<0, M>high_m, F>high_f
        label = FeatureEngineer._classify_cluster(center, self.THRESHOLDS)
        assert label == "核心高价值客户"

    def test_custom_thresholds(self):
        """自定义阈值：更严格的 R 阈值"""
        strict_th = {"high_r": 2.0, "high_m": 0.3, "high_f": 0.3}
        center = [1.0, 0.1, 0.1]  # R=1.0 < strict high_r=2.0
        label = FeatureEngineer._classify_cluster(center, strict_th)
        assert label != "流失/沉睡客户"
