"""
ETL 数据质量报告单元测试
覆盖 DataQualityReport 的全部方法，包括指标收集、告警、聚类画像和报告输出。
"""

import json
from etl.data_quality import DataQualityReport


# ═══════════════════════════════════════
#  基础功能测试
# ═══════════════════════════════════════

class TestDataQualityReportBasic:
    """DataQualityReport 基础方法测试"""

    def test_add_metric(self):
        """记录指标后可正确读取"""
        dq = DataQualityReport()
        dq.add_metric("原始行数", 1000000)
        dq.add_metric("清洗后行数", 950000)
        assert dq.metrics["原始行数"] == 1000000
        assert dq.metrics["清洗后行数"] == 950000

    def test_add_metric_overwrite(self):
        """同名指标后值覆盖"""
        dq = DataQualityReport()
        dq.add_metric("行数", 100)
        dq.add_metric("行数", 200)
        assert dq.metrics["行数"] == 200

    def test_add_warning(self):
        """记录告警后可正确读取"""
        dq = DataQualityReport()
        dq.add_warning("缺失价格字段")
        dq.add_warning("渠道字段为空")
        assert len(dq.warnings) == 2
        assert dq.warnings[0] == "缺失价格字段"

    def test_add_cluster_profile(self):
        """记录聚类画像后可正确读取"""
        dq = DataQualityReport()
        dq.add_cluster_profile(
            cluster_id=0, label="核心高价值", r_mean=10.5, f_mean=5.2, m_mean=800.0, user_count=1500
        )
        assert len(dq.cluster_profiles) == 1
        profile = dq.cluster_profiles[0]
        assert profile["label"] == "核心高价值"
        assert profile["user_count"] == 1500

    def test_empty_report(self):
        """空报告的初始状态正确"""
        dq = DataQualityReport()
        assert dq.metrics == {}
        assert dq.warnings == []
        assert dq.cluster_profiles == []


# ═══════════════════════════════════════
#  报告输出测试
# ═══════════════════════════════════════

class TestDataQualityReportOutput:
    """报告输出格式测试"""

    def test_print_report_no_warnings(self, capsys):
        """无告警时输出 '无质量告警'"""
        dq = DataQualityReport()
        dq.add_metric("行数", 1000)
        dq.print_report()
        captured = capsys.readouterr()
        assert "无质量告警" in captured.out
        assert "行数" in captured.out

    def test_print_report_with_warnings(self, capsys):
        """有告警时输出告警内容"""
        dq = DataQualityReport()
        dq.add_warning("测试告警1")
        dq.add_warning("测试告警2")
        dq.print_report()
        captured = capsys.readouterr()
        assert "质量告警 (2 条)" in captured.out
        assert "测试告警1" in captured.out

    def test_print_report_with_cluster_profiles(self, capsys):
        """有聚类画像时按用户数降序输出"""
        dq = DataQualityReport()
        dq.add_cluster_profile(0, "小群体", 1.0, 1.0, 1.0, 100)
        dq.add_cluster_profile(1, "大群体", 2.0, 2.0, 2.0, 5000)
        dq.print_report()
        captured = capsys.readouterr()
        # 验证大群体在前（按 user_count 降序排列）
        pos_big = captured.out.index("大群体")
        pos_small = captured.out.index("小群体")
        assert pos_big < pos_small


# ═══════════════════════════════════════
#  JSON 导出测试
# ═══════════════════════════════════════

class TestDataQualityReportJsonExport:
    """to_json_dict() 序列化测试"""

    def test_json_dict_structure(self):
        """导出字典包含必要字段"""
        dq = DataQualityReport()
        dq.add_metric("行数", 1000)
        dq.add_warning("测试告警")
        dq.add_cluster_profile(0, "标签A", 1.0, 2.0, 3.0, 100)

        result = dq.to_json_dict()
        assert "run_time" in result
        assert "elapsed_seconds" in result
        assert isinstance(result["elapsed_seconds"], float)

        # metrics 应为可解析的 JSON 字符串
        metrics = json.loads(result["metrics"])
        assert metrics["行数"] == 1000

        # warnings 应为可解析的 JSON 字符串
        warnings = json.loads(result["warnings"])
        assert len(warnings) == 1

        # cluster_profiles 应为可解析的 JSON 字符串
        profiles = json.loads(result["cluster_profiles"])
        assert profiles[0]["label"] == "标签A"

    def test_empty_json_dict(self):
        """空报告的 JSON 导出"""
        dq = DataQualityReport()
        result = dq.to_json_dict()
        metrics = json.loads(result["metrics"])
        assert metrics == {}
        warnings = json.loads(result["warnings"])
        assert warnings == []
