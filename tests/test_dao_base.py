"""
DAO 工具函数单元测试
覆盖 safe_float / safe_int 的各种边界情况。
"""

from dao.base import safe_float, safe_int


# ═══════════════════════════════════════
#  safe_float 测试
# ═══════════════════════════════════════

class TestSafeFloat:
    """safe_float 函数测试集"""

    def test_normal_number(self):
        """正常数值输入"""
        assert safe_float(3.14) == 3.14

    def test_string_number(self):
        """字符串数值输入"""
        assert safe_float("99.9") == 99.9

    def test_integer_input(self):
        """整数输入转为 float"""
        assert safe_float(42) == 42.0

    def test_none_returns_default(self):
        """None 输入，使用默认值 0.0"""
        assert safe_float(None) == 0.0

    def test_none_with_none_default(self):
        """None 输入 + default=None → 返回 None（环比修复核心验证）"""
        result = safe_float(None, default=None)
        assert result is None

    def test_invalid_string(self):
        """非法字符串返回默认值"""
        assert safe_float("abc") == 0.0

    def test_empty_string(self):
        """空字符串返回默认值"""
        assert safe_float("") == 0.0


# ═══════════════════════════════════════
#  safe_int 测试
# ═══════════════════════════════════════

class TestSafeInt:
    """safe_int 函数测试集"""

    def test_normal_integer(self):
        """正常整数"""
        assert safe_int(42) == 42

    def test_float_input(self):
        """浮点数截断"""
        assert safe_int(3.7) == 3

    def test_string_number(self):
        """字符串整数"""
        assert safe_int("100") == 100

    def test_none_returns_default(self):
        """None 输入返回默认值 0"""
        assert safe_int(None) == 0

    def test_invalid_string(self):
        """非法字符串返回默认值"""
        assert safe_int("xyz") == 0

    def test_custom_default(self):
        """自定义默认兜底值"""
        assert safe_int(None, default=-1) == -1
