"""
ETL 配置管理器单元测试
测试 ConfigManager 的配置预检逻辑（mock 掉文件系统和 Hadoop 环境）。
"""

import pytest
from unittest.mock import patch, MagicMock


# ═══════════════════════════════════════
#  配置预检逻辑测试
# ═══════════════════════════════════════

class TestConfigValidation:
    """ConfigManager._validate_config 测试"""

    def _make_valid_config(self):
        """生成一份完整合法的配置字典"""
        return {
            "ch_host": "localhost",
            "ch_port": 8123,
            "ch_user": "default",
            "ch_password": "password123",
            "ch_database": "default",
            "behavior_csv": "UserBehavior.csv",
            "items_csv": "items_simulated.csv",
            "users_csv": "users_simulated.csv",
            "data_limit": None,
            "driver_memory": "4g",
            "default_parallelism": "8",
            "locality_wait": "3s",
            "rfm_weights": {"R": -0.2, "F": 0.3, "M": 0.5},
            "rfm_thresholds": {"high_r": 0.5, "high_m": 0.3, "high_f": 0.3},
            "model_save_path": None,
        }

    @patch("etl.config_manager.os.path.exists", return_value=True)
    @patch("etl.config_manager.platform.system", return_value="Linux")
    def test_valid_config_passes(self, mock_sys, mock_exists):
        """合法配置 → 预检通过，不抛出异常"""
        from etl.config_manager import ConfigManager
        config = self._make_valid_config()

        mgr = ConfigManager.__new__(ConfigManager)
        mgr.config = config
        # 不应抛出异常
        mgr._validate_config()

    @patch("etl.config_manager.os.path.exists", return_value=True)
    def test_missing_required_string_field(self, mock_exists):
        """必填字段缺失 → SystemExit"""
        from etl.config_manager import ConfigManager
        config = self._make_valid_config()
        config["ch_host"] = ""  # 置空
        
        mgr = ConfigManager.__new__(ConfigManager)
        mgr.config = config
        with pytest.raises(SystemExit):
            mgr._validate_config()

    @patch("etl.config_manager.os.path.exists", return_value=True)
    def test_invalid_port(self, mock_exists):
        """端口为负数 → SystemExit"""
        from etl.config_manager import ConfigManager
        config = self._make_valid_config()
        config["ch_port"] = -1
        
        mgr = ConfigManager.__new__(ConfigManager)
        mgr.config = config
        with pytest.raises(SystemExit):
            mgr._validate_config()

    @patch("etl.config_manager.os.path.exists", return_value=True)
    def test_invalid_rfm_weights(self, mock_exists):
        """RFM 权重缺少键 → SystemExit"""
        from etl.config_manager import ConfigManager
        config = self._make_valid_config()
        config["rfm_weights"] = {"R": -0.2}  # 缺少 F 和 M
        
        mgr = ConfigManager.__new__(ConfigManager)
        mgr.config = config
        with pytest.raises(SystemExit):
            mgr._validate_config()

    @patch("etl.config_manager.os.path.exists", return_value=True)
    def test_invalid_rfm_thresholds(self, mock_exists):
        """RFM 阈值不是字典 → SystemExit"""
        from etl.config_manager import ConfigManager
        config = self._make_valid_config()
        config["rfm_thresholds"] = "not_a_dict"
        
        mgr = ConfigManager.__new__(ConfigManager)
        mgr.config = config
        with pytest.raises(SystemExit):
            mgr._validate_config()

    @patch("etl.config_manager.os.path.exists")
    def test_csv_file_not_found(self, mock_exists):
        """CSV 文件不存在 → SystemExit"""
        from etl.config_manager import ConfigManager
        config = self._make_valid_config()

        # config.json 存在但 CSV 文件不存在
        def exists_side_effect(path):
            if path.endswith(".csv"):
                return False
            return True
        mock_exists.side_effect = exists_side_effect
        
        mgr = ConfigManager.__new__(ConfigManager)
        mgr.config = config
        with pytest.raises(SystemExit):
            mgr._validate_config()
