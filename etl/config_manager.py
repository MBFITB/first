"""
配置管理器
负责配置加载、预检校验和 Windows Hadoop 环境自动配置。
"""

import os
import sys
import json
import platform


class ConfigManager:
    """配置加载、校验和环境准备"""

    def __init__(self, config_path="config.json"):
        self.config_path = config_path
        self.config = self._load_config()
        self._validate_config()
        self._setup_hadoop()
        self._build_jdbc_info()

    # ── 配置加载 ──

    def _load_config(self) -> dict:
        """加载配置：优先 config.json，回退到环境变量/默认值"""
        base_config = {
            "ch_host": os.environ.get("CH_HOST", "localhost"),
            "ch_port": int(os.environ.get("CH_PORT", "8123")),
            "ch_user": os.environ.get("CH_USER", "default"),
            "ch_password": os.environ.get("CH_PASSWORD", "password123"),
            "ch_database": os.environ.get("CH_DATABASE", "default"),

            "behavior_csv": os.environ.get("BEHAVIOR_CSV", "UserBehavior.csv"),
            "items_csv": os.environ.get("ITEMS_CSV", "items_simulated.csv"),
            "users_csv": os.environ.get("USERS_CSV", "users_simulated.csv"),
            "data_limit": None,

            "driver_memory": os.environ.get("SPARK_DRIVER_MEMORY", "4g"),
            "default_parallelism": os.environ.get("SPARK_PARALLELISM", "8"),
            "locality_wait": os.environ.get("SPARK_LOCALITY_WAIT", "3s"),

            "rfm_weights": {"R": -0.2, "F": 0.3, "M": 0.5},
            "rfm_thresholds": {
                "high_r": 0.5,
                "high_m": 0.3,
                "high_f": 0.3
            },
            "model_save_path": os.environ.get("RFM_MODEL_PATH", None),
        }

        if os.path.exists(self.config_path):
            try:
                with open(self.config_path, "r", encoding="utf-8") as f:
                    ext_cfg = json.load(f)
                    base_config.update(ext_cfg)
            except Exception as e:
                print(f"未能加载外部 config.json，回退到环境/默认配置: {e}")

        return base_config

    # ── 配置预检 ──

    def _validate_config(self):
        """CONFIG 预检：在 Spark 初始化前发现配置错误"""
        cfg = self.config
        errors = []

        # 必填字符串字段
        for key in ["ch_host", "ch_user", "ch_password", "ch_database",
                     "behavior_csv", "items_csv", "users_csv",
                     "driver_memory", "default_parallelism"]:
            val = cfg.get(key)
            if not val or not isinstance(val, str):
                errors.append(f"'{key}' 必须为非空字符串，当前值: {val!r}")

        # 端口必须为正整数
        port = cfg.get("ch_port")
        if not isinstance(port, int) or port <= 0:
            errors.append(f"'ch_port' 必须为正整数，当前值: {port!r}")

        # CSV 文件可达性
        for csv_key in ["behavior_csv", "items_csv", "users_csv"]:
            path = cfg.get(csv_key, "")
            if path and not os.path.exists(path):
                errors.append(f"'{csv_key}' 指向的文件不存在: {path}")

        # RFM 权重必须包含 R/F/M 三个浮点数
        weights = cfg.get("rfm_weights", {})
        if not isinstance(weights, dict):
            errors.append(f"'rfm_weights' 必须为字典，当前类型: {type(weights).__name__}")
        else:
            for k in ["R", "F", "M"]:
                if k not in weights or not isinstance(weights[k], (int, float)):
                    errors.append(f"'rfm_weights.{k}' 必须为数值，当前: {weights.get(k)!r}")

        # RFM 阈值检查
        thresholds = cfg.get("rfm_thresholds", {})
        if not isinstance(thresholds, dict):
            errors.append(f"'rfm_thresholds' 必须为字典")
        else:
            for k in ["high_r", "high_m", "high_f"]:
                if k not in thresholds or not isinstance(thresholds[k], (int, float)):
                    errors.append(f"'rfm_thresholds.{k}' 必须为数值")

        if errors:
            print("\n❌ CONFIG 配置预检失败:")
            for e in errors:
                print(f"  • {e}")
            raise SystemExit(1)

        print("✅ CONFIG 配置预检通过")

    # ── Windows Hadoop 环境 ──

    def _setup_hadoop(self):
        """Windows 下自动配置 Hadoop 环境（winutils.exe 占位）"""
        if platform.system() != "Windows":
            return

        hadoop_home = os.environ.get("HADOOP_HOME")
        if not hadoop_home:
            hadoop_home = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "hadoop_home")
            hadoop_home = os.path.normpath(hadoop_home)
            hadoop_bin = os.path.join(hadoop_home, "bin")
            os.makedirs(hadoop_bin, exist_ok=True)

            winutils_path = os.path.join(hadoop_bin, "winutils.exe")
            if not os.path.exists(winutils_path):
                open(winutils_path, "w").close()

            os.environ["HADOOP_HOME"] = hadoop_home
            os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")
            print(f"✅ Windows Hadoop 环境已自动配置: {hadoop_home}")
        else:
            print(f"✅ 检测到 HADOOP_HOME: {hadoop_home}")

    # ── JDBC 信息 ──

    def _build_jdbc_info(self):
        """构建 JDBC 连接信息"""
        cfg = self.config
        self.jdbc_url = f"jdbc:clickhouse://{cfg['ch_host']}:{cfg['ch_port']}/{cfg['ch_database']}"
        self.jdbc_driver = "com.clickhouse.jdbc.ClickHouseDriver"
        self.jdbc_props = {"user": cfg["ch_user"], "password": cfg["ch_password"]}

    def __getitem__(self, key):
        """支持 config['key'] 语法访问"""
        return self.config[key]

    def get(self, key, default=None):
        """支持 config.get('key', default) 语法"""
        return self.config.get(key, default)
