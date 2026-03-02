"""
数据写入模块
负责 ClickHouse 原子写入（EXCHANGE TABLES）和 SQLite 回退写入。
"""

import os
import json
import datetime
import sqlite3

import clickhouse_connect


class DataWriter:
    """ClickHouse 原子写入 + SQLite 回退"""

    # ClickHouse 建表 DDL
    TABLE_DDL = {
        "buy_fact": """
            CREATE TABLE IF NOT EXISTS buy_fact (
                date Date, user_id Int32, order_id String, item_id Int32,
                category_id Int32, price Decimal(10, 2), channel String, age_group String
            ) ENGINE = MergeTree() ORDER BY (date, user_id)
        """,
        "user_rfm": """
            CREATE TABLE IF NOT EXISTS user_rfm (
                user_id Int32, rfm_label String
            ) ENGINE = MergeTree() ORDER BY (user_id)
        """,
        "cohort_matrix": """
            CREATE TABLE IF NOT EXISTS cohort_matrix (
                cohort_date Date, day_diff Int32, active_users Int32, cohort_users Int32
            ) ENGINE = MergeTree() ORDER BY (cohort_date, day_diff)
        """,
        "user_funnel_mart": """
            CREATE TABLE IF NOT EXISTS user_funnel_mart (
                user_id Int32, has_pv Int32, has_cart Int32, has_buy Int32, date Date
            ) ENGINE = MergeTree() ORDER BY (user_id)
        """,
        "user_funnel_loose_mart": """
            CREATE TABLE IF NOT EXISTS user_funnel_loose_mart (
                user_id Int32, has_pv Int32, has_cart Int32, has_buy Int32, date Date
            ) ENGINE = MergeTree() ORDER BY (user_id)
        """,
        "etl_dq_log": """
            CREATE TABLE IF NOT EXISTS etl_dq_log (
                run_time DateTime,
                elapsed_seconds Float64,
                metrics String,
                warnings String,
                cluster_profiles String
            ) ENGINE = MergeTree() ORDER BY (run_time)
        """,
    }

    def __init__(self, config, dq):
        self.config = config
        self.dq = dq
        self.ch_available = False
        self.client = None

    def write_all(self, write_tasks):
        """
        [6/6] 写入数据库。
        :param write_tasks: [(DataFrame, table_name), ...]
        """
        print("[6/6] 写入数据库...")
        self._detect_clickhouse()

        if self.ch_available:
            self._write_clickhouse(write_tasks)
        else:
            self._write_sqlite(write_tasks)

    def _detect_clickhouse(self):
        """尝试连接 ClickHouse"""
        try:
            self.client = clickhouse_connect.get_client(
                host=self.config["ch_host"],
                port=self.config["ch_port"],
                username=self.config["ch_user"],
                password=self.config["ch_password"],
            )
            self.client.command("SELECT 1")
            self.ch_available = True
            print("  ✔ ClickHouse 连接成功，使用原子写入模式")
        except Exception as e:
            print(f"  [WARN] ClickHouse 不可用 ({e})，回退到 SQLite 写入")

    # ── ClickHouse 原子写入路径 ──

    def _write_clickhouse(self, write_tasks):
        """ClickHouse 原子写入（EXCHANGE TABLES 策略）"""
        # 确保所有表存在
        for table_name, ddl in self.TABLE_DDL.items():
            self.client.command(ddl)
            print(f"  ✔ 表 {table_name} 已确保存在")

        # 写入业务数据
        for df_spark, table_name in write_tasks:
            self._write_atomic(df_spark, table_name)
            print(f"  ✔ {table_name} 原子写入 ClickHouse 完成")

        # 写入 DQ 报告
        self._write_dq_log_clickhouse()

    def _write_atomic(self, df, table_name):
        """单表原子写入：先写临时表，再 EXCHANGE"""
        tmp_table = f"{table_name}_tmp_new"
        self.client.command(f"DROP TABLE IF EXISTS {tmp_table}")
        self.client.command(f"CREATE TABLE {tmp_table} AS {table_name}")

        pdf = df.toPandas()
        self.client.insert_df(tmp_table, pdf)

        self.client.command(f"EXCHANGE TABLES {table_name} AND {tmp_table}")
        self.client.command(f"DROP TABLE IF EXISTS {tmp_table}")

    def _write_dq_log_clickhouse(self):
        """将 DQ 报告写入 ClickHouse etl_dq_log 表"""
        try:
            dq_data = self.dq.to_json_dict()
            self.client.command(
                "INSERT INTO etl_dq_log (run_time, elapsed_seconds, metrics, warnings, cluster_profiles) "
                "VALUES ({rt:DateTime}, {el:Float64}, {mt:String}, {wn:String}, {cp:String})",
                parameters={
                    'rt': dq_data['run_time'],
                    'el': dq_data['elapsed_seconds'],
                    'mt': dq_data['metrics'],
                    'wn': dq_data['warnings'],
                    'cp': dq_data['cluster_profiles'],
                }
            )
            print("  ✔ 数据质量报告已写入 etl_dq_log")
        except Exception as e:
            print(f"  [WARN] 写入 etl_dq_log 失败: {e}")

    # ── SQLite 回退路径 ──

    def _write_sqlite(self, write_tasks):
        """SQLite 回退写入"""
        sqlite_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ecommerce.db")
        sqlite_path = os.path.normpath(sqlite_path)
        print(f"  SQLite 写入路径: {sqlite_path}")

        conn = sqlite3.connect(sqlite_path)

        for df_spark, table_name in write_tasks:
            pdf = df_spark.toPandas()
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            pdf.to_sql(table_name, conn, if_exists="replace", index=False)
            print(f"  ✔ {table_name} 已写入 SQLite ({len(pdf)} 行)")

        # 创建索引加速查询
        index_stmts = [
            "CREATE INDEX IF NOT EXISTS idx_buy_fact_date ON buy_fact(date)",
            "CREATE INDEX IF NOT EXISTS idx_buy_fact_user ON buy_fact(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_funnel_date ON user_funnel_mart(date)",
            "CREATE INDEX IF NOT EXISTS idx_cohort_date ON cohort_matrix(cohort_date)",
        ]
        for stmt in index_stmts:
            try:
                conn.execute(stmt)
            except Exception:
                pass
        conn.commit()
        conn.close()
        print("  ✔ SQLite 索引已建立")
