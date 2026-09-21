"""把 Hive 侧 4 张 ADS 表导出为 CSV，供宿主侧逐行比对。

⭐ 为什么要独立成一个脚本（v2 踩的坑）：
   旧脚本在宿主侧"先 rmtree 清理目录、再 glob 去找导出的 CSV"，
   等于把自己刚导出的文件删掉 → 永远报"Hive 侧无数据"。
   正确顺序是 **清理 → 导出 → 读取**，本脚本只负责"导出"这一步，
   清理由外层脚本/命令在调用前完成。

⚠️ 机读产物用 ASCII 文件名（验收脚本要能稳定读）。

在容器内执行：
    docker exec -i bd-spark /opt/spark/bin/spark-submit --master local[2] \
        /workspace/07_bigdata/export_ads_csv.py
"""
from __future__ import annotations

import sys
from pathlib import Path

from pyspark.sql import SparkSession

DB = "credit_dwh_lc"
TABLES = ["ads_risk_segment_lc", "ads_vintage_lc",
          "ads_roll_rate_lc", "ads_delinq_monthly_lc"]
OUT = Path("/workspace/08_benchmark/_exp")


def main() -> int:
    spark = (SparkSession.builder
             .appName("stage3-export-ads")
             .config("spark.sql.catalogImplementation", "hive")
             .enableHiveSupport()
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    spark.catalog.setCurrentDatabase(DB)

    for t in TABLES:
        df = spark.sql(f"SELECT * FROM {DB}.{t}")
        n = df.count()
        # coalesce(1)：4 张表都很小（18 / 2.4 万 / 108 / 3105 行），单文件便于宿主读取
        (df.coalesce(1).write.mode("overwrite")
           .option("header", "true")
           .option("nullValue", "")          # NULL 写成空字段，宿主侧做 NULL 兜底
           .csv(f"{OUT}/{t}"))
        print(f"OK {t}: {n:,} rows -> {OUT}/{t}")

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
