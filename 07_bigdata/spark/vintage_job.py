"""PySpark 版 Vintage 计算 —— 与 Hive SQL 版结果必须一致。

设计要点（面试会问）：
1. 用 **DataFrame API** 而不是 SQL 字符串 —— 便于复用、便于加校验
2. 窗口函数**显式声明帧** —— 避免默认帧（RANGE）在重复排序键上的语义歧义
3. 显式设置 shuffle 分区数与 AQE —— 让性能可控、可复现
4. 结果写回 Hive 表，好让 SQL 侧能直接逐行比对

执行（容器内）：
    docker exec -i bd-spark /opt/spark/bin/spark-submit \
        --master spark://spark:7077 --executor-memory 1500m --total-executor-cores 4 \
        /workspace/07_bigdata/spark/vintage_job.py
"""
from __future__ import annotations

import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def build_spark(app_name: str = "vintage_job") -> SparkSession:
    return (SparkSession.builder
            .appName(app_name)
            .config("spark.sql.catalogImplementation", "hive")
            # ⭐ 生产习惯：显式设置，不依赖默认值
            .config("spark.sql.shuffle.partitions", "20")
            .config("spark.sql.adaptive.enabled", "true")
            .enableHiveSupport()
            .getOrCreate())


def calc_vintage(spark: SparkSession, mob_max: int = 24):
    """Vintage 矩阵：放款批次 × 账龄 → 累计不良率（ALL 粒度）。

    ⭐ 核心：用窗口函数取"曾核销"标志，避免同一笔贷款被逐月重复计数。
    """
    snap = (spark.table("credit_dwh_lc.dws_loan_snapshot_lc")
            .filter((F.col("mob") >= 1) & (F.col("mob") <= mob_max)))

    # ⚠️ 显式声明 ROWS 帧：默认帧是 RANGE，排序键有重复值时语义不同
    w = (Window
         .partitionBy("issue_month", "loan_id")
         .orderBy("mob")
         .rowsBetween(Window.unboundedPreceding, Window.currentRow))

    snap = snap.withColumn("ever_bad", F.max("is_bad_month").over(w))

    return (snap
            .groupBy("issue_month", "mob")
            .agg(F.count(F.lit(1)).alias("exposure_cnt"),
                 F.sum("ever_bad").alias("bad_cnt"))
            .withColumn("bad_rate",
                        F.round(F.col("bad_cnt") / F.col("exposure_cnt"), 6))
            .withColumn("grade", F.lit("ALL"))
            .select("issue_month", "grade", "mob", "bad_rate", "exposure_cnt"))


def main() -> int:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        vintage = calc_vintage(spark)
        (vintage.write.mode("overwrite").format("orc")
         .saveAsTable("credit_dwh_lc.ads_vintage_spark"))

        cnt = spark.table("credit_dwh_lc.ads_vintage_spark").count()
        print(f"OK ads_vintage_spark 写入完成：{cnt:,} 行")
        vintage.orderBy("issue_month", "mob").show(5, truncate=False)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
