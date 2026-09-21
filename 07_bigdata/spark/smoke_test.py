"""阶段三环境探针：验证 PySpark 能连上外部 Hive Metastore。

在容器内执行：
  docker exec -i bd-spark /opt/spark/bin/spark-submit \
      --master local[2] /workspace/07_bigdata/spark/smoke_test.py
"""
from pyspark.sql import SparkSession


def build_spark(app_name: str = "stage3-smoke") -> SparkSession:
    return (SparkSession.builder
            .appName(app_name)
            .config("spark.sql.catalogImplementation", "hive")
            .enableHiveSupport()
            .getOrCreate())


def conf_or(spark, key: str) -> str:
    """安全读取一个 Spark 配置。

    ⚠️ 踩过的坑：不要用 spark.conf.get(key, "(default)") 这种回退值写法 ——
       Spark 会对**返回值**做校验，于是 "(default)" 会被当成
       spark.sql.hive.metastore.version 的值，抛出：
         IllegalArgumentException: '(default)' ... Unsupported Hive Metastore version
       正确做法是取不到就抛异常，自己接住。
    """
    try:
        return spark.conf.get(key)
    except Exception:
        return "(未设置，用默认值)"


def main() -> int:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("PySpark 环境探针")
    print("=" * 60)

    # ① 能连上 Metastore 吗（这一步会触发 thrift 调用）
    dbs = [r.namespace for r in spark.sql("SHOW DATABASES").collect()]
    print(f"✅ SHOW DATABASES 成功，共 {len(dbs)} 个库")
    print(f"   包含 credit_dwh_lc: {'credit_dwh_lc' in dbs}")

    # ② 版本信息（记录到验收记录里）
    print(f"   spark.version = {spark.version}")
    print(f"   spark.master = {spark.sparkContext.master}")
    print(f"   spark.sql.warehouse.dir = {conf_or(spark, 'spark.sql.warehouse.dir')}")
    print(f"   hive.metastore.uris = "
          f"{conf_or(spark, 'spark.hadoop.hive.metastore.uris')}")
    print(f"   metastore.version = "
          f"{conf_or(spark, 'spark.sql.hive.metastore.version')}")

    # ③ 能写临时视图并做真实计算吗（不落表，纯内存，安全）
    df = spark.createDataFrame([("A", 1000.0), ("B", 2000.0), ("A", 500.0)],
                               "grade string, amt double")
    rows = (df.groupBy("grade").sum("amt").orderBy("grade").collect())
    print("✅ 计算正常：" + ", ".join(f"{r[0]}={r[1]:.0f}" for r in rows))

    if "credit_dwh_lc" in dbs:
        n = spark.sql("SHOW TABLES IN credit_dwh_lc").count()
        print(f"✅ credit_dwh_lc 下有 {n} 张表")

    print("=" * 60)
    print("探针结束：PySpark + 外部 Metastore 可用")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
