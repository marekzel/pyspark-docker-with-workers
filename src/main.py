from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    count,
    avg,
    round as spark_round,
    desc,
)

spark = (
    SparkSession.builder
    .appName("TransactionsAnalysis")
    .master("spark://spark-master:7077")
    .config("spark.executor.cores", "1")
    .config("spark.executor.memory", "1g")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n=== READING DATA ===")

transactions = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/data/transactions.csv")
)

cards = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/data/cards.csv")
)

print(f"Transactions: {transactions.count()}")
print(f"Cards: {cards.count()}")

print("\n=== TRANSACTIONS ===")
transactions.show(10, truncate=False)

print("\n=== JOIN + AGGREGATION ===")

result = (
    transactions
    .join(cards, "card_id", "left")
    .groupBy("card_name")
    .agg(
        spark_round(spark_sum("amount"), 2).alias("total_amount"),
        count("*").alias("transaction_count"),
        spark_round(avg("amount"), 2).alias("average_transaction"),
    )
    .orderBy(desc("total_amount"))
)

result.show(truncate=False)

print("\n=== SAVING PARQUET ===")

result.write.mode("overwrite").parquet("/output/card_summary")

print("\n=== DONE ===")

spark.stop()
