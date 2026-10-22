from pyspark.sql import SparkSession
import shutil
import os

POWERBI_EXPORT_DIR = os.environ.get("POWERBI_EXPORT_DIR", "./powerbi_exports")
os.makedirs(POWERBI_EXPORT_DIR, exist_ok=True)

spark = (
    SparkSession.builder
    .appName("ExportFunnelForPowerBI")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

funnel = spark.read.format("delta").load("delta-lake/gold/daily_funnel")

tmp = "powerbi_tmp_funnel"

funnel.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(tmp)

for file in os.listdir(tmp):
    if file.startswith("part-") and file.endswith(".csv"):
        shutil.copy2(
            os.path.join(tmp, file),
            os.path.join(POWERBI_EXPORT_DIR, "daily funnel wal.csv")
        )
        break

print("FUNNEL CSV UPDATED")
print("ROWS:", funnel.count())

spark.stop()
