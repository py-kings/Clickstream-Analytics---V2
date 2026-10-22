from src.spark_utils import build_spark_session
from pyspark.sql.functions import col, count, when, round as spark_round
import os
import shutil

POWERBI_EXPORT_DIR = os.environ.get("POWERBI_EXPORT_DIR", "./powerbi_exports")
os.makedirs(POWERBI_EXPORT_DIR, exist_ok=True)

spark = build_spark_session("ExportAbandonmentForPowerBI")

sessions = spark.read.format("delta").load("delta-lake/gold/sessions")

cart_sessions = sessions.filter(col("addtocarts") > 0)

result = cart_sessions.agg(
    count("*").alias("total_cart_sessions"),
    count(
        when(col("transactions") == 0, 1)
    ).alias("abandoned_sessions")
)

result = result.withColumn(
    "abandonment_rate",
    spark_round(
        col("abandoned_sessions") / col("total_cart_sessions"),
        4
    )
)

result.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("powerbi_tmp_abandonment")

for file in os.listdir("powerbi_tmp_abandonment"):
    if file.startswith("part-") and file.endswith(".csv"):
        shutil.copy2(
            os.path.join("powerbi_tmp_abandonment", file),
            os.path.join(POWERBI_EXPORT_DIR, "cart_abondntment.csv")
        )
        break

print("ABANDONMENT CSV UPDATED")
result.show()

spark.stop()

