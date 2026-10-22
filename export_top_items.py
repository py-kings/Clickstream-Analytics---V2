from src.spark_utils import build_spark_session
from pyspark.sql.functions import col, countDistinct, when, round as spark_round
import os
import shutil

POWERBI_EXPORT_DIR = os.environ.get("POWERBI_EXPORT_DIR", "./powerbi_exports")
os.makedirs(POWERBI_EXPORT_DIR, exist_ok=True)

spark = build_spark_session("ExportTopItemsForPowerBI")

df = spark.read.format("delta").load("delta-lake/silver")

top_items = (
    df.filter(col("itemid").isNotNull())
      .groupBy("itemid")
      .agg(
          countDistinct(
              when(col("event") == "view", col("visitorid"))
          ).alias("viewers"),

          countDistinct(
              when(col("event") == "addtocart", col("visitorid"))
          ).alias("carters"),

          countDistinct(
              when(col("event") == "transaction", col("visitorid"))
          ).alias("buyers")
      )
      .withColumn(
          "item_conversion_rate",
          spark_round(
              when(col("viewers") > 0,
                   col("buyers") / col("viewers") * 100)
              .otherwise(0),
              2
          )
      )
      .orderBy(col("buyers").desc(), col("viewers").desc())
)

top_items.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("powerbi_tmp_top_items")

for file in os.listdir("powerbi_tmp_top_items"):
    if file.startswith("part-") and file.endswith(".csv"):
        shutil.copy2(
            os.path.join("powerbi_tmp_top_items", file),
            os.path.join(POWERBI_EXPORT_DIR, "top_items.csv")
        )
        break

print("TOP ITEMS CSV UPDATED")
print("ROWS:", top_items.count())
top_items.show(10, truncate=False)

spark.stop()



