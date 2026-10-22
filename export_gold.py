
from pathlib import Path
import shutil

from pyspark.sql.functions import col, countDistinct, when, round as spark_round
from src.spark_utils import build_spark_session

ROOT = Path(__file__).resolve().parent
GOLD_PATH = ROOT / "delta-lake" / "gold"
SILVER_PATH = ROOT / "delta-lake" / "silver"
OUTPUT_DIR = ROOT / "powerbi_data"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

spark = build_spark_session("ExportTopItemsForPowerBI")

try:
    df = spark.read.format("delta").load(str(SILVER_PATH))

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
            ).alias("buyers"),
        )
        .withColumn(
            "item_conversion_rate",
            spark_round(
                when(
                    col("viewers") > 0,
                    col("buyers") / col("viewers") * 100
                ).otherwise(0),
                2,
            ),
        )
        .orderBy(col("buyers").desc(), col("viewers").desc())
    )

    temp_dir = ROOT / "powerbi_tmp_top_items"
    top_items.coalesce(1).write.mode("overwrite").option(
        "header", True
    ).csv(str(temp_dir))

    csv_file = next(temp_dir.glob("part-*.csv"), None)
    if csv_file is None:
        raise FileNotFoundError("Top items CSV was not generated.")

    destination = OUTPUT_DIR / "top_items.csv"
    shutil.copy2(csv_file, destination)

    print("TOP ITEMS EXPORT COMPLETE")
    print("ROWS:", top_items.count())
    print("FILE:", destination)
    top_items.show(10, truncate=False)

finally:
    spark.stop()
