from pathlib import Path
import shutil
from src.spark_utils import build_spark_session

ROOT = Path(__file__).resolve().parent
GOLD = ROOT / "delta-lake" / "gold"
OUTPUT = ROOT / "powerbi_data"

def export_table(spark, table, folder):
    source = GOLD / table
    target = OUTPUT / folder
    target.mkdir(parents=True, exist_ok=True)

    temp = ROOT / f"tmp_export_{table}"
    df = spark.read.format("delta").load(str(source))
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(temp))

    csv_file = next(temp.glob("part-*.csv"), None)
    if csv_file is None:
        raise FileNotFoundError(f"No CSV generated for {table}")

    for old in target.glob("part-*.csv"):
        old.unlink()

    shutil.copy2(csv_file, target / f"part-{table}.csv")
    print(f"{table}: {df.count()} rows exported")

def main():
    spark = build_spark_session("ExportGoldForSnowflake")
    try:
        export_table(spark, "daily_funnel", "daily_funnel")
        export_table(spark, "sessions", "sessions")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
