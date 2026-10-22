from pyspark.sql import SparkSession
import shutil
import os

POWERBI_EXPORT_DIR = os.environ.get("POWERBI_EXPORT_DIR", "./powerbi_exports")
os.makedirs(POWERBI_EXPORT_DIR, exist_ok=True)

spark = (
    SparkSession.builder
    .appName("ExportGoldForPowerBI")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

sessions = spark.read.format("delta").load("delta-lake/gold/sessions")

# Temporary Spark CSV output folders
tmp1 = "powerbi_tmp_session_behaviour"
tmp2 = "powerbi_tmp_session"

sessions.select(
    "visitorid",
    "session_id",
    "event_count",
    "distinct_items",
    "views",
    "addtocarts",
    "transactions",
    "converted"
).coalesce(1).write.mode("overwrite").option("header", "true").csv(tmp1)

sessions.coalesce(1).write.mode("overwrite").option("header", "true").csv(tmp2)

# Find Spark-generated CSV files
def move_csv(folder, destination):
    for file in os.listdir(folder):
        if file.startswith("part-") and file.endswith(".csv"):
            shutil.copy2(
                os.path.join(folder, file),
                destination
            )
            break

move_csv(
    tmp1,
    os.path.join(POWERBI_EXPORT_DIR, "session behaviou.csv")
)

move_csv(
    tmp2,
    os.path.join(POWERBI_EXPORT_DIR, "session.csv")
)

print("SESSION CSVs UPDATED")
print("ROWS:", sessions.count())

spark.stop()