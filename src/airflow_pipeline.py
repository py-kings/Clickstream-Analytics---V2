from src.spark_utils import build_spark_session
from src.bronze_layer import BronzeLayer
from src.silver_layer import SilverLayer


def main():
    """Bounded Bronze -> Silver run used by the Airflow DAG.

    Uses earliest Kafka offsets and Delta version 0 so the first run processes
    everything produced so far. Later runs resume from their checkpoints.
    Do not run the continuous Bronze/Silver streams against the same tables.
    """
    spark = build_spark_session('AirflowBoundedPipeline')

    try:
        print('=== AIRFLOW: BRONZE ===')
        bronze = BronzeLayer(spark, starting_offsets='earliest')
        bronze_query = bronze.run(checkpoint_id='airflow_bronze')
        bronze_query.processAllAvailable()
        bronze_query.stop()
        print('✓ Bronze bounded processing completed')

        print('=== AIRFLOW: SILVER ===')
        silver = SilverLayer(spark, starting_version='0')
        silver_query = silver.run(checkpoint_id='airflow_silver')
        silver_query.processAllAvailable()
        silver_query.stop()
        print('✓ Silver bounded processing completed')

        print('=== AIRFLOW: BRONZE → SILVER COMPLETE ===')

    finally:
        spark.stop()


if __name__ == '__main__':
    main()
