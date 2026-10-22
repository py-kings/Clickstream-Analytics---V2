"""
Forecasting module with MLflow tracking.
Time-series prediction on daily funnel metrics using Prophet.
"""
import logging
import io
import mlflow
import mlflow.sklearn
import pandas as pd
from pyspark.sql import SparkSession
from prophet import Prophet
from src.config import config


logger = logging.getLogger(__name__)


def summarize_forecast(forecast: pd.DataFrame, history_len: int) -> tuple:
    """Return (mean, std) of predicted yhat over the future rows.

    Rows with index >= history_len are the forecast horizon. The std is the
    spread of predictions across that horizon (0.0 when only one row).
    """
    future_yhat = forecast.loc[forecast.index >= history_len, "yhat"]
    if future_yhat.empty:
        raise ValueError("forecast contains no rows beyond the training history")
    pred_mean = float(future_yhat.mean())
    pred_std = float(future_yhat.std()) if len(future_yhat) > 1 else 0.0
    return pred_mean, pred_std


class ForecastingPipeline:
    """Forecast daily viewers/conversions using Prophet + MLflow."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.forecast_days = config.forecast.forecast_days
        self.mlflow_run = None

    def read_gold_daily_funnel(self) -> pd.DataFrame:
        """Read daily funnel from Gold layer."""
        try:
            df_spark = self.spark.read.format("delta").load(
                f"{config.delta.gold_path}/daily_funnel"
            )
            df = df_spark.toPandas()
            logger.info(f"Loaded {len(df)} days of historical data")
            return df
        except Exception as e:
            logger.error(f"Failed to read Gold daily_funnel: {e}")
            raise

    def prepare_prophet_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert to Prophet format (ds, y)."""
        prophet_df = (
            df
            .rename(columns={"event_date": "ds", "viewers": "y"})
            [["ds", "y"]]
            .sort_values("ds")
        )
        prophet_df["ds"] = pd.to_datetime(prophet_df["ds"])
        logger.info(f"Prepared {len(prophet_df)} rows for Prophet")
        return prophet_df

    def train_forecast_model(self, df: pd.DataFrame):
        """Train Prophet model and log to MLflow."""
        with mlflow.start_run(run_name="prophet_forecast") as run:
            self.mlflow_run = run

            logger.info("Training Prophet model...")
            model = Prophet(
                weekly_seasonality=True,
                daily_seasonality=False,
                yearly_seasonality=False,
                interval_width=0.95,
            )
            model.fit(df)

            # Log parameters
            mlflow.log_param("forecast_days", self.forecast_days)
            mlflow.log_param("weekly_seasonality", True)
            mlflow.log_param("yearly_seasonality", False)

            # Generate forecast
            future = model.make_future_dataframe(periods=self.forecast_days)
            forecast = model.predict(future)

            # Extract prediction metrics over the forecast horizon
            last_actual = df["y"].iloc[-1]
            pred_mean, pred_std = summarize_forecast(forecast, len(df))

            mlflow.log_metric("last_actual_viewers", float(last_actual))
            mlflow.log_metric("forecast_mean", pred_mean)
            mlflow.log_metric("forecast_std", pred_std)

            logger.info(f"✓ Model trained. Forecasted {self.forecast_days} days")
            return model, forecast

    def generate_forecast_df(self, model, forecast: pd.DataFrame) -> pd.DataFrame:
        """Extract forecast results."""
        result = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].tail(self.forecast_days)
        result = result.rename(columns={
            "ds": "date",
            "yhat": "predicted_viewers",
            "yhat_lower": "viewers_lower_bound",
            "yhat_upper": "viewers_upper_bound",
        })
        logger.info(f"Generated forecast dataframe: {len(result)} rows")
        return result

    def save_forecast_to_s3(self, df: pd.DataFrame, bucket: str, key: str):
        """Save forecast to S3."""
        try:
            import boto3
            s3 = boto3.client(
                "s3",
                aws_access_key_id=config.aws.access_key_id,
                aws_secret_access_key=config.aws.secret_access_key,
                region_name=config.aws.region,
            )

            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)

            s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
            logger.info(f"✓ Forecast saved to s3://{bucket}/{key}")

            mlflow.log_param("forecast_s3_path", f"s3://{bucket}/{key}")
        except Exception as e:
            logger.error(f"Failed to save forecast to S3: {e}")
            # Don't fail the entire pipeline if S3 fails

    def run(self) -> pd.DataFrame:
        """Run full forecasting pipeline."""
        logger.info("Starting forecasting pipeline...")

        # Read historical data
        df_historical = self.read_gold_daily_funnel()
        if len(df_historical) < 14:
            logger.warning(f"⚠ Only {len(df_historical)} days of data; Prophet needs ≥14 days")
            return None

        # Prepare data
        prophet_df = self.prepare_prophet_data(df_historical)

        # Train and forecast
        model, forecast_raw = self.train_forecast_model(prophet_df)

        # Extract results
        forecast_df = self.generate_forecast_df(model, forecast_raw)

        # Save to S3 (optional)
        if config.aws.access_key_id:
            s3_key = "predicted/viewers_forecast/viewers_forecast.parquet"
            self.save_forecast_to_s3(forecast_df, config.aws.bucket, s3_key)

        logger.info("✓ Forecasting pipeline complete")
        return forecast_df


def main():
    from src.spark_utils import build_spark_session

    # Setup MLflow
    mlflow.set_experiment("clickstream_forecasting")

    spark = build_spark_session("Forecasting")
    pipeline = ForecastingPipeline(spark)
    forecast_df = pipeline.run()

    if forecast_df is not None:
        print("\n📊 Forecast Results:")
        print(forecast_df.to_string(index=False))

    spark.stop()


if __name__ == "__main__":
    main()
