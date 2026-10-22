"""
Tests for src.forecasting.

Prophet needs a CmdStan install that is not available in every environment,
so these tests substitute a deterministic stub for the Prophet class. The
production methods (summarize_forecast, train_forecast_model) run unchanged.
"""
import mlflow
import numpy as np
import pandas as pd
import pytest

import src.forecasting as forecasting
from src.forecasting import ForecastingPipeline, summarize_forecast


class FakeProphet:
    """Deterministic stand-in for prophet.Prophet."""

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def fit(self, df):
        self.history = df
        return self

    def make_future_dataframe(self, periods):
        last = self.history["ds"].max()
        future = pd.date_range(last + pd.Timedelta(days=1), periods=periods, freq="D")
        return pd.DataFrame({"ds": list(self.history["ds"]) + list(future)})

    def predict(self, future):
        n_future = len(future) - len(self.history)
        yhat = np.concatenate([
            self.history["y"].values.astype(float),
            np.linspace(100.0, 200.0, n_future),
        ])
        return pd.DataFrame({
            "ds": future["ds"],
            "yhat": yhat,
            "yhat_lower": yhat * 0.9,
            "yhat_upper": yhat * 1.1,
        })


def test_summarize_forecast_uses_only_future_rows():
    forecast = pd.DataFrame({"yhat": [100.0] * 10 + [1.0, 2.0, 3.0, 4.0, 5.0]})
    mean, std = summarize_forecast(forecast, history_len=10)

    assert mean == pytest.approx(3.0)
    assert std == pytest.approx(np.std([1, 2, 3, 4, 5], ddof=1))


def test_summarize_forecast_single_future_row_has_zero_std():
    forecast = pd.DataFrame({"yhat": [5.0, 7.0]})
    mean, std = summarize_forecast(forecast, history_len=1)

    assert mean == 7.0
    assert std == 0.0


def test_summarize_forecast_without_future_rows_raises():
    forecast = pd.DataFrame({"yhat": [5.0, 7.0]})
    with pytest.raises(ValueError):
        summarize_forecast(forecast, history_len=2)


def test_train_forecast_model_logs_metrics_without_crashing(monkeypatch, tmp_path):
    """Regression test: train_forecast_model used to raise TypeError on len(float)."""
    monkeypatch.setenv("MLFLOW_TRACKING_URI", (tmp_path / "mlruns").as_uri())
    monkeypatch.setattr(forecasting, "Prophet", FakeProphet)

    history = pd.DataFrame({
        "ds": pd.date_range("2026-01-01", periods=30, freq="D"),
        "y": np.arange(100, 130, dtype=float),
    })

    pipeline = ForecastingPipeline(spark=None)
    model, forecast = pipeline.train_forecast_model(history)

    client = mlflow.tracking.MlflowClient()
    metrics = client.get_run(pipeline.mlflow_run.info.run_id).data.metrics
    assert {"last_actual_viewers", "forecast_mean", "forecast_std"} <= set(metrics)
    assert metrics["last_actual_viewers"] == 129.0
    assert metrics["forecast_std"] > 0
