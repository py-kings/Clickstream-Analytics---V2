# STATIC DEMO ONLY: the metrics and pipeline statuses below are hardcoded.
# They are NOT read from Delta, Gold, or Airflow. Do not treat them as health checks.
from flask import Flask, render_template

app = Flask(__name__)

metrics = {
    "events": "8,164",
    "sessions": "6,849",
    "transactions": "420",
    "conversion": "6.8%",
}

pipeline = [
    ("Kafka", "Healthy"),
    ("Bronze", "Completed"),
    ("Silver", "Completed"),
    ("Gold", "Completed"),
]


@app.route("/")
def dashboard():
    return render_template(
        "index.html",
        metrics=metrics,
        pipeline=pipeline,
    )


@app.route("/architecture")
def architecture():
    return render_template("architecture.html")


@app.route("/powerbi")
def powerbi():
    return render_template("powerbi.html")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=False)
