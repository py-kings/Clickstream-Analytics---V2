.PHONY: help setup install clean test lint format run-bronze run-silver run-gold run-forecast simulate docker-up docker-down docs airflow-init airflow-scheduler airflow-webserver list-tables check-data

VENV_DIR := venv
LOCAL_DELTA_BASE ?= ./delta-lake
PYTHON := $(VENV_DIR)/bin/python
PIP := $(VENV_DIR)/bin/pip

help:
	@echo "Clickstream Analytics v2 - Development Tasks"
	@echo ""
	@echo "Setup:"
	@echo "  make setup           - Create venv and install dependencies"
	@echo "  make install         - Install/upgrade dependencies"
	@echo "  make clean           - Remove venv, cache, artifacts"
	@echo ""
	@echo "Docker:"
	@echo "  make docker-up       - Start Kafka, Zookeeper, MLflow"
	@echo "  make docker-down     - Stop all Docker containers"
	@echo ""
	@echo "Development:"
	@echo "  make lint            - Run flake8 linter"
	@echo "  make format          - Format code (black)"
	@echo "  make test            - Run pytest"
	@echo "  make test-cov        - Run pytest with coverage"
	@echo ""
	@echo "Pipeline:"
	@echo "  make run-bronze      - Start Bronze layer (streaming)"
	@echo "  make run-silver      - Start Silver layer (streaming)"
	@echo "  make run-gold        - Run Gold layer (batch)"
	@echo "  make run-forecast    - Run forecasting"
	@echo "  make simulate        - Run event simulator (50 events/sec, 5 min)"
	@echo ""
	@echo "Documentation:"
	@echo "  make docs            - Open README, SETUP, ARCHITECTURE"
	@echo ""

setup:
	@echo "Setting up Python virtual environment..."
	python -m venv $(VENV_DIR)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	@echo "✓ Setup complete. Activate with: source venv/bin/activate"

install:
	$(PIP) install --upgrade -r requirements.txt

clean:
	@echo "Cleaning up..."
	rm -rf $(VENV_DIR)
	rm -rf __pycache__ .pytest_cache .coverage htmlcov
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	rm -rf dist/ build/ *.egg-info
	@echo "✓ Cleanup complete"

docker-up:
	@echo "Starting Docker services..."
	docker-compose up -d
	@echo ""
	@echo "Services starting:"
	@echo "  Kafka:   localhost:9092"
	@echo "  MLflow:  http://localhost:5000"
	@echo ""
	@docker ps --format "table {{.Names}}\t{{.Ports}}\t{{.Status}}"

docker-down:
	@echo "Stopping Docker services..."
	docker-compose down
	@echo "✓ Services stopped"

docker-logs:
	docker-compose logs -f

lint:
	@echo "Running flake8..."
	$(PIP) install -q flake8
	$(PYTHON) -m flake8 src/ tests/ --max-line-length=120 --show-source

format:
	@echo "Running black formatter..."
	$(PIP) install -q black
	$(PYTHON) -m black src/ tests/ --line-length=120

test:
	@echo "Running pytest..."
	$(PYTHON) -m pytest tests/ -v

test-cov:
	@echo "Running pytest with coverage..."
	$(PYTHON) -m pytest tests/ -v --cov=src --cov-report=html --cov-report=term
	@echo "Coverage report: htmlcov/index.html"

run-bronze:
	@echo "Starting Bronze layer..."
	$(PYTHON) -m src.bronze_layer

run-silver:
	@echo "Starting Silver layer..."
	$(PYTHON) -m src.silver_layer

run-gold:
	@echo "Running Gold layer aggregations..."
	$(PYTHON) -m src.gold_layer

run-forecast:
	@echo "Running forecasting pipeline..."
	$(PYTHON) -m src.forecasting

simulate:
	@echo "Simulating events: 50 events/sec for 300s..."
	$(PYTHON) -m src.simulator 50 300

docs:
	@echo "Opening documentation..."
	@command -v open >/dev/null 2>&1 && open README.md || xdg-open README.md || notepad README.md
	@echo "Files available:"
	@echo "  README.md       - Overview & quick start"
	@echo "  SETUP.md        - Detailed setup guide"
	@echo "  ARCHITECTURE.md - Design & implementation details"

list-tables:
	@echo "Delta Lake tables available:"
	@find $(LOCAL_DELTA_BASE) -type d -name '_delta_log' -prune -o -type d -maxdepth 3 -print | grep -v '_delta_log' | sort

check-data:
	@echo "Checking data in each layer..."
	@$(PYTHON) -c "from pyspark.sql import SparkSession; \
		spark = SparkSession.builder.appName('check').master('local[1]').getOrCreate(); \
		for layer in ['bronze', 'silver', 'gold']: \
			try: \
				df = spark.read.format('delta').load(f'$(LOCAL_DELTA_BASE)/{layer}'); \
				print(f'✓ {layer}: {df.count()} rows'); \
			except: \
				print(f'✗ {layer}: no data'); \
		spark.stop()"

airflow-init:
	airflow db init
	@echo "Creating admin user with a generated password (printed once by Airflow)."
	airflow users create --username admin --use-random-password --firstname Admin --lastname User --role Admin --email admin@example.com

airflow-scheduler:
	airflow scheduler

airflow-webserver:
	airflow webserver --port 8080

version:
	@echo "Component versions:"
	$(PYTHON) -c "import pyspark; print(f'  Spark: {pyspark.__version__}')"
	$(PYTHON) -c "import kafka; print(f'  Kafka-Python: {kafka.__version__}')"
	$(PYTHON) -c "import pandas; print(f'  Pandas: {pandas.__version__}')"
	$(PYTHON) -c "import prophet; print(f'  Prophet: {prophet.__version__}')"
	$(PYTHON) -c "import mlflow; print(f'  MLflow: {mlflow.__version__}')"
	@docker --version 2>/dev/null || echo "  Docker: not installed"
	@docker-compose --version 2>/dev/null || echo "  Docker Compose: not installed"
