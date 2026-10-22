"""
Event Simulator: Generates synthetic clickstream events from Retail Rocket schema.
Produces to Kafka topic with realistic temporal patterns.
"""
import json
import time
import random
import logging
from datetime import datetime
from typing import Iterator, Dict, Any
from kafka import KafkaProducer
from src.config import config


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ClickstreamEventGenerator:
    """Generates realistic clickstream events."""

    def __init__(self):
        self.event_types = ["view", "addtocart", "transaction"]
        self.item_ids = list(range(1000, 10000))
        self.visitor_ids = list(range(1, 10001))
        self.conversion_rate = 0.05  # 5% conversion
        self.cart_add_rate = 0.20    # 20% add to cart

    def generate_events(self, num_events: int) -> Iterator[Dict[str, Any]]:
        """Generate num_events realistic clickstream events."""
        base_timestamp = int(datetime.now().timestamp() * 1000)

        for i in range(num_events):
            visitor_id = random.choice(self.visitor_ids)
            item_id = random.choice(self.item_ids)
            event_type = self._decide_event_type()
            timestamp = base_timestamp + (i * 100)  # 100ms apart

            transaction_id = None
            if event_type == "transaction":
                transaction_id = random.randint(100000, 999999)

            event = {
                "timestamp": timestamp,
                "visitorid": visitor_id,
                "event": event_type,
                "itemid": item_id,
                "transactionid": transaction_id,
            }
            yield event

    def _decide_event_type(self) -> str:
        """Decide event type based on probabilities."""
        rand = random.random()
        if rand < self.conversion_rate:
            return "transaction"
        elif rand < self.conversion_rate + self.cart_add_rate:
            return "addtocart"
        else:
            return "view"


class KafkaEventProducer:
    """Produces events to Kafka topic."""

    def __init__(self, kafka_config=None):
        self.kafka_config = kafka_config or config.kafka
        self.generator = ClickstreamEventGenerator()
        self.producer = None
        self._initialize_producer()

    def _initialize_producer(self):
        """Initialize Kafka producer."""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_config.bootstrap_servers.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
            )
            logger.info(f"Kafka producer initialized: {self.kafka_config.bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            raise

    def produce_events(self, rate_per_sec: int = 10, duration_sec: int = 60):
        """
        Produce events at specified rate for duration_sec.
        rate_per_sec: events per second
        duration_sec: run duration
        """
        events_per_batch = max(1, rate_per_sec)
        interval = 1.0 / rate_per_sec if rate_per_sec > 0 else 0.1

        start_time = time.time()
        event_count = 0

        try:
            logger.info(f"Starting event production: {rate_per_sec} events/sec for {duration_sec}s")
            for event in self.generator.generate_events(num_events=rate_per_sec * duration_sec):
                if time.time() - start_time > duration_sec:
                    break

                self.producer.send(
                    self.kafka_config.topic_events,
                    value=event,
                    key=str(event["visitorid"]).encode("utf-8"),
                )
                event_count += 1

                if event_count % events_per_batch == 0:
                    logger.info(f"Produced {event_count} events...")
                    time.sleep(interval * events_per_batch)

            self.producer.flush()
            elapsed = time.time() - start_time
            logger.info(f"✓ Produced {event_count} events in {elapsed:.2f}s")

        except Exception as e:
            logger.error(f"Error during event production: {e}")
            raise
        finally:
            if self.producer:
                self.producer.close()

    def produce_events_continuous(self, rate_per_sec: int = 10):
        """Produce events continuously until interrupted."""
        interval = 1.0 / rate_per_sec if rate_per_sec > 0 else 0.1
        event_count = 0

        try:
            logger.info(f"Starting continuous event production: {rate_per_sec} events/sec")
            while True:
                for event in self.generator.generate_events(num_events=rate_per_sec):
                    self.producer.send(
                        self.kafka_config.topic_events,
                        value=event,
                        key=str(event["visitorid"]).encode("utf-8"),
                    )
                    event_count += 1

                if event_count % (rate_per_sec * 10) == 0:
                    logger.info(f"Produced {event_count} events...")

                time.sleep(interval * rate_per_sec)

        except KeyboardInterrupt:
            logger.info("Production stopped by user")
        except Exception as e:
            logger.error(f"Error during continuous production: {e}")
            raise
        finally:
            if self.producer:
                self.producer.close()


if __name__ == "__main__":
    import sys

    rate = int(sys.argv[1]) if len(sys.argv) > 1 else config.simulator.event_rate_per_sec
    duration = int(sys.argv[2]) if len(sys.argv) > 2 else 300

    producer = KafkaEventProducer()
    producer.produce_events(rate_per_sec=rate, duration_sec=duration)
