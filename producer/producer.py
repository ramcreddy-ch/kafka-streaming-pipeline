#!/usr/bin/env python3
"""
Kafka Event Producer
Generates real-time events and publishes to Kafka topic.
"""

import json
import time
import random
import logging
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'raw-events'


class EventProducer:
    """Kafka producer for generating and sending events."""
    
    def __init__(self, bootstrap_servers: str):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3,
            retry_backoff_ms=1000
        )
        logger.info(f"Producer connected to {bootstrap_servers}")
    
    def generate_event(self) -> dict:
        """Generate a sample event."""
        event_types = ['page_view', 'click', 'purchase', 'login', 'logout']
        users = ['user_001', 'user_002', 'user_003', 'user_004', 'user_005']
        pages = ['/home', '/products', '/cart', '/checkout', '/profile']
        
        event = {
            'event_id': f"evt_{int(time.time() * 1000)}_{random.randint(1000, 9999)}",
            'event_type': random.choice(event_types),
            'user_id': random.choice(users),
            'page': random.choice(pages),
            'timestamp': datetime.utcnow().isoformat(),
            'properties': {
                'session_id': f"session_{random.randint(100, 999)}",
                'device': random.choice(['mobile', 'desktop', 'tablet']),
                'browser': random.choice(['Chrome', 'Firefox', 'Safari']),
                'country': random.choice(['US', 'UK', 'DE', 'FR', 'IN'])
            }
        }
        
        # Add purchase-specific data
        if event['event_type'] == 'purchase':
            event['properties']['amount'] = round(random.uniform(10.0, 500.0), 2)
            event['properties']['currency'] = 'USD'
            event['properties']['product_id'] = f"prod_{random.randint(100, 999)}"
        
        return event
    
    def send_event(self, topic: str, event: dict, key: str = None):
        """Send event to Kafka topic."""
        try:
            future = self.producer.send(topic, key=key, value=event)
            record_metadata = future.get(timeout=10)
            logger.info(
                f"Sent: {event['event_type']} | "
                f"Topic: {record_metadata.topic} | "
                f"Partition: {record_metadata.partition} | "
                f"Offset: {record_metadata.offset}"
            )
        except KafkaError as e:
            logger.error(f"Failed to send event: {e}")
    
    def run(self, events_per_second: float = 1.0):
        """Continuously produce events."""
        logger.info(f"Starting producer - {events_per_second} events/sec")
        
        try:
            while True:
                event = self.generate_event()
                self.send_event(TOPIC_NAME, event, key=event['user_id'])
                time.sleep(1.0 / events_per_second)
        except KeyboardInterrupt:
            logger.info("Producer stopped by user")
        finally:
            self.producer.flush()
            self.producer.close()
            logger.info("Producer closed")


def main():
    producer = EventProducer(KAFKA_BOOTSTRAP_SERVERS)
    producer.run(events_per_second=2.0)


if __name__ == '__main__':
    main()
