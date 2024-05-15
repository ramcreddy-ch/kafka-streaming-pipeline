#!/usr/bin/env python3
"""
Kafka Stream Processor
Real-time stream processing: transforms, filters, and routes events.
"""

import json
import logging
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from collections import defaultdict
import threading
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
INPUT_TOPIC = 'raw-events'
OUTPUT_TOPIC = 'processed-events'
ALERT_TOPIC = 'alerts'
CONSUMER_GROUP = 'stream-processor-group'


class StreamProcessor:
    """Real-time stream processor for Kafka events."""
    
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        
        # Consumer for input topic
        self.consumer = KafkaConsumer(
            INPUT_TOPIC,
            bootstrap_servers=bootstrap_servers,
            group_id=CONSUMER_GROUP,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None
        )
        
        # Producer for output topics
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all'
        )
        
        # In-memory state for aggregations
        self.event_counts = defaultdict(int)
        self.user_activity = defaultdict(list)
        
        logger.info(f"Stream processor initialized")
    
    def transform(self, event: dict) -> dict:
        """Apply transformations to the event."""
        transformed = event.copy()
        
        # Add processing metadata
        transformed['processed_timestamp'] = datetime.utcnow().isoformat()
        transformed['processor_version'] = '1.0.0'
        
        # Enrich with derived fields
        event_type = event.get('event_type', 'unknown')
        transformed['event_category'] = self._categorize_event(event_type)
        
        # Calculate session duration for activity events
        if 'properties' in transformed:
            props = transformed['properties']
            if 'session_id' in props:
                transformed['session_hash'] = hash(props['session_id']) % 1000
        
        return transformed
    
    def _categorize_event(self, event_type: str) -> str:
        """Categorize event types."""
        categories = {
            'page_view': 'engagement',
            'click': 'engagement',
            'purchase': 'transaction',
            'login': 'authentication',
            'logout': 'authentication'
        }
        return categories.get(event_type, 'other')
    
    def filter_for_alerts(self, event: dict) -> bool:
        """Determine if event should trigger an alert."""
        # High-value purchases
        if event.get('event_type') == 'purchase':
            amount = event.get('properties', {}).get('amount', 0)
            if amount > 200:
                return True
        
        # Suspicious activity: multiple logins
        user_id = event.get('user_id')
        if user_id and event.get('event_type') == 'login':
            self.user_activity[user_id].append(datetime.utcnow())
            # Check for multiple logins in short period
            recent = [t for t in self.user_activity[user_id] 
                     if (datetime.utcnow() - t).seconds < 60]
            if len(recent) > 3:
                return True
        
        return False
    
    def aggregate(self, event: dict):
        """Update aggregations."""
        event_type = event.get('event_type', 'unknown')
        self.event_counts[event_type] += 1
        
        # Log aggregation stats periodically
        total = sum(self.event_counts.values())
        if total % 10 == 0:
            logger.info(f"Aggregation stats: {dict(self.event_counts)}")
    
    def send_to_topic(self, topic: str, event: dict, key: str = None):
        """Send event to output topic."""
        try:
            future = self.producer.send(topic, key=key, value=event)
            future.get(timeout=10)
        except KafkaError as e:
            logger.error(f"Failed to send to {topic}: {e}")
    
    def process_event(self, event: dict):
        """Main processing logic for each event."""
        # Step 1: Transform
        transformed = self.transform(event)
        
        # Step 2: Aggregate
        self.aggregate(transformed)
        
        # Step 3: Route to output topic
        key = transformed.get('user_id')
        self.send_to_topic(OUTPUT_TOPIC, transformed, key)
        
        # Step 4: Check for alerts
        if self.filter_for_alerts(transformed):
            alert = {
                'alert_type': 'event_alert',
                'original_event': transformed,
                'alert_timestamp': datetime.utcnow().isoformat(),
                'severity': 'high'
            }
            self.send_to_topic(ALERT_TOPIC, alert, key)
            logger.warning(f"Alert triggered for event: {transformed.get('event_id')}")
        
        logger.debug(f"Processed event: {transformed.get('event_id')}")
    
    def run(self):
        """Main processing loop."""
        logger.info(f"Starting stream processor: {INPUT_TOPIC} -> {OUTPUT_TOPIC}")
        
        try:
            for message in self.consumer:
                event = message.value
                logger.info(
                    f"Received: {event.get('event_type')} | "
                    f"User: {event.get('user_id')} | "
                    f"Partition: {message.partition}"
                )
                self.process_event(event)
                
        except KeyboardInterrupt:
            logger.info("Stream processor stopped by user")
        finally:
            self.consumer.close()
            self.producer.flush()
            self.producer.close()
            logger.info("Stream processor closed")


def main():
    processor = StreamProcessor(KAFKA_BOOTSTRAP_SERVERS)
    processor.run()


if __name__ == '__main__':
    main()
