#!/usr/bin/env python3
"""
Kafka Event Consumer
Consumes events from Kafka topic and processes them.
"""

import json
import logging
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'processed-events'
CONSUMER_GROUP = 'event-consumer-group'


class EventConsumer:
    """Kafka consumer for processing events."""
    
    def __init__(self, bootstrap_servers: str, topic: str, group_id: str):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            auto_commit_interval_ms=5000,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None
        )
        self.topic = topic
        logger.info(f"Consumer connected to {bootstrap_servers}, topic: {topic}")
    
    def process_event(self, event: dict) -> dict:
        """Process a single event."""
        # Add processing metadata
        event['processed_at'] = datetime.utcnow().isoformat()
        event['consumer_group'] = CONSUMER_GROUP
        
        # Example: categorize by event type
        event_type = event.get('event_type', 'unknown')
        
        if event_type == 'purchase':
            amount = event.get('properties', {}).get('amount', 0)
            if amount > 100:
                event['priority'] = 'high'
                logger.info(f"High-value purchase detected: ${amount}")
            else:
                event['priority'] = 'normal'
        elif event_type in ['login', 'logout']:
            event['priority'] = 'low'
        else:
            event['priority'] = 'normal'
        
        return event
    
    def run(self):
        """Continuously consume and process events."""
        logger.info(f"Starting consumer for topic: {self.topic}")
        
        try:
            for message in self.consumer:
                logger.info(
                    f"Received: Partition={message.partition}, "
                    f"Offset={message.offset}, Key={message.key}"
                )
                
                event = message.value
                processed_event = self.process_event(event)
                
                logger.info(
                    f"Processed: {processed_event.get('event_type')} | "
                    f"User: {processed_event.get('user_id')} | "
                    f"Priority: {processed_event.get('priority')}"
                )
                
        except KeyboardInterrupt:
            logger.info("Consumer stopped by user")
        finally:
            self.consumer.close()
            logger.info("Consumer closed")


def main():
    consumer = EventConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        topic=TOPIC_NAME,
        group_id=CONSUMER_GROUP
    )
    consumer.run()


if __name__ == '__main__':
    main()
