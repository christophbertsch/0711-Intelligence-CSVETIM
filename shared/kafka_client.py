"""
Kafka client utilities for the CSV Import Guardian Agent System.
"""

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Callable, Dict, List, Optional

import aiokafka
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaError
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

from shared.config import get_kafka_config, get_settings
from shared.events import BaseEvent, deserialize_event, EVENT_TYPE_MAP, KAFKA_TOPICS

logger = logging.getLogger(__name__)


class KafkaManager:
    """Kafka connection and topic management."""
    
    def __init__(self):
        self.settings = get_settings()
        self.kafka_config = get_kafka_config()
        self.producer: Optional[AIOKafkaProducer] = None
        self.admin_client: Optional[KafkaAdminClient] = None
        
    async def initialize(self):
        """Initialize Kafka connections and create topics."""
        # Create admin client for topic management
        self.admin_client = KafkaAdminClient(
            bootstrap_servers=self.settings.kafka_bootstrap_servers,
            security_protocol=self.settings.kafka_security_protocol,
        )
        
        # Create topics if they don't exist
        await self.create_topics()
        
        # Initialize producer
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.settings.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            compression_type='gzip',
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=1,
            enable_idempotence=True,
        )
        await self.producer.start()
        
    async def create_topics(self):
        """Create Kafka topics with proper configuration."""
        try:
            existing_topics = self.admin_client.list_topics()
            topics_to_create = []
            
            for topic_name, config in KAFKA_TOPICS.items():
                if topic_name not in existing_topics:
                    topic = NewTopic(
                        name=topic_name,
                        num_partitions=config["partitions"],
                        replication_factor=config["replication_factor"],
                        topic_configs=config.get("config", {})
                    )
                    topics_to_create.append(topic)
            
            if topics_to_create:
                logger.info(f"Creating {len(topics_to_create)} Kafka topics")
                self.admin_client.create_topics(topics_to_create)
                logger.info("Topics created successfully")
            else:
                logger.info("All topics already exist")
                
        except Exception as e:
            logger.error(f"Error creating topics: {e}")
            raise
    
    async def close(self):
        """Close Kafka connections."""
        if self.producer:
            await self.producer.stop()
        if self.admin_client:
            self.admin_client.close()
    
    async def publish_event(
        self,
        topic: str,
        event: BaseEvent,
        key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None
    ):
        """Publish an event to a Kafka topic."""
        if not self.producer:
            raise RuntimeError("Kafka producer not initialized")
        
        try:
            # Convert event to dict
            event_data = event.dict()
            
            # Add headers
            kafka_headers = []
            if headers:
                for k, v in headers.items():
                    kafka_headers.append((k, v.encode('utf-8')))
            
            # Add standard headers
            kafka_headers.extend([
                ("event_type", event.event_type.encode('utf-8')),
                ("client_id", event.client_id.encode('utf-8')),
                ("trace_id", event.trace_id.encode('utf-8')),
            ])
            
            # Use client_id:ingest_id as default key for partitioning
            if not key:
                key = f"{event.client_id}:{event.ingest_id}"
            
            # Send message
            await self.producer.send_and_wait(
                topic=topic,
                value=event_data,
                key=key,
                headers=kafka_headers,
            )
            
            logger.debug(f"Published event {event.event_type} to topic {topic}")
            
        except Exception as e:
            logger.error(f"Error publishing event to {topic}: {e}")
            raise
    
    async def get_consumer(
        self,
        topics: List[str],
        group_id: str,
        auto_offset_reset: str = "earliest"
    ) -> AIOKafkaConsumer:
        """Create a Kafka consumer for specified topics."""
        consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=self.settings.kafka_bootstrap_servers,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            consumer_timeout_ms=1000,
            max_poll_records=100,
        )
        
        await consumer.start()
        return consumer


# Global Kafka manager instance
_kafka_manager: Optional[KafkaManager] = None


async def get_kafka_manager() -> KafkaManager:
    """Get the global Kafka manager instance."""
    global _kafka_manager
    
    if _kafka_manager is None:
        _kafka_manager = KafkaManager()
        await _kafka_manager.initialize()
    
    return _kafka_manager


class EventPublisher:
    """High-level event publishing interface."""
    
    def __init__(self, kafka_manager: KafkaManager):
        self.kafka_manager = kafka_manager
    
    async def publish(
        self,
        event: BaseEvent,
        key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None
    ):
        """Publish an event to the appropriate topic."""
        topic = event.event_type
        await self.kafka_manager.publish_event(topic, event, key, headers)
    
    async def publish_batch(self, events: List[BaseEvent]):
        """Publish multiple events efficiently."""
        for event in events:
            await self.publish(event)


class EventConsumer:
    """High-level event consumption interface."""
    
    def __init__(
        self,
        topics: List[str],
        group_id: str,
        handler: Callable[[BaseEvent], Any],
        kafka_manager: Optional[KafkaManager] = None
    ):
        self.topics = topics
        self.group_id = group_id
        self.handler = handler
        self.kafka_manager = kafka_manager
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.running = False
    
    async def start(self):
        """Start consuming events."""
        if not self.kafka_manager:
            self.kafka_manager = await get_kafka_manager()
        
        self.consumer = await self.kafka_manager.get_consumer(
            self.topics,
            self.group_id
        )
        
        self.running = True
        logger.info(f"Started consumer {self.group_id} for topics {self.topics}")
        
        try:
            async for message in self.consumer:
                if not self.running:
                    break
                
                try:
                    # Deserialize event
                    event_data = message.value
                    event = deserialize_event(event_data)
                    
                    # Process event
                    await self.handler(event)
                    
                    # Commit offset
                    await self.consumer.commit()
                    
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    # Could implement dead letter queue here
                    
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            await self.stop()
    
    async def stop(self):
        """Stop consuming events."""
        self.running = False
        if self.consumer:
            await self.consumer.stop()
        logger.info(f"Stopped consumer {self.group_id}")


@asynccontextmanager
async def kafka_transaction(kafka_manager: KafkaManager):
    """Context manager for Kafka transactions."""
    producer = kafka_manager.producer
    if not producer:
        raise RuntimeError("Kafka producer not initialized")
    
    transaction = producer.begin_transaction()
    try:
        yield producer
        await producer.commit_transaction()
    except Exception:
        await producer.abort_transaction()
        raise


# Health check functions

async def check_kafka_health() -> dict:
    """Check Kafka connectivity and topic health."""
    try:
        kafka_manager = await get_kafka_manager()
        
        # Test producer
        test_event = BaseEvent(
            event_id="health-check",
            event_type="health.check",
            trace_id="health-check",
            client_id="system",
            ingest_id="health-check",
            source_agent="health-check",
            idempotency_key="health-check"
        )
        
        # Try to send to a test topic (don't actually send)
        producer_healthy = kafka_manager.producer is not None
        
        # Check topic existence
        admin_client = kafka_manager.admin_client
        topics = admin_client.list_topics() if admin_client else []
        
        return {
            "status": "healthy",
            "producer": "ok" if producer_healthy else "error",
            "topics_count": len(topics),
            "topics": list(topics)[:10],  # First 10 topics
        }
        
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
        }


# Utility functions

def get_partition_key(client_id: str, ingest_id: str) -> str:
    """Generate a partition key for consistent message routing."""
    return f"{client_id}:{ingest_id}"


def extract_headers(message) -> Dict[str, str]:
    """Extract headers from a Kafka message."""
    headers = {}
    if message.headers:
        for key, value in message.headers:
            headers[key] = value.decode('utf-8')
    return headers


async def wait_for_kafka_ready(timeout: int = 30):
    """Wait for Kafka to be ready."""
    start_time = asyncio.get_event_loop().time()
    
    while True:
        try:
            health = await check_kafka_health()
            if health["status"] == "healthy":
                logger.info("Kafka is ready")
                return
        except Exception as e:
            logger.debug(f"Kafka not ready: {e}")
        
        if asyncio.get_event_loop().time() - start_time > timeout:
            raise TimeoutError(f"Kafka not ready after {timeout} seconds")
        
        await asyncio.sleep(1)


# Message retry and dead letter queue

class RetryableEventHandler:
    """Event handler with retry logic and dead letter queue."""
    
    def __init__(
        self,
        handler: Callable[[BaseEvent], Any],
        max_retries: int = 3,
        retry_delay: float = 1.0,
        dlq_topic: Optional[str] = None
    ):
        self.handler = handler
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.dlq_topic = dlq_topic
    
    async def __call__(self, event: BaseEvent) -> Any:
        """Handle event with retry logic."""
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                return await self.handler(event)
            except Exception as e:
                last_exception = e
                logger.warning(f"Event handling failed (attempt {attempt + 1}): {e}")
                
                if attempt < self.max_retries:
                    await asyncio.sleep(self.retry_delay * (2 ** attempt))
                else:
                    # Send to dead letter queue if configured
                    if self.dlq_topic:
                        await self._send_to_dlq(event, str(e))
                    
                    logger.error(f"Event handling failed after {self.max_retries + 1} attempts")
                    raise last_exception
    
    async def _send_to_dlq(self, event: BaseEvent, error: str):
        """Send failed event to dead letter queue."""
        try:
            kafka_manager = await get_kafka_manager()
            
            # Add error information to event
            dlq_event = event.copy()
            dlq_event.event_type = f"dlq.{event.event_type}"
            
            await kafka_manager.publish_event(
                self.dlq_topic,
                dlq_event,
                headers={"error": error, "original_event_type": event.event_type}
            )
            
        except Exception as e:
            logger.error(f"Failed to send event to DLQ: {e}")


# Monitoring and metrics

class KafkaMetrics:
    """Kafka metrics collector."""
    
    def __init__(self, kafka_manager: KafkaManager):
        self.kafka_manager = kafka_manager
    
    async def get_producer_metrics(self) -> Dict[str, Any]:
        """Get producer metrics."""
        if not self.kafka_manager.producer:
            return {}
        
        # Note: aiokafka doesn't expose detailed metrics like Java client
        # This is a placeholder for custom metrics collection
        return {
            "messages_sent": 0,  # Would need custom counter
            "bytes_sent": 0,     # Would need custom counter
            "errors": 0,         # Would need custom counter
        }
    
    async def get_consumer_lag(self, group_id: str) -> Dict[str, int]:
        """Get consumer lag for a group."""
        # This would require additional Kafka admin operations
        # Placeholder implementation
        return {}
    
    async def get_topic_metrics(self) -> Dict[str, Any]:
        """Get topic-level metrics."""
        try:
            admin_client = self.kafka_manager.admin_client
            if not admin_client:
                return {}
            
            topics = admin_client.list_topics()
            return {
                "topic_count": len(topics),
                "topics": list(topics),
            }
            
        except Exception as e:
            logger.error(f"Error getting topic metrics: {e}")
            return {}