"""
Base agent framework for the CSV Import Guardian Agent System.
"""

import asyncio
import logging
import signal
import sys
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from opentelemetry import trace
try:
    from opentelemetry.exporter.jaeger.thrift import JaegerExporter
except ImportError:
    # Fallback for different OpenTelemetry versions
    try:
        from opentelemetry.exporter.jaeger import JaegerExporter
    except ImportError:
        JaegerExporter = None
try:
    from opentelemetry.instrumentation.logging import LoggingInstrumentor
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    TELEMETRY_AVAILABLE = True
except ImportError:
    LoggingInstrumentor = None
    Resource = None
    TracerProvider = None
    BatchSpanProcessor = None
    TELEMETRY_AVAILABLE = False

from shared.config import get_agent_settings, get_settings
from shared.database import get_async_session
from shared.events import BaseEvent
from shared.kafka_client import EventConsumer, EventPublisher, get_kafka_manager
from shared.storage import get_storage_manager

logger = logging.getLogger(__name__)


class BaseAgent(ABC):
    """Base class for all CSV Import Guardian agents."""
    
    def __init__(self, agent_name: str):
        self.agent_name = agent_name
        self.settings = get_agent_settings(agent_name)
        self.config = self.settings.get_agent_config()
        
        # Core components
        self.kafka_manager = None
        self.event_publisher = None
        self.event_consumer = None
        self.storage_manager = None
        self.tracer = None
        
        # State management
        self.running = False
        self.health_status = "starting"
        self.last_heartbeat = None
        self.processed_events = 0
        self.failed_events = 0
        
        # Setup tracing
        self._setup_tracing()
        
        # Setup signal handlers
        self._setup_signal_handlers()
    
    def _setup_tracing(self):
        """Setup distributed tracing."""
        if not self.settings.enable_tracing or not TELEMETRY_AVAILABLE:
            if not TELEMETRY_AVAILABLE:
                logger.warning("OpenTelemetry not available, tracing disabled")
            return
        
        try:
            # Create tracer provider
            resource = Resource.create({
                "service.name": f"csv-guardian-{self.agent_name}",
                "service.version": "1.0.0",
            })
            
            trace.set_tracer_provider(TracerProvider(resource=resource))
            
            # Setup Jaeger exporter if available
            if JaegerExporter:
                try:
                    jaeger_exporter = JaegerExporter(
                        agent_host_name="localhost",
                        agent_port=6831,
                        collector_endpoint=self.settings.jaeger_endpoint,
                    )
                    
                    span_processor = BatchSpanProcessor(jaeger_exporter)
                    trace.get_tracer_provider().add_span_processor(span_processor)
                except Exception as e:
                    logger.warning(f"Failed to initialize Jaeger tracing: {e}")
            else:
                logger.warning("Jaeger exporter not available, tracing disabled")
            
            # Setup logging instrumentation
            if LoggingInstrumentor:
                LoggingInstrumentor().instrument(set_logging_format=True)
            
            self.tracer = trace.get_tracer(__name__)
            logger.info("Distributed tracing initialized")
            
        except Exception as e:
            logger.warning(f"Failed to setup tracing: {e}")
            self.tracer = trace.get_tracer(__name__)  # Fallback to no-op tracer
    
    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating shutdown...")
            asyncio.create_task(self.shutdown())
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    async def initialize(self):
        """Initialize agent components."""
        logger.info(f"Initializing agent: {self.agent_name}")
        
        try:
            # Initialize Kafka
            self.kafka_manager = await get_kafka_manager()
            self.event_publisher = EventPublisher(self.kafka_manager)
            
            # Initialize storage
            self.storage_manager = await get_storage_manager()
            
            # Setup event consumer
            if self.config["input_topics"]:
                self.event_consumer = EventConsumer(
                    topics=self.config["input_topics"],
                    group_id=self.config["consumer_group_id"],
                    handler=self._handle_event_wrapper,
                    kafka_manager=self.kafka_manager
                )
            
            # Agent-specific initialization
            await self.on_initialize()
            
            self.health_status = "healthy"
            logger.info(f"Agent {self.agent_name} initialized successfully")
            
        except Exception as e:
            self.health_status = "unhealthy"
            logger.error(f"Failed to initialize agent {self.agent_name}: {e}")
            raise
    
    async def start(self):
        """Start the agent."""
        logger.info(f"Starting agent: {self.agent_name}")
        
        try:
            await self.initialize()
            
            self.running = True
            
            # Start event consumer if configured
            if self.event_consumer:
                consumer_task = asyncio.create_task(self.event_consumer.start())
            else:
                consumer_task = None
            
            # Start heartbeat task
            heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            
            # Start agent-specific tasks
            agent_tasks = await self.start_background_tasks()
            
            # Wait for all tasks
            tasks = [heartbeat_task]
            if consumer_task:
                tasks.append(consumer_task)
            if agent_tasks:
                tasks.extend(agent_tasks)
            
            await asyncio.gather(*tasks, return_exceptions=True)
            
        except Exception as e:
            logger.error(f"Error running agent {self.agent_name}: {e}")
            self.health_status = "unhealthy"
            raise
        finally:
            await self.shutdown()
    
    async def shutdown(self):
        """Shutdown the agent gracefully."""
        if not self.running:
            return
        
        logger.info(f"Shutting down agent: {self.agent_name}")
        self.running = False
        self.health_status = "shutting_down"
        
        try:
            # Stop event consumer
            if self.event_consumer:
                await self.event_consumer.stop()
            
            # Agent-specific cleanup
            await self.on_shutdown()
            
            # Close connections
            if self.kafka_manager:
                await self.kafka_manager.close()
            
            self.health_status = "stopped"
            logger.info(f"Agent {self.agent_name} shutdown complete")
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
            self.health_status = "error"
    
    async def _handle_event_wrapper(self, event: BaseEvent):
        """Wrapper for event handling with tracing and error handling."""
        with self.tracer.start_as_current_span(
            f"{self.agent_name}.handle_event",
            attributes={
                "event.type": event.event_type,
                "event.client_id": event.client_id,
                "event.ingest_id": event.ingest_id,
                "event.trace_id": event.trace_id,
            }
        ) as span:
            try:
                # Update trace context
                span.set_attribute("agent.name", self.agent_name)
                span.set_attribute("event.id", event.event_id)
                
                # Process event
                await self.handle_event(event)
                
                self.processed_events += 1
                span.set_attribute("processing.status", "success")
                
            except Exception as e:
                self.failed_events += 1
                span.set_attribute("processing.status", "error")
                span.set_attribute("error.message", str(e))
                span.record_exception(e)
                
                logger.error(f"Error processing event {event.event_id}: {e}")
                
                # Publish error event
                await self._publish_error_event(event, str(e))
                
                # Re-raise for retry logic
                raise
    
    async def _publish_error_event(self, original_event: BaseEvent, error_message: str):
        """Publish an error event."""
        try:
            from shared.events import ProcessingErrorEvent
            
            error_event = ProcessingErrorEvent(
                event_id=f"error-{original_event.event_id}",
                trace_id=original_event.trace_id,
                client_id=original_event.client_id,
                ingest_id=original_event.ingest_id,
                source_agent=self.agent_name,
                idempotency_key=f"error-{original_event.idempotency_key}",
                error_code="PROCESSING_ERROR",
                error_message=error_message,
                failed_stage=self.agent_name,
                is_recoverable=True,
                retry_count=0,
            )
            
            await self.event_publisher.publish(error_event)
            
        except Exception as e:
            logger.error(f"Failed to publish error event: {e}")
    
    async def _heartbeat_loop(self):
        """Send periodic heartbeat signals."""
        while self.running:
            try:
                self.last_heartbeat = asyncio.get_event_loop().time()
                
                # Log health status periodically
                if self.processed_events % 100 == 0 and self.processed_events > 0:
                    logger.info(
                        f"Agent {self.agent_name} health: "
                        f"processed={self.processed_events}, "
                        f"failed={self.failed_events}, "
                        f"status={self.health_status}"
                    )
                
                await asyncio.sleep(30)  # Heartbeat every 30 seconds
                
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")
                await asyncio.sleep(30)
    
    async def get_health_status(self) -> Dict[str, Any]:
        """Get agent health status."""
        current_time = asyncio.get_event_loop().time()
        
        return {
            "agent_name": self.agent_name,
            "status": self.health_status,
            "running": self.running,
            "processed_events": self.processed_events,
            "failed_events": self.failed_events,
            "success_rate": (
                (self.processed_events - self.failed_events) / max(self.processed_events, 1)
            ),
            "last_heartbeat": self.last_heartbeat,
            "uptime_seconds": current_time - (self.last_heartbeat or current_time),
            "config": self.config,
        }
    
    @asynccontextmanager
    async def database_session(self):
        """Get a database session with proper cleanup."""
        async with get_async_session() as session:
            yield session
    
    async def publish_event(self, event: BaseEvent):
        """Publish an event to Kafka."""
        await self.event_publisher.publish(event)
    
    # Abstract methods that subclasses must implement
    
    @abstractmethod
    async def handle_event(self, event: BaseEvent):
        """Handle an incoming event. Must be implemented by subclasses."""
        pass
    
    async def on_initialize(self):
        """Called during agent initialization. Override for custom setup."""
        pass
    
    async def on_shutdown(self):
        """Called during agent shutdown. Override for custom cleanup."""
        pass
    
    async def start_background_tasks(self) -> List[asyncio.Task]:
        """Start background tasks. Override to add custom tasks."""
        return []


class StatefulAgent(BaseAgent):
    """Base class for agents that maintain state."""
    
    def __init__(self, agent_name: str):
        super().__init__(agent_name)
        self.state = {}
        self.state_lock = asyncio.Lock()
    
    async def get_state(self, key: str, default: Any = None) -> Any:
        """Get a value from agent state."""
        async with self.state_lock:
            return self.state.get(key, default)
    
    async def set_state(self, key: str, value: Any):
        """Set a value in agent state."""
        async with self.state_lock:
            self.state[key] = value
    
    async def update_state(self, updates: Dict[str, Any]):
        """Update multiple state values."""
        async with self.state_lock:
            self.state.update(updates)
    
    async def clear_state(self):
        """Clear all state."""
        async with self.state_lock:
            self.state.clear()


class BatchProcessingAgent(BaseAgent):
    """Base class for agents that process events in batches."""
    
    def __init__(self, agent_name: str, batch_size: Optional[int] = None):
        super().__init__(agent_name)
        self.batch_size = batch_size or self.config["batch_size"]
        self.event_batch = []
        self.batch_lock = asyncio.Lock()
        self.batch_timer = None
        self.batch_timeout = 30  # seconds
    
    async def handle_event(self, event: BaseEvent):
        """Add event to batch for processing."""
        async with self.batch_lock:
            self.event_batch.append(event)
            
            # Process batch if it's full
            if len(self.event_batch) >= self.batch_size:
                await self._process_batch()
            else:
                # Set timer for batch timeout
                self._reset_batch_timer()
    
    async def _process_batch(self):
        """Process the current batch of events."""
        if not self.event_batch:
            return
        
        batch = self.event_batch.copy()
        self.event_batch.clear()
        
        if self.batch_timer:
            self.batch_timer.cancel()
            self.batch_timer = None
        
        try:
            await self.process_batch(batch)
        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            # Could implement retry logic here
    
    def _reset_batch_timer(self):
        """Reset the batch timeout timer."""
        if self.batch_timer:
            self.batch_timer.cancel()
        
        self.batch_timer = asyncio.create_task(self._batch_timeout())
    
    async def _batch_timeout(self):
        """Handle batch timeout."""
        await asyncio.sleep(self.batch_timeout)
        
        async with self.batch_lock:
            if self.event_batch:
                await self._process_batch()
    
    @abstractmethod
    async def process_batch(self, events: List[BaseEvent]):
        """Process a batch of events. Must be implemented by subclasses."""
        pass
    
    async def on_shutdown(self):
        """Process any remaining events in batch during shutdown."""
        async with self.batch_lock:
            if self.event_batch:
                await self._process_batch()
        
        await super().on_shutdown()


# Agent registry and factory

class AgentRegistry:
    """Registry for agent classes."""
    
    _agents = {}
    
    @classmethod
    def register(cls, name: str, agent_class: type):
        """Register an agent class."""
        cls._agents[name] = agent_class
    
    @classmethod
    def get_agent_class(cls, name: str) -> type:
        """Get an agent class by name."""
        if name not in cls._agents:
            raise ValueError(f"Unknown agent: {name}")
        return cls._agents[name]
    
    @classmethod
    def create_agent(cls, name: str) -> BaseAgent:
        """Create an agent instance."""
        agent_class = cls.get_agent_class(name)
        return agent_class(name)
    
    @classmethod
    def list_agents(cls) -> List[str]:
        """List all registered agents."""
        return list(cls._agents.keys())


def register_agent(name: str):
    """Decorator to register an agent class."""
    def decorator(agent_class):
        AgentRegistry.register(name, agent_class)
        return agent_class
    return decorator


# Utility functions

async def run_agent(agent_name: str):
    """Run a single agent."""
    try:
        agent = AgentRegistry.create_agent(agent_name)
        await agent.start()
    except KeyboardInterrupt:
        logger.info("Agent interrupted by user")
    except Exception as e:
        logger.error(f"Agent {agent_name} failed: {e}")
        sys.exit(1)


async def run_multiple_agents(agent_names: List[str]):
    """Run multiple agents concurrently."""
    tasks = []
    
    for agent_name in agent_names:
        agent = AgentRegistry.create_agent(agent_name)
        task = asyncio.create_task(agent.start())
        tasks.append(task)
    
    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logger.info("Agents interrupted by user")
        # Graceful shutdown is handled by signal handlers
    except Exception as e:
        logger.error(f"Error running agents: {e}")
        sys.exit(1)


# Health check utilities

async def check_agent_health(agent: BaseAgent) -> Dict[str, Any]:
    """Check the health of an agent."""
    try:
        return await agent.get_health_status()
    except Exception as e:
        return {
            "agent_name": agent.agent_name,
            "status": "error",
            "error": str(e),
        }


async def check_all_agents_health() -> Dict[str, Dict[str, Any]]:
    """Check the health of all registered agents."""
    health_status = {}
    
    for agent_name in AgentRegistry.list_agents():
        try:
            agent = AgentRegistry.create_agent(agent_name)
            health_status[agent_name] = await check_agent_health(agent)
        except Exception as e:
            health_status[agent_name] = {
                "status": "error",
                "error": str(e),
            }
    
    return health_status