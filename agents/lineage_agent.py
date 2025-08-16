"""
Lineage Agent - Tracks end-to-end data lineage and relationships.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple
from uuid import uuid4

from shared.base_agent import BaseAgent, register_agent
from shared.events import (
    BaseEvent, FileReceivedEvent, FileProfiledEvent, MappingAppliedEvent,
    ValidationResultsEvent, NormalizationDoneEvent, PersistenceUpsertedEvent,
    ExportCompletedEvent, LineageUpdatedEvent
)

logger = logging.getLogger(__name__)


@register_agent("lineage")
class LineageAgent(BaseAgent):
    """
    LineageAgent tracks comprehensive data lineage throughout the processing pipeline.
    
    Responsibilities:
    - Build end-to-end lineage graphs from file to export
    - Track data transformations and quality changes
    - Enable impact analysis and root cause investigation
    - Support compliance and audit requirements
    - Provide lineage visualization data
    - Track data dependencies and relationships
    """
    
    def __init__(self, agent_name: str):
        super().__init__(agent_name)
        self.lineage_cache = {}  # In-memory cache for active lineage graphs
        
        # Event type to lineage node type mapping
        self.event_node_mapping = {
            "file.received": "file",
            "file.profiled": "profile",
            "mapping.applied": "mapping",
            "validation.results": "validation",
            "normalization.done": "normalization",
            "persistence.upserted": "product",
            "export.completed": "export",
        }
    
    async def handle_event(self, event: BaseEvent):
        """Handle incoming events and update lineage."""
        try:
            # Create or update lineage node for this event
            await self._create_lineage_node(event)
            
            # Create edges based on event type
            await self._create_lineage_edges(event)
            
            # Update lineage cache
            await self._update_lineage_cache(event)
            
            # Emit lineage updated event for significant milestones
            if self._is_significant_milestone(event):
                await self._emit_lineage_updated_event(event)
            
        except Exception as e:
            logger.error(f"Error updating lineage for event {event.event_type}: {e}")
    
    async def _create_lineage_node(self, event: BaseEvent):
        """Create a lineage node for the event."""
        node_type = self.event_node_mapping.get(event.event_type, "unknown")
        
        # Generate node metadata based on event type
        metadata = await self._generate_node_metadata(event)
        
        async with self.database_session() as session:
            # Check if node already exists
            existing_query = """
                SELECT node_id FROM lineage_node
                WHERE client_id = :client_id 
                  AND ingest_id = :ingest_id 
                  AND node_type = :node_type
            """
            
            result = await session.execute(existing_query, {
                "client_id": event.client_id,
                "ingest_id": event.ingest_id,
                "node_type": node_type,
            })
            
            existing_node = result.fetchone()
            
            if existing_node:
                # Update existing node
                update_query = """
                    UPDATE lineage_node 
                    SET metadata = :metadata, updated_at = NOW()
                    WHERE node_id = :node_id
                """
                
                await session.execute(update_query, {
                    "node_id": existing_node[0],
                    "metadata": metadata,
                })
            else:
                # Create new node
                node_id = str(uuid4())
                
                insert_query = """
                    INSERT INTO lineage_node (
                        node_id, node_type, entity_id, client_id, ingest_id,
                        metadata, created_at
                    ) VALUES (
                        :node_id, :node_type, :entity_id, :client_id, :ingest_id,
                        :metadata, :created_at
                    )
                """
                
                await session.execute(insert_query, {
                    "node_id": node_id,
                    "node_type": node_type,
                    "entity_id": self._extract_entity_id(event),
                    "client_id": event.client_id,
                    "ingest_id": event.ingest_id,
                    "metadata": metadata,
                    "created_at": datetime.utcnow(),
                })
            
            await session.commit()
    
    async def _generate_node_metadata(self, event: BaseEvent) -> Dict[str, Any]:
        """Generate metadata for lineage node based on event type."""
        base_metadata = {
            "event_id": event.event_id,
            "trace_id": event.trace_id,
            "source_agent": event.source_agent,
            "timestamp": event.timestamp.isoformat(),
        }
        
        # Add event-specific metadata
        if isinstance(event, FileReceivedEvent):
            base_metadata.update({
                "filename": event.filename,
                "size_bytes": event.size_bytes,
                "content_type": event.content_type,
                "uri": event.uri,
            })
        
        elif isinstance(event, FileProfiledEvent):
            base_metadata.update({
                "columns": event.columns,
                "row_count": event.row_count,
                "column_count": event.column_count,
                "encoding": event.encoding,
                "delimiter": event.delimiter,
                "quality_score": event.quality_score,
            })
        
        elif isinstance(event, MappingAppliedEvent):
            base_metadata.update({
                "template_id": str(event.template_id) if event.template_id else None,
                "template_version": event.template_version,
                "rows_processed": event.rows_processed,
            })
        
        elif isinstance(event, ValidationResultsEvent):
            base_metadata.update({
                "summary": event.summary,
                "quality_score": event.quality_metrics.get("overall_score") if event.quality_metrics else None,
                "gate_status": event.gate_status,
            })
        
        elif isinstance(event, NormalizationDoneEvent):
            base_metadata.update({
                "rows_processed": event.rows_processed,
                "quality_score": event.quality_score,
            })
        
        elif isinstance(event, PersistenceUpsertedEvent):
            base_metadata.update({
                "products_upserted": event.products_upserted,
                "product_ids": event.product_ids[:10],  # Limit for metadata size
            })
        
        elif isinstance(event, ExportCompletedEvent):
            base_metadata.update({
                "export_format": event.export_format,
                "file_size": event.file_size,
                "record_count": event.record_count,
                "download_url": event.download_url,
            })
        
        return base_metadata
    
    def _extract_entity_id(self, event: BaseEvent) -> Optional[str]:
        """Extract the primary entity ID from the event."""
        if isinstance(event, PersistenceUpsertedEvent):
            # For persistence events, use the first product ID
            return event.product_ids[0] if event.product_ids else None
        elif isinstance(event, ExportCompletedEvent):
            return str(event.export_id)
        else:
            # For other events, use ingest_id as entity_id
            return event.ingest_id
    
    async def _create_lineage_edges(self, event: BaseEvent):
        """Create lineage edges based on event relationships."""
        current_node_type = self.event_node_mapping.get(event.event_type, "unknown")
        
        # Define edge creation rules based on event type
        edge_rules = {
            "file.profiled": [("file", "processes")],
            "mapping.applied": [("profile", "processes")],
            "validation.results": [("mapping", "processes")],
            "normalization.done": [("validation", "processes")],
            "persistence.upserted": [("normalization", "generates")],
            "export.completed": [("product", "derives_from")],
        }
        
        rules = edge_rules.get(event.event_type, [])
        
        async with self.database_session() as session:
            for source_node_type, edge_type in rules:
                await self._create_edge_between_nodes(
                    session, event, source_node_type, current_node_type, edge_type
                )
    
    async def _create_edge_between_nodes(self, session, event: BaseEvent, source_node_type: str, target_node_type: str, edge_type: str):
        """Create an edge between two nodes."""
        # Find source node
        source_query = """
            SELECT node_id FROM lineage_node
            WHERE client_id = :client_id 
              AND ingest_id = :ingest_id 
              AND node_type = :source_node_type
            ORDER BY created_at DESC
            LIMIT 1
        """
        
        source_result = await session.execute(source_query, {
            "client_id": event.client_id,
            "ingest_id": event.ingest_id,
            "source_node_type": source_node_type,
        })
        
        source_node = source_result.fetchone()
        if not source_node:
            logger.warning(f"Source node not found: {source_node_type} for {event.ingest_id}")
            return
        
        # Find target node
        target_query = """
            SELECT node_id FROM lineage_node
            WHERE client_id = :client_id 
              AND ingest_id = :ingest_id 
              AND node_type = :target_node_type
            ORDER BY created_at DESC
            LIMIT 1
        """
        
        target_result = await session.execute(target_query, {
            "client_id": event.client_id,
            "ingest_id": event.ingest_id,
            "target_node_type": target_node_type,
        })
        
        target_node = target_result.fetchone()
        if not target_node:
            logger.warning(f"Target node not found: {target_node_type} for {event.ingest_id}")
            return
        
        # Check if edge already exists
        existing_edge_query = """
            SELECT edge_id FROM lineage_edge
            WHERE source_node_id = :source_node_id 
              AND target_node_id = :target_node_id
              AND edge_type = :edge_type
        """
        
        existing_result = await session.execute(existing_edge_query, {
            "source_node_id": source_node[0],
            "target_node_id": target_node[0],
            "edge_type": edge_type,
        })
        
        if existing_result.fetchone():
            return  # Edge already exists
        
        # Create new edge
        edge_id = str(uuid4())
        
        insert_edge_query = """
            INSERT INTO lineage_edge (
                edge_id, source_node_id, target_node_id, edge_type,
                metadata, created_at
            ) VALUES (
                :edge_id, :source_node_id, :target_node_id, :edge_type,
                :metadata, :created_at
            )
        """
        
        await session.execute(insert_edge_query, {
            "edge_id": edge_id,
            "source_node_id": source_node[0],
            "target_node_id": target_node[0],
            "edge_type": edge_type,
            "metadata": {
                "event_id": event.event_id,
                "created_by": event.source_agent,
            },
            "created_at": datetime.utcnow(),
        })
    
    async def _update_lineage_cache(self, event: BaseEvent):
        """Update in-memory lineage cache for fast queries."""
        cache_key = f"{event.client_id}:{event.ingest_id}"
        
        if cache_key not in self.lineage_cache:
            self.lineage_cache[cache_key] = {
                "nodes": {},
                "edges": [],
                "last_updated": datetime.utcnow(),
            }
        
        # Add/update node in cache
        node_type = self.event_node_mapping.get(event.event_type, "unknown")
        self.lineage_cache[cache_key]["nodes"][node_type] = {
            "event_type": event.event_type,
            "timestamp": event.timestamp,
            "metadata": await self._generate_node_metadata(event),
        }
        
        self.lineage_cache[cache_key]["last_updated"] = datetime.utcnow()
    
    def _is_significant_milestone(self, event: BaseEvent) -> bool:
        """Determine if event represents a significant milestone."""
        significant_events = {
            "file.profiled",
            "validation.results", 
            "persistence.upserted",
            "export.completed"
        }
        
        return event.event_type in significant_events
    
    async def _emit_lineage_updated_event(self, event: BaseEvent):
        """Emit lineage updated event."""
        lineage_event = LineageUpdatedEvent(
            event_id=f"lineage-{event.ingest_id}-{event.event_type}",
            trace_id=event.trace_id,
            client_id=event.client_id,
            ingest_id=event.ingest_id,
            source_agent=self.agent_name,
            idempotency_key=f"lineage-{event.client_id}-{event.ingest_id}-{event.event_type}",
            milestone=event.event_type,
            lineage_graph=await self._get_current_lineage_graph(event),
        )
        
        await self.publish_event(lineage_event)
    
    async def _get_current_lineage_graph(self, event: BaseEvent) -> Dict[str, Any]:
        """Get current lineage graph for the ingest."""
        async with self.database_session() as session:
            # Get all nodes for this ingest
            nodes_query = """
                SELECT node_id, node_type, entity_id, metadata, created_at
                FROM lineage_node
                WHERE client_id = :client_id AND ingest_id = :ingest_id
                ORDER BY created_at
            """
            
            nodes_result = await session.execute(nodes_query, {
                "client_id": event.client_id,
                "ingest_id": event.ingest_id,
            })
            
            nodes = []
            node_ids = []
            
            for row in nodes_result.fetchall():
                node_id, node_type, entity_id, metadata, created_at = row
                nodes.append({
                    "node_id": node_id,
                    "node_type": node_type,
                    "entity_id": entity_id,
                    "metadata": metadata,
                    "created_at": created_at.isoformat(),
                })
                node_ids.append(node_id)
            
            # Get all edges between these nodes
            edges = []
            if node_ids:
                edges_query = """
                    SELECT edge_id, source_node_id, target_node_id, edge_type, metadata, created_at
                    FROM lineage_edge
                    WHERE source_node_id = ANY(:node_ids) OR target_node_id = ANY(:node_ids)
                    ORDER BY created_at
                """
                
                edges_result = await session.execute(edges_query, {"node_ids": node_ids})
                
                for row in edges_result.fetchall():
                    edge_id, source_id, target_id, edge_type, metadata, created_at = row
                    edges.append({
                        "edge_id": edge_id,
                        "source_node_id": source_id,
                        "target_node_id": target_id,
                        "edge_type": edge_type,
                        "metadata": metadata,
                        "created_at": created_at.isoformat(),
                    })
            
            return {
                "nodes": nodes,
                "edges": edges,
                "client_id": event.client_id,
                "ingest_id": event.ingest_id,
                "generated_at": datetime.utcnow().isoformat(),
            }
    
    async def get_lineage_for_product(self, product_id: str) -> Dict[str, Any]:
        """Get complete lineage for a specific product."""
        async with self.database_session() as session:
            # Find all lineage paths that lead to this product
            query = """
                WITH RECURSIVE lineage_path AS (
                    -- Start with the product node
                    SELECT node_id, node_type, entity_id, client_id, ingest_id, metadata, 0 as depth
                    FROM lineage_node
                    WHERE entity_id = :product_id AND node_type = 'product'
                    
                    UNION ALL
                    
                    -- Recursively find parent nodes
                    SELECT ln.node_id, ln.node_type, ln.entity_id, ln.client_id, ln.ingest_id, ln.metadata, lp.depth + 1
                    FROM lineage_node ln
                    JOIN lineage_edge le ON ln.node_id = le.source_node_id
                    JOIN lineage_path lp ON le.target_node_id = lp.node_id
                    WHERE lp.depth < 10  -- Prevent infinite recursion
                )
                SELECT * FROM lineage_path ORDER BY depth DESC, node_type
            """
            
            result = await session.execute(query, {"product_id": product_id})
            rows = result.fetchall()
            
            nodes = []
            for row in rows:
                nodes.append({
                    "node_id": row[0],
                    "node_type": row[1],
                    "entity_id": row[2],
                    "client_id": row[3],
                    "ingest_id": row[4],
                    "metadata": row[5],
                    "depth": row[6],
                })
            
            return {
                "product_id": product_id,
                "lineage_nodes": nodes,
                "generated_at": datetime.utcnow().isoformat(),
            }
    
    async def get_impact_analysis(self, node_id: str) -> Dict[str, Any]:
        """Get impact analysis for a lineage node."""
        async with self.database_session() as session:
            # Find all downstream nodes affected by this node
            query = """
                WITH RECURSIVE downstream AS (
                    -- Start with the given node
                    SELECT node_id, node_type, entity_id, client_id, ingest_id, 0 as depth
                    FROM lineage_node
                    WHERE node_id = :node_id
                    
                    UNION ALL
                    
                    -- Recursively find downstream nodes
                    SELECT ln.node_id, ln.node_type, ln.entity_id, ln.client_id, ln.ingest_id, d.depth + 1
                    FROM lineage_node ln
                    JOIN lineage_edge le ON ln.node_id = le.target_node_id
                    JOIN downstream d ON le.source_node_id = d.node_id
                    WHERE d.depth < 10  -- Prevent infinite recursion
                )
                SELECT * FROM downstream WHERE depth > 0 ORDER BY depth, node_type
            """
            
            result = await session.execute(query, {"node_id": node_id})
            rows = result.fetchall()
            
            impacted_nodes = []
            for row in rows:
                impacted_nodes.append({
                    "node_id": row[0],
                    "node_type": row[1],
                    "entity_id": row[2],
                    "client_id": row[3],
                    "ingest_id": row[4],
                    "depth": row[5],
                })
            
            # Group by type for summary
            impact_summary = {}
            for node in impacted_nodes:
                node_type = node["node_type"]
                if node_type not in impact_summary:
                    impact_summary[node_type] = 0
                impact_summary[node_type] += 1
            
            return {
                "source_node_id": node_id,
                "impacted_nodes": impacted_nodes,
                "impact_summary": impact_summary,
                "total_impacted": len(impacted_nodes),
                "generated_at": datetime.utcnow().isoformat(),
            }
    
    async def get_lineage_statistics(self, client_id: Optional[str] = None) -> Dict[str, Any]:
        """Get lineage statistics for monitoring and analytics."""
        async with self.database_session() as session:
            where_clause = "WHERE client_id = :client_id" if client_id else ""
            params = {"client_id": client_id} if client_id else {}
            
            # Node statistics
            node_stats_query = f"""
                SELECT node_type, COUNT(*) as count
                FROM lineage_node
                {where_clause}
                GROUP BY node_type
                ORDER BY count DESC
            """
            
            node_result = await session.execute(node_stats_query, params)
            node_stats = {row[0]: row[1] for row in node_result.fetchall()}
            
            # Edge statistics
            edge_stats_query = f"""
                SELECT le.edge_type, COUNT(*) as count
                FROM lineage_edge le
                JOIN lineage_node ln ON le.source_node_id = ln.node_id
                {where_clause.replace('client_id', 'ln.client_id') if where_clause else ''}
                GROUP BY le.edge_type
                ORDER BY count DESC
            """
            
            edge_result = await session.execute(edge_stats_query, params)
            edge_stats = {row[0]: row[1] for row in edge_result.fetchall()}
            
            # Recent activity
            recent_activity_query = f"""
                SELECT DATE(created_at) as date, COUNT(*) as nodes_created
                FROM lineage_node
                {where_clause}
                AND created_at >= NOW() - INTERVAL '30 days'
                GROUP BY DATE(created_at)
                ORDER BY date DESC
                LIMIT 30
            """
            
            activity_result = await session.execute(recent_activity_query, params)
            recent_activity = [
                {"date": row[0].isoformat(), "nodes_created": row[1]}
                for row in activity_result.fetchall()
            ]
            
            return {
                "client_id": client_id,
                "node_statistics": node_stats,
                "edge_statistics": edge_stats,
                "recent_activity": recent_activity,
                "total_nodes": sum(node_stats.values()),
                "total_edges": sum(edge_stats.values()),
                "generated_at": datetime.utcnow().isoformat(),
            }
    
    async def cleanup_old_lineage(self, retention_days: int = 90):
        """Clean up old lineage data beyond retention period."""
        async with self.database_session() as session:
            # Delete old edges first (due to foreign key constraints)
            edge_cleanup_query = """
                DELETE FROM lineage_edge
                WHERE created_at < NOW() - INTERVAL '%s days'
            """ % retention_days
            
            edge_result = await session.execute(edge_cleanup_query)
            edges_deleted = edge_result.rowcount
            
            # Delete old nodes
            node_cleanup_query = """
                DELETE FROM lineage_node
                WHERE created_at < NOW() - INTERVAL '%s days'
            """ % retention_days
            
            node_result = await session.execute(node_cleanup_query)
            nodes_deleted = node_result.rowcount
            
            await session.commit()
            
            logger.info(f"Cleaned up {nodes_deleted} nodes and {edges_deleted} edges older than {retention_days} days")
            
            return {
                "nodes_deleted": nodes_deleted,
                "edges_deleted": edges_deleted,
                "retention_days": retention_days,
            }
    
    async def on_initialize(self):
        """Initialize agent-specific components."""
        logger.info("LineageAgent initialized")
        
        # Could initialize graph databases, visualization tools, etc.
        
        # Start cleanup task
        import asyncio
        asyncio.create_task(self._periodic_cleanup())
    
    async def _periodic_cleanup(self):
        """Periodic cleanup of old lineage data."""
        import asyncio
        
        while True:
            try:
                # Run cleanup weekly
                await asyncio.sleep(7 * 24 * 3600)  # 7 days
                await self.cleanup_old_lineage()
            except Exception as e:
                logger.error(f"Error in periodic lineage cleanup: {e}")
                await asyncio.sleep(3600)  # Retry in 1 hour