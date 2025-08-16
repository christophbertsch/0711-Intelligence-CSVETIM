"""
Persistence Agent - Handles canonical data storage and product management.
"""

import hashlib
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from shared.base_agent import BaseAgent, register_agent
from shared.events import BaseEvent, NormalizationDoneEvent, PersistenceUpsertedEvent

logger = logging.getLogger(__name__)


@register_agent("persistence")
class PersistenceAgent(BaseAgent):
    """
    PersistenceAgent handles canonical data storage and product lifecycle management.
    
    Responsibilities:
    - Idempotent upserts into canonical product tables
    - Product deduplication and merging
    - Crosswalk table maintenance (client SKU -> product UUID)
    - Feature value management with versioning
    - Data lineage tracking
    - Product lifecycle status management
    """
    
    def __init__(self, agent_name: str):
        super().__init__(agent_name)
        self.batch_size = self.config["batch_size"]
        self.enable_deduplication = self.config.get("enable_deduplication", True)
        self.dedup_threshold = self.config.get("dedup_threshold", 0.8)
    
    async def handle_event(self, event: BaseEvent):
        """Handle incoming events."""
        if isinstance(event, NormalizationDoneEvent):
            await self._persist_normalized_data(event)
        else:
            logger.warning(f"Unhandled event type: {event.event_type}")
    
    async def _persist_normalized_data(self, event: NormalizationDoneEvent):
        """Persist normalized data to canonical tables."""
        try:
            logger.info(f"Persisting data for client {event.client_id}, ingest {event.ingest_id}")
            
            # Load normalized data
            normalized_data = await self._load_normalized_data(event)
            
            # Process data in batches
            total_rows = len(normalized_data)
            processed_rows = 0
            upserted_products = []
            persistence_issues = []
            
            for batch_start in range(0, total_rows, self.batch_size):
                batch_end = min(batch_start + self.batch_size, total_rows)
                batch_data = normalized_data[batch_start:batch_end]
                
                # Process batch
                batch_results, batch_issues = await self._process_batch(batch_data, event)
                upserted_products.extend(batch_results)
                persistence_issues.extend(batch_issues)
                
                processed_rows += len(batch_data)
                
                # Emit progress for large files
                if total_rows > self.batch_size:
                    await self._emit_persistence_progress(event, processed_rows, total_rows)
            
            # Update job status
            await self._update_job_status(event, "completed")
            
            # Emit persistence.upserted event
            await self._emit_persistence_upserted_event(event, upserted_products, persistence_issues)
            
            logger.info(f"Successfully persisted {len(upserted_products)} products for {event.ingest_id}")
            
        except Exception as e:
            logger.error(f"Error persisting data: {e}")
            await self._handle_persistence_error(event, str(e))
    
    async def _load_normalized_data(self, event: NormalizationDoneEvent) -> List[Dict[str, Any]]:
        """Load normalized data from staging."""
        async with self.database_session() as session:
            query = """
                SELECT row_num, data
                FROM stg_row
                WHERE client_id = :client_id AND ingest_id = :ingest_id
                ORDER BY row_num
            """
            
            result = await session.execute(query, {
                "client_id": event.client_id,
                "ingest_id": event.ingest_id,
            })
            
            rows = result.fetchall()
            return [{"row_num": row[0], **row[1]} for row in rows]
    
    async def _process_batch(self, batch_data: List[Dict[str, Any]], event: NormalizationDoneEvent) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Process a batch of normalized data."""
        upserted_products = []
        issues = []
        
        async with self.database_session() as session:
            for row_data in batch_data:
                try:
                    # Skip rows with normalization errors
                    if row_data.get("normalization_error"):
                        issues.append({
                            "row_num": row_data.get("row_num"),
                            "type": "normalization_error",
                            "message": row_data["normalization_error"],
                        })
                        continue
                    
                    # Process single product
                    product_result = await self._process_single_product(row_data, event, session)
                    if product_result:
                        upserted_products.append(product_result)
                    
                except Exception as e:
                    issue = {
                        "row_num": row_data.get("row_num"),
                        "type": "persistence_error",
                        "message": str(e),
                    }
                    issues.append(issue)
                    logger.warning(f"Error processing row {row_data.get('row_num')}: {e}")
            
            await session.commit()
        
        return upserted_products, issues
    
    async def _process_single_product(self, row_data: Dict[str, Any], event: NormalizationDoneEvent, session) -> Optional[Dict[str, Any]]:
        """Process a single product row."""
        product_data = row_data.get("product", {})
        features_data = row_data.get("features", [])
        
        # Extract key identifiers
        client_sku = product_data.get("sku")
        if not client_sku:
            raise ValueError("Missing required SKU")
        
        gtin = product_data.get("gtin")
        
        # Find or create product
        product_id = await self._find_or_create_product(
            client_sku, gtin, product_data, event.client_id, session
        )
        
        # Update product core data
        await self._update_product_core(product_id, product_data, event.client_id, session)
        
        # Update feature values
        await self._update_feature_values(product_id, features_data, event.client_id, session)
        
        # Update crosswalk
        await self._update_crosswalk(event.client_id, client_sku, product_id, session)
        
        # Create lineage entry
        await self._create_lineage_entry(product_id, event, row_data.get("row_num"), session)
        
        return {
            "product_id": str(product_id),
            "client_sku": client_sku,
            "gtin": gtin,
            "row_num": row_data.get("row_num"),
        }
    
    async def _find_or_create_product(self, client_sku: str, gtin: Optional[str], product_data: Dict[str, Any], client_id: str, session) -> str:
        """Find existing product or create new one."""
        product_id = None
        
        # First, check crosswalk table
        crosswalk_query = """
            SELECT product_id FROM product_crosswalk
            WHERE client_id = :client_id AND src_key = :src_key
        """
        
        result = await session.execute(crosswalk_query, {
            "client_id": client_id,
            "src_key": client_sku,
        })
        
        row = result.fetchone()
        if row:
            product_id = row[0]
            logger.debug(f"Found existing product via crosswalk: {product_id}")
            return product_id
        
        # If GTIN provided, check for existing product with same GTIN
        if gtin and self.enable_deduplication:
            gtin_query = """
                SELECT product_id FROM product
                WHERE gtin = :gtin AND gtin IS NOT NULL
                LIMIT 1
            """
            
            result = await session.execute(gtin_query, {"gtin": gtin})
            row = result.fetchone()
            
            if row:
                existing_product_id = row[0]
                logger.info(f"Found existing product with GTIN {gtin}: {existing_product_id}")
                
                # Check if we should merge or create separate
                should_merge = await self._should_merge_products(
                    existing_product_id, product_data, client_id, session
                )
                
                if should_merge:
                    logger.info(f"Merging with existing product: {existing_product_id}")
                    return existing_product_id
        
        # Create new product
        product_id = str(uuid4())
        
        create_query = """
            INSERT INTO product (
                product_id, sku, gtin, product_name, brand, 
                etim_class, lifecycle_status, created_at
            ) VALUES (
                :product_id, :sku, :gtin, :product_name, :brand,
                :etim_class, :lifecycle_status, :created_at
            )
        """
        
        await session.execute(create_query, {
            "product_id": product_id,
            "sku": client_sku,  # Use client SKU as default
            "gtin": gtin,
            "product_name": product_data.get("product_name"),
            "brand": product_data.get("brand"),
            "etim_class": product_data.get("etim_class"),
            "lifecycle_status": "active",
            "created_at": datetime.utcnow(),
        })
        
        logger.info(f"Created new product: {product_id}")
        return product_id
    
    async def _should_merge_products(self, existing_product_id: str, new_product_data: Dict[str, Any], client_id: str, session) -> bool:
        """Determine if products should be merged based on similarity."""
        if not self.enable_deduplication:
            return False
        
        # Get existing product data
        existing_query = """
            SELECT product_name, brand, etim_class
            FROM product
            WHERE product_id = :product_id
        """
        
        result = await session.execute(existing_query, {"product_id": existing_product_id})
        row = result.fetchone()
        
        if not row:
            return False
        
        existing_name, existing_brand, existing_etim_class = row
        
        # Calculate similarity score
        similarity_score = self._calculate_product_similarity(
            {
                "product_name": existing_name,
                "brand": existing_brand,
                "etim_class": existing_etim_class,
            },
            new_product_data
        )
        
        return similarity_score >= self.dedup_threshold
    
    def _calculate_product_similarity(self, product1: Dict[str, Any], product2: Dict[str, Any]) -> float:
        """Calculate similarity score between two products."""
        scores = []
        
        # Name similarity
        name1 = product1.get("product_name", "").lower()
        name2 = product2.get("product_name", "").lower()
        if name1 and name2:
            name_similarity = self._string_similarity(name1, name2)
            scores.append(name_similarity * 0.4)  # 40% weight
        
        # Brand similarity
        brand1 = product1.get("brand", "").lower()
        brand2 = product2.get("brand", "").lower()
        if brand1 and brand2:
            if brand1 == brand2:
                scores.append(0.3)  # 30% weight for exact brand match
            else:
                brand_similarity = self._string_similarity(brand1, brand2)
                scores.append(brand_similarity * 0.3)
        
        # ETIM class similarity
        etim1 = product1.get("etim_class", "")
        etim2 = product2.get("etim_class", "")
        if etim1 and etim2:
            if etim1 == etim2:
                scores.append(0.3)  # 30% weight for exact ETIM match
            else:
                # Could implement ETIM hierarchy similarity here
                scores.append(0.0)
        
        return sum(scores) if scores else 0.0
    
    def _string_similarity(self, str1: str, str2: str) -> float:
        """Calculate string similarity using Jaccard similarity."""
        if not str1 or not str2:
            return 0.0
        
        # Simple word-based Jaccard similarity
        words1 = set(str1.split())
        words2 = set(str2.split())
        
        intersection = words1.intersection(words2)
        union = words1.union(words2)
        
        return len(intersection) / len(union) if union else 0.0
    
    async def _update_product_core(self, product_id: str, product_data: Dict[str, Any], client_id: str, session):
        """Update product core data."""
        # Build update query dynamically based on available data
        update_fields = []
        params = {"product_id": product_id}
        
        updatable_fields = [
            "product_name", "brand", "description", "category",
            "manufacturer", "model", "etim_class", "price", "currency",
            "length", "width", "height", "diameter", "weight"
        ]
        
        for field in updatable_fields:
            if field in product_data and product_data[field] is not None:
                update_fields.append(f"{field} = :{field}")
                params[field] = product_data[field]
        
        if update_fields:
            update_fields.append("updated_at = :updated_at")
            params["updated_at"] = datetime.utcnow()
            
            update_query = f"""
                UPDATE product 
                SET {', '.join(update_fields)}
                WHERE product_id = :product_id
            """
            
            await session.execute(update_query, params)
    
    async def _update_feature_values(self, product_id: str, features_data: List[Dict[str, Any]], client_id: str, session):
        """Update ETIM feature values."""
        for feature in features_data:
            etim_feature = feature.get("etim_feature")
            if not etim_feature:
                continue
            
            # Upsert feature value
            upsert_query = """
                INSERT INTO feature_value (
                    product_id, etim_feature, value_raw, unit_raw,
                    value_normalized, unit_normalized, source_column, created_at
                ) VALUES (
                    :product_id, :etim_feature, :value_raw, :unit_raw,
                    :value_normalized, :unit_normalized, :source_column, :created_at
                )
                ON CONFLICT (product_id, etim_feature)
                DO UPDATE SET
                    value_raw = EXCLUDED.value_raw,
                    unit_raw = EXCLUDED.unit_raw,
                    value_normalized = EXCLUDED.value_normalized,
                    unit_normalized = EXCLUDED.unit_normalized,
                    source_column = EXCLUDED.source_column,
                    updated_at = NOW()
            """
            
            await session.execute(upsert_query, {
                "product_id": product_id,
                "etim_feature": etim_feature,
                "value_raw": feature.get("value_raw"),
                "unit_raw": feature.get("unit_raw"),
                "value_normalized": feature.get("value_converted"),
                "unit_normalized": feature.get("unit_normalized"),
                "source_column": feature.get("source_column"),
                "created_at": datetime.utcnow(),
            })
    
    async def _update_crosswalk(self, client_id: str, src_key: str, product_id: str, session):
        """Update product crosswalk table."""
        upsert_query = """
            INSERT INTO product_crosswalk (
                client_id, src_key, product_id, created_at
            ) VALUES (
                :client_id, :src_key, :product_id, :created_at
            )
            ON CONFLICT (client_id, src_key)
            DO UPDATE SET
                product_id = EXCLUDED.product_id,
                updated_at = NOW()
        """
        
        await session.execute(upsert_query, {
            "client_id": client_id,
            "src_key": src_key,
            "product_id": product_id,
            "created_at": datetime.utcnow(),
        })
    
    async def _create_lineage_entry(self, product_id: str, event: NormalizationDoneEvent, row_num: int, session):
        """Create data lineage entry."""
        # Create lineage node for this product instance
        lineage_id = str(uuid4())
        
        node_query = """
            INSERT INTO lineage_node (
                node_id, node_type, entity_id, client_id, ingest_id,
                metadata, created_at
            ) VALUES (
                :node_id, :node_type, :entity_id, :client_id, :ingest_id,
                :metadata, :created_at
            )
        """
        
        await session.execute(node_query, {
            "node_id": lineage_id,
            "node_type": "product",
            "entity_id": product_id,
            "client_id": event.client_id,
            "ingest_id": event.ingest_id,
            "metadata": {
                "row_num": row_num,
                "trace_id": event.trace_id,
                "processing_stage": "persistence",
            },
            "created_at": datetime.utcnow(),
        })
        
        # Create edge from file to product
        edge_query = """
            INSERT INTO lineage_edge (
                edge_id, source_node_id, target_node_id, edge_type,
                metadata, created_at
            )
            SELECT 
                :edge_id, ln_file.node_id, :target_node_id, :edge_type,
                :metadata, :created_at
            FROM lineage_node ln_file
            WHERE ln_file.node_type = 'file' 
              AND ln_file.client_id = :client_id 
              AND ln_file.ingest_id = :ingest_id
            LIMIT 1
        """
        
        await session.execute(edge_query, {
            "edge_id": str(uuid4()),
            "target_node_id": lineage_id,
            "edge_type": "generates",
            "metadata": {"row_num": row_num},
            "created_at": datetime.utcnow(),
            "client_id": event.client_id,
            "ingest_id": event.ingest_id,
        })
    
    async def _emit_persistence_progress(self, event: NormalizationDoneEvent, processed_rows: int, total_rows: int):
        """Emit progress update for large files."""
        logger.info(f"Persistence progress: {processed_rows}/{total_rows} rows processed")
    
    async def _update_job_status(self, event: NormalizationDoneEvent, status: str):
        """Update job status."""
        async with self.database_session() as session:
            query = """
                UPDATE stg_file 
                SET status = :status, updated_at = NOW()
                WHERE client_id = :client_id AND ingest_id = :ingest_id
            """
            
            await session.execute(query, {
                "status": status,
                "client_id": event.client_id,
                "ingest_id": event.ingest_id,
            })
            
            # Update import_job status
            job_query = """
                UPDATE import_job 
                SET status = :status, completed_at = :completed_at, updated_at = NOW()
                WHERE client_id = :client_id AND ingest_id = :ingest_id
            """
            
            await session.execute(job_query, {
                "status": status,
                "completed_at": datetime.utcnow(),
                "client_id": event.client_id,
                "ingest_id": event.ingest_id,
            })
            
            await session.commit()
    
    async def _emit_persistence_upserted_event(self, event: NormalizationDoneEvent, upserted_products: List[Dict[str, Any]], issues: List[Dict[str, Any]]):
        """Emit persistence.upserted event."""
        persistence_event = PersistenceUpsertedEvent(
            event_id=f"persisted-{event.ingest_id}",
            trace_id=event.trace_id,
            client_id=event.client_id,
            ingest_id=event.ingest_id,
            source_agent=self.agent_name,
            idempotency_key=f"persisted-{event.client_id}-{event.ingest_id}",
            products_upserted=len(upserted_products),
            product_ids=[p["product_id"] for p in upserted_products],
            persistence_issues=issues,
        )
        
        await self.publish_event(persistence_event)
    
    async def _handle_persistence_error(self, event: NormalizationDoneEvent, error_message: str):
        """Handle persistence errors."""
        logger.error(f"Persistence error for {event.ingest_id}: {error_message}")
        
        # Update database status
        async with self.database_session() as session:
            query = """
                UPDATE stg_file 
                SET status = 'persistence_failed', 
                    updated_at = NOW()
                WHERE client_id = :client_id AND ingest_id = :ingest_id
            """
            
            await session.execute(query, {
                "client_id": event.client_id,
                "ingest_id": event.ingest_id,
            })
            
            job_query = """
                UPDATE import_job 
                SET status = 'failed', 
                    error_message = :error_message,
                    updated_at = NOW()
                WHERE client_id = :client_id AND ingest_id = :ingest_id
            """
            
            await session.execute(job_query, {
                "error_message": error_message,
                "client_id": event.client_id,
                "ingest_id": event.ingest_id,
            })
            
            await session.commit()
    
    async def on_initialize(self):
        """Initialize agent-specific components."""
        logger.info("PersistenceAgent initialized")
        
        # Could initialize deduplication models, similarity algorithms, etc.