"""
Export Agent - Handles BMEcat, ETIM, and other format exports.
"""

import json
import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from io import BytesIO, StringIO
from typing import Any, Dict, List, Optional
from uuid import uuid4
import csv

from shared.base_agent import BaseAgent, register_agent
from shared.events import BaseEvent, ExportRequestedEvent, ExportCompletedEvent

logger = logging.getLogger(__name__)


@register_agent("export")
class ExportAgent(BaseAgent):
    """
    ExportAgent generates exports in various formats (BMEcat, ETIM, CSV, JSON).
    
    Responsibilities:
    - Generate BMEcat XML with ETIM features
    - Create ETIM-native exports
    - Generate CSV/JSON exports with flexible schemas
    - Validate exports against XSD schemas
    - Handle large dataset streaming
    - Support incremental and full exports
    """
    
    def __init__(self, agent_name: str):
        super().__init__(agent_name)
        self.batch_size = self.config.get("export_batch_size", 10000)
        self.max_export_size = self.config.get("max_export_size", 1000000)
        
        # Export format handlers
        self.format_handlers = {
            "bmecat": self._export_bmecat,
            "etim": self._export_etim,
            "csv": self._export_csv,
            "json": self._export_json,
        }
    
    async def handle_event(self, event: BaseEvent):
        """Handle incoming events."""
        if isinstance(event, ExportRequestedEvent):
            await self._process_export_request(event)
        else:
            logger.warning(f"Unhandled event type: {event.event_type}")
    
    async def _process_export_request(self, event: ExportRequestedEvent):
        """Process an export request."""
        try:
            logger.info(f"Processing export request for client {event.client_id}")
            
            export_format = event.export_format.lower()
            if export_format not in self.format_handlers:
                raise ValueError(f"Unsupported export format: {export_format}")
            
            # Update export status to processing
            await self._update_export_status(event, "processing", {"stage": "starting", "percent": 0})
            
            # Get export handler
            handler = self.format_handlers[export_format]
            
            # Execute export
            export_result = await handler(event)
            
            # Validate export if requested
            if event.include_validation:
                validation_result = await self._validate_export(export_result, export_format)
                export_result["validation_report"] = validation_result
            
            # Upload export file to storage
            download_url = await self._upload_export_file(event, export_result)
            export_result["download_url"] = download_url
            
            # Update export status to completed
            await self._update_export_status(event, "completed", {
                "stage": "completed", 
                "percent": 100,
                "file_size": export_result.get("file_size", 0),
                "record_count": export_result.get("record_count", 0),
            })
            
            # Emit export.completed event
            await self._emit_export_completed_event(event, export_result)
            
            logger.info(f"Export completed successfully: {event.export_id}")
            
        except Exception as e:
            logger.error(f"Error processing export: {e}")
            await self._handle_export_error(event, str(e))
    
    async def _export_bmecat(self, event: ExportRequestedEvent) -> Dict[str, Any]:
        """Export data in BMEcat format."""
        logger.info(f"Generating BMEcat export for {event.client_id}")
        
        # Create BMEcat XML structure
        root = ET.Element("BMECAT", version="2005")
        
        # Header
        header = ET.SubElement(root, "HEADER")
        catalog = ET.SubElement(header, "CATALOG")
        
        # Catalog info
        ET.SubElement(catalog, "LANGUAGE").text = "en"
        ET.SubElement(catalog, "CATALOG_ID").text = f"{event.client_id}-{datetime.utcnow().strftime('%Y%m%d')}"
        ET.SubElement(catalog, "CATALOG_VERSION").text = "1.0"
        ET.SubElement(catalog, "CATALOG_NAME").text = f"Product Catalog - {event.client_id}"
        ET.SubElement(catalog, "CATALOG_DATE").text = datetime.utcnow().strftime("%Y-%m-%d")
        ET.SubElement(catalog, "CURRENCY").text = "EUR"
        
        # Supplier info
        supplier = ET.SubElement(header, "SUPPLIER")
        ET.SubElement(supplier, "SUPPLIER_ID").text = event.client_id
        ET.SubElement(supplier, "SUPPLIER_NAME").text = event.client_id.title()
        
        # T_NEW_CATALOG section
        t_new_catalog = ET.SubElement(root, "T_NEW_CATALOG")
        
        # Load products for export
        products = await self._load_products_for_export(event)
        
        await self._update_export_status(event, "processing", {"stage": "loading_data", "percent": 10})
        
        # Add products to BMEcat
        record_count = 0
        for i, product in enumerate(products):
            await self._add_product_to_bmecat(t_new_catalog, product, event)
            record_count += 1
            
            # Update progress
            if i % 1000 == 0:
                progress = min(90, 10 + (i / len(products)) * 80)
                await self._update_export_status(event, "processing", {
                    "stage": "generating_xml", 
                    "percent": progress,
                    "records_processed": i
                })
        
        # Convert to string
        xml_string = self._prettify_xml(root)
        
        return {
            "content": xml_string,
            "content_type": "application/xml",
            "file_extension": ".xml",
            "file_size": len(xml_string.encode('utf-8')),
            "record_count": record_count,
        }
    
    async def _add_product_to_bmecat(self, parent: ET.Element, product: Dict[str, Any], event: ExportRequestedEvent):
        """Add a single product to BMEcat XML."""
        article = ET.SubElement(parent, "ARTICLE")
        
        # Supplier AID
        ET.SubElement(article, "SUPPLIER_AID").text = product.get("sku", "")
        
        # Article details
        article_details = ET.SubElement(article, "ARTICLE_DETAILS")
        ET.SubElement(article_details, "DESCRIPTION_SHORT").text = product.get("product_name", "")
        ET.SubElement(article_details, "DESCRIPTION_LONG").text = product.get("description", "")
        ET.SubElement(article_details, "EAN").text = product.get("gtin", "")
        ET.SubElement(article_details, "MANUFACTURER_NAME").text = product.get("brand", "")
        ET.SubElement(article_details, "MANUFACTURER_AID").text = product.get("manufacturer_part_number", "")
        
        # ETIM classification
        if product.get("etim_class"):
            etim_class = ET.SubElement(article_details, "CLASSIFICATION_SYSTEM")
            ET.SubElement(etim_class, "CLASSIFICATION_SYSTEM_NAME").text = "ETIM"
            ET.SubElement(etim_class, "CLASSIFICATION_SYSTEM_VERSION").text = event.etim_version or "9.0"
            
            classification = ET.SubElement(etim_class, "CLASSIFICATION_GROUP")
            ET.SubElement(classification, "CLASSIFICATION_GROUP_ID").text = product["etim_class"]
            
            # Add ETIM features
            features = product.get("features", [])
            if features:
                feature_system = ET.SubElement(article_details, "FEATURE_SYSTEM")
                ET.SubElement(feature_system, "FEATURE_SYSTEM_NAME").text = "ETIM"
                
                for feature in features:
                    feature_elem = ET.SubElement(feature_system, "FEATURE")
                    ET.SubElement(feature_elem, "FNAME").text = feature.get("etim_feature", "")
                    ET.SubElement(feature_elem, "FVALUE").text = str(feature.get("value_normalized", ""))
                    if feature.get("unit_normalized"):
                        ET.SubElement(feature_elem, "FUNIT").text = feature["unit_normalized"]
        
        # Article order details
        order_details = ET.SubElement(article, "ARTICLE_ORDER_DETAILS")
        ET.SubElement(order_details, "ORDER_UNIT").text = "PCE"
        ET.SubElement(order_details, "CONTENT_UNIT").text = "1"
        
        # Price details
        if product.get("price"):
            price_details = ET.SubElement(article, "ARTICLE_PRICE_DETAILS")
            datetime_elem = ET.SubElement(price_details, "DATETIME")
            ET.SubElement(datetime_elem, "DATE").text = datetime.utcnow().strftime("%Y-%m-%d")
            ET.SubElement(datetime_elem, "TIME").text = datetime.utcnow().strftime("%H:%M:%S")
            
            price_elem = ET.SubElement(price_details, "ARTICLE_PRICE")
            ET.SubElement(price_elem, "PRICE_AMOUNT").text = str(product["price"])
            ET.SubElement(price_elem, "PRICE_CURRENCY").text = product.get("currency", "EUR")
            ET.SubElement(price_elem, "TAX").text = "0.19"  # Default 19% VAT
    
    async def _export_etim(self, event: ExportRequestedEvent) -> Dict[str, Any]:
        """Export data in ETIM native format."""
        logger.info(f"Generating ETIM export for {event.client_id}")
        
        # Create ETIM XML structure
        root = ET.Element("ETIM_Export")
        root.set("version", event.etim_version or "9.0")
        root.set("created", datetime.utcnow().isoformat())
        
        # Header
        header = ET.SubElement(root, "Header")
        ET.SubElement(header, "Supplier").text = event.client_id
        ET.SubElement(header, "Language").text = "en"
        ET.SubElement(header, "Currency").text = "EUR"
        
        # Products section
        products_elem = ET.SubElement(root, "Products")
        
        # Load products
        products = await self._load_products_for_export(event)
        
        record_count = 0
        for product in products:
            product_elem = ET.SubElement(products_elem, "Product")
            
            # Basic product info
            ET.SubElement(product_elem, "SupplierAID").text = product.get("sku", "")
            ET.SubElement(product_elem, "GTIN").text = product.get("gtin", "")
            ET.SubElement(product_elem, "ProductName").text = product.get("product_name", "")
            ET.SubElement(product_elem, "Brand").text = product.get("brand", "")
            
            # ETIM classification
            if product.get("etim_class"):
                classification = ET.SubElement(product_elem, "ETIMClassification")
                ET.SubElement(classification, "ClassCode").text = product["etim_class"]
                
                # Features
                features = product.get("features", [])
                if features:
                    features_elem = ET.SubElement(classification, "Features")
                    
                    for feature in features:
                        feature_elem = ET.SubElement(features_elem, "Feature")
                        ET.SubElement(feature_elem, "FeatureCode").text = feature.get("etim_feature", "")
                        ET.SubElement(feature_elem, "Value").text = str(feature.get("value_normalized", ""))
                        if feature.get("unit_normalized"):
                            ET.SubElement(feature_elem, "Unit").text = feature["unit_normalized"]
            
            record_count += 1
        
        xml_string = self._prettify_xml(root)
        
        return {
            "content": xml_string,
            "content_type": "application/xml",
            "file_extension": ".xml",
            "file_size": len(xml_string.encode('utf-8')),
            "record_count": record_count,
        }
    
    async def _export_csv(self, event: ExportRequestedEvent) -> Dict[str, Any]:
        """Export data in CSV format."""
        logger.info(f"Generating CSV export for {event.client_id}")
        
        # Load products
        products = await self._load_products_for_export(event)
        
        if not products:
            return {
                "content": "",
                "content_type": "text/csv",
                "file_extension": ".csv",
                "file_size": 0,
                "record_count": 0,
            }
        
        # Determine columns based on export profile
        columns = self._get_csv_columns(event.export_profile, products)
        
        # Generate CSV
        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=columns)
        writer.writeheader()
        
        record_count = 0
        for product in products:
            row = self._product_to_csv_row(product, columns)
            writer.writerow(row)
            record_count += 1
        
        csv_content = output.getvalue()
        
        return {
            "content": csv_content,
            "content_type": "text/csv",
            "file_extension": ".csv",
            "file_size": len(csv_content.encode('utf-8')),
            "record_count": record_count,
        }
    
    async def _export_json(self, event: ExportRequestedEvent) -> Dict[str, Any]:
        """Export data in JSON format."""
        logger.info(f"Generating JSON export for {event.client_id}")
        
        # Load products
        products = await self._load_products_for_export(event)
        
        # Create JSON structure
        export_data = {
            "metadata": {
                "export_id": str(event.export_id),
                "client_id": event.client_id,
                "export_format": "json",
                "export_profile": event.export_profile,
                "created_at": datetime.utcnow().isoformat(),
                "etim_version": event.etim_version,
                "record_count": len(products),
            },
            "products": products
        }
        
        json_content = json.dumps(export_data, indent=2, default=str)
        
        return {
            "content": json_content,
            "content_type": "application/json",
            "file_extension": ".json",
            "file_size": len(json_content.encode('utf-8')),
            "record_count": len(products),
        }
    
    async def _load_products_for_export(self, event: ExportRequestedEvent) -> List[Dict[str, Any]]:
        """Load products for export based on filter criteria."""
        async with self.database_session() as session:
            # Build query based on filter criteria
            where_conditions = ["p.lifecycle_status = 'active'"]
            params = {}
            
            # Client filter
            if event.client_id != "global":
                where_conditions.append("EXISTS (SELECT 1 FROM product_crosswalk pc WHERE pc.product_id = p.product_id AND pc.client_id = :client_id)")
                params["client_id"] = event.client_id
            
            # Apply additional filters
            filter_criteria = event.filter_criteria or {}
            
            if filter_criteria.get("etim_class"):
                where_conditions.append("p.etim_class = :etim_class")
                params["etim_class"] = filter_criteria["etim_class"]
            
            if filter_criteria.get("brand"):
                where_conditions.append("p.brand = :brand")
                params["brand"] = filter_criteria["brand"]
            
            if filter_criteria.get("updated_since"):
                where_conditions.append("p.updated_at >= :updated_since")
                params["updated_since"] = filter_criteria["updated_since"]
            
            where_clause = "WHERE " + " AND ".join(where_conditions)
            
            # Main product query
            query = f"""
                SELECT 
                    p.product_id, p.sku, p.gtin, p.product_name, p.brand,
                    p.description, p.category, p.manufacturer, p.model,
                    p.etim_class, p.price, p.currency, p.length, p.width,
                    p.height, p.diameter, p.weight, p.created_at, p.updated_at
                FROM product p
                {where_clause}
                ORDER BY p.updated_at DESC
                LIMIT :limit
            """
            
            params["limit"] = min(self.max_export_size, filter_criteria.get("limit", self.max_export_size))
            
            result = await session.execute(query, params)
            product_rows = result.fetchall()
            
            products = []
            for row in product_rows:
                product = {
                    "product_id": row[0],
                    "sku": row[1],
                    "gtin": row[2],
                    "product_name": row[3],
                    "brand": row[4],
                    "description": row[5],
                    "category": row[6],
                    "manufacturer": row[7],
                    "model": row[8],
                    "etim_class": row[9],
                    "price": float(row[10]) if row[10] else None,
                    "currency": row[11],
                    "length": float(row[12]) if row[12] else None,
                    "width": float(row[13]) if row[13] else None,
                    "height": float(row[14]) if row[14] else None,
                    "diameter": float(row[15]) if row[15] else None,
                    "weight": float(row[16]) if row[16] else None,
                    "created_at": row[17].isoformat() if row[17] else None,
                    "updated_at": row[18].isoformat() if row[18] else None,
                }
                
                # Load features for this product
                features = await self._load_product_features(row[0], session)
                product["features"] = features
                
                products.append(product)
            
            return products
    
    async def _load_product_features(self, product_id: str, session) -> List[Dict[str, Any]]:
        """Load ETIM features for a product."""
        query = """
            SELECT etim_feature, value_raw, unit_raw, value_normalized, unit_normalized
            FROM feature_value
            WHERE product_id = :product_id
            ORDER BY etim_feature
        """
        
        result = await session.execute(query, {"product_id": product_id})
        feature_rows = result.fetchall()
        
        features = []
        for row in feature_rows:
            features.append({
                "etim_feature": row[0],
                "value_raw": row[1],
                "unit_raw": row[2],
                "value_normalized": float(row[3]) if row[3] and str(row[3]).replace('.', '').isdigit() else row[3],
                "unit_normalized": row[4],
            })
        
        return features
    
    def _get_csv_columns(self, export_profile: str, products: List[Dict[str, Any]]) -> List[str]:
        """Determine CSV columns based on export profile."""
        base_columns = ["sku", "gtin", "product_name", "brand", "description", "etim_class"]
        
        if export_profile == "full":
            columns = base_columns + [
                "category", "manufacturer", "model", "price", "currency",
                "length", "width", "height", "diameter", "weight"
            ]
            
            # Add feature columns
            feature_columns = set()
            for product in products:
                for feature in product.get("features", []):
                    etim_feature = feature.get("etim_feature")
                    if etim_feature:
                        feature_columns.add(f"feature_{etim_feature}")
                        feature_columns.add(f"feature_{etim_feature}_unit")
            
            columns.extend(sorted(feature_columns))
            
        elif export_profile == "etim_only":
            columns = ["sku", "etim_class"]
            
            # Add all ETIM features
            feature_columns = set()
            for product in products:
                for feature in product.get("features", []):
                    etim_feature = feature.get("etim_feature")
                    if etim_feature:
                        feature_columns.add(f"feature_{etim_feature}")
                        feature_columns.add(f"feature_{etim_feature}_unit")
            
            columns.extend(sorted(feature_columns))
            
        elif export_profile == "pricing":
            columns = ["sku", "gtin", "product_name", "price", "currency"]
            
        else:  # standard
            columns = base_columns + ["price", "currency"]
        
        return columns
    
    def _product_to_csv_row(self, product: Dict[str, Any], columns: List[str]) -> Dict[str, Any]:
        """Convert product to CSV row."""
        row = {}
        
        # Basic fields
        for col in columns:
            if col.startswith("feature_"):
                continue  # Handle features separately
            row[col] = product.get(col, "")
        
        # Feature fields
        features_by_code = {f.get("etim_feature"): f for f in product.get("features", [])}
        
        for col in columns:
            if col.startswith("feature_"):
                if col.endswith("_unit"):
                    feature_code = col[8:-5]  # Remove "feature_" prefix and "_unit" suffix
                    feature = features_by_code.get(feature_code)
                    row[col] = feature.get("unit_normalized", "") if feature else ""
                else:
                    feature_code = col[8:]  # Remove "feature_" prefix
                    feature = features_by_code.get(feature_code)
                    row[col] = feature.get("value_normalized", "") if feature else ""
        
        return row
    
    def _prettify_xml(self, elem: ET.Element) -> str:
        """Return a pretty-printed XML string."""
        from xml.dom import minidom
        
        rough_string = ET.tostring(elem, encoding='unicode')
        reparsed = minidom.parseString(rough_string)
        return reparsed.toprettyxml(indent="  ")
    
    async def _validate_export(self, export_result: Dict[str, Any], export_format: str) -> Dict[str, Any]:
        """Validate export against schema/rules."""
        validation_result = {
            "valid": True,
            "warnings": 0,
            "errors": 0,
            "schema_version": "1.0",
            "validation_time": datetime.utcnow().isoformat(),
            "issues": []
        }
        
        try:
            content = export_result["content"]
            
            if export_format in ["bmecat", "etim"]:
                # XML validation
                try:
                    ET.fromstring(content)
                    validation_result["issues"].append({
                        "type": "info",
                        "message": "XML is well-formed"
                    })
                except ET.ParseError as e:
                    validation_result["valid"] = False
                    validation_result["errors"] += 1
                    validation_result["issues"].append({
                        "type": "error",
                        "message": f"XML parse error: {e}"
                    })
            
            elif export_format == "json":
                # JSON validation
                try:
                    json.loads(content)
                    validation_result["issues"].append({
                        "type": "info",
                        "message": "JSON is valid"
                    })
                except json.JSONDecodeError as e:
                    validation_result["valid"] = False
                    validation_result["errors"] += 1
                    validation_result["issues"].append({
                        "type": "error",
                        "message": f"JSON parse error: {e}"
                    })
            
            # Size validation
            file_size = export_result.get("file_size", 0)
            if file_size > 100 * 1024 * 1024:  # 100MB warning
                validation_result["warnings"] += 1
                validation_result["issues"].append({
                    "type": "warning",
                    "message": f"Large export file: {file_size / 1024 / 1024:.1f}MB"
                })
            
        except Exception as e:
            validation_result["valid"] = False
            validation_result["errors"] += 1
            validation_result["issues"].append({
                "type": "error",
                "message": f"Validation error: {e}"
            })
        
        return validation_result
    
    async def _upload_export_file(self, event: ExportRequestedEvent, export_result: Dict[str, Any]) -> str:
        """Upload export file to storage and return download URL."""
        content = export_result["content"]
        file_extension = export_result["file_extension"]
        
        # Generate filename
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"export_{event.client_id}_{timestamp}_{event.export_id}{file_extension}"
        
        # Upload to storage
        object_key = self.storage_manager.generate_object_key(
            event.client_id, str(event.export_id), filename, "exports"
        )
        
        file_obj = BytesIO(content.encode('utf-8'))
        
        uri = await self.storage_manager.upload_file(
            file_obj,
            object_key,
            content_type=export_result["content_type"],
            metadata={
                "export_id": str(event.export_id),
                "client_id": event.client_id,
                "export_format": event.export_format,
                "record_count": str(export_result.get("record_count", 0)),
            }
        )
        
        # Generate signed download URL (valid for 24 hours)
        download_url = await self.storage_manager.generate_presigned_url(
            object_key, expiration=86400
        )
        
        return download_url
    
    async def _update_export_status(self, event: ExportRequestedEvent, status: str, progress: Dict[str, Any]):
        """Update export status (would typically update a database table)."""
        logger.info(f"Export {event.export_id} status: {status} - {progress}")
        
        # In a full implementation, this would update an exports table
        # For now, just log the status
    
    async def _emit_export_completed_event(self, event: ExportRequestedEvent, export_result: Dict[str, Any]):
        """Emit export.completed event."""
        completed_event = ExportCompletedEvent(
            event_id=f"export-completed-{event.export_id}",
            trace_id=event.trace_id,
            client_id=event.client_id,
            ingest_id=f"export-{event.export_id}",
            source_agent=self.agent_name,
            idempotency_key=f"export-completed-{event.client_id}-{event.export_id}",
            export_id=event.export_id,
            export_format=event.export_format,
            download_url=export_result["download_url"],
            file_size=export_result["file_size"],
            record_count=export_result["record_count"],
            validation_report=export_result.get("validation_report"),
        )
        
        await self.publish_event(completed_event)
    
    async def _handle_export_error(self, event: ExportRequestedEvent, error_message: str):
        """Handle export errors."""
        logger.error(f"Export error for {event.export_id}: {error_message}")
        
        # Update export status to failed
        await self._update_export_status(event, "failed", {
            "stage": "failed",
            "percent": 0,
            "error": error_message
        })
    
    async def on_initialize(self):
        """Initialize agent-specific components."""
        logger.info("ExportAgent initialized")
        
        # Could load XSD schemas, initialize validation libraries, etc.