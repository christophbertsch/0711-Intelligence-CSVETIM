#!/usr/bin/env python3
"""
Database initialization script for CSV Import Guardian Agent System.
"""

import asyncio
import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from shared.config import get_settings
from shared.database import get_database_manager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def init_database():
    """Initialize the database with schema and sample data."""
    try:
        logger.info("Initializing database...")
        
        # Get database manager
        db_manager = await get_database_manager()
        
        # Test connection
        async with db_manager.get_async_session() as session:
            result = await session.execute("SELECT 1 as test")
            row = result.fetchone()
            if row and row[0] == 1:
                logger.info("Database connection successful")
            else:
                raise Exception("Database connection test failed")
        
        logger.info("Database initialized successfully")
        
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        sys.exit(1)


async def create_sample_data():
    """Create sample data for testing."""
    try:
        logger.info("Creating sample data...")
        
        db_manager = await get_database_manager()
        
        async with db_manager.get_async_session() as session:
            # Create sample client template
            template_query = """
                INSERT INTO map_template (template_id, client_id, name, description, version, is_active)
                VALUES (
                    uuid_generate_v4(),
                    'demo',
                    'Demo Fastener Template',
                    'Sample template for fastener products',
                    1,
                    true
                )
                ON CONFLICT (client_id, name, version) DO NOTHING
                RETURNING template_id
            """
            
            result = await session.execute(template_query)
            template_row = result.fetchone()
            
            if template_row:
                template_id = template_row[0]
                logger.info(f"Created sample template: {template_id}")
                
                # Add field mappings
                field_mappings = [
                    ("SKU", "product", "sku", True),
                    ("Name", "product", "product_name", False),
                    ("Brand", "product", "brand", False),
                    ("GTIN", "product", "gtin", False),
                ]
                
                for src_col, target_table, target_col, is_required in field_mappings:
                    field_query = """
                        INSERT INTO map_field (
                            template_id, src_column, target_table, target_column,
                            transform_type, is_required
                        ) VALUES (
                            :template_id, :src_column, :target_table, :target_column,
                            'direct', :is_required
                        )
                        ON CONFLICT (template_id, src_column, target_table, target_column) DO NOTHING
                    """
                    
                    await session.execute(field_query, {
                        "template_id": template_id,
                        "src_column": src_col,
                        "target_table": target_table,
                        "target_column": target_col,
                        "is_required": is_required,
                    })
                
                # Add feature mappings
                feature_mappings = [
                    ("Length", "EF001001", "Length_Unit"),
                    ("Diameter", "EF001002", "Diameter_Unit"),
                    ("Material", "EF001003", None),
                ]
                
                for src_col, etim_feature, unit_col in feature_mappings:
                    feature_query = """
                        INSERT INTO map_feature (
                            template_id, src_column, etim_feature, unit_column,
                            transform_type, is_required
                        ) VALUES (
                            :template_id, :src_column, :etim_feature, :unit_column,
                            'direct', false
                        )
                        ON CONFLICT (template_id, src_column, etim_feature) DO NOTHING
                    """
                    
                    await session.execute(feature_query, {
                        "template_id": template_id,
                        "src_column": src_col,
                        "etim_feature": etim_feature,
                        "unit_column": unit_col,
                    })
                
                await session.commit()
                logger.info("Sample mappings created")
            
        logger.info("Sample data created successfully")
        
    except Exception as e:
        logger.error(f"Sample data creation failed: {e}")


async def main():
    """Main function."""
    logger.info("Starting database initialization...")
    
    await init_database()
    await create_sample_data()
    
    logger.info("Database initialization complete!")


if __name__ == "__main__":
    asyncio.run(main())