-- CSV Import Guardian Database Schema
-- Optimized for multi-tenant CSV processing with ETIM support

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Staging tables for raw CSV data
CREATE TABLE stg_file (
    client_id TEXT NOT NULL,
    ingest_id TEXT NOT NULL,
    uri TEXT NOT NULL,
    filename TEXT NOT NULL,
    size_bytes BIGINT,
    encoding TEXT,
    delimiter TEXT,
    has_header BOOLEAN DEFAULT TRUE,
    row_count INTEGER,
    column_count INTEGER,
    profile JSONB,
    status TEXT DEFAULT 'uploaded',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (client_id, ingest_id)
);

CREATE TABLE stg_row (
    client_id TEXT NOT NULL,
    ingest_id TEXT NOT NULL,
    row_num INTEGER NOT NULL,
    data JSONB NOT NULL,
    row_hash BYTEA,
    issues JSONB DEFAULT '[]'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (client_id, ingest_id, row_num),
    FOREIGN KEY (client_id, ingest_id) REFERENCES stg_file(client_id, ingest_id)
);

-- Canonical product schema
CREATE TABLE product (
    product_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    client_id TEXT NOT NULL,
    sku TEXT NOT NULL,
    gtin TEXT,
    brand TEXT,
    manufacturer TEXT,
    product_name TEXT,
    description TEXT,
    category TEXT,
    etim_class TEXT,
    lifecycle_status TEXT DEFAULT 'active',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (client_id, sku)
);

CREATE TABLE feature_value (
    product_id UUID NOT NULL REFERENCES product(product_id) ON DELETE CASCADE,
    etim_feature TEXT NOT NULL,
    value_raw TEXT,
    unit_raw TEXT,
    value_norm NUMERIC,
    unit_norm TEXT,
    confidence NUMERIC DEFAULT 1.0,
    source_column TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (product_id, etim_feature)
);

-- Mapping metadata
CREATE TABLE map_template (
    template_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    client_id TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    version INTEGER DEFAULT 1,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (client_id, name, version)
);

CREATE TABLE map_field (
    template_id UUID NOT NULL REFERENCES map_template(template_id) ON DELETE CASCADE,
    src_column TEXT NOT NULL,
    target_table TEXT NOT NULL,
    target_column TEXT NOT NULL,
    transform_type TEXT DEFAULT 'direct',
    transform_config JSONB DEFAULT '{}'::jsonb,
    is_required BOOLEAN DEFAULT FALSE,
    default_value TEXT,
    validation_rules JSONB DEFAULT '[]'::jsonb,
    PRIMARY KEY (template_id, src_column, target_table, target_column)
);

CREATE TABLE map_feature (
    template_id UUID NOT NULL REFERENCES map_template(template_id) ON DELETE CASCADE,
    src_column TEXT NOT NULL,
    etim_feature TEXT NOT NULL,
    unit_column TEXT,
    transform_type TEXT DEFAULT 'direct',
    transform_config JSONB DEFAULT '{}'::jsonb,
    is_required BOOLEAN DEFAULT FALSE,
    validation_rules JSONB DEFAULT '[]'::jsonb,
    PRIMARY KEY (template_id, src_column, etim_feature)
);

-- Validation rules
CREATE TABLE validation_rule (
    rule_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT NOT NULL,
    description TEXT,
    rule_type TEXT NOT NULL, -- 'required', 'format', 'range', 'domain', 'etim'
    target_type TEXT NOT NULL, -- 'field', 'feature', 'product'
    target_name TEXT NOT NULL,
    rule_config JSONB NOT NULL,
    severity TEXT DEFAULT 'ERROR', -- 'ERROR', 'WARN', 'INFO'
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Job tracking
CREATE TABLE import_job (
    job_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    client_id TEXT NOT NULL,
    ingest_id TEXT NOT NULL,
    template_id UUID REFERENCES map_template(template_id),
    status TEXT DEFAULT 'created', -- 'created', 'profiling', 'mapping', 'validating', 'executing', 'completed', 'failed'
    progress JSONB DEFAULT '{}'::jsonb,
    error_message TEXT,
    started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (client_id, ingest_id) REFERENCES stg_file(client_id, ingest_id)
);

-- Validation results
CREATE TABLE validation_result (
    result_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_id UUID NOT NULL REFERENCES import_job(job_id) ON DELETE CASCADE,
    rule_id UUID NOT NULL REFERENCES validation_rule(rule_id),
    client_id TEXT NOT NULL,
    ingest_id TEXT NOT NULL,
    row_num INTEGER,
    severity TEXT NOT NULL,
    message TEXT NOT NULL,
    context JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Data lineage
CREATE TABLE lineage_node (
    node_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    node_type TEXT NOT NULL, -- 'file', 'job', 'template', 'product', 'export'
    node_name TEXT NOT NULL,
    client_id TEXT NOT NULL,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (node_type, node_name, client_id)
);

CREATE TABLE lineage_edge (
    edge_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    from_node_id UUID NOT NULL REFERENCES lineage_node(node_id),
    to_node_id UUID NOT NULL REFERENCES lineage_node(node_id),
    edge_type TEXT NOT NULL, -- 'processes', 'generates', 'uses', 'derives_from'
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (from_node_id, to_node_id, edge_type)
);

-- ETIM reference data (simplified)
CREATE TABLE etim_class (
    class_code TEXT PRIMARY KEY,
    class_name TEXT NOT NULL,
    parent_class TEXT,
    description TEXT,
    version TEXT DEFAULT '9.0'
);

CREATE TABLE etim_feature (
    feature_code TEXT PRIMARY KEY,
    feature_name TEXT NOT NULL,
    data_type TEXT NOT NULL, -- 'N' (numeric), 'A' (alphanumeric), 'L' (logical)
    unit TEXT,
    description TEXT,
    version TEXT DEFAULT '9.0'
);

CREATE TABLE etim_class_feature (
    class_code TEXT NOT NULL REFERENCES etim_class(class_code),
    feature_code TEXT NOT NULL REFERENCES etim_feature(feature_code),
    is_mandatory BOOLEAN DEFAULT FALSE,
    sort_order INTEGER,
    PRIMARY KEY (class_code, feature_code)
);

-- Unit of measure mappings
CREATE TABLE uom_mapping (
    source_unit TEXT NOT NULL,
    target_unit TEXT NOT NULL,
    conversion_factor NUMERIC NOT NULL,
    conversion_offset NUMERIC DEFAULT 0,
    unit_type TEXT, -- 'length', 'weight', 'volume', etc.
    PRIMARY KEY (source_unit, target_unit)
);

-- Indexes for performance
CREATE INDEX idx_stg_row_client_ingest ON stg_row(client_id, ingest_id);
CREATE INDEX idx_stg_row_hash ON stg_row USING hash(row_hash);
CREATE INDEX idx_product_client_sku ON product(client_id, sku);
CREATE INDEX idx_product_gtin ON product(gtin) WHERE gtin IS NOT NULL;
CREATE INDEX idx_feature_value_product ON feature_value(product_id);
CREATE INDEX idx_feature_value_feature ON feature_value(etim_feature);
CREATE INDEX idx_validation_result_job ON validation_result(job_id);
CREATE INDEX idx_validation_result_severity ON validation_result(severity);
CREATE INDEX idx_lineage_from_node ON lineage_edge(from_node_id);
CREATE INDEX idx_lineage_to_node ON lineage_edge(to_node_id);

-- Row Level Security (RLS)
ALTER TABLE stg_file ENABLE ROW LEVEL SECURITY;
ALTER TABLE stg_row ENABLE ROW LEVEL SECURITY;
ALTER TABLE product ENABLE ROW LEVEL SECURITY;
ALTER TABLE feature_value ENABLE ROW LEVEL SECURITY;
ALTER TABLE import_job ENABLE ROW LEVEL SECURITY;
ALTER TABLE validation_result ENABLE ROW LEVEL SECURITY;
ALTER TABLE lineage_node ENABLE ROW LEVEL SECURITY;

-- Sample ETIM data
INSERT INTO etim_class (class_code, class_name, description) VALUES
('EC000001', 'Fastener', 'General fastening elements'),
('EC000123', 'Screw', 'Threaded fasteners'),
('EC000456', 'Bolt', 'Threaded fasteners with nuts');

INSERT INTO etim_feature (feature_code, feature_name, data_type, unit, description) VALUES
('EF001001', 'Length', 'N', 'mm', 'Overall length'),
('EF001002', 'Diameter', 'N', 'mm', 'Nominal diameter'),
('EF001003', 'Material', 'A', '', 'Material designation'),
('EF001004', 'Thread pitch', 'N', 'mm', 'Distance between threads'),
('EF001005', 'Head type', 'A', '', 'Type of screw head');

INSERT INTO etim_class_feature (class_code, feature_code, is_mandatory, sort_order) VALUES
('EC000123', 'EF001001', TRUE, 1),
('EC000123', 'EF001002', TRUE, 2),
('EC000123', 'EF001003', FALSE, 3),
('EC000123', 'EF001004', TRUE, 4),
('EC000123', 'EF001005', FALSE, 5);

-- Sample UoM mappings
INSERT INTO uom_mapping (source_unit, target_unit, conversion_factor, unit_type) VALUES
('mm', 'cm', 0.1, 'length'),
('cm', 'mm', 10.0, 'length'),
('m', 'mm', 1000.0, 'length'),
('kg', 'g', 1000.0, 'weight'),
('g', 'kg', 0.001, 'weight'),
('lb', 'kg', 0.453592, 'weight');

-- Sample validation rules
INSERT INTO validation_rule (name, description, rule_type, target_type, target_name, rule_config, severity) VALUES
('Required SKU', 'Product SKU must be present', 'required', 'field', 'sku', '{}', 'ERROR'),
('GTIN Format', 'GTIN must be 8, 12, 13, or 14 digits', 'format', 'field', 'gtin', '{"pattern": "^\\d{8}|\\d{12}|\\d{13}|\\d{14}$"}', 'ERROR'),
('Positive Length', 'Length must be positive', 'range', 'feature', 'EF001001', '{"min": 0, "exclusive_min": true}', 'ERROR'),
('Valid Material', 'Material must be from approved list', 'domain', 'feature', 'EF001003', '{"values": ["Steel", "Stainless Steel", "Brass", "Aluminum"]}', 'WARN');

-- Functions for common operations
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Triggers for updated_at
CREATE TRIGGER update_stg_file_updated_at BEFORE UPDATE ON stg_file FOR EACH ROW EXECUTE FUNCTION update_updated_at();
CREATE TRIGGER update_product_updated_at BEFORE UPDATE ON product FOR EACH ROW EXECUTE FUNCTION update_updated_at();
CREATE TRIGGER update_feature_value_updated_at BEFORE UPDATE ON feature_value FOR EACH ROW EXECUTE FUNCTION update_updated_at();
CREATE TRIGGER update_map_template_updated_at BEFORE UPDATE ON map_template FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- Data Quality Reports table
CREATE TABLE dq_report (
    report_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id VARCHAR(100) NOT NULL,
    ingest_id VARCHAR(255) NOT NULL,
    quality_score DECIMAL(5,4) NOT NULL,
    error_count INTEGER DEFAULT 0,
    warning_count INTEGER DEFAULT 0,
    info_count INTEGER DEFAULT 0,
    report_urls JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_dq_report_client ON dq_report(client_id);
CREATE INDEX idx_dq_report_ingest ON dq_report(ingest_id);
CREATE INDEX idx_dq_report_created ON dq_report(created_at);
CREATE INDEX idx_dq_report_quality ON dq_report(quality_score);