ALTER TABLE multi_source_correlations ADD COLUMN edges_json JSONB NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE multi_source_correlations ADD COLUMN group_scope TEXT NOT NULL DEFAULT 'shared_entity';
ALTER TABLE multi_source_correlations ADD COLUMN partial BOOLEAN NOT NULL DEFAULT FALSE;
