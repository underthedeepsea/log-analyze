ALTER TABLE multi_source_correlations ADD COLUMN edges_json TEXT NOT NULL DEFAULT '[]';
ALTER TABLE multi_source_correlations ADD COLUMN group_scope TEXT NOT NULL DEFAULT 'shared_entity';
ALTER TABLE multi_source_correlations ADD COLUMN partial INTEGER NOT NULL DEFAULT 0 CHECK (partial IN (0, 1));
