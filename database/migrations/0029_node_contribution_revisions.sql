ALTER TABLE node_risk_ingestions ADD COLUMN physical_key TEXT;
ALTER TABLE node_risk_ingestions ADD COLUMN semantic_revision TEXT;
ALTER TABLE node_risk_ingestions ADD COLUMN is_current INTEGER NOT NULL DEFAULT 1;
ALTER TABLE node_risk_ingestions ADD COLUMN contribution_json TEXT;
CREATE UNIQUE INDEX uq_node_risk_current_physical ON node_risk_ingestions(physical_key) WHERE is_current=1 AND physical_key IS NOT NULL;
ALTER TABLE node_risk_events ADD COLUMN derivation_status TEXT NOT NULL DEFAULT 'current';
ALTER TABLE node_risk_events ADD COLUMN current_occurrence_count INTEGER;
