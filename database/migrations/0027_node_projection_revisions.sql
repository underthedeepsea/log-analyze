CREATE TABLE node_risk_projection_revisions (
    cluster TEXT NOT NULL,
    node_id TEXT NOT NULL,
    revision INTEGER NOT NULL DEFAULT 0,
    projected_revision INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (cluster, node_id)
);
CREATE INDEX idx_node_risk_projection_dirty ON node_risk_projection_revisions(projected_revision, revision);
CREATE TABLE node_risk_score_samples (
    sample_id TEXT PRIMARY KEY,
    cluster TEXT NOT NULL,
    node_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    assessed_at TEXT NOT NULL,
    overall_score REAL NOT NULL
);
CREATE INDEX idx_node_risk_score_samples_day ON node_risk_score_samples(cluster, node_id, assessed_at);
