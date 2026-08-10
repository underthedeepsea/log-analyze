CREATE TABLE IF NOT EXISTS knowledge_packages (
    package_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS knowledge_package_versions (
    package_id TEXT NOT NULL REFERENCES knowledge_packages(package_id) ON DELETE CASCADE,
    version TEXT NOT NULL,
    manifest_json JSONB NOT NULL,
    package_sha256 TEXT NOT NULL CHECK (length(package_sha256) = 64),
    artifact_path TEXT NOT NULL,
    compressed_bytes BIGINT NOT NULL CHECK (compressed_bytes >= 0),
    expanded_bytes BIGINT NOT NULL CHECK (expanded_bytes >= 0),
    platform_min_version TEXT NOT NULL,
    platform_max_version_exclusive TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('installed', 'retired')),
    installed_by TEXT NOT NULL,
    installed_at TIMESTAMPTZ NOT NULL,
    state_version INTEGER NOT NULL DEFAULT 1 CHECK (state_version >= 1),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (package_id, version)
);

CREATE INDEX IF NOT EXISTS idx_knowledge_package_versions_status
    ON knowledge_package_versions(status, updated_at DESC);

CREATE TABLE IF NOT EXISTS knowledge_package_assets (
    package_id TEXT NOT NULL,
    version TEXT NOT NULL,
    asset_id TEXT NOT NULL,
    asset_type TEXT NOT NULL,
    asset_path TEXT NOT NULL,
    asset_sha256 TEXT NOT NULL CHECK (length(asset_sha256) = 64),
    media_type TEXT NOT NULL,
    target_domain TEXT,
    target_resource_id TEXT,
    target_version TEXT,
    status TEXT NOT NULL CHECK (status IN ('disabled', 'materialized', 'retired', 'failed')),
    error_code TEXT,
    error_message TEXT,
    state_version INTEGER NOT NULL DEFAULT 1 CHECK (state_version >= 1),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (package_id, version, asset_id),
    FOREIGN KEY (package_id, version) REFERENCES knowledge_package_versions(package_id, version) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_knowledge_package_assets_status
    ON knowledge_package_assets(status, updated_at DESC);

CREATE TABLE IF NOT EXISTS knowledge_package_dependencies (
    package_id TEXT NOT NULL,
    version TEXT NOT NULL,
    dependency_package_id TEXT NOT NULL,
    dependency_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (package_id, version, dependency_package_id, dependency_version),
    FOREIGN KEY (package_id, version) REFERENCES knowledge_package_versions(package_id, version) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS knowledge_package_imports (
    upload_id TEXT PRIMARY KEY,
    filename TEXT NOT NULL,
    staging_path TEXT,
    artifact_path TEXT,
    package_sha256 TEXT CHECK (package_sha256 IS NULL OR length(package_sha256) = 64),
    compressed_bytes BIGINT NOT NULL CHECK (compressed_bytes >= 0),
    expanded_bytes BIGINT NOT NULL DEFAULT 0 CHECK (expanded_bytes >= 0),
    status TEXT NOT NULL CHECK (status IN ('uploaded', 'validating', 'validated', 'installing', 'installed', 'rejected', 'failed')),
    report_json JSONB NOT NULL,
    confirmed_by TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_knowledge_package_imports_status
    ON knowledge_package_imports(status, updated_at DESC);

CREATE TABLE IF NOT EXISTS knowledge_package_audit_events (
    audit_id TEXT PRIMARY KEY,
    package_id TEXT,
    version TEXT,
    asset_id TEXT,
    action TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('success', 'failed', 'denied')),
    actor TEXT,
    roles_json JSONB NOT NULL,
    request_id TEXT,
    attributes_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_knowledge_package_audit_created
    ON knowledge_package_audit_events(created_at DESC, audit_id DESC);
CREATE INDEX IF NOT EXISTS idx_knowledge_package_audit_resource
    ON knowledge_package_audit_events(package_id, version, asset_id, created_at DESC);
