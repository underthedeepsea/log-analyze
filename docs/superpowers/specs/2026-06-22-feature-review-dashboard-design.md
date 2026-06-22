# Feature Review Dashboard Design

## Goal

Provide a temporary local web application that accepts a generated `result.json`, uses local Ollama only to identify important log features, lets an operator review every candidate, and exports approved features for manual import into an external RCA expert system. This repository must not generate RCA conclusions, impact assessments, or remediation advice.

## Architecture

The existing batch pipeline remains responsible for normalization, Drain3 mining, window aggregation, and risk scoring. It stops after producing `risk_entities` and related debug artifacts; `rca_results` and all Mock/Ollama RCA code are removed.

A lightweight Python standard-library HTTP service hosts a vanilla HTML/CSS/JavaScript dashboard on `127.0.0.1:8080`. The browser parses and validates an uploaded `result.json`, then sends it to the service. The service stores jobs only in memory and processes eligible risk entities serially, ordered by descending risk score. Server-Sent Events (SSE) publish queued, running, completed, failed, retried, and job-completed states without blocking the page.

Ollama remains local at `http://127.0.0.1:11434`, with `qwen3:1.7b` as the dashboard default and environment/CLI overrides. A single failed entity does not stop the remaining queue.

## Feature Extraction Contract

Ollama receives only scored, aggregated evidence: entity identity, risk score/level, component, severity, template text/hash, category, counts, first/last seen timestamps, affected entities, and rule hints. `samples`, `raw_sample`, and raw log text are excluded.

For each entity above the configurable threshold (default `40`), Ollama returns candidate features with:

- `feature_type`, `title`, `summary`, and `importance`;
- related components, template hashes, tags, and selection reason.

The service validates the model response and attaches authoritative source facts itself: candidate ID, entity, cluster, risk information, occurrence count, time range, affected entities, and sanitized source templates. The model cannot overwrite those fields. Candidate IDs are stable hashes of the entity, time window, feature type, and sorted template hashes.

## Review Workflow

The selected UI is a dark operations dashboard with upload controls, metrics, extraction queue, progress, candidate list, and review details visible together.

Each feature begins as `pending`. The operator may edit its title, summary, importance, tags, and reviewer note, then mark it `approved` or `rejected`. Decisions remain in the in-memory job. Failed entity extraction can be retried individually. Export is disabled until at least one feature is approved.

The service exposes these local interfaces:

- `POST /api/jobs` — validate input and create an extraction job;
- `GET /api/jobs/{job_id}` — current snapshot for initial load or reconnect;
- `GET /api/jobs/{job_id}/events` — SSE progress stream with heartbeat;
- `POST /api/jobs/{job_id}/entities/{entity_id}/retry` — retry a failed entity;
- `PATCH /api/jobs/{job_id}/features/{candidate_id}` — edit or decide a feature;
- `POST /api/jobs/{job_id}/export` — return the approved JSON package;
- `GET /api/ollama/status` — report local service/model availability.

## Export Package

The generic JSON package uses `schema_version: "1.0"` and contains generation time, source summary, model metadata, review statistics, and `approved_features`. Each approved feature includes its stable ID, approval timestamp, reviewer note, entity/risk facts, edited feature fields, time/count facts, affected entities, and sanitized source templates. Rejected and pending candidates are never exported.

## Validation and Failure Handling

Uploads must be JSON objects containing a `risk_entities` array and are limited to 10 MB. Invalid files are rejected before job creation. Ollama connection, timeout, HTTP, JSON, and schema errors are attached to the affected entity while the queue continues. SSE reconnects use the snapshot endpoint as the source of truth. Only loopback binding is supported by default; no database, authentication, Kafka, Elasticsearch, or external network service is introduced.

## Testing and Acceptance

Unit tests cover evidence sanitization, structured response validation, stable candidate IDs, upload validation, serial ordering, failure continuation, review transitions, editing rules, and export filtering. HTTP integration tests cover job creation, snapshot, SSE events, retry, review, export, and static assets without requiring a live Ollama instance.

Manual acceptance uses the existing sample `result.json` and local `qwen3:1.7b`: upload returns immediately, progress changes while extraction runs, candidates appear as entities finish, individual features can be edited/approved/rejected, and the downloaded package contains only approved sanitized features with no RCA fields or raw logs.
