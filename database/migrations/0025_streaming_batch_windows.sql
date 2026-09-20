CREATE TABLE streaming_batch_windows (
    task_id TEXT NOT NULL REFERENCES streaming_tasks(task_id) ON DELETE CASCADE,
    window_id TEXT NOT NULL,
    item_index INTEGER NOT NULL CHECK (item_index >= 0),
    window_json TEXT NOT NULL,
    PRIMARY KEY (task_id, window_id, item_index),
    FOREIGN KEY (task_id, window_id) REFERENCES streaming_window_commits(task_id, window_id) ON DELETE CASCADE
);

ALTER TABLE streaming_window_commits ADD COLUMN payload_hash TEXT;

CREATE INDEX idx_streaming_batch_windows_iteration
    ON streaming_batch_windows(task_id, window_id, item_index);
