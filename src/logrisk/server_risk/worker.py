from __future__ import annotations

import queue
import threading
from typing import Any

from logrisk.server_risk.contracts import ServerRiskError


class ReportWorker:
    """One local executor; no model or queue is started until explicitly dispatched."""

    def __init__(self, reports: Any) -> None:
        self.reports = reports
        self.queue: queue.Queue[str | None] = queue.Queue(maxsize=64)
        self.lock = threading.Lock()
        self.thread: threading.Thread | None = None
        self.closed = threading.Event()
        self.active_report_id: str | None = None

    def submit(self, report_id: str) -> None:
        if self.closed.is_set():
            raise ServerRiskError("report_worker_closed", "报告Worker已关闭", 503)
        try:
            self.queue.put_nowait(report_id)
        except queue.Full as exc:
            self.reports.fail(report_id, "report_queue_full")
            raise ServerRiskError("report_queue_full", "报告队列已满", 503) from exc
        with self.lock:
            if self.thread is None or not self.thread.is_alive():
                self.thread = threading.Thread(target=self._run, daemon=True, name="node-risk-report-worker")
                self.thread.start()

    def _run(self) -> None:
        while True:
            report_id = self.queue.get()
            try:
                if report_id is None:
                    return
                self.active_report_id = report_id
                if self.closed.is_set():
                    self.reports.cancel(report_id)
                self.reports.execute(report_id)
            finally:
                self.active_report_id = None
                self.queue.task_done()

    def close(self) -> None:
        if self.closed.is_set():
            return
        self.closed.set()
        if self.thread and self.thread.is_alive():
            if self.active_report_id:
                self.reports.cancel(self.active_report_id)
            while True:
                try:
                    pending = self.queue.get_nowait()
                except queue.Empty:
                    break
                if pending is not None:
                    self.reports.cancel(pending)
                self.queue.task_done()
            self.queue.put_nowait(None)
            self.thread.join(timeout=1)
            if self.thread.is_alive():
                raise ServerRiskError("report_worker_cleanup_failed", "报告执行资源尚未回收")
