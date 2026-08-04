# Django 与 Airflow 生产部署指南

本方案将 LOGRISK 接入已有 **Django 4.2.16** 控制面与 **Airflow 2.3.2** `CeleryExecutor`。Django、Airflow 元数据库和 LOGRISK PostgreSQL 必须是三个独立数据库；Django 不创建 LOGRISK Model，也不直接读写 LOGRISK 表。

## 组件边界

- Django：复用 PACAS/RBAC 身份，提供同源 API 与静态页面入口。
- Airflow：`logrisk_input_preprocess` DAG 负责已上传日志的预处理和 Drain3，`logrisk_analysis` DAG 负责规则复用和模型特征识别；CPU 任务使用 `logrisk_cpu_pool` / `logrisk_cpu`，模型任务使用 `logrisk_llm_pool` / `logrisk_llm`。
- LOGRISK PostgreSQL：唯一的结构化业务状态权威。
- `LOGRISK_SHARED_ROOT`：所有 Web 与 Worker 都能读写的共享目录，保存上传本体、Drain3 产物和导出文件；数据库只保存受控相对路径和摘要。

## 安装与配置

在 Django、Airflow Scheduler 和每台 Celery Worker 安装同一版本的 LOGRISK 代码。Django 环境安装：

```bash
pip install -r requirements.txt -r requirements-django.txt
```

PostgreSQL 驱动通过 `requirements-postgres.txt` 安装。使用环境变量配置，不把真实密码、DSN 或 Token 写入 Settings、Airflow Variable、DAG conf 或 XCom：

```bash
export LOGRISK_DATABASE_URL='由密钥系统注入'
export LOGRISK_SHARED_ROOT='/mnt/logrisk'
export LOGRISK_AIRFLOW_URL='https://airflow.example.internal'
export LOGRISK_AIRFLOW_TOKEN='由密钥系统注入'
```

将 `logrisk_django` 加入 Django `INSTALLED_APPS`，并通过示例 URL 配置挂载路由。反向代理应将 `/assets/` 映射到 `collectstatic` 后的 `logrisk/` 静态资源；`/api/` 始终优先于 SPA 回退。

## 显式迁移与上线顺序

服务启动不会自动迁移数据库。停机窗口内先执行：

```bash
python manage.py logrisk_migrate --check --json
python manage.py logrisk_migrate --json
python manage.py logrisk_check --json
```

确认 `pending_migrations=0` 且共享目录可写后，部署 Django、`logrisk_input_preprocess` / `logrisk_analysis` 两个 Airflow DAG 和 Worker。上传完成后，Django 先持久化输入编排记录，再只向 `logrisk_input_preprocess` 传递输入任务 ID、编排运行 ID 和请求 ID；完成后浏览器再按既有流程创建特征任务。Airflow 恢复时可运行 `python manage.py logrisk_reconcile_dispatch --json`，它只重试 `pending_dispatch` 或 `dispatch_failed` 的特征与输入编排记录，不启动本地回退。

若 Airflow 已经接受 DAG Run，但 Worker 在完成回写前中断，可先运行 `python manage.py logrisk_reconcile_runs --dry-run --json` 查看活动运行，再去掉 `--dry-run` 同步 `dispatched`、`running` 和 `cancel_requested` 状态。该命令会校验 DAG Run 的任务/编排标识；网络错误、标识不匹配和未知状态只记录为本次报告错误，不会猜测为成功，也不会把 DAG conf、XCom 或日志内容写入 LOGRISK。

## 安全与回滚

PACAS/RBAC 是唯一身份权威。Django 写请求要求现有认证用户和配置角色；运行审计仅保存操作、主体、角色、请求 ID 和脱敏状态。原始日志不得进入 DAG conf 或 XCom，亦不得写入 Trace、审计或错误响应。

回滚时停止 Django、Scheduler 和 Worker，回退代码与 DAG 后先运行 `logrisk_migrate --check`。数据库 migration 不回滚；如需恢复数据，使用迁移前的 PostgreSQL 备份。保留共享目录，并只在确认没有活动任务后按现有 Retention 流程清理。
