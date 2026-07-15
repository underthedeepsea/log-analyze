# Drain3 配置治理设计

## 目标

将 `configs/drain3_recommended.ini` 从隐藏的运行参数提升为可查看、可演进、可评测和可回滚的配置基线。系统管理 Drain3 算法参数与脱敏规则，不把学习到的日志模板误称为基础配置，也不保存完整原始日志。

本功能版本为 `1.18.0`。系统基线文件保持只读；已发布配置只影响发布后创建的新分析任务，运行中的任务继续使用创建时锁定的配置版本。

## 配置模型与文件存储

- `configs/drain3_recommended.ini`：系统基线，始终保留且不被页面改写。
- `state/drain_quality/configs/<config_id>/<version>.ini`：候选及历史完整 INI 快照。
- `state/drain_quality/config_catalog.json`：名称、版本、状态、来源、说明、创建人、时间和关联评测。
- `state/drain_quality/active_config.json`：当前发布版本指针，使用原子文件替换更新。
- 每个版本保存完整配置，不设计字段继承；读取和回滚逻辑保持直接、可审计。

状态包括 `baseline`、`candidate`、`validated`、`published`、`rolled_back`。候选编辑产生新版本，不覆盖历史版本。

## 校验与发布

保存候选前执行三层校验：INI 语法、Drain3 参数范围、脱敏正则编译。系统还会用固定脱敏样例进行快速预览，展示每条规则的匹配结果，但不保存用户生产日志。

候选配置必须关联已完成的 Gold Dataset 评测才能发布，并满足现有硬门槛：关键风险召回率不低于 100%、过度合并率不高于 2%、正常日志误报率不高于 2%。发布仍需人工确认。回滚等同于将历史完整快照重新设为活动版本，并记录追加式事件。

新任务创建时解析活动指针，将 `config_id`、版本及内容 Hash 写入任务元数据。普通上传和大文件流水线统一使用该快照，避免任务运行期间配置变化。

## API

- `GET /api/drain-quality/configs`：配置目录、活动版本和评测摘要。
- `GET /api/drain-quality/configs/{id}/versions/{version}`：结构化参数、脱敏规则和 INI 原文。
- `POST /api/drain-quality/configs`：从基线、现有版本或导入 INI 创建候选。
- `POST /api/drain-quality/configs/{id}/versions`：保存候选新版本。
- `POST /api/drain-quality/configs/{id}/validate`：执行语法、参数、正则和样例校验。
- `POST /api/drain-quality/configs/{id}/publish`：验证评测门槛并人工发布。
- `POST /api/drain-quality/configs/{id}/rollback`：人工回滚到指定历史版本。

所有写接口校验 `expected_version` 和 `confirmed`，冲突返回明确错误，不静默覆盖。

## 页面设计

质量中心新增“Drain3 配置”入口，采用已确认的橙色白底布局：

- 顶部显示当前生效配置、脱敏规则数、候选数和最近评测收益。
- 左侧为配置版本列表，右侧为配置详情。
- 详情提供“结构化配置”“脱敏规则”“INI 原文”“版本差异”四个视图。
- 结构化表单覆盖常用 DRAIN、SNAPSHOT 和 PROFILING 参数。
- 脱敏规则按占位符、正则、状态管理，支持新增、编辑、停用、排序和测试。
- 基线只提供“复制为候选”；候选依次经过配置校验、数据集评测和人工发布。
- 参数与规则差异按字段展示，发布历史显示操作者、评测任务和配置 Hash。

## 安全与错误处理

禁止任意路径读写和目录穿越；配置 ID 使用服务端生成标识。正则测试限制输入长度和规则数量，避免高成本表达式阻塞服务。导入文件限制为 UTF-8 INI，错误需指出 section、字段或规则序号。配置无效、评测缺失、门槛未通过或版本冲突时拒绝发布，当前活动版本保持不变。

## 测试

- 配置解析、序列化、版本追加、原子活动指针和回滚。
- 参数边界、无效 INI、无效正则、路径安全和并发版本冲突。
- 评测门槛、人工确认和未通过配置不得发布。
- 普通与大文件任务锁定同一活动配置快照。
- API 契约、React 页面交互、脱敏规则编辑、差异和发布状态。
- 全量 pytest、前端语法、Shell 校验和浏览器视觉检查。
