# 药物警戒案例处理系统

使用 Python 标准库实现的独立原型，覆盖多渠道案例接入、去重、随访更正、严重性医学裁定、分国家报告、逾期升级、跨区域权限和案例合并审计。

## 运行

要求 Python 3.11+。

```bash
python3 app.py --db pharmacovigilance.db
```

默认监听 `127.0.0.1:8201`。首页为 `http://127.0.0.1:8201/`，健康检查为 `/health`。

所有接口使用请求头 `X-User-Id`、`X-Role` 和区域角色必需的 `X-Region`。角色为 `reporter`、`regional_lead`、`medical_reviewer`、`global_admin`。

## 主要接口

- `POST /api/cases`：录入案例，`dedupe_key` 相同则返回已存在案例。
- `GET /api/cases`、`GET /api/cases/{id}`：按权限查询。
- `POST /api/cases/{id}/followups`：用 `expected_revision` 防止覆盖随访。
- `POST /api/cases/{id}/medical-review`：医学审核员更新严重性、死亡和关联性。
- `POST /api/cases/{id}/reports`、`POST /api/reports/{id}/submit`：生成并提交分国家报告。
- `POST /api/cases/{id}/merge`：全局管理员合并重复案例。
- `POST /api/escalate-overdue`、`GET /api/overdue`：逾期检查与升级。

### 渠道来源历史对账

渠道来源常晚于随访补录。为避免用旧严重性覆盖已经提交的国家报告，补录与对账遵循以下规则：

- `POST /api/cases/{id}/sources`：补录一条渠道来源。必须带 `received_at`（**原始接收时间**）、`expected_revision`（**所基于的案例修订号**）、`serious`/`fatal`（严重性口径）和 `dedupe_key`。
- 案例当前应采用的严重性按**版本号 + 时间**在时间线上定位：随访（`followup`）与医学裁定（`medical_review`）权威性高于渠道来源。锚定旧修订号的迟到来源只入历史并标记 `superseded`，不会覆盖随访或医学裁定后的当前结论。
- 当迟到来源改变严重性口径且成为当前依据时：**未提交**的报告立即按新依据重算期限（`report_impact.recomputed`）；**已提交**的报告原样保留（`archived=1`，不再可提交），并生成版本号 +1 的新报告，状态为 `resubmission_required`（`report_impact.resubmitted`），新报告提交后才是当前有效版本。
- 两人同时补录同一修订号时，只有修订号匹配并先提交者生效；另一人收到 `409 revision_conflict` 且其来源不落库。实现上以每案例进程内互斥锁 + 全新数据库连接 + 提交时条件更新三重保证。
- `POST /api/sources/batch`：批量补录，`items` 每项含 `case_id`。各项独立提交，单项失败不影响其他项；失败后用相同 `dedupe_key` 重试只补未处理来源，已确认来源返回 `idempotent` 且不重复计数。返回 `applied/superseded/duplicated/conflicted/failed` 计数。
- `GET /api/cases/{id}/reconcile`：三方对账，交叉核对案例当前严重性/接收时间/期限、来源历史（`applied` 与 `superseded`）以及每个国家的有效/归档报告版本。全部对得上时 `consistent=true`，否则在 `mismatches` 中列出具体不一致项，用于判断哪份报告有效。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 主要局限

该实现使用请求头模拟身份，不含生产级登录、签名和密钥管理；SQLite 与标准库 HTTP 服务适合单机原型。分国家规则采用内置严重 15 天、死亡 7 天、非严重 90 天规则，接入真实监管网关前需按当地法规扩展。
