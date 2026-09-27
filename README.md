# 文创市集寄售结算与争议冻结基础服务

本项目提供中医文化夜市的通用后台基础能力，负责活动机构、服务站点、操作者和结构化参考资料的登记，内置角色权限、请求幂等、SQLite 事务与哈希串联审计。新的业务模块可以在这些稳定边界之上增加领域状态、规则和接口。

当前已内置 **寄售结算与争议冻结模块**（`settlement.py`），覆盖闭市结算的完整链路：

- **批次起点**：入场确认时开立寄售批次，委托数量、结算规则快照和经手摊位共同构成批次起点；
- **追加事件**：销售、退回、损耗与调拨只通过追加事件改变数量账，原始小票永不改写；
- **离线补传**：终端事件按 `(terminal_id, terminal_seq)` 去重，内容一致为安全重放，内容不同登记序列分叉并拒绝，避免同一批作品被计算两次；
- **跨摊调拨**：调出、调入双方回执齐全后才追加生效事件，生效前数量只作预留；
- **结算草案**：闭市生成可复算的草案，每个事件只结算一次；各方只能查看自己的明细，并基于同一份规则快照确认；
- **争议冻结**：带证据的数量异议只冻结相关批次和款项，无争议部分仍可进入付款清单；解除或调整以追加决定落地，不覆盖原始小票、既有分配，也不制造重复应付；
- **连续进度**：未决争议、冻结依据、确认进度和分叉记录全部持久化，进程恢复后保持连续。

## 目录

- `src/night_market_foundation/`：领域模型、SQLite 存储、权限服务、审计链、寄售结算模块、HTTP 路由和离线验收；
- `tests/`：基础规则、事务边界、接口路由、结算链路和端到端验收测试。

## 环境

- Linux
- Python 3.11 或更高版本
- 运行时仅使用 Python 标准库和 SQLite

## 测试

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## 构建检查

```bash
python3 -m compileall -q src tests
```

## 离线验收

```bash
PYTHONPATH=src python3 -m night_market_foundation.acceptance
PYTHONPATH=src python3 -m night_market_foundation.settlement_acceptance
```

基础验收登记活动机构、操作者、站点和参考资料，核对幂等回执与审计链；结算验收执行一条完整结算链（批次、事件、重放与分叉、调拨、草案、异议冻结、追加决定、进程恢复），成功时输出一行 `status` 为 `ok` 的 JSON 并以退出码 `0` 结束。

## HTTP 服务

```bash
PYTHONPATH=src python3 -m night_market_foundation.api --database night_market.sqlite3 --host 127.0.0.1 --port 8080
```

健康检查使用 `GET /health`。写入接口通过 `X-Actor-Id` 标识操作者，服务重启后 SQLite 中的业务状态和审计链继续保留。

### 结算模块接口

| 方法与路径 | 说明 |
| --- | --- |
| `POST /settlement/rules` | 登记结算规则（三方基点之和为 10000） |
| `POST /settlement/parties` | 登记创作者、摊位或公益方并绑定查看组织 |
| `POST /settlement/batches` | 入场确认开立寄售批次（委托数量、规则快照、经手摊位） |
| `POST /settlement/events` | 追加销售、退回或损耗事件，支持终端序列去重 |
| `POST /settlement/transfers` | 登记跨摊调拨（预留调出方可用数量） |
| `POST /settlement/transfer-confirmations` | 登记调出或调入回执，双方齐全后调拨生效 |
| `POST /settlement/drafts` | 闭市生成可复算结算草案 |
| `POST /settlement/draft-confirmations` | 参与方基于同一规则快照确认草案 |
| `POST /settlement/disputes` | 提交带证据的数量异议并冻结相关批次款项 |
| `POST /settlement/dispute-decisions` | 追加决定：解除、作废事件或调整数量 |
| `GET /settlement/ledger?batch_id=` | 批次数量账与每个事件的有效状态 |
| `GET /settlement/draft?draft_id=` | 工作人员查看草案全量明细 |
| `GET /settlement/statement?draft_id=&party_id=` | 参与方查看自己的明细及金额采用的有效事件 |
| `GET /settlement/payment-list?draft_id=` | 付款清单：无争议应付款与冻结部分（附依据） |
| `GET /settlement/progress?site_id=` | 未决争议、冻结、确认进度、待生效调拨与分叉记录 |
| `GET /settlement/verify-draft?draft_id=` | 按草案记录的事件与规则快照复算校验 |
