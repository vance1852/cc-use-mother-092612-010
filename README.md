# 文创市集寄售结算与争议冻结基础服务

本项目提供中医文化夜市的通用后台基础能力，负责活动机构、服务站点、操作者和结构化参考资料的登记，内置角色权限、请求幂等、SQLite 事务与哈希串联审计。在此之上实现了闭市结算模块：以入场确认的批次为起点，用只追加的数量事件记账，闭市后产生可复算的结算草案，并支持带证据的异议冻结与追加式决定。

## 目录

- `src/night_market_foundation/`：领域模型、SQLite 存储、权限服务、审计链、HTTP 路由和离线验收；
  - `service.py`：主体、场所、资料登记等基础能力；
  - `settlement_service.py`：入场批次、数量事件、跨摊调拨、闭市草案、异议与决定；
  - `ledger.py`：由“批次起点 + 追加事件”重算数量账与金额账的纯函数；
- `tests/`：基础规则、事务边界、接口路由、结算链路和端到端验收测试。

## 结算模型

- **批次起点**：`POST /intakes` 入场确认登记创作者、摊位、作品、委托数量、单价与三方分账比例（万分比，合计必须 10000）。批次不可变，同一作品在同一摊位重复登记且条款一致时安全重放。
- **追加数量账**：`POST /quantity-events` 只追加销售/退回/损耗事件，余额由重算得出，不允许出现负库存或退回超过已售。离线终端按 `(terminal_id, terminal_seq)` 识别安全重放（同序号同内容返回原事件）与序列分叉（同序号不同内容报 `sequence_fork`），序号跳档会标记 `gap_before`。
- **跨摊调拨**：`POST /transfers` 记录来源方回执，`POST /transfers/acknowledge` 记录目标方回执；双方回执齐全后才在同一事务里落成调出/调入两笔事件，之前不产生任何数量变化。
- **闭市草案**：`POST /sites/close` 关闭普通数量通道（存在未决调拨时拒绝），`POST /settlements` 生成结算版本，携带规则快照摘要、内容摘要与事件水位，同一事件账重算结果一致。
- **按方视图**：`GET /settlements` 中创作者/摊位/公益方只能看到自己相关的明细与己方金额；`POST /settlements/confirm` 要求确认方携带与草案一致的规则快照。
- **异议冻结**：`POST /disputes` 提交带证据的异议，只冻结相关批次，无争议批次仍进入 `GET /payment-list`；`POST /disputes/decide` 以追加决定解除、驳回或调整（调整落成调整事件并产生新结算版本），原始小票与既有分配不被覆盖。进程重启后未决异议与确认进度从 SQLite 恢复，保持连续。

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
```

验收命令会在临时 SQLite 数据库中跑通基础登记链与完整结算链（入场、追加重放、调拨双回执、闭市草案、当事方确认、异议冻结与解除），核对幂等回执与审计链，成功时输出一行 `status` 为 `ok` 的 JSON 并以退出码 `0` 结束。

## HTTP 服务

```bash
PYTHONPATH=src python3 -m night_market_foundation.api --database night_market.sqlite3 --host 127.0.0.1 --port 8080
```

健康检查使用 `GET /health`。写入接口通过 `X-Actor-Id` 标识操作者，服务重启后 SQLite 中的业务状态和审计链继续保留。
