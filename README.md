# 币安 USDT 合约只读监控

按用户提供的《提示词.md》实现。程序读取公开行情，计算 EMA12、EMA50、EMA200 和前20根量价比较，六项条件全部满足才记录提醒。四个周期独立运行：15分钟、1小时、4小时、日线。没有交易、账户或资金接口，也不需要交易 API 密钥。

## 本机启动

在本文件所在目录打开 PowerShell。本次交付的本地目录已准备好 Python 依赖和历史数据库。重新解压代码包时先执行安装。

```powershell
# 新安装时执行一次；本机已准备好的目录无需重复
.\setup.ps1

# 初始化数据库（重复执行安全）
.\start-local.ps1 -Command init

# 持续运行，默认仅控制台和本地事件日志提醒
.\start-local.ps1 -Command run

# 在另一个窗口查看状态、请求优雅停止
.\start-local.ps1 -Command status
.\start-local.ps1 -Command stop

# 只补齐历史，不持续订阅
.\start-local.ps1 -Command bootstrap

# 停机后重新生成四份 Excel
.\start-local.ps1 -Command export

# 短时验证：初始化/补数结束后再运行300秒
.\start-local.ps1 -Command run -Seconds 300
```

同一个数据目录仅允许一个写入实例。`export` 命令应在监控停止后执行；正常运行时每60秒自动批量导出，不需要另开导出进程。双击 Excel 查看时可能锁住文件，程序会另存时间戳快照并标记待替换，下一轮重试。

本次验收运行结束后已停止，不会在关闭电脑或此对话后自行持续运行。启动 `run` 并保持机器和网络可用，才会继续监控。

## 配置与提醒

编辑 `config.json` 后重启。默认量比阈值2、相对实体倍数2、实体占比至少0.60、上影占比至多0.30。六项规则还包括严格突破前20最高价、严格EMA多头排列和EMA12上升、成交量严格超过前20最大量、开收盘涨幅严格超过前20最大涨幅。没有多周期共振要求、冷却期或额外EMA斜率条件。

配置内容和公式版本生成策略哈希，历史信号保留旧版本。恢复时继续使用已保存的EMA种子与增量状态，不重置为新的1500根滑动种子。运行时覆盖 `MONITOR_NODE` 也纳入配置版本，因此请固定启动方式。

默认仅分析初始化阶段的最新K线；设置 `historical_analysis=true` 后，首次初始化会计算全部可判断历史K线。现有数据库不会自动倒带重算旧记录。需要完整历史研究时使用新的 `data_dir`，避免改变正在运行的监控状态。`historical_summary=true` 会在初始化结束后向本地日志和控制台输出一条去重历史汇总，默认关闭。

提醒文件是 `runtime/events.jsonl`，详细通知状态持久化在数据库 `outbox` 和 `deliveries` 表。`HISTORICAL` 不进入实时通知；`RECOVERED` 保留补数来源；发现时间晚于收盘边界120秒的信号为 `EXPIRED`，仍保存但不即时通知。队列等待也计入发现延迟。

只有明确授权并配置后，才启用外部 HTTPS webhook：将 `webhook_authorized` 设为 `true`，并在 `MONITOR_WEBHOOK_URL` 环境变量中设置接收地址。URL不得写进配置或日志。代码不会替你添加联系人、群组或机器人。发送使用 `Idempotency-Key: signal_id`；接收端应按该ID去重。网络故障或进程在发送成功后、写入送达状态前退出时，外部渠道仍可能重复投递，不能保证严格恰好一次。

## 输出与数据

- `excel/`：四个周期各一份工作簿，均含监控概览、触发记录、前20根对照、数据状态与异常、参数与说明。
- `runtime/monitor.sqlite3`：全部已采集原始K线、增量状态、判断、信号、配置版本、通知发件箱及异常记录。
- `runtime/health.json`：运行状态、每周期就绪数量、订阅连接、消息计数、补数积压、延迟和最近导出状态。
- `runtime/monitor.log`：轮转结构化日志。`runtime/events.jsonl`：轮转提醒日志。日志轮转不删除数据库中的信号和通知历史。
- `runtime/export-cache/`：可重新生成的导出缓存和检查记录。数据库是事实来源。

Excel中的价格和统计是引擎已计算并保存的数值快照，不是可编辑的策略计算器。价格按精度显示；UTC毫秒和北京时间同时保留。四个文件的快照一致性由一次数据库读事务保证。日线使用交易所UTC日线，北京08:00结束。空白表示不可用，不能按0理解。

信号及前20根对照接近容量上限时按UTC月份分卷；主文件保留最新一卷，旧卷另存，原始记录仍留在数据库。异常记录也会分卷。监控概览始终列出完整当前清单，不截断为触发者。容量参数用于明细归档，不限制应监控的交易对数。

## 架构与恢复

`core.py` 管理指标、信号和SQLite事务；`transport.py` 只允许三个公共GET端点并订阅公开K线；`service.py` 管理清单、预热、顺序处理与补数；`notifications.py` 分发持久化发件箱；`export.py` 和 `excel_builder.mjs` 在独立工作线程/子进程中导出。市场队列容量10000，补数去重队列容量10000，每连接默认200个订阅；每个交易对和周期有独立锁。信号队列使用持久化表承接；Excel串行刷新合并期间变化，不积累重复导出任务。

REST权重使用滚动窗口预算，且根据exchangeInfo返回的分钟上限保留至少25%余量。初始1500根请求权重10，增量最多499根权重2；有界并发、429/418退避、服务器时间校准。WebSocket自动响应ping，断线重连补数，连接存活23小时后主动轮换。后台每10秒检查遗漏收盘，清单默认每15分钟刷新。空清单或刷新失败保留上次有效清单并标记过期。

发现缺口、OHLC异常或冲突重复时，停止受影响的交易对/周期判断，其他任务继续。有效但不连续的源K线仍可保存供排查，不虚构缺失K线。新上市不足200根为NOT_READY；200至1499根为SHORT_WARMUP，允许触发；1500根起为READY。数据错误和预热状态分别记录，不能把READY等同于网络实时可用。

重启直接运行 `run`，会恢复EMA状态并从最后一根后面补齐。备份前使用 `stop` 并等待状态 `running=false`，再复制整个 `runtime` 目录；不要在运行时仅复制主SQLite文件而漏掉WAL。恢复时把备份放回配置指定数据目录，保持相同策略配置。不要删除种子或手工修改状态。

数据库结构在 `monitor/core.py` 的 `SCHEMA`，初始版本1。启动幂等创建缺少的表；本版本不提供破坏性迁移。未来结构升级应先停机备份再迁移。SQLite使用WAL、FULL同步和唯一约束；信号与发件箱在同一事务提交。

## 部署条件和测试

实际验证环境为 Windows、Python 3.12.14、Node 24.19.0；Python依赖固定在 `requirements.lock`。Excel作者库为本机Codex bundled `@oai/artifact-tool`，版本见 `runtime-versions.json`。本项目没有把该库复制进代码包。另一台机器必须具备该运行库，并用 `ARTIFACT_TOOL_MODULE_ROOT` 指定其Node依赖根目录。仅安装Python依赖不足以导出Excel；行情监控和落库仍可工作，导出故障会记录。Linux后台服务的长期部署尚未在本次验收中验证。

本机连接币安需要现有系统代理，程序读取系统/环境代理。不要把代理凭据写进日志。部署前确认允许访问 `https://fapi.binance.com` 与 `wss://fstream.binance.com/market/stream`，并为数据和输出目录留足磁盘空间。建议监控磁盘余量、health时间戳和异常数量，长期运行前完成至少24小时的断线与重连观察。

```powershell
# 若使用setup.ps1安装的目标目录
$env:PYTHONPATH = (Resolve-Path runtime\python-deps).Path
python -m unittest discover -s tests -v
# 集中3000根收盘的合成负载测试，数据写临时目录
$env:PYTHONPATH = "$env:PYTHONPATH;$PWD"
python tests\load_test.py
```

如 `python` 不在PATH，请使用Python 3.12的完整路径。单元测试与合成负载数据独立于真实行情数据库，不能作为真实触发样例。实际结果、覆盖数量和未验证事项见 `验证报告.md`。

## 官方接口依据

研究时先使用用户指定的币安MCP核验exchangeInfo。生产持续订阅和批量采集使用币安公开REST/WebSocket，不通过MCP执行订单或账户操作。

- [USDⓈ-M公共REST行情接口](https://developers.binance.info/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data)：交易规则、服务器时间、交易价格K线及权重。
- [WebSocket连接规则](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Connect)：当前market路由、连接时长、控制消息限制和订阅数量限制。
- [K线流字段](https://developers.binance.info/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/market)：`k.x`收盘确认和K线字段。

接口文档于2026-10-14核验，真实数据和最终验收时间见报告。阈值未被证明最优，程序不评价策略盈利能力。
