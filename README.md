# 只读查询 API（带守护的 PostgreSQL 连接池）

数据库值班场景：多个应用共用有限的 PostgreSQL 连接。客户端等慢 SQL 超时后，
查询可能仍占着会话；未清理的事务也可能影响下一个请求。本服务用
FastAPI + 自研异步连接池解决这些问题：

- 每个请求可**分别设置**“等待连接的超时”（`acquire_timeout`）和
  “执行查询的超时”（`query_timeout`）；
- 排队中的请求被取消后**立即释放队列名额**（排队不占用连接）；
- 查询超时、客户端断开、SQL 报错后，连接标记为“脏”，**在确认会话
  状态已清理之前绝不再借出**：先做服务端取消（`pg_cancel_backend` 等价的
  CancelRequest）、回滚遗留事务、再用 `SELECT 1` 验证；无法确认的一律
  **关闭并更换新连接**；
- 服务端取消基于 libpq 的同步阻塞 `PQcancel()`，因此**放在独立工作线程
  执行**，绝不占用事件循环：取消卡住期间 `/pool/stats` 与其他连接上的
  查询照常处理；取消本身受独立的 `cancel_timeout` 约束（并处于
  `cleanup_timeout` 总预算内），超时或报错一律关闭旧连接再补位；
- 连接数有硬上限：每条连接始终属于一个槽位，更换时先关旧再开新，
  取消与归还交错也不会越限；
- 会话级只读（`default_transaction_read_only=on`），本 API 只暴露
  固定的 SELECT 查询，不执行任意 SQL。

## 快速开始

```bash
docker compose up --build
```

- API 文档（Swagger UI）：http://localhost:8000/docs
- 连接池指标：http://localhost:8000/pool/stats
- PostgreSQL：`localhost:5432`，用户/密码/库均为 `app`/`app`/`demo`

连接池大小用环境变量 `POOL_SIZE` 控制（默认 4，见 `docker-compose.yml`）。

## 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/query/normal` | 普通查询（`SELECT ... FROM items LIMIT n`） |
| POST | `/query/slow` | 延时查询（`SELECT pg_sleep(n)`），模拟慢 SQL |
| POST | `/query/error` | 报错查询：`autocommit` 单语句报错；`transaction` 在 `BEGIN` 后报错，故意留下失败事务 |
| GET | `/pool/stats` | 连接池指标：`in_use`（在用数）、`waiting`（等待数）、`idle`、`cleaning`、`opened_connections`、`cancel_fault` 及各类计数器 |
| POST | `/demo/burst` | 后台并发发起 n 个慢查询，便于在 `/pool/stats` 观察排队 |
| GET/POST | `/debug/cancel-fault` | 故障注入（演示/自测）：`""` 关闭、`hang` 模拟取消调用长时间不返回、`error` 模拟取消调用报错 |

查询类接口的请求体都支持 `acquire_timeout` / `query_timeout`（秒，`null` 表示不限）。

响应码约定：`200` 成功；`503` 等连接超时；`504` 查询执行超时；
`400` SQL 错误；`502` 连接级故障（如后端被杀）；`499` 客户端中途断开。

池的超时也可用环境变量覆盖（`docker-compose.yml` 里按需加）：
`CANCEL_TIMEOUT`（默认 5 秒，等待服务端取消返回的上限）、
`CLEANUP_TIMEOUT`（默认 10 秒，单条连接清理的总预算）、
`DRAIN_GRACE`（默认 5 秒，取消成功后等查询收尾的时间）；
`CANCEL_FAULT` / `CANCEL_FAULT_SECONDS` 可在启动时就打开故障注入。

## 在 /docs 里演示

1. **排队**：先 `POST /demo/burst`（默认 8 个 3 秒慢查询，池只有 4 条
   连接），立刻反复 `GET /pool/stats`，可看到 `in_use=4, waiting=4`。
2. **等连接超时**：池被 burst 占满时，`POST /query/slow` 设
   `acquire_timeout=1`，约 1 秒后返回 `503`。
3. **查询超时**：`POST /query/slow` 设 `delay_seconds=10, query_timeout=1`，
   约 1 秒后返回 `504`；`/pool/stats` 里 `cleaning` 短暂为 1（服务端
   取消 + 验证会话），随后恢复，`counters.recycled` 或 `replaced` 加一。
4. **SQL 报错**：`POST /query/error`，`mode=transaction` 会留下失败
   事务；连接池回滚并验证后才复用（`counters.recycled` 加一）。
5. **客户端取消**：用 `curl` 发一个慢查询后按 `Ctrl-C` 中断，
   `/pool/stats` 的 `waiting`/`in_use` 会立刻回落，
   `counters.queue_cancelled` 或 `client_disconnects` 加一。
6. **取消卡住（核心场景）**：`POST /debug/cancel-fault` 设
   `{"mode":"hang","hang_seconds":12}`，再 `POST /query/slow` 设
   `delay_seconds=8, query_timeout=1`：请求约 1 秒即返回 `504`；
   随后的 `cancel_timeout`（默认 5s，本地演示可调到 3s）内，反复
   `GET /pool/stats` 与 `POST /query/normal` 都保持毫秒级响应——
   取消阻塞只发生在工作线程，事件循环不冻结。超时后该槽位
   `cleaning` 短暂为 1，旧连接被关闭、新连接补位，
   `counters.server_cancel_timeouts` 与 `replaced` 各加一；
   再 `POST /debug/cancel-fault` 设 `{"mode":""}` 恢复正常。
7. **取消报错**：`POST /debug/cancel-fault` 设 `{"mode":"error"}`，
   重复慢查询超时：`counters.server_cancel_errors` 与 `replaced`
   各加一，即使旧查询之后自行结束、会话仍能响应，旧连接也一律关闭
   更换，绝不复用。演示完同样把模式重置为 `""`。

## 自测

```bash
pip install -r tests/requirements.txt
# 可选：提供 psql 连接串后，会额外做服务端核对
# （连接数不超过上限、无残留活跃查询、无失败事务）
export PSQL="postgresql://app:app@127.0.0.1:5432/demo"
python tests/selftest.py
```

覆盖：普通查询、排队与并发上限、等连接超时、排队取消释放名额、
查询超时后的连接清理/更换、两类 SQL 报错后的复用、客户端断开、
**服务端取消卡住（hang）与取消报错（error）两条故障回收路径**、
以及取消与归还交错的压力测试（全程校验
`idle + in_use + cleaning == size` 且 `opened_connections <= size`）。

另有一个不依赖 HTTP 的复现/回归脚本（直接加载 `app/pool.py`，
通过 monkeypatch 模拟 `PQcancel` 同步阻塞），可对比修复前后事件循环
的卡顿差异（需能直接连到 PostgreSQL，并把 `app/` 作为参数传入）：

```bash
MODE=hang  python3 tests/repro_cancel_hang.py app
MODE=error python3 tests/repro_cancel_hang.py app
```

`tests/test_coro_cancel.py` 则验证早期 psycopg 3.2 的“协程版
`cancel()`”会在工作线程的临时事件循环里被正确驱动。

## 关键设计

- **槽位模型**：池大小固定，每条连接处于 `idle / in_use / cleaning`
  三态之一；更换连接时先关闭旧连接再创建新连接，因此打开的连接数
  永远不会超过上限。
- **排队不持名额**：`acquire()` 的等待者只挂在条件变量上，被取消
  （如客户端断开）时从队列移除即可，不会泄漏连接。
- **脏连接不出借**：归还时若连接可能仍在执行查询（超时/断开）或
  刚发生 SQL 错误，则转入后台清理：先在**独立工作线程**发送服务端
  取消（libpq 的 `PQcancel()` 是同步阻塞调用，放在事件循环线程会
  冻结整个服务），取消受 `cancel_timeout` 约束；取消成功后等待查询
  任务收尾，再回滚遗留事务，最后 `SELECT 1` 验证会话空闲。取消
  超时/报错或任何一步失败，都先取消仍挂在该连接上的查询任务（摘掉
  其 socket 读回调，保证连接能真正关闭、不残留 `CLOSE_WAIT`），
  再关闭并新建连接顶替。清理期间该槽位不可借出，但不影响其他槽位。
- **借出前把关**：借出时先做本地状态检查；空闲超过
  `checkout_verify_after` 秒（默认 10）的连接额外做一次 `SELECT 1`
  活性验证，防止数据库重启后把已死的空闲连接借给请求。

## 已知限制

- 连接关闭失败（如对端无响应且超时）时，客户端按已关闭记账，服务端
  可能短暂残留连接，依赖 PostgreSQL 的 TCP keepalive 最终回收；
  `/pool/stats` 的 `opened_connections` 是客户端视角。
- 排队唤醒不保证严格 FIFO（`asyncio.Condition` 语义），极端情况下
  可能有请求等待偏久，但都会在自己的 `acquire_timeout` 内得到确定结果。
- 单进程单事件循环；多副本部署时每个进程各有自己的连接池，
  总连接数 = 副本数 × `POOL_SIZE`，需要值班员按此规划。
- 活性验证只在“借出时”进行；验证通过与实际执行之间的极短窗口内
  连接仍可能失效（此时该请求报错、连接被更换，属正常兜底路径）。
- 取消超时放弃的是“等待”：工作线程里那次阻塞的 `PQcancel()` 无法被
  强行中断（同步 C 调用），线程会在后台等到它自行返回或进程退出
  （daemon 线程，不阻挡关停）。回收流程不依赖它的结果，到点即关闭
  旧连接并补位。
- 取消信号未能送达（取消卡住/报错）时，后端那条查询不会被中止，会
  一直跑到语句自身结束；旧连接已在客户端侧关闭，PostgreSQL 在查询
  结束、回到读循环发现对端断开后回收该后端进程。这期间
  `pg_stat_activity` 里可能短暂看到该活跃查询，属预期行为。
