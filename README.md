# 深空辐照标定 · 多站投票封签系统

多座地面站对同一标定批次复测投票，达到阈值后封存一份**不可变标定证书**。
系统解决并发到达的相反读数问题：原子冻结、幂等重传、冲突隔离、
并发下证书唯一、异常中断后持久恢复。

零第三方依赖：Python 3.11 标准库（`http.server` + `sqlite3`）+ 原生前端。

## 目录结构

```
app/
  store.py    领域逻辑与 SQLite 持久层（原子冻结/重传/冲突/封签/恢复）
  server.py   HTTP 服务（页面、静态资源、/healthz、JSON API）
web/
  index.html / styles.css / app.js  原生 JS 控制台
tests/
  test_store.py  领域测试（14 项，含并发）
  test_http.py   接口测试（6 项，含并发封签与重启恢复）
  verify.py      Compose verify 服务入口：等待就绪 → 代码测试 → HTTP 冒烟
Dockerfile
docker-compose.yml
.env.example
```

## 快速开始

### Compose（验收方式）

```bash
# 1. 交付页面 + 健康检查（宿主端口可配置，默认 8080）
HOST_PORT=18080 docker compose up -d --build web
curl http://localhost:18080/healthz
# 浏览器打开 http://localhost:18080

# 2. 启动验收（等 web 健康后自动执行，完成即退出，退出码报告结果）
docker compose --profile verify up --build verify
docker compose --profile verify ps   # 查看 verify 退出码（0 成功）
```

`verify` 依次执行：

1. 等待页面与 `/healthz` 可用；
2. `unittest` 代码测试，覆盖**幂等重传**（同站同 vote_id 同摘要只回放、
   不增票）与**冲突票隔离**（不同摘要 / vote_id 改绑内容 / 同站换号再投）；
3. API/HTTP 冒烟：健康响应、可操作错误反馈、**并发达到阈值只生成一份证书**、
   **SIGKILL 中断后同库重启恢复**（已封存不回到收集中、迟到票不改写证书）。

### 本地直接运行（无 Docker）

```bash
HOST=127.0.0.1 PORT=8080 DB_PATH=./data/cal.db python3 -m app.server
# 另一个终端
BASE_URL=http://127.0.0.1:8080 python3 tests/verify.py
python3 -m unittest discover -s tests
```

## 页面能力（审查员视角）

录入批次标识、固定站点名单与阈值；为站点提交摘要与稳定投票标识；
每 1.5 秒轮询持续展示：冻结配置、赞成票进度、冲突站点（含原因）、
封存后的不可变证书（证书号、赞成站点、冻结/封存时间、SHA-256 指纹）。

**防过期覆盖**：前端仅按 `(状态秩, version)` 严格单调推进渲染，且只采用
最新一次发起的请求结果——刷新、轮询乱序或过期响应都无法把
`SEALED` 拉回收集中。服务端以条件更新 + 事务保证封存事实本身不可回退。

## 核心规则

| 场景 | 行为 |
|---|---|
| 首次有效投票 | 与该票写入在**同一事务**原子冻结名单、阈值、摘要 |
| 同站 + 同 vote_id + 同摘要重传 | 回放首次结果，HTTP 202，**不增票** |
| 同 vote_id 绑定内容改变 | 记 `VOTE_ID_CONTENT_CHANGED` 冲突，不回放、不封存 |
| 与冻结摘要不同的投票 | 记 `SUMMARY_MISMATCH` 冲突，**不参与封存** |
| 同站换新 vote_id 再投 | 记 `STATION_ALREADY_VOTED` 冲突，不增票 |
| 名单外站点 / 参数无效 / 配置不符 | 4xx + `{code,message,hint}` 可操作拒绝 |
| 多站并发达到阈值 | 条件更新 `COLLECTING→SEALED` + 证书主键唯一，**只出一份证书** |
| 封存后新票 | 409 `LATE_VOTE`，不记录、不改写证书；完全相同的重传仍可回放 |
| 进程崩溃/断电 | SQLite WAL + 事务原子性；重启后从持久记录恢复，封存态永不回退 |

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/healthz` | 健康响应 |
| POST | `/api/batches` | 创建批次（配置校验，冲突返回 400/409 + hint） |
| GET | `/api/batches/{id}` | 批次实况（赞成票、冲突、证书；支持 ETag/304） |
| POST | `/api/batches/{id}/votes` | 提交投票（创建 201 / 回放 202 / 冲突也 201 但 decision=CONFLICT） |
| GET | `/api/batches` | 批次索引 |

## 持久化与并发保证

* 所有写操作在进程级 RLock 内执行 `BEGIN IMMEDIATE`，SQLite 写锁全局
  串行化；首次冻结与封签均为带旧状态断言的条件 `UPDATE`，并发竞争时
  只有一个事务能推进状态。
* `votes` 上对 `(batch_id, station) WHERE decision='APPROVE'` 的部分唯一
  索引是防并发重复赞成的最终兜底。
* 证书号与内容指纹均由冻结配置、赞成站点、时间戳规范化后 SHA-256 得出，
  可通过 `Store.verify_certificate` 重算核验，页面展示同一指纹。
