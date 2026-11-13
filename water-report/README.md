# 水质结果报送控制台（FastAPI + SQLite + Playwright）

把核准的水样检测结果，通过**真实浏览器**录入只有网页表单的旧报送站，并保证
**断网/重试/进程重启都不会造成重复入库或漏报**。

## 一、为什么不会重复入库（核心语义）

后台对每一行 `(样本号, 项目)` 业务键严格执行"先查后填"，所有旧站操作都由
Chromium 真实页面完成（查询页 `/search`、录入页 `/new`、保存 `/save`），
**不直连旧站接口或数据库**。

| 浏览器查到的情况 | 本地行状态 | 动作 |
|---|---|---|
| 查询失败（错误页/超时/异常） | `QUERY_FAILED` | **绝不当成"不存在"**，不填表，可重试 |
| 远端无记录 | `SAVED` | 才执行新增，记录远端记录编号 |
| 已有同值记录 | `ALREADY_SENT` | 视为已送，**不重复保存** |
| 已有异值记录 | `CONFLICT` | **整批停止**，页面同时展示本地值与远端值 |

### 保存后丢回执 / 进程退出

旧站保存与本地进度是**两个独立事务，绝不一起提交**：

1. 本地先把行置为瞬时态 `SAVING`（独立提交）；
2. 浏览器点保存，旧站自己的事务落库；
3. 只有看到明确的成功页（含远端记录编号）才把本地置为 `SAVED`；
4. 若拿到错误页/超时/进程在第 2 步前后退出 → 行置为 `UNVERIFIED`（待核实），
   **不会再做第二次保存**；
5. 重启或续跑时，对 `UNVERIFIED` 行**先重新查询核实**：
   - 远端已有同值 → 补记远端编号，置 `SAVED`；
   - 远端有异值 → `CONFLICT` 停批；
   - 确认远端确实没有 → 才允许重新填写。
   进程启动时的 `recover_stale_runs()` 会把残留的 `SAVING` 一律转为
   `UNVERIFIED`，并清理残留运行锁。

### 每批只运行一份

- 进程内 `WorkerManager` 单 worker + SQLite `worker_lock` 单行表双保险；
- 运行中再次启动返回 HTTP 409；停止请求只在行边界生效，不打断页面事务。

## 一·补、上传建批的有界并发与锁等待（不卡死页面）

实验室多份 CSV 并发上传时，建批是 SQLite 写事务，遇锁会按 SQLite 原
**30 秒 `busy_timeout`** 忙等。修复前阻塞发生在事件循环线程，持锁等待会把
首页、原批次进度轮询和停止操作一起卡住。现在的设计：

- 建批路由为 `async`，所有阻塞工作（落盘 + 数据库事务）放入**专用有界上传
  线程池**，事件循环永不被占用——持锁期间首页、进度页、停止操作均在毫秒级响应；
- `BoundedUploadGate` 提供**有界执行名额**（默认 4，与线程池同尺寸）和
  **有界排队容量**（默认 16），执行 + 排队总容量 20；满额立即返回带
  `Retry-After` 的**可重试 503**，不等待；
- 排队请求可取消（客户端断开连接，或演示面板的"取消全部排队请求"）：
  **尚未开始执行的请求取消后绝不建批**；
- 已经开始的数据库操作**不可取消**：必须完成 `commit` 或 `rollback` 并关闭
  连接后才归还执行名额，取消不会提前释放名额；
- 建批在**单事务**内写入批次与全部行，任何失败整体回滚；30 秒忙等仍未拿到
  写锁时返回**可重试 503 且不登记批次**，解锁后可原样重新上传；
- 页面/停止等其余请求仍走 uvicorn/anyio 的**默认共享线程池（维持原配置）**，
  与上传专用池隔离，因此上传拥塞不会饿死页面处理。

容量与等待均可经环境变量调整：`UPLOAD_MAX_ACTIVE`、`UPLOAD_MAX_QUEUED`；
SQLite 忙等待保持 `db.connect()` 里的 30 秒 `busy_timeout` 不变。

### 可控演示（持锁 / 取消与开始执行交错）

首页有"并发/锁等待可控演示面板"，也可用脚本对运行中的服务演练：

```bash
python demo_concurrency.py http://localhost:8000
```

脚本会：打开 14 秒持锁窗口 → 并发 24 份上传（4 执行忙等锁、16 排队、
4 个满额 503）→ 持锁期间测量首页/进度/停止延迟（均 <1s）→ 窗口内取消全部
排队（16 个快速 503 且不登记批次，4 个执行名额保留）→ 锁释放后执行中的请求
完成建批 → 解锁后重新上传成功。也可在首页面板点"持有写锁 35 秒"观察超过
30 秒忙等后的可重试 503。


## 二、目录结构

```
legacy/                模拟"只有网页表单"的旧报送站（独立 FastAPI + SQLite）
  app.py               仅页面路由；允许重复键；两档断连开关
  templates/           查询/录入/保存/错误页
reporter/              报送控制台
  app.py               Web：上传建批、开始/停止/续跑、逐行状态、事件日志
  browser_worker.py    Playwright 后台：查询→判定→填表→回执判定→续跑核实
  db.py                本地 SQLite：批次/行/事件/单实例锁/崩溃恢复
  csvkit.py            三列 CSV 校验（非负十进制、业务键唯一）
  templates/           控制台页面（2 秒轮询，无需手动刷新）
data/                  示例 CSV 与本机运行数据（*.db 已 gitignore）
tests/                 单元测试 + 38 项端到端实测
docker-compose.yml     一键起旧站 + 控制台，数据落命名卷
```

## 三、用 Docker Compose 启动（推荐演示方式）

```bash
docker compose up --build
```

| 服务 | 地址 | 说明 |
|---|---|---|
| 报送控制台 | http://localhost:8000 | 上传 CSV、报送、续跑 |
| 旧报送站 | http://localhost:8001 | 页面操作 + 断连开关 + 记录核对 |

数据持久化：`legacy-data`（旧站 SQLite）、`reporter-data`（本地进度、上传文件、
截图）。`docker compose down` 后数据仍在；`down -v` 才会清空。

旧站首页有三个演练按钮：

- **开启：仅保存后断连** —— `/save` 先落库再返回 500，模拟"保存成功但回执丢失"；
- **开启：全站断连** —— 连查询也失败，用于验证"查询失败不能当成不存在"；
- **全部恢复正常**。

## 四、浏览器演示剧本（约 5 分钟）

1. 打开 http://localhost:8000，上传 `data/sample-batch-a.csv`，点**开始报送**：
   逐行变为 `SAVED`，每行显示远端记录编号，批次 `DONE`。
2. 用同一文件再建一批并开始：全部 `ALREADY_SENT`，旧站记录数不增加。
3. 打开旧站 http://localhost:8001，点**开启：仅保存后断连**；
   上传 `data/sample-batch-b.csv` 并开始：两行均为 `UNVERIFIED`
   （旧站记录其实已写入，可在旧站"全部记录"核对）。
4. 旧站点**全部恢复正常**，回到控制台点**续跑（先核实待核实行）**：
   两行经查询核实后变 `SAVED` 并补全远端编号，**旧站无重复行**。
5. 再次开启**全站断连**后建批开始：行停在 `QUERY_FAILED`，说明明确提示
   "不得视为不存在"，旧站没有任何新增；恢复后续跑即成功。
6. 异值停批：对旧站已存在的键上传不同结果的 CSV，首行 `CONFLICT`，
   说明里同时给出本地值与远端编号/值，批次停止、后续行不处理。
   人工在旧站更正后，可在该行点**重置待核实**，再续跑（仍先查询核实，不盲填）。

## 五、CSV 格式

表头三列（支持中英文表头与 UTF-8/GB18030 编码）：

```csv
样本号,项目,结果
W2026-001,pH,7.2
W2026-001,浊度,3.50
```

- 结果必须是非负有限十进制数（`0`、`7.2`、`0.00` 均可，负数/`abc`/`Infinity` 拒绝）；
- `(样本号, 项目)` 在文件内不得重复；空行自动跳过；校验失败整批不建。

## 六、本机直接运行（无 Docker 时）

需要 Python 3.11+、Playwright Chromium：

```bash
python3 -m venv --without-pip .venv          # 按本机环境引导 pip
.venv/bin/pip install -r reporter/requirements.txt
.venv/bin/python -m playwright install chromium
./run-local.sh                               # 旧站 :8011，控制台 :8010
```

可用环境变量：`LEGACY_PORT`、`REPORTER_PORT`、`HEADLESS=0`（有显示器时看真实填表）、
`SLOW_MO_MS`、`BROWSER_CHANNEL=chromium`、`LEGACY_URL`、`REPORTER_DB`、`LEGACY_DB`。

## 七、测试

```bash
.venv/bin/python -m pytest tests/unit_test.py -q          # 纯单元测试（含原子建批回滚）
.venv/bin/python -m pytest tests/gate_test.py -q          # 有界闸门并发/取消/交错测试
.venv/bin/python -m pytest tests/concurrency_test.py -q   # 真实 uvicorn 40 并发响应性实测
# 含超过 30 秒忙等的锁超时慢用例：
RUN_SLOW_LOCK_TEST=1 .venv/bin/python -m pytest tests/concurrency_test.py -q
# 端到端（需先按第六节起好两个服务，驱动真实 Chromium）：
REPORTER_URL=http://127.0.0.1:8010 LEGACY_URL=http://127.0.0.1:8011 \
  .venv/bin/python -I tests/e2e_test.py
```

并发实测覆盖（真实 uvicorn + httpx 并发流量）：

- 总容量满额立即 503；排队取消后不登记批次，已执行者锁释放后正常建批；
- 持锁窗口内 40 份并发上传时，首页/进度 API/停止操作最大延迟均远低于 1 秒；
- 写锁持有超过 30 秒忙等 → 可重试 503 且批次零登记，解锁后重传成功；
- 取消与"开始执行"交错：已取消的排队者绝不进入执行，已授名额者取消不生效。

端到端覆盖：CSV 校验、先查后填、同值不重复、异值停批双方值、旧站允许重复键、
保存后断连→待核实→续跑无重复、查询失败不入库、单实例 409、重启恢复 SAVING→待核实、
冲突人工更正后重置再核实。

## 八、关键环境变量

| 变量 | 默认值 | 含义 |
|---|---|---|
| `LEGACY_URL` | `http://legacy:8000` | 旧站基址，后台只经浏览器访问 |
| `HEADLESS` | `1` | 设 `0` 可观察真实浏览器操作 |
| `SLOW_MO_MS` | `0`（compose 内 150） | 浏览器动作放慢，便于演示 |
| `BROWSER_CHANNEL` | 空 | 本机缺 headless-shell 时设 `chromium` |
| `UPLOAD_MAX_ACTIVE` | `4` | 上传建批执行名额（专用线程池同尺寸） |
| `UPLOAD_MAX_QUEUED` | `16` | 上传排队名额，执行+排队满额返回可重试 503 |
| `REPORTER_DB` / `UPLOAD_DIR` / `SCREENSHOT_DIR` | `/data/...` | 本地持久化路径 |

> SQLite 写锁忙等待固定为 30 秒（`db.connect()` 的 `busy_timeout=30000`），
> 不在环境变量中调整；超时时建批接口返回可重试 503 且不登记批次。
