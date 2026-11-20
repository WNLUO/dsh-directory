<!-- 总控协议、令牌持久化和单实例部署约定。 -->
# dsh-directory

公用中转的**总控 / 地址目录**。只干一件事：知道「哪台设备现在挂在哪、能用哪些地址连它」。

- 设备（插件宿主）用长连接把当前所有可用地址报到总控；
- APP 用长连接向总控查 / 订阅这些地址；
- 地址没变化时**一个字都不推**，链路保持静默；
- 在线地址与订阅只放内存；设备令牌独立落盘，重启后继续使用原令牌查询新上报的地址。

这是本项目的后端。以后要做的管理前端放在同一目录的另一个子文件夹里（例如 `web/`），
和后端分开。

## 目录

```
dsh-directory/
  README.md
  rust/            # 后端（Rust + axum + tokio）
  deploy/          # systemd 单元 + nginx 反代片段
```

## 跑起来

```bash
cd rust
cargo run --release
# 默认监听 0.0.0.0:8787
```

环境变量：

| 变量 | 说明 | 默认 |
| --- | --- | --- |
| `BIND` | 监听地址 | `0.0.0.0:8787` |
| `TOKEN_STORE_PATH` | 设备令牌日志路径；目录须可写，现有文件读取失败则拒绝启动 | `data/tokens.jsonl` |
| `MAX_DEVICES` | 设备记录总量上限 | `100000` |
| `MAX_TOKENS` | 设备令牌记录总量上限（超出按最久未注册淘汰） | `200000` |
| `MAX_CONNS_PER_IP` | 单来源 IP 的并发连接上限 | `50` |
| `REG_RATE_PER_SEC` | 全局新注册速率（每秒） | `500` |
| `REG_RATE_BURST` | 全局注册突发上限 | `1000` |

健康检查：`GET /healthz` → `{"ok":true,"devices":N,"watchers":M}`。

## 身份、加密与令牌

两件事分开：**内容端到端加密**（总控看不到），**身份**用每设备一枚令牌（总控用来防冒充）。

- 设备用 `instanceId` 连 `/ws/device`。**首次注册**总控发一枚专属令牌；之后重连/覆盖必须带上
  同一枚，否则当冒充拒绝（插件把令牌落在 relay.json，重启也带得上）。
- 上报的**载荷是密文**：插件用「实例历史密钥 HK」把全部连接地址封成信封（AES-256-GCM），AAD 为
  `dsh-history-v1|control|<instanceId>`；总控只存这串密文、不持钥、解不开。
- 插件把令牌经「插件↔APP」已有链路交给 APP；APP 用同一把 HK 解开载荷。HK 只在配对时下发给这台
  设备的手机，总控永远拿不到。

令牌同时校验设备重连与 APP 查/订阅：拿不出已有令牌就不能覆盖设备地址或读取载荷。载荷内容
对总控不透明，APP 还需要自身的 HK 才能解开。

令牌及最近注册时间写入 `TOKEN_STORE_PATH`，注册成功前完成持久写入；总控重启后继续校验同一枚
令牌。设备在线地址、连接和订阅不落盘，重启后 APP 可用原令牌先订阅，等待插件重新上报新地址。
日志按容量保留令牌并定期压缩；令牌文件丢失会重新签发，损坏或不可写不会静默退回空令牌表。

首次从纯内存版本升级时，没有可导入的历史令牌文件；插件会按原流程收到新令牌并转交 APP。
升级后的后续重启只要保留令牌文件，就不再换令牌。

## 端到端加密（总控是盲的）

- 复用历史离线 blob 那套：HK（32 字节）+ AES-256-GCM，信封 `{v:1,z:0,n,c}`，两端逐字节一致
  （插件 `lib/mirror/history-blob.cjs`、APP `services/viewmodels/history-blob.ts`）。
- AAD 绑 `scope='control'` 与 `instanceId`：换设备、换用途都解不开，也防重放到别的实例。
- **取舍**：总控既然解不开，就不能在服务端按地址做“挑哪台公用中转 / 编址”这类事——需要读地址的
  逻辑只能放在插件 / APP 侧。总控在这里只当“保底查询”的盲存储。

## 协议（WebSocket + JSON）

### 设备侧 `/ws/device?instanceId=<id>[&token=<token>]`

连上后先发 `register`，之后载荷变化发 `update`（每次都是**整份**密文，不是增量）：

```json
{ "type": "register", "payload": "<加密信封 JSON 字符串>" }
{ "type": "update",   "payload": "<加密信封 JSON 字符串>" }
```

`payload` 是 HK 封出来的信封（解开的明文形如 `{ "addresses": [ { "kind": "relay", "url": "..." } ] }`）。
总控不解析它；单条上限 64KB。

总控回：`registered`（带 `token`）/ `updated` / `error`。连接断开 = 设备离线，订阅者收到离线事件。

### APP 侧 `/ws/app?instanceId=<id>&token=<token>`

令牌不对直接 401。连上后发 `query` / `watch` / `unwatch`。总控推给 APP：

```json
{ "type": "snapshot", "instanceId": "...", "online": true, "payload": "<密文或 null>" }
{ "type": "update",   "instanceId": "...", "payload": "<密文>" }
{ "type": "offline",  "instanceId": "..." }
```

APP 用 HK 解 `payload`。**静默规则**：只有在明文地址真变化时设备才会重发（插件侧比较），总控不在
服务端做去重；设备不发就没有推送。

## 防滥用与容量

注册没有额外口令，就靠「令牌绑定 + 容量上限 + 限速」把口子收住；令牌持久化，在线计数和限速驻内存：

- **令牌绑定防冒充**：一台设备首次注册拿到一枚令牌，之后重连/覆盖必须带同一枚；只知道
  `instanceId`、拿不出令牌的人改不动别人的地址、也查不了。
- **令牌表也有上限**：`MAX_TOKENS` 到线后淘汰「最久没来注册」的，挡住「不停换 instanceId
  把内存撑爆」；被淘汰的设备重连会拿一枚新令牌，插件自己更新、再转交 APP。
- **满载优先淘汰「没人查」的**：设备记录到 `MAX_DEVICES` 后，再来新设备时踢掉「最久没被 APP 查、
  且没有活跃订阅」的那条腾位置；全都在被订阅、实在挤不下就回 `server busy` 拒绝新注册，不硬扛。
- **全局注册限速**：新注册走令牌桶（`REG_RATE_PER_SEC` / `REG_RATE_BURST`），CC 洪峰时快速拒绝。
- **每 IP 并发上限**：单来源 IP 最多 `MAX_CONNS_PER_IP` 条连接，超了直接 429。真实 IP 从代理头
  （`X-Forwarded-For` / `X-Real-IP`）取；流量过 CDN，这一条只是粗挡。

## 设计取舍

- **在线状态只放内存**：这一层是「设备 → 地址」的即时表，设备断线即失效，重启不恢复旧地址。
  只有鉴权令牌通过追加日志落盘，避免重启让 APP 失去兜底查询能力；当前为单实例，令牌文件不能由
  多个进程同时写入。
- **令牌不校验 APP 是不是真的那台设备**：只校验它拿到了那台设备的令牌。设备与 APP 的信任
  建立在它们自己那条链路上，总控不掺和。
- **不做流量代理**：数据面（APP 直连中转）不经过总控，总控只在「查地址」这一刻出现，
  不在关键链路上。

## 部署（当前线上）

- 服务器：`SHIYAN`（Rocky Linux 9，x86_64），与官网共用域名 `ai-hub.wnluo.com`。
- 后端：编译好的二进制放在 `/opt/dsh-directory/dsh-directory`，以 systemd 服务
  `dsh-directory` 常驻，只听 `127.0.0.1:8787`（不对公网暴露）。
- 配置：`/opt/dsh-directory/dsh-directory.env`（`BIND` 等环境覆盖，权限 600，不入库）。
- 令牌：systemd 单元通过 `StateDirectory` 创建 `/var/lib/dsh-directory`（服务用户可写、目录 700），
  `TOKEN_STORE_PATH=/var/lib/dsh-directory/tokens.jsonl`；新文件权限 600。更新二进制或重启服务须保留
  该目录，并纳入备份；不把令牌文件放进源码或发布包。容器部署须给该路径挂持久卷。
- 入口：宝塔站点伪静态里加一段反代，把 `https://ai-hub.wnluo.com/findme/` 转到本机后端，
  支持 WebSocket。片段见 `deploy/nginx-location.conf`，systemd 单元见 `deploy/dsh-directory.service`。

更新流程：本机 `cargo build --release` 或把源码传上去在服务器上编，替换 `/opt/dsh-directory/dsh-directory`，
再 `systemctl restart dsh-directory`。首次更新服务单元需执行 `systemctl daemon-reload`。改了 nginx
片段才需要 `nginx -t && nginx -s reload`。

定向验证：`cd rust && cargo test --locked state::tests && cargo test --locked token_store::tests`。测试使用
独立临时目录，不读取或写入部署令牌文件。

上线后校验：`curl https://ai-hub.wnluo.com/findme/healthz` 应返回 `{"ok":true,...}`；
设备/APP 的 WebSocket 走 `wss://ai-hub.wnluo.com/findme/ws/device` 与 `.../ws/app`。
