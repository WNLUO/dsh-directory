# dsh-directory

公用中转的**总控 / 地址目录**。只干一件事：知道「哪台设备现在挂在哪、能用哪些地址连它」。

- 设备（插件宿主）用长连接把当前所有可用地址报到总控；
- APP 用长连接向总控查 / 订阅这些地址；
- 地址没变化时**一个字都不推**，链路保持静默；
- 全内存、无数据库，进程重启后设备重连即恢复。

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
| `MAX_DEVICES` | 设备记录总量上限 | `100000` |
| `MAX_TOKENS` | 设备令牌记录总量上限（超出按最久未注册淘汰） | `200000` |
| `MAX_CONNS_PER_IP` | 单来源 IP 的并发连接上限 | `50` |
| `REG_RATE_PER_SEC` | 全局新注册速率（每秒） | `500` |
| `REG_RATE_BURST` | 全局注册突发上限 | `1000` |

健康检查：`GET /healthz` → `{"ok":true,"devices":N,"watchers":M}`。

## 身份与令牌

令牌不是全局共享的，而是**每台设备一个**，由这台设备的插件和它的 APP 共用：

1. 设备用 `instanceId` 连上 `/ws/device` 注册。**首次注册**总控发一枚专属令牌；之后重连/覆盖
   **必须带上同一枚令牌**，否则当冒充拒绝（插件把令牌落在 relay.json，重启也带得上）；
2. 总控给这台设备发一个专属令牌（注册回包里带 `token`），插件把它存下来；
3. 插件通过它和 APP 之间已有的那条连接，把令牌告诉 APP（这条链路不经过总控）；
4. APP 拿着这枚令牌连 `/ws/app` 查 / 订阅这台设备。

令牌只保护「查地址」这一侧：别人拿不到这枚令牌就查不到这台设备的地址。设备注册本身是开放的——
谁知道某个 `instanceId` 就能为它注册/覆盖地址，所以 `instanceId` 要当作不可猜的随机串用。
老用户升级插件后走的就是这条：APP 先连上插件，插件再从总控拿到令牌转交给它。

令牌随设备记录存在内存里，**不随设备掉线删除**（重连要拿回同一个，APP 手里那枚才不会失效），
进程重启后清空——设备重连会拿一枚新的，插件再同步给 APP 即可。

## 协议（WebSocket + JSON）

### 设备侧 `/ws/device?instanceId=<id>`

连上后先发 `register`，之后地址变化发 `update`（每次都是**整组**地址，不是增量）：

```json
{ "type": "register", "addresses": [ { "kind": "relay", "url": "wss://relay.example/tunnel" },
                                     { "kind": "lan",   "url": "http://192.168.1.5:49906" },
                                     { "kind": "ipv6",  "url": "http://[fd00::5]:49906" } ] }
```

`kind` 可取 `relay` / `lan` / `ipv6` / `direct`，可省略。空地址、重复项在入库前被丢掉。

总控回：

- `{"type":"registered","instanceId":"...","token":"..."}` — 注册成功，给出该设备令牌；
- `{"type":"updated"}` — 一次 `update` 被收下；
- `{"type":"error","message":"..."}` — 报文不合法 / 未先注册。

连接断开 = 这台设备离线，订阅它的 APP 会收到离线事件。

### APP 侧 `/ws/app?instanceId=<id>&token=<token>`

令牌不对直接 401，连不上。连上后发：

- `{"type":"query"}` — 查一次，回一份 `snapshot`；
- `{"type":"watch"}` — 订阅：先回一份 `snapshot`，之后只在变化 / 掉线时推送；
- `{"type":"unwatch"}` — 取消订阅。

总控推给 APP：

```json
{ "type": "snapshot", "instanceId": "...", "online": true, "addresses": [ ... ] }
{ "type": "update",   "instanceId": "...", "addresses": [ ... ] }   // 订阅期间地址变了
{ "type": "offline",  "instanceId": "..." }                          // 订阅期间设备掉了
```

**静默规则**：只有地址真的变了（比较时与顺序无关）才推 `update`；没变化不发任何东西，
不靠定时心跳刷数据。设备重连把地址换个顺序不算变化。

## 防滥用与容量

注册没有额外口令，就靠「令牌绑定 + 容量上限 + 限速」把口子收住，全在内存里：

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

- **内存而非数据库**：这一层就是一张「设备 → 地址」的即时表，设备是长连接、断线即失效，
  落盘意义不大。单进程足够；要水平扩容或重启不丢，把状态层换成 Redis 即可（`state.rs` 是
  唯一碰状态的地方）。
- **令牌不校验 APP 是不是真的那台设备**：只校验它拿到了那台设备的令牌。设备与 APP 的信任
  建立在它们自己那条链路上，总控不掺和。
- **不做流量代理**：数据面（APP 直连中转）不经过总控，总控只在「查地址」这一刻出现，
  不在关键链路上。

## 部署（当前线上）

- 服务器：`SHIYAN`（Rocky Linux 9，x86_64），与官网共用域名 `ai-hub.wnluo.com`。
- 后端：编译好的二进制放在 `/opt/dsh-directory/dsh-directory`，以 systemd 服务
  `dsh-directory` 常驻，只听 `127.0.0.1:8787`（不对公网暴露）。
- 配置：`/opt/dsh-directory/dsh-directory.env`（只放 `BIND`，权限 600，不入库）。
- 入口：宝塔站点伪静态里加一段反代，把 `https://ai-hub.wnluo.com/findme/` 转到本机后端，
  支持 WebSocket。片段见 `deploy/nginx-location.conf`，systemd 单元见 `deploy/dsh-directory.service`。

更新流程：本机 `cargo build --release` 或把源码传上去在服务器上编，替换 `/opt/dsh-directory/dsh-directory`，
再 `systemctl restart dsh-directory`。改了 nginx 片段才需要 `nginx -t && nginx -s reload`。

上线后校验：`curl https://ai-hub.wnluo.com/findme/healthz` 应返回 `{"ok":true,...}`；
设备/APP 的 WebSocket 走 `wss://ai-hub.wnluo.com/findme/ws/device` 与 `.../ws/app`。
