# 总控后端 · 备忘

> 这是「总控后端」的常驻笔记：术语、部署现状、已定/待定的设计。重要变化随手更新这里，
> 后面找东西先看这一页，不用再去扫代码。

## 术语

- **总控后端** = 本项目（仓库 `WNLUO/dsh-directory`，代码在 `rust/`）。它是一张「设备 → 连接地址」
  的地址目录：设备长连接上报地址，APP 长连接查询/订阅地址。不代理数据流量。

## 部署现状（线上）

- 服务器：`SHIYAN`（Rocky Linux 9，x86_64），与官网共用域名。
- 二进制：`/opt/dsh-directory/dsh-directory`，systemd 服务名 `dsh-directory`，只监听 `127.0.0.1:8787`。
- 配置：`/opt/dsh-directory/dsh-directory.env`（只放 `BIND`）。
- 公网入口：`https://ai-hub.wnluo.com/findme/`；WebSocket 走 `/findme/ws/device`、`/findme/ws/app`。
- 反代：宝塔伪静态 `html_ai-hub.wnluo.com.conf` 里追加的 `location /findme/`（片段见 `deploy/nginx-location.conf`）。
- 存储：**全内存、单实例**，进程重启清空；设备重连即恢复。不接 Redis（决定：只放一台总控）。
- 身份：每台设备一枚令牌。首次注册总控下发，之后重连/覆盖必须带同一枚（拿不出就拒绝，防冒充）；
  插件把令牌落在 relay.json 并在重连 URL 上带上，同时经状态心跳转交 APP。APP 拿它查询/订阅。

## 防滥用与容量（已实现）

注册没有额外口令，靠「令牌绑定 + 容量上限 + 限速」收口，全在内存里、O(1)，默认值可用环境变量覆盖：

1. **令牌绑定防冒充**：首次注册发一枚令牌；之后重连/覆盖必须带同一枚，否则拒绝。只知道
   `instanceId`、拿不出令牌的人改不动别人地址、也查不了。
2. **令牌表也有上限**：`MAX_TOKENS`（默认 20 万）到线后淘汰「最久没来注册」的，挡住「不停换
   instanceId 撑爆内存」。被淘汰的设备重连拿新令牌，插件自行更新并转交 APP。
3. **满载优先淘汰「没人查」的**：`MAX_DEVICES`（默认 10 万）到线后，踢掉「最久没被 APP 查、且
   没有活跃订阅」的那条腾位；全都在用就快速拒绝（`server busy`）。
4. **CC 洪峰**：全局令牌桶（`REG_RATE_PER_SEC` 默认 500/s、`REG_RATE_BURST` 默认 1000）+ 每 IP
   并发上限（`MAX_CONNS_PER_IP` 默认 50，过 CDN 时真实 IP 从代理头取、精度有限）。
5. 不再按「没人查就断开」做定时清理——那会和插件断线重连互相打架、造成连接风暴；闲置只靠上面的
   满载淘汰 + 限速来兜。

## 插件 / APP 集成（已接入，均为内置、用户无感）

- **内置常量**：总控地址（`https://ai-hub.wnluo.com/findme` 与对应 wss）内置在插件（`lib/relay/control.js`，
  可用 `DSH_CONTROL_URL` 覆盖联调）与 APP 侧；公用中转 host 清单在插件里也是内置（`DSH_CONTROL_PUBLIC_HOSTS`
  可覆盖）。用户不配置。
- **插件**：**只上报落在公用中转清单上的中转地址**（自建中转、局域网、内网 IPv6 一律不上报，避免泄露
  用户内网/自建信息）；地址变化推 update；令牌落 relay.json、重连带上，并经已鉴权的状态心跳交给 APP；
  断线退避重连。
- **APP**：心跳里的 `control` 落进会话；所有直接地址都探不通时（且不在「只走局域网 / 只走直连」两档），
  用令牌连 `/ws/app` 订阅一次拿回中转地址并入会话（排队首）作为又一条发现来源；刚判离线时主动催一次，
  带回 60s 节流、离线跳过、失败静默。
- 两侧测试：插件 `test/control-addresses.test.cjs`、`test/control-client.test.cjs`（含真 WebSocket）；
  APP 侧 `tsc` 通过、相关守卫绿。后端 `cargo test` 覆盖令牌绑定/上限/限速/满载淘汰。
