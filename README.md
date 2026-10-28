# M2_P2P_DB

A P2P distributed data store built on **iroh** (QUIC, NAT traversal, no central server) and **Automerge** (CRDT sync). Runs natively on Windows as a Rust executable — no Docker, no centralized database server, no permanently-online central node.

> 本專案基於 [n0-computer/iroh](https://github.com/n0-computer/iroh) 的 clone，新增 workspace 成員 `M2_P2P_DB/`。iroh 本體程式碼未做任何修改；所有改動都集中在 crate 本身與 workspace 設定。

## 資料模型

每筆紀錄固定這些欄位（存在 Automerge 文件中，key 為 `record:<uuid>`）：

```json
{
  "record_id": "1eea15db-86e4-43b9-b732-84eb222ff659",
  "updated_at": "2026-10-15T02:28:30+00:00",
  "m2_apex_ver": "2026.10.1",
  "cp_acc": "open_source",
  "ghname": "oahsiao",
  "loginname": "v-oahsiao",
  "cp_aic": 87226,
  "device_id": "local",
  "device_name": "MY-PC",
  "info1": "xx",
  "info2": "yy"
}
```

- `record_id` — 建立時自動產生的 UUID v4
- `updated_at` — ISO 8601 時間戳，每次寫入自動更新。預設 UTC (`+00:00`)；在節點環境設 `M2_TZ_OFFSET_MINUTES=480` 即輸出 `+08:00` 這類本機時區偏移
- `m2_apex_ver` — 當下使用者的 M2 Apex 版本（字串，如 `2026.10.1`）
- `cp_acc` — Copilot 帳號種類（字串）：`feature_flag`＝Microsoft GitHub Copilot feature flag、`open_source`＝Open Source、`emu`＝EMU 帳號（`v-` account）。原樣儲存、不做驗證，之後新增種類不需改程式
- `ghname` — 顯示名（GitHub 風格，如 `oahsiao`）
- `loginname` — 帳號名（如 `v-oahsiao`）
- `device_id` — 可選，預設 `"local"`，通常放 iroh endpoint ID 標記資料來源
- `device_name` — 人類可讀的裝置名（如 `MY-PC`）

> Schema 變更提醒：舊版的 `username` 已由 `ghname` + `loginname` 取代，並新增 `device_name`；其後再新增 `m2_apex_ver` + `cp_acc`（緊接在 `updated_at` 之後）。舊版程式寫出的 `doc.automerge` 仍保留舊欄位名，新程式讀取時對應欄位為空字串；請刪除舊 data-dir 重新開始。

## 架構

| 元件 | 角色 |
|---|---|
| **iroh** | P2P 傳輸層：QUIC 連線、NAT 打洞、relay 中繼。無中央伺服器 |
| **Automerge** | CRDT 文件：多節點並發編輯自動合併，無衝突 |
| **mDNS**（`iroh-mdns-address-lookup`） | 區網節點自動互相發現，零手動設定 |
| **Gossip 迴圈** | 每 30 秒對所有已知 peers 差量同步 |

同步協定：Automerge sync protocol 跑在 iroh QUIC stream 上（ALPN `m2-p2p-db/sync/0`）。文件以原子寫入（暫存檔 + rename）存於 `<data-dir>/doc.automerge`。

**節點如何自動對齊（align）：**

1. **mDNS 自動發現** — 區網上的節點互相找到對方並記進 peer 名單
2. **peer 名單存在文件裡跟著資料傳播** — `peer:<endpoint-id>` 鍵本身就是 CRDT 資料的一部分
3. **30 秒 gossip** — 每個節點定期對所有已知 peers 同步（只傳差量）

新節點在區網內**完全不需要手動 connect**；跨網路（無法 mDNS）時只需 `listen <peer-id>` 或 `connect <peer-id>` 種一個認識關係，之後名單隨資料擴散，gossip 接手。20 個節點不必手動建 190 對連線。

## 建置

```bash
cd D:\CODE\GITHUB\M2_P2P_DB
cargo build -p M2_P2P_DB
```

執行檔：`target\debug\M2_P2P_DB.exe`。

## 使用範例

### 啟動兩個節點（兩個終端機）

```bash
# 終端機 1
.\target\debug\M2_P2P_DB.exe --data-dir nodeA listen
```

輸出：

```
node endpoint id: 5105f5362e6e2fbfe2992b59c435e16b3857abdfc25b8c39d948e19da5e1cdc9
data dir: nodeA
listening for sync connections (ALPN m2-p2p-db/sync/0)
mdns discovery: enabled (LAN peers are discovered automatically)
```

```bash
# 終端機 2（同一台機器換 data-dir 即另一節點；不同機器直接跑同指令）
.\target\debug\M2_P2P_DB.exe --data-dir nodeB listen
```

### CRUD（第三個終端機）

```bash
# 新增（record_id 自動產生 UUID，updated_at 自動蓋時間）
.\target\debug\M2_P2P_DB.exe --data-dir nodeA create --ghname oahsiao --loginname v-oahsiao --cp-aic 87226 --info1 xx --info2 yy --device-name MY-PC --m2-apex-ver 2026.10.1 --cp-acc open_source

# 輸出：
# created 1eea15db-86e4-43b9-b732-84eb222ff659
# { "record_id": "1eea15db-...", "updated_at": "...", "m2_apex_ver": "2026.10.1", "cp_acc": "open_source", ... }

# 讀取（用上面記下的 UUID）
.\target\debug\M2_P2P_DB.exe --data-dir nodeA read 1eea15db-86e4-43b9-b732-84eb222ff659

# 更新（只給要改的欄位，updated_at 自動重蓋）
.\target\debug\M2_P2P_DB.exe --data-dir nodeA update 1eea15db-86e4-43b9-b732-84eb222ff659 --cp-aic 90000 --device-name NEW-PC --cp-acc emu

# 刪除
.\target\debug\M2_P2P_DB.exe --data-dir nodeA delete 1eea15db-86e4-43b9-b732-84eb222ff659

# 列出節點上全部紀錄
.\target\debug\M2_P2P_DB.exe --data-dir nodeA list
```

### 驗證自動同步

兩個節點都 `listen` 後，對 A 新增一筆，**不下任何 connect**，等 30–60 秒：

```bash
.\target\debug\M2_P2P_DB.exe --data-dir nodeB list
```

B 上就會看到 A 的資料——mDNS 發現 + gossip 已自動對齊。更新與刪除同樣會傳播。

### 跨網路（無區網）情境

```bash
# 新節點啟動時種一個遠端 peer 的 endpoint ID，之後 gossip 接手
.\target\debug\M2_P2P_DB.exe --data-dir nodeC listen <遠端節點的endpoint-id>
```

### 除錯日誌

```bash
$env:RUST_LOG="iroh=info,M2_P2P_DB=debug"; .\target\debug\M2_P2P_DB.exe --data-dir nodeA listen
```

## 改動內容（相對於上游 iroh）

1. **新增 `M2_P2P_DB/` crate**（本專案全部功能所在）：
   - `src/main.rs` — 完整應用：iroh `Endpoint`/`Router` + 自訂 ALPN 上跑 Automerge sync、mDNS 自動發現、30 秒 gossip 迴圈、磁碟持久化（原子寫入）、CLI（listen / connect / create / read / update / delete / list）
   - `Cargo.toml` — 依賴：`iroh`（path 引用本 repo）、`automerge 0.12`、`iroh-mdns-address-lookup 0.6`、`clap`、`uuid` 等
   - `README.md` — crate 級技術文件（設計筆記）
2. **根 `Cargo.toml`**：
   - workspace members 加入 `"M2_P2P_DB"`
   - 新增 `[patch.crates-io]`：把 `iroh`/`iroh-base` 統一 patch 到本 repo checkout，讓第三方 crate（`iroh-mdns-address-lookup`）依賴的發行版 iroh 與本 repo 的 iroh 合併為同一份，避免型別衝突
3. **`Cargo.lock`** — 隨新增依賴更新（automerge、uuid、iroh-mdns-address-lookup 及其依賴）

iroh 本體、其他 workspace 成員與範例皆未改動。

## 已知限制

- **同欄位並發寫入採 LWW**：兩節點同時改同一筆紀錄的 `cp_aic`，時間戳新的贏；不同欄位、不同紀錄的並發寫入則完全無衝突合併
- **Schema 相容性（新舊版本混跑）**：新增欄位是加法且合併安全的。紀錄是扁平 map、每次寫入都是 per-key op，所以舊版 binary 讀到不認識的鍵只會忽略（不報錯），舊版 `update` 也不會刪掉它們（只 put 自己知道的欄位）——新欄位能存活並繼續傳播；反之新程式讀舊資料時，新欄位為空字串。唯一做不到的方向是「舊版設定或顯示新欄位」。要一起更新的是本 crate 之外、把欄位集合視為固定的消費者（嚴格 JSON schema、固定欄位的匯入腳本等）。已由 `merges_with_data_written_by_an_older_build` 測試守住
- **gossip 間隔固定 30 秒**（`src/main.rs` 中 `Duration::from_secs(30)`）
- mDNS 失敗（防火牆/VPN 限制）會自動降級為手動 connect，功能不受影響
- 同步狀態存在記憶體（每 peer 一個 `sync::State`），20 節點規模沒有問題

## License

iroh 上游授權（MIT / Apache-2.0）適用於本 repo 的 iroh 部分；`M2_P2P_DB` crate 同樣採 MIT / Apache-2.0。
