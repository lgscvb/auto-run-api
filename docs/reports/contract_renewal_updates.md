# 合約續約更新清單

產生時間：2025-12-16
資料來源：2025 客戶繳費表

---

## 需要建立新合約的客戶

這些客戶已續約繳費，但資料庫的合約仍顯示 expired，需要建立新合約：

### 1. 一貝兒美容工作室（位置 5）

| 欄位 | 舊合約 #553 | 新合約資料 |
|------|------------|-----------|
| 客戶ID | 1903 | 1903 |
| 狀態 | expired | **active** |
| 起始日 | 2023-10-01 | **2025-10-01** |
| 到期日 | 2025-10-01 | **2026-04-01** (6個月) |
| 月租 | 1490 | 1490 |
| 繳費週期 | - | 6m |
| 繳費紀錄 | - | 2025-04-30 刷卡 8940, 2025-11-03 繳 8940 |
| 發票 | - | ✔️ |
| 備註 | - | 合約已給 |

### 2. 吉爾哈登工作室（位置 6）

| 欄位 | 舊合約 #547 | 新合約資料 |
|------|------------|-----------|
| 客戶ID | 1897 | 1897 |
| 狀態 | expired | **active** |
| 起始日 | 2024-06-05 | **2025-06-05** |
| 到期日 | 2025-06-05 | **2026-06-05** (1年) |
| 月租 | 1500 | 1500 |
| 繳費週期 | - | Y (年繳) |
| 繳費紀錄 | - | 2025-05-23 繳 18000 |
| 發票 | - | ✔️ |
| 備註 | - | 續約回傳 |

**注意**：此客戶有另一筆合約 #172 (13500/m)，可能是辦公室？

### 3. 七分之二的探索有限公司（位置 69）

**注意**：有兩個客戶記錄，應使用 ID=1958（較新）

| 欄位 | 舊合約 #608 | 新合約資料 |
|------|------------|-----------|
| 客戶ID | **1958** | 1958 |
| 狀態 | expired | **active** |
| 起始日 | 2023-09-21 | **2025-09-21** |
| 到期日 | 2025-09-21 | **2026-09-21** (1年) |
| 月租 | 1490 | **1690** (20280/12) |
| 繳費週期 | - | Y (年繳 20280) |
| 繳費紀錄 | - | 2025-09-30 繳 20280 (全年) |
| 發票 | - | ✔️ |

⚠️ ID=2227 是舊的重複記錄，合約 #1049 已過期

### 4. 小倩媽咪行銷工作室（位置 93）

| 欄位 | 舊合約 #573 | 新合約資料 |
|------|------------|-----------|
| 客戶ID | 1923 | 1923 |
| 狀態 | expired | **active** |
| 起始日 | 2024-12-02 | **2025-12-02** |
| 到期日 | 2025-12-02 | **2026-12-02** (1年) |
| 月租 | 1800 | **1690** (20280/12) |
| 繳費週期 | - | Y (年繳 20280) |
| 繳費紀錄 | - | 2025-11-11 繳 20280 (全年) |
| 發票 | - | ✔️ |
| 備註 | - | 要續約 |

### 5. 華為秝喨國際有限公司（位置 61）- 確認中

| 欄位 | 舊合約 #574 | 繳費紀錄 |
|------|------------|---------|
| 客戶ID | 1924 | 1924 |
| 狀態 | expired | **待確認** |
| 到期日 | 2024-12-10 | 已過期 |
| 繳費紀錄 | - | 2025-06-02 現金 8940 (6個月), 12月待確認 |

### 6. 蕭家如（個人）- 無異議自動續約

| 欄位 | 舊合約 #603 | 新合約資料 |
|------|------------|-----------|
| 客戶ID | 1953 | 1953 |
| 狀態 | expired | **active** |
| 起始日 | 2023-08-07 | 2023-08-07 |
| 到期日 | 2024-08-07 | **2099-12-31** (無異議自動續約) |
| 月租 | 3,000 | 3,000 |
| 繳費週期 | - | M (月付) |
| 繳費紀錄 | - | 2025 年持續月付 $3,000 |
| 備註 | - | 無異議自動續約（月付） |

### 7. 吉品智慧科技有限公司 - 一次繳完

| 欄位 | 舊合約 #624 | 新合約資料 |
|------|------------|-----------|
| 客戶ID | 1974 | 1974 |
| 狀態 | expired | **active** |
| 起始日 | 2024-05-01 | 2024-05-01 |
| 到期日 | 2025-05-01 | **2026-03-15** (民國 115/03/15) |
| 月租 | 12,000 | 12,000 |
| 繳費紀錄 | - | $77,600 + $96,000 + $112,000 = $285,600 (一次繳完) |

---

## 確認已遷出（空位）

| 位置 | 公司 | 狀態 |
|------|------|------|
| 55 | 微笑玩家國際貿易有限公司 | 不續約（已確認） |
| 60 | 光緯企業社 | 已遷出 |

---

## SQL 更新範本

### 方案 A：更新現有合約（如果是同一份合約續約）

```sql
-- 一貝兒美容工作室
UPDATE contracts SET
    status = 'active',
    end_date = '2026-04-01',
    renewal_status = 'renewed',
    renewal_paid_at = '2025-11-03',
    renewal_invoiced_at = '2025-11-03',
    renewal_notes = '合約已給'
WHERE id = 553;

-- 吉爾哈登工作室
UPDATE contracts SET
    status = 'active',
    end_date = '2026-06-05',
    renewal_status = 'renewed',
    renewal_paid_at = '2025-05-23',
    renewal_invoiced_at = '2025-05-23',
    renewal_signed_at = '2025-05-23',
    renewal_notes = '續約回傳'
WHERE id = 547;

-- 七分之二的探索 (使用客戶ID=1958的合約#608)
UPDATE contracts SET
    status = 'active',
    start_date = '2025-09-21',
    end_date = '2026-09-21',
    monthly_rent = 1690,
    renewal_status = 'renewed',
    renewal_paid_at = '2025-09-30',
    renewal_invoiced_at = '2025-09-30'
WHERE id = 608;

-- 小倩媽咪
UPDATE contracts SET
    status = 'active',
    end_date = '2026-12-02',
    monthly_rent = 1690,
    renewal_status = 'renewed',
    renewal_paid_at = '2025-11-11',
    renewal_invoiced_at = '2025-11-11',
    renewal_notes = '要續約'
WHERE id = 573;

-- 蕭家如（無異議自動續約）
UPDATE contracts SET
    status = 'active',
    end_date = '2099-12-31',
    renewal_status = 'renewed',
    renewal_notes = '無異議自動續約（月付）'
WHERE id = 603;

-- 吉品智慧科技（一次繳完）
UPDATE contracts SET
    status = 'active',
    end_date = '2026-03-15',
    renewal_status = 'renewed',
    renewal_notes = '一次繳完'
WHERE id = 624;
```

### 方案 B：建立新合約（如果是新合約）

需要使用 CRM 前端或 API 建立新合約。

---

## 欄位說明

| 欄位 | 說明 |
|------|------|
| renewal_status | none / pending / confirmed / renewed |
| renewal_notified_at | 通知續約的日期 |
| renewal_confirmed_at | 客戶確認續約的日期 |
| renewal_paid_at | 繳費日期 |
| renewal_invoiced_at | 開立發票日期 |
| renewal_signed_at | 簽回合約日期 |
| renewal_notes | 備註 |

---

## 待確認事項

1. ~~**七分之二的探索**：有兩個客戶記錄~~ ✅ 使用 ID=1958，合約 #608
2. **華為秝喨**：12月是否續約？（6月有繳 8940）
3. ~~**續約方式**：是更新現有合約還是建立新合約？~~ ✅ 更新現有合約
4. ~~**蕭家如 #603**：持續月付但合約過期~~ ✅ 無異議自動續約，end_date 設為 2099-12-31
5. ~~**吉品智慧科技 #624**：大額付款~~ ✅ 一次繳完，到期日 2026-03-15

---

## ✅ 位置更新 SQL（已執行 2025-12-16）

```sql
-- 一貝兒美容工作室 - 位置 5
UPDATE contracts SET position_number = 5 WHERE id = 553;

-- 吉爾哈登工作室 - 位置 6
UPDATE contracts SET position_number = 6 WHERE id = 547;

-- 七分之二的探索 - 位置 69
UPDATE contracts SET position_number = 69 WHERE id = 608;

-- 小倩媽咪 - 位置 93
UPDATE contracts SET position_number = 93 WHERE id = 573;
```

---

## 發票狀態更新 SQL

⚠️ 發現：所有 211 筆付款記錄的 invoice_status 都是 pending

根據 2025 客戶繳費表中有「發票 ✔️」標記的，應更新 invoice_status：

```sql
-- 一貝兒美容工作室 (2025-04, 2025-10)
UPDATE payments SET invoice_status = 'issued' WHERE id IN (60, 171);

-- 吉爾哈登工作室 (2025-06)
UPDATE payments SET invoice_status = 'issued' WHERE id = 89;

-- 七分之二的探索 (2025-09)
UPDATE payments SET invoice_status = 'issued' WHERE id = 144;

-- 小倩媽咪 (2025-12)
UPDATE payments SET invoice_status = 'issued' WHERE id = 201;

-- 華為秝喨 (2025-06)
UPDATE payments SET invoice_status = 'issued' WHERE id = 97;
```

### payments 表欄位說明

| 欄位 | 說明 | 可能值 |
|------|------|--------|
| payment_status | 付款狀態 | pending, paid, overdue |
| invoice_status | 發票狀態 | pending, issued, sent |
| invoice_number | 發票號碼 | 字串 |
| invoice_date | 開票日期 | 日期 |
| reminder_count | 催繳次數 | 數字 |
| last_reminder_at | 最後催繳日期 | 日期時間 |
