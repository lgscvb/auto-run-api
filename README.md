# Hour Jungle CRM

PostgreSQL + PostgREST + MCP Server 架構的 CRM 系統

## 架構

```
┌─────────────────────────────────────────────────────────────────┐
│                        Client Layer                              │
├─────────────────┬─────────────────┬─────────────────────────────┤
│   LINE Bot      │   Simple WebUI  │   AI Agent (Claude)         │
└────────┬────────┴────────┬────────┴─────────────┬───────────────┘
         │                 │                      │
         ▼                 ▼                      ▼
┌─────────────────────────────────────────────────────────────────┐
│                      MCP Server (Python)                         │
│  ┌──────────────┬──────────────┬──────────────┬──────────────┐  │
│  │ crm_query    │ payment_ops  │ line_notify  │ report_gen   │  │
│  └──────────────┴──────────────┴──────────────┴──────────────┘  │
└─────────────────────────────┬───────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    PostgREST (API Layer)                         │
│     自動生成 RESTful API + JWT 認證 + RLS 安全                    │
└─────────────────────────────┬───────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    PostgreSQL (Core)                             │
│  Tables | Views | Functions | Triggers | RLS Policies           │
└─────────────────────────────────────────────────────────────────┘
```

## 快速開始

### 1. 環境準備

```bash
# 複製環境變數
cp .env.example .env

# 編輯 .env 填入密碼
nano .env
```

### 2. 啟動服務

```bash
docker-compose up -d
```

### 3. 驗證服務

```bash
# 檢查健康狀態
curl http://localhost:8080/health

# 列出可用工具
curl http://localhost:8080/tools

# 測試 PostgREST API
curl http://localhost:3000/branches
```

### 4. 匯入資料

```bash
# 設定環境變數
export POSTGRES_HOST=localhost
export POSTGRES_PASSWORD=your_password

# 執行匯入
python scripts/import_data.py --data-dir /path/to/csv/files
```

## 目錄結構

```
hourjungle-crm/
├── docker-compose.yml      # Docker 編排
├── .env.example            # 環境變數範例
├── sql/
│   └── init/
│       ├── 01_schema.sql   # 資料表結構
│       ├── 02_views.sql    # Views
│       ├── 03_functions.sql # Functions & Triggers
│       ├── 04_rls.sql      # Row-Level Security
│       └── 05_seed.sql     # 初始資料
├── mcp-server/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py             # FastAPI + MCP Server
│   └── tools/
│       ├── crm_tools.py    # CRM 工具
│       ├── line_tools.py   # LINE 工具
│       └── report_tools.py # 報表工具
├── nginx/
│   └── nginx.conf          # Nginx 配置
├── scripts/
│   └── import_data.py      # 資料匯入腳本
└── .github/
    └── workflows/
        └── deploy.yml      # CI/CD
```

## API 端點

### MCP Server (Port 8080)

| 端點 | 說明 |
|------|------|
| `GET /health` | 健康檢查 |
| `GET /tools` | 列出所有工具 |
| `POST /tools/call` | 調用工具 |
| `POST /mcp/initialize` | MCP 初始化 |
| `POST /mcp/tools/list` | MCP 工具列表 |
| `POST /mcp/tools/call` | MCP 工具調用 |

### PostgREST (Port 3000)

| 端點 | 說明 |
|------|------|
| `GET /branches` | 場館列表 |
| `GET /customers` | 客戶列表 |
| `GET /contracts` | 合約列表 |
| `GET /payments` | 付款列表 |
| `GET /v_customer_summary` | 客戶摘要 View |
| `GET /v_payments_due` | 應收款 View |
| `GET /v_renewal_reminders` | 續約提醒 View |

## MCP 工具列表

### 查詢工具
- `crm_search_customers` - 搜尋客戶
- `crm_get_customer_detail` - 客戶詳情
- `crm_list_payments_due` - 應收款列表
- `crm_list_renewals_due` - 續約提醒

### 操作工具
- `crm_create_customer` - 建立客戶
- `crm_update_customer` - 更新客戶
- `crm_record_payment` - 記錄繳費
- `crm_create_contract` - 建立合約

### LINE 工具
- `line_send_message` - 發送訊息
- `line_send_payment_reminder` - 繳費提醒
- `line_send_renewal_reminder` - 續約提醒

### 報表工具
- `report_revenue_summary` - 營收摘要
- `report_overdue_list` - 逾期報表
- `report_commission_due` - 佣金報表

## 部署

### GCP VM 設定

```bash
# 創建 VM
gcloud compute instances create hourjungle-crm \
  --zone=asia-east1-b \
  --machine-type=e2-medium \
  --boot-disk-size=50GB \
  --boot-disk-type=pd-ssd \
  --image-family=ubuntu-2204-lts \
  --image-project=ubuntu-os-cloud

# 安裝 Docker
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER

# Clone 專案
git clone https://github.com/YOUR_USERNAME/hourjungle-crm.git /opt/hourjungle-crm

# 啟動
cd /opt/hourjungle-crm
cp .env.example .env
nano .env  # 填入密碼
docker-compose up -d
```

### GitHub Actions Secrets

| Secret | 說明 |
|--------|------|
| `GCP_SSH_KEY` | SSH 私鑰 |
| `GCP_USER` | VM 使用者 |
| `GCP_HOST` | VM IP |

## 資料庫

### 場館 (branches)

| ID | Code | Name |
|----|------|------|
| 1 | DZ | 大忠館 |
| 2 | TWD | 台灣大道環瑞館 |

### 資料表

- `branches` - 場館
- `accounting_firms` - 會計事務所
- `customers` - 客戶
- `contracts` - 合約
- `payments` - 付款
- `commissions` - 佣金
- `audit_logs` - 審計日誌
- `notification_queue` - 通知佇列
- `system_settings` - 系統設定

## License

Private - Hour Jungle
