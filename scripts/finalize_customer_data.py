#!/usr/bin/env python3
"""
完成客戶資料處理：
1. 更新 LINE UID（從 id_mapping.csv）
2. 標記潛客 vs 正式客戶
3. 輸出缺少 UID 的客戶清單（供主動補齊）
4. 輸出潛客清單（有 UID 但不在 CRM 中）
"""

import pandas as pd
from pathlib import Path

# 路徑設定
SCRIPT_DIR = Path(__file__).parent
OUTPUT_DIR = SCRIPT_DIR / "output"

# 讀取資料
print("=== 讀取資料 ===")
customers_df = pd.read_csv(OUTPUT_DIR / "customers_cleaned.csv")
contracts_df = pd.read_csv(OUTPUT_DIR / "contracts_cleaned.csv")
id_mapping_df = pd.read_csv(OUTPUT_DIR / "id_mapping.csv")
id_unmatched_df = pd.read_csv(OUTPUT_DIR / "id_unmatched.csv")

print(f"客戶資料: {len(customers_df)} 筆")
print(f"合約資料: {len(contracts_df)} 筆")
print(f"ID 對應: {len(id_mapping_df)} 筆")
print(f"未匹配 LINE: {len(id_unmatched_df)} 筆")

# === 1. 更新 LINE UID ===
print("\n=== 1. 更新 LINE UID ===")
updated_count = 0
for idx, row in id_mapping_df.iterrows():
    legacy_id = row['legacy_id']
    line_uid = row['line_user_id']

    # 找到客戶
    mask = customers_df['legacy_id'] == legacy_id
    if mask.any():
        current_uid = customers_df.loc[mask, 'line_user_id'].iloc[0]
        if pd.isna(current_uid) or current_uid == '':
            customers_df.loc[mask, 'line_user_id'] = line_uid
            updated_count += 1

print(f"更新了 {updated_count} 筆 LINE UID")

# 統計 LINE UID 填充率
uid_filled = customers_df['line_user_id'].notna() & (customers_df['line_user_id'] != '')
print(f"LINE UID 填充率: {uid_filled.sum()}/{len(customers_df)} ({uid_filled.sum()/len(customers_df)*100:.1f}%)")

# === 2. 標記客戶狀態 ===
print("\n=== 2. 標記客戶狀態 ===")

from datetime import datetime

# 判斷合約是否有效（end_date >= 今天）
today = datetime.now().strftime('%Y-%m-%d')
contracts_df['is_active'] = contracts_df['end_date'].fillna('') >= today

# 有效合約的客戶
active_contract_ids = set(contracts_df[contracts_df['is_active']]['customer_legacy_id'].unique())
all_contract_ids = set(contracts_df['customer_legacy_id'].unique())

print(f"有效合約客戶: {len(active_contract_ids)} 筆")
print(f"所有有合約記錄的客戶: {len(all_contract_ids)} 筆")

# 更新狀態
for idx, row in customers_df.iterrows():
    legacy_id = row['legacy_id']
    if legacy_id in active_contract_ids:
        customers_df.loc[idx, 'status'] = 'active'
    else:
        # 檢查是否有任何合約記錄
        has_any_contract = legacy_id in all_contract_ids
        if has_any_contract:
            customers_df.loc[idx, 'status'] = 'churned'  # 有合約但已過期
        else:
            customers_df.loc[idx, 'status'] = 'prospect'  # 無合約

# 統計
status_counts = customers_df['status'].value_counts()
print("客戶狀態統計:")
for status, count in status_counts.items():
    print(f"  {status}: {count} 筆")

# === 3. 輸出缺少 UID 的正式客戶清單 ===
print("\n=== 3. 輸出缺少 UID 的客戶清單 ===")

# 正式客戶但沒有 LINE UID
missing_uid = customers_df[
    (customers_df['status'] == 'active') &
    (customers_df['line_user_id'].isna() | (customers_df['line_user_id'] == ''))
]

missing_uid_list = missing_uid[['legacy_id', 'name', 'company_name', 'phone', 'status']].copy()
missing_uid_list.to_csv(OUTPUT_DIR / "missing_uid_customers.csv", index=False, encoding='utf-8-sig')
print(f"缺少 LINE UID 的正式客戶: {len(missing_uid_list)} 筆 → missing_uid_customers.csv")

# === 4. 輸出潛客清單（有 UID 但不在 CRM 中）===
print("\n=== 4. 輸出潛客清單 ===")

# 這些是從 LINE 來的但不在客戶資料中的人
prospects = id_unmatched_df.copy()
prospects['status'] = 'prospect_from_line'
prospects.to_csv(OUTPUT_DIR / "prospects_from_line.csv", index=False, encoding='utf-8-sig')
print(f"LINE 潛客（有 UID 但不在 CRM）: {len(prospects)} 筆 → prospects_from_line.csv")

# === 5. 儲存更新後的客戶資料 ===
print("\n=== 5. 儲存更新後的資料 ===")
customers_df.to_csv(OUTPUT_DIR / "customers_final.csv", index=False, encoding='utf-8-sig')
print(f"最終客戶資料: {len(customers_df)} 筆 → customers_final.csv")

# === 6. 生成年底補助提醒清單 ===
print("\n=== 6. 生成年底補助提醒清單 ===")

# 篩選：正式客戶 + 沒有 LINE UID
reminder_list = missing_uid_list.copy()
reminder_list['reminder_message'] = reminder_list.apply(
    lambda row: f"親愛的 {row['company_name'] if pd.notna(row['company_name']) else row['name']} 負責人您好，\n\n又到了年底了！提醒您今年的勞動部補助申請截止日為 12/31。\n\n如果您尚未申請，請盡速申請，或留下您的統一編號，我們可以直接幫您查詢資格，或免費協助您先提出申請搶先卡位。\n\nHour Jungle 商務中心",
    axis=1
)
reminder_list.to_csv(OUTPUT_DIR / "reminder_list.csv", index=False, encoding='utf-8-sig')
print(f"年底補助提醒清單: {len(reminder_list)} 筆 → reminder_list.csv")

# === 總結 ===
print("\n" + "="*50)
print("總結")
print("="*50)
print(f"正式客戶 (active): {status_counts.get('active', 0)} 筆")
print(f"  - 有 LINE UID: {(customers_df['status'] == 'active').sum() - len(missing_uid_list)} 筆")
print(f"  - 缺少 LINE UID: {len(missing_uid_list)} 筆 (待補齊)")
print(f"流失客戶 (churned): {status_counts.get('churned', 0)} 筆")
print(f"潛客 (prospect): {status_counts.get('prospect', 0)} 筆")
print(f"LINE 潛客 (有 UID 但不在 CRM): {len(prospects)} 筆")
