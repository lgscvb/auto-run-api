#!/usr/bin/env python3
"""
Hour Jungle CRM - Excel 資料清洗腳本
清洗財務 Excel 檔案並輸出標準 CSV

原始檔案:
- 客戶資料表crm.xlsx (大忠館)
- 環瑞客戶資料表crm.xlsx (環瑞館)
- 2025 客戶繳費.xlsx
- 2026客戶繳費.xlsx
- 環2025客戶繳費.xlsx
"""

import pandas as pd
import re
import os
from datetime import datetime
from typing import Optional, Dict, List, Any
import json

# 設定路徑
SOURCE_DIR = "/Users/daihaoting_1/Downloads/原始資料"
OUTPUT_DIR = "/Users/daihaoting_1/Desktop/code/hourjungle-crm/scripts/output"

# 確保輸出目錄存在
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ============================================================================
# 資料清洗函數
# ============================================================================

def convert_roc_date(roc_date: str) -> Optional[str]:
    """民國年轉西元年: 114/04/08 → 2025-04-08"""
    if not roc_date or pd.isna(roc_date):
        return None

    # 轉成字串並清理
    date_str = str(roc_date).strip()
    if not date_str or date_str in ['N', '-', '待補', 'n', '']:
        return None

    # 嘗試多種格式
    patterns = [
        r'(\d{2,3})[/.](\d{1,2})[/.](\d{1,2})',  # 114/04/08 or 114.04.08
        r'(\d{2,3})[/.](\d{1,2})',  # 114/04 (只有年月)
    ]

    for pattern in patterns:
        match = re.search(pattern, date_str)
        if match:
            groups = match.groups()
            if len(groups) >= 2:
                year = int(groups[0]) + 1911
                month = groups[1].zfill(2)
                day = groups[2].zfill(2) if len(groups) > 2 else '01'
                try:
                    # 驗證日期有效性
                    datetime(year, int(month), int(day))
                    return f"{year}-{month}-{day}"
                except ValueError:
                    pass

    return None


def parse_amount(amount_str: str) -> Optional[int]:
    """解析金額: '2000/m' → 2000"""
    if not amount_str or pd.isna(amount_str):
        return None

    amount_str = str(amount_str).strip()
    if amount_str in ['N', '-', '待補', 'n', '']:
        return None

    # 移除千分位逗號，提取數字
    clean = amount_str.replace(',', '').replace('，', '')
    match = re.search(r'(\d+)', clean)
    if match:
        return int(match.group(1))

    return None


def parse_deposit(deposit_str: str) -> Dict[str, Any]:
    """解析押金: '6000/未退' → {'amount': 6000, 'status': '未退'}"""
    result = {'amount': None, 'status': None}

    if not deposit_str or pd.isna(deposit_str):
        return result

    deposit_str = str(deposit_str).strip()

    # 解析金額
    amount_match = re.search(r'(\d+)', deposit_str.replace(',', ''))
    if amount_match:
        result['amount'] = int(amount_match.group(1))

    # 解析狀態
    if '未退' in deposit_str:
        result['status'] = 'not_returned'
    elif '已退' in deposit_str:
        result['status'] = 'returned'
    elif '已扣' in deposit_str:
        result['status'] = 'deducted'

    return result


def map_customer_type(category: str) -> str:
    """映射客戶類別"""
    if not category or pd.isna(category):
        return 'individual'

    category = str(category).strip().lower()

    # 公司類型
    company_keywords = ['有限公司', '股份有限公司', '事務所', '律師', '記帳士']
    for kw in company_keywords:
        if kw in category:
            return 'company'

    # 行號/獨資
    sole_keywords = ['行號', '營登', '商行', '企業社', '工作室', '工程行', '商舖', '商店']
    for kw in sole_keywords:
        if kw in category:
            return 'sole_proprietorship'

    return 'individual'


def format_legacy_id(raw_id: str, branch: str) -> Optional[str]:
    """格式化 Legacy ID: 4 → DZ-004, V01 → HR-001"""
    if not raw_id or pd.isna(raw_id):
        return None

    raw_id = str(raw_id).strip()

    if branch == 'DZ':  # 大忠館
        # 提取數字
        match = re.search(r'(\d+)', raw_id)
        if match:
            num = int(match.group(1))
            return f"DZ-{num:03d}"

    elif branch == 'HR':  # 環瑞館
        # V01 → HR-001
        match = re.search(r'V?(\d+)', raw_id, re.IGNORECASE)
        if match:
            num = int(match.group(1))
            return f"HR-{num:03d}"

    return raw_id


def validate_tax_id(tax_id: str) -> Optional[str]:
    """驗證統一編號"""
    if not tax_id or pd.isna(tax_id):
        return None

    tax_id = str(tax_id).strip()
    if tax_id in ['N', '-', '待補', 'n', '', '沒有統編']:
        return None

    # 清理特殊字符
    clean = re.sub(r'[^\w]', '', tax_id)

    # 標準統編: 8位數字
    if re.match(r'^[1-9]\d{7}$', clean):
        return clean

    # 外國人稅籍編號
    if re.match(r'^[A-Z]\d{9}$', clean):
        return clean

    return None


def format_phone(phone: str) -> Optional[str]:
    """格式化電話號碼"""
    if not phone or pd.isna(phone):
        return None

    # 移除非數字字符
    clean = re.sub(r'[^\d]', '', str(phone))

    if not clean or len(clean) < 8:
        return None

    # 手機: 09開頭
    if clean.startswith('09') and len(clean) == 10:
        return clean

    # 市話
    if len(clean) >= 8:
        return clean

    return None


def validate_email(email: str) -> Optional[str]:
    """驗證 email 格式"""
    if not email or pd.isna(email):
        return None

    email = str(email).strip().lower()
    if email in ['n', '-', '']:
        return None

    # 簡單 email 驗證
    if re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', email):
        return email

    return None


def clean_string(value: str) -> Optional[str]:
    """清理字串"""
    if not value or pd.isna(value):
        return None

    value = str(value).strip()
    if value in ['N', '-', '待補', 'n', '', 'N/A', 'na']:
        return None

    return value


# ============================================================================
# 客戶資料清洗
# ============================================================================

def clean_customer_data(file_path: str, branch: str) -> pd.DataFrame:
    """清洗客戶資料 Excel"""
    print(f"\n{'='*60}")
    print(f"處理: {os.path.basename(file_path)} (場館: {branch})")
    print('='*60)

    # 讀取 Excel
    df = pd.read_excel(file_path, sheet_name=0)

    print(f"原始資料: {len(df)} 行")
    print(f"欄位: {list(df.columns)}")

    # 清洗後的資料
    cleaned_rows = []
    issues = []

    for idx, row in df.iterrows():
        try:
            # 跳過標題行或空行
            raw_id = row.get('編號')
            name = row.get('姓名')

            if pd.isna(raw_id) and pd.isna(name):
                continue

            # 過濾非資料行（如標題、說明等）
            if str(raw_id).strip() in ['編號', '', 'A1', 'A2', 'A3', 'A4', 'A5', 'A6', 'A7', 'A8',
                                       'A9', 'A10', 'A11', 'A12', 'B1', 'B2', 'B3', 'B4', 'B5',
                                       'B6', 'B7', 'B8', 'B9', 'B10', 'B11', 'B12', 'C1', 'C2',
                                       'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'C9', 'C10', 'C11', 'C12']:
                continue

            legacy_id = format_legacy_id(raw_id, branch)
            if not legacy_id:
                continue

            # 清洗各欄位
            company_col = '公司' if '公司' in df.columns else '公司名稱'
            category_col = '類別' if '類別' in df.columns else None
            tax_col = 'Co number' if 'Co number' in df.columns else '統編'
            phone_col = '聯絡電話' if '聯絡電話' in df.columns else 'Phone'

            # 外國人名單（ID Number 為居留證/護照號碼，非台灣身分證）
            foreigner_ids = ['DZ-228', 'DZ-230', 'DZ-254', 'HR-026']

            cleaned = {
                'legacy_id': legacy_id,
                'original_id': str(raw_id),  # 原始編號（方便對照）
                'excel_row': idx + 2,  # Excel 列號（+2 因為 header 和 0-indexed）
                'branch_id': 1 if branch == 'DZ' else 2,
                'branch_code': branch,
                'name': clean_string(name),
                'company_name': clean_string(row.get(company_col)),
                'customer_type': map_customer_type(row.get(category_col) if category_col else row.get(company_col)),
                'phone': format_phone(row.get(phone_col)),
                'email': validate_email(row.get('Mail')),
                'address': clean_string(row.get('Add')),
                'company_tax_id': validate_tax_id(row.get(tax_col)),
                'id_number': clean_string(row.get('Id number')),
                'birthday': convert_roc_date(row.get('生日')),
                'notes': clean_string(row.get('備註')),
                'industry_notes': clean_string(row.get('行業')),
                'source_channel': 'migration',
                'status': 'active',  # 預設為 active，後續根據合約狀態更新
                'is_foreigner': legacy_id in foreigner_ids,  # 標記外國人
            }

            # 解析合約資訊
            start_date = convert_roc_date(row.get('起始日期'))
            end_date = convert_roc_date(row.get('合約到期日'))
            monthly_rent = parse_amount(row.get('金額'))
            deposit_info = parse_deposit(row.get('押金'))
            payment_method = clean_string(row.get('繳費方式'))

            # 合約資訊
            cleaned['contract'] = {
                'start_date': start_date,
                'end_date': end_date,
                'monthly_rent': monthly_rent,
                'deposit': deposit_info['amount'],
                'deposit_status': deposit_info['status'],
                'payment_method': payment_method,
            }

            # 過濾掉空姓名的資料行
            if not cleaned['name']:
                issues.append({
                    'excel_row': idx + 2,
                    'original_id': str(raw_id),
                    'legacy_id': legacy_id,
                    'name': '',
                    'company_name': clean_string(row.get(company_col)) or '',
                    'issue': '缺少姓名（已跳過）'
                })
                continue  # 跳過沒有姓名的資料

            # 記錄問題（但仍保留資料）
            if not cleaned['phone']:
                issues.append({
                    'excel_row': idx + 2,
                    'original_id': str(raw_id),
                    'legacy_id': legacy_id,
                    'name': cleaned['name'],
                    'company_name': cleaned['company_name'] or '',
                    'issue': '缺少電話'
                })

            # 公司類型但缺少統編
            if cleaned['customer_type'] in ['company', 'sole_proprietorship'] and not cleaned['company_tax_id']:
                issues.append({
                    'excel_row': idx + 2,
                    'original_id': str(raw_id),
                    'legacy_id': legacy_id,
                    'name': cleaned['name'],
                    'company_name': cleaned['company_name'] or '',
                    'issue': '公司類型但缺少統編'
                })

            cleaned_rows.append(cleaned)

        except Exception as e:
            issues.append({'row': idx + 2, 'legacy_id': str(raw_id), 'issue': f'處理錯誤: {e}'})

    print(f"清洗後: {len(cleaned_rows)} 筆有效資料")
    print(f"問題資料: {len(issues)} 筆")

    return pd.DataFrame(cleaned_rows), issues


# ============================================================================
# Markdown 報告生成
# ============================================================================

def generate_issues_markdown(all_issues, contract_issues, customers_df):
    """生成問題資料 Markdown 報告"""
    from datetime import datetime

    lines = []
    lines.append("# Hour Jungle CRM - 問題資料待處理報告")
    lines.append(f"\n生成時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("\n---\n")

    # 1. 缺少電話
    phone_issues = [i for i in all_issues if i['issue'] == '缺少電話']
    lines.append("## 1. 缺少電話（5 筆）")
    lines.append("\n> HR-011、HR-019 為客戶尚未提供資料\n")
    lines.append("| Excel 列 | 原始編號 | Legacy ID | 姓名 | 公司 | 檔案 |")
    lines.append("|----------|----------|-----------|------|------|------|")
    for i in phone_issues:
        file_name = i.get('file', '客戶資料表crm.xlsx')
        lines.append(f"| {i.get('excel_row', '')} | {i.get('original_id', '')} | {i['legacy_id']} | {i.get('name', '')} | {i.get('company_name', '')} | {file_name} |")

    # 2. 公司類型但缺少統編
    tax_issues = [i for i in all_issues if i['issue'] == '公司類型但缺少統編']
    lines.append(f"\n## 2. 公司類型但缺少統編（{len(tax_issues)} 筆）")
    lines.append("\n| Excel 列 | 原始編號 | Legacy ID | 姓名 | 公司 | 檔案 |")
    lines.append("|----------|----------|-----------|------|------|------|")
    for i in tax_issues:
        file_name = i.get('file', '客戶資料表crm.xlsx')
        lines.append(f"| {i.get('excel_row', '')} | {i.get('original_id', '')} | {i['legacy_id']} | {i.get('name', '')} | {i.get('company_name', '')} | {file_name} |")

    # 3. 外國人（ID Number 為居留證/護照）
    foreigner_data = customers_df[customers_df['is_foreigner'] == True] if 'is_foreigner' in customers_df.columns else []
    lines.append(f"\n## 3. 外國人客戶（{len(foreigner_data)} 筆）")
    lines.append("\n> ID Number 為居留證號或護照號碼，已標記 is_foreigner = true\n")
    lines.append("| Excel 列 | 原始編號 | Legacy ID | 姓名 | 公司 | ID Number |")
    lines.append("|----------|----------|-----------|------|------|-----------|")
    if len(foreigner_data) > 0:
        for idx, row in foreigner_data.iterrows():
            lines.append(f"| {row.get('excel_row', '')} | {row.get('original_id', '')} | {row['legacy_id']} | {row['name']} | {row.get('company_name', '')} | {row.get('id_number', '')} |")

    # 4. 合約日期問題
    lines.append(f"\n## 4. 合約日期問題（{len(contract_issues)} 筆）")
    lines.append("\n| Excel 列 | 原始編號 | Legacy ID | 姓名 | 起始日期 | 到期日 | 問題 |")
    lines.append("|----------|----------|-----------|------|----------|--------|------|")
    for i in contract_issues:
        lines.append(f"| {i.get('excel_row', '')} | {i.get('original_id', '')} | {i['legacy_id']} | {i.get('name', '')} | {i['start_date']} | {i['end_date']} | {i['issue']} |")

    # 5. 缺少姓名（已跳過）
    name_issues = [i for i in all_issues if i['issue'] == '缺少姓名（已跳過）']
    lines.append(f"\n## 5. 缺少姓名（已跳過，{len(name_issues)} 筆）")
    lines.append("\n> 這些是 Excel 底部的空白列，只有編號但無實際資料\n")
    lines.append("| Excel 列 | 原始編號 | Legacy ID | 檔案 |")
    lines.append("|----------|----------|-----------|------|")
    for i in name_issues[:10]:  # 只顯示前10筆
        file_name = i.get('file', '客戶資料表crm.xlsx')
        lines.append(f"| {i.get('excel_row', '')} | {i.get('original_id', '')} | {i['legacy_id']} | {file_name} |")
    if len(name_issues) > 10:
        lines.append(f"\n... 還有 {len(name_issues) - 10} 筆")

    lines.append("\n---\n")
    lines.append("## 統計摘要\n")
    lines.append(f"- 缺少電話: {len(phone_issues)} 筆")
    lines.append(f"- 公司缺統編: {len(tax_issues)} 筆")
    lines.append(f"- 外國人: {len(foreigner_data)} 筆")
    lines.append(f"- 合約日期問題: {len(contract_issues)} 筆")
    lines.append(f"- 缺少姓名（空白列）: {len(name_issues)} 筆")

    return '\n'.join(lines)


# ============================================================================
# 主程式
# ============================================================================

def main():
    print("Hour Jungle CRM - Excel 資料清洗")
    print("="*60)

    all_customers = []
    all_contracts = []
    all_issues = []

    # 處理大忠館客戶資料
    dz_file = os.path.join(SOURCE_DIR, "客戶資料表crm.xlsx")
    if os.path.exists(dz_file):
        df_dz, issues_dz = clean_customer_data(dz_file, 'DZ')
        all_customers.append(df_dz)
        all_issues.extend([{**i, 'file': '客戶資料表crm.xlsx'} for i in issues_dz])
    else:
        print(f"找不到: {dz_file}")

    # 處理環瑞館客戶資料
    hr_file = os.path.join(SOURCE_DIR, "環瑞客戶資料表crm.xlsx")
    if os.path.exists(hr_file):
        df_hr, issues_hr = clean_customer_data(hr_file, 'HR')
        all_customers.append(df_hr)
        all_issues.extend([{**i, 'file': '環瑞客戶資料表crm.xlsx'} for i in issues_hr])
    else:
        print(f"找不到: {hr_file}")

    # 合併資料
    if all_customers:
        df_all = pd.concat(all_customers, ignore_index=True)

        # 檢查重複
        duplicates = df_all[df_all.duplicated(subset=['name', 'company_name'], keep=False)]
        if len(duplicates) > 0:
            print(f"\n警告: 發現 {len(duplicates)} 筆可能重複的客戶（相同姓名+公司名）")
            dup_list = duplicates[['legacy_id', 'name', 'company_name', 'branch_code']].to_dict('records')
            for d in dup_list[:10]:  # 只顯示前10筆
                print(f"  - {d['legacy_id']}: {d['name']} / {d['company_name']} ({d['branch_code']})")
            if len(dup_list) > 10:
                print(f"  ... 還有 {len(dup_list) - 10} 筆")

        # 分離客戶和合約資料
        customers_df = df_all.drop(columns=['contract'])

        contracts_data = []
        for idx, row in df_all.iterrows():
            contract = row['contract']
            if contract.get('start_date') or contract.get('end_date'):
                contracts_data.append({
                    'customer_legacy_id': row['legacy_id'],
                    'branch_id': row['branch_id'],
                    'contract_type': 'virtual_office',  # 預設類型
                    'start_date': contract.get('start_date'),
                    'end_date': contract.get('end_date'),
                    'monthly_rent': contract.get('monthly_rent'),
                    'deposit': contract.get('deposit'),
                    'deposit_status': contract.get('deposit_status'),
                    'payment_method': contract.get('payment_method'),
                })

        contracts_df = pd.DataFrame(contracts_data)

        # 輸出 CSV
        customers_file = os.path.join(OUTPUT_DIR, "customers_cleaned.csv")
        customers_df.to_csv(customers_file, index=False, encoding='utf-8-sig')
        print(f"\n輸出: {customers_file} ({len(customers_df)} 筆)")

        contracts_file = os.path.join(OUTPUT_DIR, "contracts_cleaned.csv")
        contracts_df.to_csv(contracts_file, index=False, encoding='utf-8-sig')
        print(f"輸出: {contracts_file} ({len(contracts_df)} 筆)")

        # 檢查合約日期問題
        contract_issues = []
        for idx, row in contracts_df.iterrows():
            legacy_id = row['customer_legacy_id']
            start_date = row.get('start_date')
            end_date = row.get('end_date')

            # 從客戶資料取得 Excel 列號和姓名
            customer_row = customers_df[customers_df['legacy_id'] == legacy_id]
            excel_row = customer_row['excel_row'].values[0] if len(customer_row) > 0 else ''
            original_id = customer_row['original_id'].values[0] if len(customer_row) > 0 else ''
            name = customer_row['name'].values[0] if len(customer_row) > 0 else ''

            if pd.isna(start_date) and not pd.isna(end_date):
                contract_issues.append({
                    'excel_row': excel_row,
                    'original_id': original_id,
                    'legacy_id': legacy_id,
                    'name': name,
                    'start_date': '',
                    'end_date': str(end_date),
                    'issue': '缺少起始日期'
                })
            elif not pd.isna(start_date) and pd.isna(end_date):
                contract_issues.append({
                    'excel_row': excel_row,
                    'original_id': original_id,
                    'legacy_id': legacy_id,
                    'name': name,
                    'start_date': str(start_date),
                    'end_date': '',
                    'issue': '缺少到期日'
                })
            elif not pd.isna(start_date) and not pd.isna(end_date) and str(start_date) == str(end_date):
                contract_issues.append({
                    'excel_row': excel_row,
                    'original_id': original_id,
                    'legacy_id': legacy_id,
                    'name': name,
                    'start_date': str(start_date),
                    'end_date': str(end_date),
                    'issue': '起始日=到期日（異常）'
                })

        # 輸出問題報告 JSON
        if all_issues:
            issues_file = os.path.join(OUTPUT_DIR, "data_issues_report.json")
            with open(issues_file, 'w', encoding='utf-8') as f:
                json.dump(all_issues, f, ensure_ascii=False, indent=2)
            print(f"輸出: {issues_file} ({len(all_issues)} 筆問題)")

        # 生成 Markdown 問題報告
        md_report = generate_issues_markdown(all_issues, contract_issues, customers_df)
        md_file = "/Users/daihaoting_1/Desktop/code/data_issues_pending.md"
        with open(md_file, 'w', encoding='utf-8') as f:
            f.write(md_report)
        print(f"輸出: {md_file}")

        # 統計摘要
        print("\n" + "="*60)
        print("清洗摘要")
        print("="*60)
        print(f"總客戶數: {len(customers_df)}")
        print(f"  - 大忠館: {len(customers_df[customers_df['branch_code'] == 'DZ'])}")
        print(f"  - 環瑞館: {len(customers_df[customers_df['branch_code'] == 'HR'])}")
        print(f"總合約數: {len(contracts_df)}")
        print(f"問題資料: {len(all_issues)}")

        # 欄位統計
        print("\n欄位填充率:")
        for col in ['phone', 'email', 'company_tax_id', 'address']:
            filled = customers_df[col].notna().sum()
            rate = filled / len(customers_df) * 100
            print(f"  - {col}: {filled}/{len(customers_df)} ({rate:.1f}%)")

    print("\n清洗完成！")


if __name__ == "__main__":
    main()
