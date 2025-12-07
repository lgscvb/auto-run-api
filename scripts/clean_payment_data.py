#!/usr/bin/env python3
"""
Hour Jungle CRM - 繳費資料清洗腳本
處理 3 個繳費 Excel 檔案：
- 2025 客戶繳費.xlsx (大忠館)
- 2026客戶繳費.xlsx (大忠館)
- 環2025客戶繳費.xlsx (環瑞館)
"""

import pandas as pd
import re
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List, Any

# 路徑設定
INPUT_DIR = Path('/Users/daihaoting_1/Downloads/原始資料')
OUTPUT_DIR = Path(__file__).parent / 'output'
OUTPUT_DIR.mkdir(exist_ok=True)


def convert_roc_date(roc_date) -> Optional[str]:
    """民國年轉西元年: 114/01/10 → 2025-01-10"""
    if not roc_date or pd.isna(roc_date):
        return None

    date_str = str(roc_date).strip()

    # 處理 datetime 物件
    if isinstance(roc_date, datetime):
        return roc_date.strftime('%Y-%m-%d')

    # 處理特殊格式 0114/1/3 → 114/1/3
    date_str = re.sub(r'^0+', '', date_str)

    # 解析 114/01/10 或 114/1/10
    match = re.match(r'(\d{2,3})[/.](\d{1,2})[/.](\d{1,2})', date_str)
    if match:
        year, month, day = match.groups()
        year_int = int(year) + 1911
        return f"{year_int}-{int(month):02d}-{int(day):02d}"

    # 嘗試解析西元年格式
    try:
        dt = pd.to_datetime(date_str)
        return dt.strftime('%Y-%m-%d')
    except:
        pass

    return None


def parse_amount(amount_str) -> Optional[int]:
    """解析金額: 21600, 8940, 等"""
    if not amount_str or pd.isna(amount_str):
        return None

    # 處理數值型態
    if isinstance(amount_str, (int, float)):
        return int(amount_str)

    amount_str = str(amount_str).strip()

    # 過濾無效值
    if any(x in amount_str for x in ['已退款', '不續約', 'NaN']):
        return None

    # 解析數字
    match = re.search(r'(\d+)', amount_str.replace(',', ''))
    if match:
        return int(match.group(1))

    return None


def parse_monthly_rent(rent_str) -> Optional[int]:
    """解析月租金: 1800/m → 1800"""
    if not rent_str or pd.isna(rent_str):
        return None

    rent_str = str(rent_str).strip()
    match = re.search(r'(\d+)', rent_str.replace(',', ''))
    if match:
        return int(match.group(1))

    return None


def format_legacy_id(raw_id, branch: str) -> Optional[str]:
    """格式化 Legacy ID: 199 → DZ-199"""
    if not raw_id or pd.isna(raw_id):
        return None

    raw_str = str(raw_id).strip()

    # 過濾非數字開頭的值
    if not re.match(r'^\d', raw_str):
        return None

    match = re.search(r'(\d+)', raw_str)
    if match:
        num = int(match.group(1))
        prefix = 'DZ' if branch == 'DZ' else 'HR'
        return f"{prefix}-{num:03d}"

    return None


def parse_payment_status(row) -> str:
    """解析繳費狀態"""
    col_7 = str(row.get(7, '')) if pd.notna(row.get(7)) else ''
    col_11 = str(row.get(11, '')) if pd.notna(row.get(11)) else ''

    # 退款
    if '已退款' in col_7 or '已退款' in col_11:
        return 'refunded'

    # 不續約
    if '不續約' in col_7 or '不續約' in col_11:
        return 'cancelled'

    # 已繳費 (有繳費日期和金額)
    if pd.notna(row.get(7)) and pd.notna(row.get(8)):
        return 'paid'

    return 'pending'


def parse_contract_term(term_str) -> Dict[str, Any]:
    """解析合約期間: y=1年, 6m=6個月, M=月繳, 3M=3個月"""
    result = {'term_type': 'monthly', 'term_months': 1}

    if not term_str or pd.isna(term_str):
        return result

    term_str = str(term_str).strip().lower()

    if term_str == 'y':
        return {'term_type': 'yearly', 'term_months': 12}
    elif re.match(r'(\d+)m', term_str):
        match = re.match(r'(\d+)m', term_str)
        months = int(match.group(1))
        return {'term_type': f'{months}_months', 'term_months': months}
    elif term_str == 'm':
        return {'term_type': 'monthly', 'term_months': 1}

    return result


def clean_payment_file(file_path: Path, branch: str) -> List[Dict]:
    """清洗單一繳費 Excel 檔案"""
    print(f"\n處理: {file_path.name} ({branch})")

    try:
        df = pd.read_excel(file_path, header=None)
    except Exception as e:
        print(f"  ❌ 讀取失敗: {e}")
        return []

    payments = []
    issues = []

    for idx, row in df.iterrows():
        # 跳過標題列和空白列
        raw_id = row.get(0)
        if pd.isna(raw_id) or not str(raw_id).strip():
            continue

        # 過濾非客戶資料列
        raw_id_str = str(raw_id).strip()
        if not re.match(r'^\d+', raw_id_str):
            continue

        legacy_id = format_legacy_id(raw_id, branch)
        if not legacy_id:
            continue

        # 解析資料
        name = str(row.get(1, '')).strip() if pd.notna(row.get(1)) else ''
        company = str(row.get(2, '')).strip() if pd.notna(row.get(2)) else ''

        # 合約期間
        contract_term = parse_contract_term(row.get(3))

        # 日期
        start_date = convert_roc_date(row.get(4))
        end_date = convert_roc_date(row.get(5))

        # 金額
        monthly_rent = parse_monthly_rent(row.get(6))
        payment_amount = parse_amount(row.get(8))

        # 繳費日期
        payment_date = None
        if pd.notna(row.get(7)):
            payment_date = convert_roc_date(row.get(7))

        # 下次繳費日
        next_payment_date = None
        if pd.notna(row.get(9)):
            next_date_str = str(row.get(9)).strip()
            # 解析 115/1月 格式
            match = re.match(r'(\d{2,3})[/.]?(\d{1,2})月?', next_date_str)
            if match:
                year, month = match.groups()
                year_int = int(year) + 1911
                next_payment_date = f"{year_int}-{int(month):02d}-01"
            else:
                next_payment_date = convert_roc_date(row.get(9))

        # LINE 記事本狀態
        line_noted = '✔️' in str(row.get(10, '')) if pd.notna(row.get(10)) else False

        # 備註
        notes = str(row.get(11, '')).strip() if pd.notna(row.get(11)) else ''

        # 繳費狀態
        payment_status = parse_payment_status(row)

        # 建立繳費記錄
        payment = {
            'customer_legacy_id': legacy_id,
            'customer_name': name,
            'company_name': company,
            'branch': branch,
            'excel_row': idx + 1,

            # 合約資訊
            'contract_start_date': start_date,
            'contract_end_date': end_date,
            'term_type': contract_term['term_type'],
            'term_months': contract_term['term_months'],
            'monthly_rent': monthly_rent,

            # 繳費資訊
            'payment_date': payment_date,
            'payment_amount': payment_amount,
            'next_payment_date': next_payment_date,
            'payment_status': payment_status,

            # 其他
            'line_noted': line_noted,
            'notes': notes,

            # 來源檔案
            'source_file': file_path.name
        }

        payments.append(payment)

        # 檢查問題
        if not payment_date and payment_status == 'paid':
            issues.append({
                'legacy_id': legacy_id,
                'name': name,
                'issue': '缺少繳費日期'
            })

        if not payment_amount and payment_status == 'paid':
            issues.append({
                'legacy_id': legacy_id,
                'name': name,
                'issue': '缺少繳費金額'
            })

    print(f"  ✅ 清洗完成: {len(payments)} 筆繳費記錄")

    if issues:
        print(f"  ⚠️ 發現 {len(issues)} 筆問題")
        for issue in issues[:5]:
            print(f"     - {issue['legacy_id']} {issue['name']}: {issue['issue']}")

    return payments


def main():
    print("=" * 60)
    print("Hour Jungle CRM - 繳費資料清洗")
    print("=" * 60)

    # 繳費檔案列表
    payment_files = [
        (INPUT_DIR / '2025 客戶繳費.xlsx', 'DZ'),
        (INPUT_DIR / '2026客戶繳費.xlsx', 'DZ'),
        (INPUT_DIR / '環2025客戶繳費.xlsx', 'HR'),
    ]

    all_payments = []

    for file_path, branch in payment_files:
        if file_path.exists():
            payments = clean_payment_file(file_path, branch)
            all_payments.extend(payments)
        else:
            print(f"\n⚠️ 檔案不存在: {file_path}")

    # 輸出 CSV
    if all_payments:
        df = pd.DataFrame(all_payments)

        # 按 legacy_id 排序
        df['sort_key'] = df['customer_legacy_id'].apply(
            lambda x: (x[:2], int(x.split('-')[1])) if x else ('', 0)
        )
        df = df.sort_values('sort_key').drop('sort_key', axis=1)

        output_file = OUTPUT_DIR / 'payments_cleaned.csv'
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"\n✅ 已輸出: {output_file}")

        # 統計
        print("\n" + "=" * 60)
        print("統計摘要")
        print("=" * 60)
        print(f"總繳費記錄: {len(all_payments)}")
        print(f"  - 大忠館 (DZ): {len([p for p in all_payments if p['branch'] == 'DZ'])}")
        print(f"  - 環瑞館 (HR): {len([p for p in all_payments if p['branch'] == 'HR'])}")

        # 繳費狀態統計
        status_counts = df['payment_status'].value_counts()
        print("\n繳費狀態:")
        for status, count in status_counts.items():
            print(f"  - {status}: {count}")

        # 金額統計
        total_amount = df['payment_amount'].sum()
        print(f"\n總繳費金額: ${total_amount:,.0f}")

        # 來源檔案統計
        print("\n來源檔案:")
        for src, count in df['source_file'].value_counts().items():
            print(f"  - {src}: {count} 筆")
    else:
        print("\n⚠️ 沒有繳費記錄")

    return all_payments


if __name__ == '__main__':
    main()
