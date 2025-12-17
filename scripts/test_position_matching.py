#!/usr/bin/env python3
"""
測試位置與租戶匹配
根據 PPT 提取的公司名稱，匹配資料庫中的客戶和合約
"""

import json
import httpx
from difflib import SequenceMatcher

# PostgREST URL (本地 Docker)
POSTGREST_URL = "http://localhost:3000"

# PPT 提取的位置數據
PPT_POSITIONS = [
    {"position_number": 1, "company_name": "廖氏商行"},
    {"position_number": 2, "company_name": "江小咪商行"},
    {"position_number": 3, "company_name": "洛酷科技有限公司"},
    {"position_number": 4, "company_name": "鑫秝喨國際有限公司"},
    {"position_number": 5, "company_name": "一貝兒美容工作室"},
    {"position_number": 6, "company_name": "吉爾哈登工作室"},
    {"position_number": 7, "company_name": "緁作工作室"},
    {"position_number": 8, "company_name": "流星有限公司"},
    {"position_number": 9, "company_name": "晨甯水產行"},
    {"position_number": 10, "company_name": "子昇有限公司"},
    {"position_number": 11, "company_name": "優翼科技工程有限公司"},
    {"position_number": 12, "company_name": "季節東京媄睫專業坊"},
    {"position_number": 13, "company_name": "楊董企業社"},
    {"position_number": 14, "company_name": "程晧事業有限公司"},
    {"position_number": 15, "company_name": "昇瑪商行"},
    {"position_number": 16, "company_name": "機車俠機車行"},
    {"position_number": 17, "company_name": "兩兩空間製作所有限公司"},
    {"position_number": 18, "company_name": "辰緻國際股份有限公司"},
    {"position_number": 19, "company_name": "頌芝承工作室"},
    {"position_number": 20, "company_name": "立湟有限公司"},
    {"position_number": 21, "company_name": "旭營興業有限公司"},
    {"position_number": 22, "company_name": "台灣心零售股份有限公司"},
    {"position_number": 23, "company_name": "超省購生活用品企業社"},
    {"position_number": 24, "company_name": "明偉水產行"},
    {"position_number": 25, "company_name": "隱士播放清單商店"},
    {"position_number": 26, "company_name": "起床打單有限公司"},
    {"position_number": 27, "company_name": "恩梯科技股份有限公司"},
    {"position_number": 28, "company_name": "獨自紅有限公司"},
    {"position_number": 29, "company_name": "益群團購顧問有限公司"},
    {"position_number": 30, "company_name": "景泰批發實業社"},
    {"position_number": 31, "company_name": "泉家鑫企業社"},
    {"position_number": 32, "company_name": "利奇商行"},
    {"position_number": 33, "company_name": "至溢營造有限公司"},
    {"position_number": 34, "company_name": "萊益國際股份有限公司台中分公司"},
    {"position_number": 35, "company_name": "花芙辰寶國際行銷管理顧問有限公司"},
    {"position_number": 36, "company_name": "明采文創工作室"},
    {"position_number": 37, "company_name": "貽順有限公司"},
    {"position_number": 38, "company_name": "知寬植行"},
    {"position_number": 39, "company_name": "小熊零件行"},
    {"position_number": 40, "company_name": "商贏企業"},
    {"position_number": 41, "company_name": "中盛建維有限公司"},
    {"position_number": 42, "company_name": "朱芸工作室"},
    {"position_number": 43, "company_name": "竺墨文創企業社"},
    {"position_number": 44, "company_name": "究鮮商行"},
    {"position_number": 45, "company_name": "新大科技有限公司"},
    {"position_number": 46, "company_name": "新遞國際物流有限公司"},
    {"position_number": 47, "company_name": "福樂寵工作室"},
    {"position_number": 48, "company_name": "由非室內裝修設計有限公司"},
    {"position_number": 49, "company_name": "農益富股份有限公司"},
    {"position_number": 50, "company_name": "原食工坊"},
    {"position_number": 51, "company_name": "帛珅有限公司"},
    {"position_number": 52, "company_name": "搖滾山姆有限公司"},
    {"position_number": 53, "company_name": "樂沐金商行"},
    {"position_number": 54, "company_name": "鼎盛行銷"},
    {"position_number": 55, "company_name": "微笑玩家國際貿易有限公司"},
    {"position_number": 56, "company_name": "仁徠貿易股份有限公司"},
    {"position_number": 57, "company_name": "照鴻貿易股份有限公司"},
    {"position_number": 58, "company_name": "日安家商行"},
    {"position_number": 59, "company_name": "上永富科技股份有限公司"},
    {"position_number": 60, "company_name": "光緯企業社"},
    {"position_number": 61, "company_name": "華為秝喨國際有限公司"},
    {"position_number": 62, "company_name": "短腿基商舖"},
    {"position_number": 63, "company_name": "金海小舖"},
    {"position_number": 64, "company_name": "順映影像有限公司"},
    {"position_number": 65, "company_name": "植光圈友善坊"},
    {"position_number": 66, "company_name": "旺玖企業社"},
    {"position_number": 67, "company_name": "鼠適圈工作室"},
    {"position_number": 68, "company_name": "滿單有限公司"},
    {"position_number": 69, "company_name": "七分之二的探索有限公司"},
    {"position_number": 70, "company_name": "步臻有限公司"},
    {"position_number": 71, "company_name": "范特希雅時光旅行小舖"},
    {"position_number": 72, "company_name": "大心沉香"},
    {"position_number": 73, "company_name": "鎧將金屬開發有限公司"},
    {"position_number": 74, "company_name": "文瀛營造有限公司"},
    {"position_number": 75, "company_name": "協通實業有限公司"},
    {"position_number": 76, "company_name": "天原興業有限公司"},
    {"position_number": 77, "company_name": "金如泰股份有限公司"},
    {"position_number": 78, "company_name": "好日來商行"},
    {"position_number": 79, "company_name": "伯樂商行"},
    {"position_number": 80, "company_name": "宏川貿易有限公司"},
    {"position_number": 81, "company_name": "興盛行銷管理顧問有限公司"},
    {"position_number": 82, "company_name": "富丞裕國際商行"},
    {"position_number": 83, "company_name": "盛豐新流量商業社"},
    {"position_number": 84, "company_name": "喂喂四聲喂工作室"},
    {"position_number": 85, "company_name": "磐星能源科技有限公司"},
    {"position_number": 86, "company_name": "承新文創有限公司"},
    {"position_number": 87, "company_name": "捌伍設計"},
    {"position_number": 88, "company_name": "溪流雲創意整合有限公司"},
    {"position_number": 89, "company_name": "智谷系統有限公司"},
    {"position_number": 90, "company_name": "顧寶藝工作室"},
    {"position_number": 91, "company_name": "仁琦科技有限公司"},
    {"position_number": 92, "company_name": "浩萊國際企業社"},
    {"position_number": 93, "company_name": "小倩媽咪行銷工作室"},
    {"position_number": 94, "company_name": "四春企業社"},
    {"position_number": 95, "company_name": "樸裕國際顧問有限公司"},
    {"position_number": 96, "company_name": "御林軍御藝美妝"},
    {"position_number": 97, "company_name": "馥諦健康事業有限公司"},
    {"position_number": 98, "company_name": "世燁環境清潔企業社"},
    {"position_number": 99, "company_name": "和和國際有限公司"},
    {"position_number": 100, "company_name": "曜森生活工作室"},
    {"position_number": 101, "company_name": "川榆室所有限公司"},
    {"position_number": 102, "company_name": "小胖芭樂水果行"},
    {"position_number": 103, "company_name": "淬矩闢梯有限公司"},
    {"position_number": 104, "company_name": "沃土謙植有限公司"},
    {"position_number": 105, "company_name": "艾瑟烘焙坊"},
    {"position_number": 106, "company_name": "球球歐瑞歐工作室"},
    {"position_number": 107, "company_name": "弎弎審美在線工作室"},
]


def normalize(s: str) -> str:
    """正規化字串：去除空格、全形空格"""
    if not s:
        return ""
    return s.replace(" ", "").replace("　", "").replace("\n", "")


def similarity(a: str, b: str) -> float:
    """計算相似度"""
    return SequenceMatcher(None, normalize(a), normalize(b)).ratio()


def find_best_match(ppt_name: str, customers: list) -> tuple:
    """找最佳匹配的客戶"""
    best_match = None
    best_score = 0

    ppt_norm = normalize(ppt_name)

    for cust in customers:
        company_name = cust.get("company_name") or ""
        customer_name = cust.get("name") or ""

        # 完全匹配
        if normalize(company_name) == ppt_norm:
            return cust, 1.0, "完全匹配"

        # 包含匹配
        if ppt_norm in normalize(company_name) or normalize(company_name) in ppt_norm:
            score = 0.9
            if score > best_score:
                best_score = score
                best_match = cust

        # 相似度匹配
        score = similarity(company_name, ppt_name)
        if score > best_score and score > 0.6:
            best_score = score
            best_match = cust

    if best_match:
        return best_match, best_score, f"相似度 {best_score:.0%}"
    return None, 0, "未匹配"


def main():
    print("=" * 80)
    print("位置與租戶匹配測試")
    print("=" * 80)

    # 取得所有客戶
    try:
        resp = httpx.get(f"{POSTGREST_URL}/customers", params={"branch_id": "eq.1"}, timeout=30)
        resp.raise_for_status()
        customers = resp.json()
        print(f"\n從資料庫取得 {len(customers)} 個客戶")
    except Exception as e:
        print(f"❌ 無法連接資料庫: {e}")
        print("請確保 Docker 已啟動: docker compose up -d")
        return

    # 取得所有合約
    try:
        resp = httpx.get(
            f"{POSTGREST_URL}/contracts",
            params={"branch_id": "eq.1", "status": "eq.active"},
            timeout=30
        )
        resp.raise_for_status()
        contracts = resp.json()
        print(f"從資料庫取得 {len(contracts)} 個有效合約")
    except Exception as e:
        print(f"❌ 取得合約失敗: {e}")
        contracts = []

    # 建立客戶 ID 到合約的映射
    customer_contracts = {}
    for ct in contracts:
        cid = ct.get("customer_id")
        if cid not in customer_contracts:
            customer_contracts[cid] = []
        customer_contracts[cid].append(ct)

    # 匹配結果
    matched = []
    unmatched = []

    print("\n" + "-" * 80)
    print("匹配結果")
    print("-" * 80)

    for pos in PPT_POSITIONS:
        ppt_name = pos["company_name"]
        pos_num = pos["position_number"]

        cust, score, match_type = find_best_match(ppt_name, customers)

        if cust:
            cust_id = cust["id"]
            db_company = cust.get("company_name") or cust.get("name")
            cts = customer_contracts.get(cust_id, [])

            if cts:
                ct = cts[0]  # 取第一個合約
                matched.append({
                    "position_number": pos_num,
                    "ppt_company": ppt_name,
                    "db_company": db_company,
                    "customer_id": cust_id,
                    "contract_id": ct["id"],
                    "match_type": match_type
                })
                print(f"✅ {pos_num:3d} | {ppt_name[:20]:20s} → {db_company[:20]:20s} | 合約#{ct['id']} | {match_type}")
            else:
                matched.append({
                    "position_number": pos_num,
                    "ppt_company": ppt_name,
                    "db_company": db_company,
                    "customer_id": cust_id,
                    "contract_id": None,
                    "match_type": match_type + " (無有效合約)"
                })
                print(f"⚠️ {pos_num:3d} | {ppt_name[:20]:20s} → {db_company[:20]:20s} | 無有效合約 | {match_type}")
        else:
            unmatched.append({
                "position_number": pos_num,
                "ppt_company": ppt_name
            })
            print(f"❌ {pos_num:3d} | {ppt_name[:20]:20s} → 未找到匹配")

    # 統計
    print("\n" + "=" * 80)
    print("統計")
    print("=" * 80)
    print(f"總位置數: {len(PPT_POSITIONS)}")
    print(f"匹配成功: {len([m for m in matched if m.get('contract_id')])}")
    print(f"匹配但無合約: {len([m for m in matched if not m.get('contract_id')])}")
    print(f"未匹配: {len(unmatched)}")

    # 輸出 UPDATE SQL
    if matched:
        print("\n" + "=" * 80)
        print("UPDATE SQL 語句")
        print("=" * 80)

        for m in matched:
            if m.get("contract_id"):
                print(f"UPDATE contracts SET position_number = {m['position_number']} WHERE id = {m['contract_id']};")

    # 輸出未匹配清單
    if unmatched:
        print("\n" + "=" * 80)
        print("未匹配的公司（需手動處理）")
        print("=" * 80)
        for u in unmatched:
            print(f"{u['position_number']:3d} | {u['ppt_company']}")

    # 儲存結果
    result = {
        "matched": matched,
        "unmatched": unmatched,
        "statistics": {
            "total": len(PPT_POSITIONS),
            "matched_with_contract": len([m for m in matched if m.get("contract_id")]),
            "matched_no_contract": len([m for m in matched if not m.get("contract_id")]),
            "unmatched": len(unmatched)
        }
    }

    with open("/Users/daihaoting_1/Desktop/position_matching_result.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n結果已儲存到: /Users/daihaoting_1/Desktop/position_matching_result.json")


if __name__ == "__main__":
    main()
