-- Migration: 021_auto_generate_payments
-- Description: 自動生成每月待繳記錄的函數
-- Date: 2025-12-18

-- ============================================================================
-- 自動生成待繳記錄函數
--
-- 使用方式：
-- 1. 手動執行：SELECT generate_monthly_payments('2025-12');
-- 2. 自動執行：設定 cron job 每月 1 號執行
-- ============================================================================

-- 建立函數：為指定月份的活躍合約生成待繳記錄
CREATE OR REPLACE FUNCTION generate_monthly_payments(target_period TEXT DEFAULT NULL)
RETURNS TABLE (
    contracts_processed INT,
    payments_created INT,
    total_amount NUMERIC,
    skipped_existing INT,
    skipped_no_payment INT
) AS $$
DECLARE
    v_target_year INT;
    v_target_month INT;
    v_target_period TEXT;
    v_target_start DATE;
    v_target_end DATE;
    v_contracts_processed INT := 0;
    v_payments_created INT := 0;
    v_total_amount NUMERIC := 0;
    v_skipped_existing INT := 0;
    v_skipped_no_payment INT := 0;
    v_contract RECORD;
    v_payment_day INT;
    v_due_date DATE;
    v_amount NUMERIC;
    v_multiplier INT;
    v_should_create BOOLEAN;
    v_existing_count INT;
BEGIN
    -- 如果沒有指定期間，使用當月
    IF target_period IS NULL THEN
        v_target_period := TO_CHAR(CURRENT_DATE, 'YYYY-MM');
    ELSE
        v_target_period := target_period;
    END IF;

    -- 解析年月
    v_target_year := EXTRACT(YEAR FROM (v_target_period || '-01')::DATE);
    v_target_month := EXTRACT(MONTH FROM (v_target_period || '-01')::DATE);
    v_target_start := (v_target_period || '-01')::DATE;
    v_target_end := (v_target_start + INTERVAL '1 month' - INTERVAL '1 day')::DATE;

    RAISE NOTICE '生成 % 待繳記錄...', v_target_period;

    -- 遍歷所有活躍合約
    FOR v_contract IN
        SELECT
            c.id,
            c.customer_id,
            c.branch_id,
            c.start_date,
            c.end_date,
            c.monthly_rent,
            COALESCE(c.payment_cycle, 'monthly') AS payment_cycle,
            COALESCE(c.payment_day, EXTRACT(DAY FROM c.start_date)::INT) AS payment_day
        FROM contracts c
        WHERE c.status = 'active'
          AND c.start_date <= v_target_end  -- 合約已開始
          AND (c.end_date IS NULL OR c.end_date >= v_target_start)  -- 合約未結束
    LOOP
        v_contracts_processed := v_contracts_processed + 1;
        v_should_create := FALSE;
        v_multiplier := 1;

        -- 根據繳費週期判斷是否需要在目標月份繳費
        CASE v_contract.payment_cycle
            WHEN 'monthly' THEN
                -- 每月都要繳
                v_should_create := TRUE;
                v_multiplier := 1;

            WHEN 'quarterly' THEN
                -- 每季繳費：開始月份、+3、+6、+9
                IF (v_target_month - EXTRACT(MONTH FROM v_contract.start_date)::INT + 12) % 3 = 0 THEN
                    v_should_create := TRUE;
                    v_multiplier := 3;
                END IF;

            WHEN 'semi_annual' THEN
                -- 每半年繳費：開始月份、+6
                IF (v_target_month - EXTRACT(MONTH FROM v_contract.start_date)::INT + 12) % 6 = 0 THEN
                    v_should_create := TRUE;
                    v_multiplier := 6;
                END IF;

            WHEN 'annual' THEN
                -- 每年繳費：只在開始月份
                IF EXTRACT(MONTH FROM v_contract.start_date) = v_target_month THEN
                    v_should_create := TRUE;
                    v_multiplier := 12;
                END IF;

            WHEN 'biennial' THEN
                -- 每兩年繳費
                IF EXTRACT(MONTH FROM v_contract.start_date) = v_target_month THEN
                    IF ((v_target_year - EXTRACT(YEAR FROM v_contract.start_date)::INT) * 12 +
                        (v_target_month - EXTRACT(MONTH FROM v_contract.start_date)::INT)) % 24 = 0 THEN
                        v_should_create := TRUE;
                        v_multiplier := 24;
                    END IF;
                END IF;

            WHEN 'triennial' THEN
                -- 每三年繳費
                IF EXTRACT(MONTH FROM v_contract.start_date) = v_target_month THEN
                    IF ((v_target_year - EXTRACT(YEAR FROM v_contract.start_date)::INT) * 12 +
                        (v_target_month - EXTRACT(MONTH FROM v_contract.start_date)::INT)) % 36 = 0 THEN
                        v_should_create := TRUE;
                        v_multiplier := 36;
                    END IF;
                END IF;

            ELSE
                -- 預設為月繳
                v_should_create := TRUE;
                v_multiplier := 1;
        END CASE;

        IF NOT v_should_create THEN
            v_skipped_no_payment := v_skipped_no_payment + 1;
            CONTINUE;
        END IF;

        -- 計算繳費日（考慮月底）
        v_payment_day := LEAST(v_contract.payment_day, EXTRACT(DAY FROM v_target_end)::INT);
        v_due_date := MAKE_DATE(v_target_year, v_target_month, v_payment_day);

        -- 確保合約在繳費日前已開始
        IF v_contract.start_date > v_due_date THEN
            v_skipped_no_payment := v_skipped_no_payment + 1;
            CONTINUE;
        END IF;

        -- 檢查是否已有記錄（paid 或 pending）
        SELECT COUNT(*) INTO v_existing_count
        FROM payments
        WHERE contract_id = v_contract.id
          AND payment_period = v_target_period
          AND payment_type = 'rent';

        IF v_existing_count > 0 THEN
            v_skipped_existing := v_skipped_existing + 1;
            CONTINUE;
        END IF;

        -- 計算金額
        v_amount := v_contract.monthly_rent * v_multiplier;

        -- 建立待繳記錄
        INSERT INTO payments (
            contract_id,
            customer_id,
            branch_id,
            payment_type,
            payment_period,
            amount,
            due_date,
            payment_status
        ) VALUES (
            v_contract.id,
            v_contract.customer_id,
            v_contract.branch_id,
            'rent',
            v_target_period,
            v_amount,
            v_due_date,
            'pending'
        );

        v_payments_created := v_payments_created + 1;
        v_total_amount := v_total_amount + v_amount;

    END LOOP;

    RAISE NOTICE '完成！處理合約: %, 建立記錄: %, 總金額: $%, 已存在跳過: %, 本月無需繳費: %',
        v_contracts_processed, v_payments_created, v_total_amount, v_skipped_existing, v_skipped_no_payment;

    RETURN QUERY SELECT
        v_contracts_processed,
        v_payments_created,
        v_total_amount,
        v_skipped_existing,
        v_skipped_no_payment;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION generate_monthly_payments IS '自動為活躍合約生成指定月份的待繳記錄';

-- 授權
GRANT EXECUTE ON FUNCTION generate_monthly_payments TO anon, authenticated;

-- ============================================================================
-- 建立視圖：待生成的繳費記錄預覽（不實際建立，只是預覽）
-- ============================================================================
CREATE OR REPLACE VIEW v_pending_payments_preview AS
WITH target AS (
    SELECT
        TO_CHAR(CURRENT_DATE, 'YYYY-MM') AS period,
        DATE_TRUNC('month', CURRENT_DATE)::DATE AS month_start,
        (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' - INTERVAL '1 day')::DATE AS month_end
)
SELECT
    c.id AS contract_id,
    c.contract_number,
    cu.name AS customer_name,
    cu.legacy_id,
    c.payment_cycle,
    c.payment_day,
    c.monthly_rent,
    t.period AS payment_period,
    MAKE_DATE(
        EXTRACT(YEAR FROM t.month_start)::INT,
        EXTRACT(MONTH FROM t.month_start)::INT,
        LEAST(COALESCE(c.payment_day, EXTRACT(DAY FROM c.start_date)::INT), EXTRACT(DAY FROM t.month_end)::INT)
    ) AS due_date,
    CASE
        WHEN EXISTS (
            SELECT 1 FROM payments p
            WHERE p.contract_id = c.id
              AND p.payment_period = t.period
              AND p.payment_type = 'rent'
        ) THEN '已存在'
        ELSE '待建立'
    END AS status
FROM contracts c
JOIN customers cu ON c.customer_id = cu.id
CROSS JOIN target t
WHERE c.status = 'active'
  AND c.start_date <= t.month_end
  AND (c.end_date IS NULL OR c.end_date >= t.month_start)
ORDER BY c.id;

GRANT SELECT ON v_pending_payments_preview TO anon, authenticated;

COMMENT ON VIEW v_pending_payments_preview IS '預覽當月應生成的待繳記錄';
