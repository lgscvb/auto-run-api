-- ============================================================================
-- Hour Jungle CRM - PostgreSQL Functions & Triggers
-- 03_functions.sql - 函數與觸發器
-- ============================================================================

-- ============================================================================
-- 1. 自動更新 updated_at 觸發器
-- ============================================================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 為所有需要的表建立觸發器
CREATE TRIGGER update_branches_updated_at
    BEFORE UPDATE ON branches
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_accounting_firms_updated_at
    BEFORE UPDATE ON accounting_firms
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_customers_updated_at
    BEFORE UPDATE ON customers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_contracts_updated_at
    BEFORE UPDATE ON contracts
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_payments_updated_at
    BEFORE UPDATE ON payments
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_commissions_updated_at
    BEFORE UPDATE ON commissions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- 2. 審計日誌觸發器
-- ============================================================================
CREATE OR REPLACE FUNCTION audit_trigger_function()
RETURNS TRIGGER AS $$
DECLARE
    old_data JSONB;
    new_data JSONB;
    changed_fields TEXT[];
    key TEXT;
BEGIN
    IF TG_OP = 'DELETE' THEN
        old_data := to_jsonb(OLD);
        INSERT INTO audit_logs (table_name, record_id, action, old_data, new_data, changed_fields)
        VALUES (TG_TABLE_NAME, OLD.id, 'DELETE', old_data, NULL, NULL);
        RETURN OLD;

    ELSIF TG_OP = 'UPDATE' THEN
        old_data := to_jsonb(OLD);
        new_data := to_jsonb(NEW);

        -- 找出變更的欄位
        SELECT array_agg(key) INTO changed_fields
        FROM (
            SELECT key
            FROM jsonb_each(old_data) AS o(key, value)
            WHERE old_data->key IS DISTINCT FROM new_data->key
            AND key NOT IN ('updated_at')  -- 排除自動更新欄位
        ) AS changed;

        -- 只有真正有變更才記錄
        IF changed_fields IS NOT NULL AND array_length(changed_fields, 1) > 0 THEN
            INSERT INTO audit_logs (table_name, record_id, action, old_data, new_data, changed_fields)
            VALUES (TG_TABLE_NAME, NEW.id, 'UPDATE', old_data, new_data, changed_fields);
        END IF;
        RETURN NEW;

    ELSIF TG_OP = 'INSERT' THEN
        new_data := to_jsonb(NEW);
        INSERT INTO audit_logs (table_name, record_id, action, old_data, new_data, changed_fields)
        VALUES (TG_TABLE_NAME, NEW.id, 'INSERT', NULL, new_data, NULL);
        RETURN NEW;
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- 為主要表建立審計觸發器
CREATE TRIGGER audit_customers
    AFTER INSERT OR UPDATE OR DELETE ON customers
    FOR EACH ROW EXECUTE FUNCTION audit_trigger_function();

CREATE TRIGGER audit_contracts
    AFTER INSERT OR UPDATE OR DELETE ON contracts
    FOR EACH ROW EXECUTE FUNCTION audit_trigger_function();

CREATE TRIGGER audit_payments
    AFTER INSERT OR UPDATE OR DELETE ON payments
    FOR EACH ROW EXECUTE FUNCTION audit_trigger_function();

CREATE TRIGGER audit_commissions
    AFTER INSERT OR UPDATE OR DELETE ON commissions
    FOR EACH ROW EXECUTE FUNCTION audit_trigger_function();

-- ============================================================================
-- 3. 逾期天數自動計算觸發器
-- ============================================================================
CREATE OR REPLACE FUNCTION calculate_overdue_days()
RETURNS TRIGGER AS $$
BEGIN
    -- 只處理 pending 狀態的付款
    IF NEW.payment_status = 'pending' AND NEW.due_date < CURRENT_DATE THEN
        NEW.payment_status := 'overdue';
        NEW.overdue_days := CURRENT_DATE - NEW.due_date;
    ELSIF NEW.payment_status = 'overdue' THEN
        NEW.overdue_days := CURRENT_DATE - NEW.due_date;
    ELSIF NEW.payment_status = 'paid' THEN
        -- 付款後計算最終逾期天數
        IF NEW.paid_at IS NOT NULL AND NEW.due_date < NEW.paid_at::DATE THEN
            NEW.overdue_days := NEW.paid_at::DATE - NEW.due_date;
        ELSE
            NEW.overdue_days := 0;
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER calculate_payment_overdue
    BEFORE INSERT OR UPDATE ON payments
    FOR EACH ROW EXECUTE FUNCTION calculate_overdue_days();

-- ============================================================================
-- 4. 佣金資格檢查函數
-- ============================================================================
CREATE OR REPLACE FUNCTION check_commission_eligibility()
RETURNS TRIGGER AS $$
BEGIN
    -- 當合約狀態變為 active 時，檢查是否需要建立佣金記錄
    IF NEW.status = 'active' AND OLD.status != 'active' THEN
        -- 如果有 broker_firm_id 且 commission_eligible = true
        IF NEW.broker_firm_id IS NOT NULL AND NEW.commission_eligible = TRUE THEN
            -- 檢查是否已存在佣金記錄
            IF NOT EXISTS (
                SELECT 1 FROM commissions
                WHERE contract_id = NEW.id
            ) THEN
                -- 取得會計事務所的佣金率
                INSERT INTO commissions (
                    accounting_firm_id,
                    customer_id,
                    contract_id,
                    amount,
                    based_on_rent,
                    contract_start,
                    eligible_date,
                    status
                )
                SELECT
                    NEW.broker_firm_id,
                    NEW.customer_id,
                    NEW.id,
                    NEW.monthly_rent * (af.commission_rate / 100),
                    NEW.monthly_rent,
                    NEW.start_date,
                    NEW.start_date + INTERVAL '6 months',
                    'pending'
                FROM accounting_firms af
                WHERE af.id = NEW.broker_firm_id;
            END IF;
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_contract_commission
    AFTER UPDATE ON contracts
    FOR EACH ROW EXECUTE FUNCTION check_commission_eligibility();

-- ============================================================================
-- 5. 會計事務所統計更新函數
-- ============================================================================
CREATE OR REPLACE FUNCTION update_firm_statistics()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        -- 新增佣金時，更新總推薦數
        UPDATE accounting_firms
        SET total_referrals = total_referrals + 1,
            updated_at = NOW()
        WHERE id = NEW.accounting_firm_id;

    ELSIF TG_OP = 'UPDATE' THEN
        -- 佣金狀態變為 paid 時，更新已付佣金總額
        IF NEW.status = 'paid' AND OLD.status != 'paid' THEN
            UPDATE accounting_firms
            SET total_commission_paid = total_commission_paid + NEW.amount,
                updated_at = NOW()
            WHERE id = NEW.accounting_firm_id;
        END IF;

    ELSIF TG_OP = 'DELETE' THEN
        -- 刪除佣金時，更新統計（少見但需處理）
        UPDATE accounting_firms
        SET total_referrals = GREATEST(total_referrals - 1, 0),
            updated_at = NOW()
        WHERE id = OLD.accounting_firm_id;
    END IF;

    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_accounting_firm_stats
    AFTER INSERT OR UPDATE OR DELETE ON commissions
    FOR EACH ROW EXECUTE FUNCTION update_firm_statistics();

-- ============================================================================
-- 6. 客戶風險等級自動更新函數
-- ============================================================================
CREATE OR REPLACE FUNCTION update_customer_risk_level()
RETURNS TRIGGER AS $$
DECLARE
    overdue_count INTEGER;
    total_overdue_days INTEGER;
    new_risk_level VARCHAR(10);
BEGIN
    -- 計算客戶的逾期統計
    SELECT
        COUNT(*),
        COALESCE(SUM(overdue_days), 0)
    INTO overdue_count, total_overdue_days
    FROM payments
    WHERE customer_id = NEW.customer_id
      AND payment_status = 'overdue';

    -- 根據逾期情況決定風險等級
    IF overdue_count >= 3 OR total_overdue_days > 60 THEN
        new_risk_level := 'high';
    ELSIF overdue_count >= 1 OR total_overdue_days > 14 THEN
        new_risk_level := 'medium';
    ELSE
        new_risk_level := 'low';
    END IF;

    -- 更新客戶風險等級
    UPDATE customers
    SET risk_level = new_risk_level,
        updated_at = NOW()
    WHERE id = NEW.customer_id
      AND risk_level != new_risk_level;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER auto_update_customer_risk
    AFTER INSERT OR UPDATE OF payment_status, overdue_days ON payments
    FOR EACH ROW EXECUTE FUNCTION update_customer_risk_level();

-- ============================================================================
-- 7. 產生合約編號函數
-- ============================================================================
CREATE OR REPLACE FUNCTION generate_contract_number()
RETURNS TRIGGER AS $$
DECLARE
    branch_code VARCHAR(10);
    year_str VARCHAR(4);
    seq_num INTEGER;
    new_number VARCHAR(50);
BEGIN
    -- 如果已有合約編號，跳過
    IF NEW.contract_number IS NOT NULL THEN
        RETURN NEW;
    END IF;

    -- 取得場館代碼
    SELECT code INTO branch_code FROM branches WHERE id = NEW.branch_id;

    -- 取得年份
    year_str := TO_CHAR(CURRENT_DATE, 'YYYY');

    -- 取得序號
    SELECT COALESCE(MAX(
        NULLIF(
            REGEXP_REPLACE(contract_number, '^[A-Z]+-[0-9]+-', ''),
            ''
        )::INTEGER
    ), 0) + 1
    INTO seq_num
    FROM contracts
    WHERE branch_id = NEW.branch_id
      AND contract_number LIKE branch_code || '-' || year_str || '-%';

    -- 產生編號: DZ-2025-001
    new_number := branch_code || '-' || year_str || '-' || LPAD(seq_num::TEXT, 3, '0');

    NEW.contract_number := new_number;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER auto_generate_contract_number
    BEFORE INSERT ON contracts
    FOR EACH ROW EXECUTE FUNCTION generate_contract_number();

-- ============================================================================
-- 8. 通知排程函數
-- ============================================================================
CREATE OR REPLACE FUNCTION schedule_payment_reminder(
    p_payment_id INTEGER,
    p_reminder_type VARCHAR(50),
    p_scheduled_at TIMESTAMPTZ DEFAULT NOW()
)
RETURNS BIGINT AS $$
DECLARE
    v_customer_id INTEGER;
    v_line_user_id VARCHAR(100);
    v_notification_id BIGINT;
    v_payload JSONB;
BEGIN
    -- 取得付款與客戶資訊
    SELECT p.customer_id, c.line_user_id
    INTO v_customer_id, v_line_user_id
    FROM payments p
    JOIN customers c ON p.customer_id = c.id
    WHERE p.id = p_payment_id;

    -- 如果客戶沒有 LINE ID，不排程
    IF v_line_user_id IS NULL OR v_line_user_id = '' THEN
        RETURN NULL;
    END IF;

    -- 建立通知 payload
    v_payload := jsonb_build_object(
        'payment_id', p_payment_id,
        'reminder_type', p_reminder_type
    );

    -- 插入通知佇列
    INSERT INTO notification_queue (
        notification_type,
        channel,
        recipient_id,
        recipient_line_id,
        payload,
        scheduled_at,
        status
    ) VALUES (
        'payment_reminder',
        'line',
        v_customer_id,
        v_line_user_id,
        v_payload,
        p_scheduled_at,
        'pending'
    )
    RETURNING id INTO v_notification_id;

    RETURN v_notification_id;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 9. 批次更新逾期狀態函數 (每日排程用)
-- ============================================================================
CREATE OR REPLACE FUNCTION batch_update_overdue_status()
RETURNS TABLE (
    updated_count INTEGER,
    total_overdue_amount NUMERIC
) AS $$
DECLARE
    v_updated_count INTEGER;
    v_total_amount NUMERIC;
BEGIN
    -- 更新所有已過期但仍為 pending 的付款
    WITH updated AS (
        UPDATE payments
        SET payment_status = 'overdue',
            overdue_days = CURRENT_DATE - due_date,
            updated_at = NOW()
        WHERE payment_status = 'pending'
          AND due_date < CURRENT_DATE
        RETURNING id, amount
    )
    SELECT COUNT(*), COALESCE(SUM(amount), 0)
    INTO v_updated_count, v_total_amount
    FROM updated;

    RETURN QUERY SELECT v_updated_count, v_total_amount;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 10. 統計報表函數
-- ============================================================================

-- 10.1 營收統計函數
CREATE OR REPLACE FUNCTION get_revenue_summary(
    p_branch_id INTEGER DEFAULT NULL,
    p_start_date DATE DEFAULT DATE_TRUNC('month', CURRENT_DATE)::DATE,
    p_end_date DATE DEFAULT (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month - 1 day')::DATE
)
RETURNS TABLE (
    branch_id INTEGER,
    branch_name VARCHAR,
    total_revenue NUMERIC,
    paid_count INTEGER,
    pending_amount NUMERIC,
    pending_count INTEGER,
    overdue_amount NUMERIC,
    overdue_count INTEGER,
    collection_rate NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        b.id AS branch_id,
        b.name AS branch_name,
        COALESCE(SUM(p.amount) FILTER (WHERE p.payment_status = 'paid'), 0) AS total_revenue,
        COUNT(*) FILTER (WHERE p.payment_status = 'paid')::INTEGER AS paid_count,
        COALESCE(SUM(p.amount) FILTER (WHERE p.payment_status = 'pending'), 0) AS pending_amount,
        COUNT(*) FILTER (WHERE p.payment_status = 'pending')::INTEGER AS pending_count,
        COALESCE(SUM(p.amount) FILTER (WHERE p.payment_status = 'overdue'), 0) AS overdue_amount,
        COUNT(*) FILTER (WHERE p.payment_status = 'overdue')::INTEGER AS overdue_count,
        CASE
            WHEN COUNT(*) > 0 THEN
                ROUND(COUNT(*) FILTER (WHERE p.payment_status = 'paid')::NUMERIC / COUNT(*) * 100, 2)
            ELSE 0
        END AS collection_rate
    FROM branches b
    LEFT JOIN payments p ON p.branch_id = b.id
        AND p.due_date BETWEEN p_start_date AND p_end_date
    WHERE (p_branch_id IS NULL OR b.id = p_branch_id)
      AND b.status = 'active'
    GROUP BY b.id, b.name
    ORDER BY b.id;
END;
$$ LANGUAGE plpgsql;

-- 10.2 客戶統計函數
CREATE OR REPLACE FUNCTION get_customer_statistics(
    p_branch_id INTEGER DEFAULT NULL
)
RETURNS TABLE (
    branch_id INTEGER,
    branch_name VARCHAR,
    total_customers INTEGER,
    active_customers INTEGER,
    prospect_customers INTEGER,
    churned_customers INTEGER,
    high_risk_customers INTEGER,
    with_line_id INTEGER,
    new_this_month INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        b.id AS branch_id,
        b.name AS branch_name,
        COUNT(c.id)::INTEGER AS total_customers,
        COUNT(c.id) FILTER (WHERE c.status = 'active')::INTEGER AS active_customers,
        COUNT(c.id) FILTER (WHERE c.status = 'prospect')::INTEGER AS prospect_customers,
        COUNT(c.id) FILTER (WHERE c.status = 'churned')::INTEGER AS churned_customers,
        COUNT(c.id) FILTER (WHERE c.risk_level = 'high')::INTEGER AS high_risk_customers,
        COUNT(c.id) FILTER (WHERE c.line_user_id IS NOT NULL AND c.line_user_id != '')::INTEGER AS with_line_id,
        COUNT(c.id) FILTER (WHERE c.created_at >= DATE_TRUNC('month', CURRENT_DATE))::INTEGER AS new_this_month
    FROM branches b
    LEFT JOIN customers c ON c.branch_id = b.id
    WHERE (p_branch_id IS NULL OR b.id = p_branch_id)
      AND b.status = 'active'
    GROUP BY b.id, b.name
    ORDER BY b.id;
END;
$$ LANGUAGE plpgsql;
