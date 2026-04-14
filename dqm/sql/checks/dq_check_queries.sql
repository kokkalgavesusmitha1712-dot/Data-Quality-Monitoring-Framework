-- ================================================================
-- dq_check_queries.sql
-- SQL-based data quality checks for the monitoring framework
-- Run these against your warehouse for a SQL-native DQ layer
-- ================================================================


-- ---------------------------------------------------------------
-- 1. Null audit — count nulls per column across key tables
-- ---------------------------------------------------------------
SELECT
    'claims'                                        AS table_name,
    'claim_id'                                      AS column_name,
    COUNT(*)                                        AS total_rows,
    SUM(CASE WHEN claim_id    IS NULL THEN 1 END)   AS null_claim_id,
    SUM(CASE WHEN member_id   IS NULL THEN 1 END)   AS null_member_id,
    SUM(CASE WHEN claim_status IS NULL THEN 1 END)  AS null_claim_status,
    SUM(CASE WHEN billed_amount IS NULL THEN 1 END) AS null_billed_amount,
    ROUND(100.0 * SUM(CASE WHEN claim_id IS NULL THEN 1 END) / COUNT(*), 2)
                                                    AS null_pct_claim_id
FROM claims;


-- ---------------------------------------------------------------
-- 2. Duplicate detection — find duplicate primary keys
-- ---------------------------------------------------------------
SELECT
    claim_id,
    COUNT(*) AS occurrences
FROM claims
GROUP BY claim_id
HAVING COUNT(*) > 1
ORDER BY occurrences DESC;


-- ---------------------------------------------------------------
-- 3. Range validation — values outside expected bounds
-- ---------------------------------------------------------------
SELECT
    claim_id,
    billed_amount,
    paid_amount,
    processing_days,
    CASE
        WHEN billed_amount  < 0           THEN 'negative_billed'
        WHEN billed_amount  > 500000      THEN 'billed_too_high'
        WHEN paid_amount    < 0           THEN 'negative_paid'
        WHEN paid_amount    > billed_amount THEN 'paid_exceeds_billed'
        WHEN processing_days < 0          THEN 'negative_processing_days'
        WHEN processing_days > 365        THEN 'processing_too_long'
        ELSE 'ok'
    END AS range_flag
FROM claims
WHERE billed_amount < 0
   OR billed_amount > 500000
   OR paid_amount < 0
   OR paid_amount > billed_amount
   OR processing_days < 0
   OR processing_days > 365;


-- ---------------------------------------------------------------
-- 4. Allowed values check — invalid categorical values
-- ---------------------------------------------------------------
SELECT claim_id, claim_status, claim_type
FROM claims
WHERE claim_status NOT IN ('approved','denied','pending','under_review')
   OR claim_type   NOT IN ('medical','pharmacy','dental','vision','behavioral');


-- ---------------------------------------------------------------
-- 5. Referential integrity — claims without matching members
-- ---------------------------------------------------------------
SELECT c.claim_id, c.member_id
FROM claims c
LEFT JOIN members m ON c.member_id = m.member_id
WHERE m.member_id IS NULL;


-- ---------------------------------------------------------------
-- 6. Data freshness — check when each table was last updated
-- ---------------------------------------------------------------
SELECT
    'claims'                        AS table_name,
    MAX(claim_date)                 AS latest_record_date,
    CURRENT_DATE - MAX(claim_date)  AS days_since_last_record,
    CASE
        WHEN CURRENT_DATE - MAX(claim_date) > 2 THEN 'STALE'
        ELSE 'FRESH'
    END                             AS freshness_status
FROM claims

UNION ALL

SELECT
    'revenue_actuals',
    MAX(period),
    DATEDIFF(day, MAX(period), CURRENT_DATE),
    CASE WHEN DATEDIFF(day, MAX(period), CURRENT_DATE) > 35 THEN 'STALE' ELSE 'FRESH' END
FROM revenue_actuals;


-- ---------------------------------------------------------------
-- 7. Daily quality score trend (from score history table)
-- ---------------------------------------------------------------
SELECT
    table_name,
    DATE(run_timestamp)             AS check_date,
    ROUND(AVG(overall_score), 1)    AS avg_daily_score,
    MIN(overall_score)              AS min_score,
    MAX(overall_score)              AS max_score,
    SUM(failed)                     AS total_fails
FROM quality_score_history
GROUP BY table_name, DATE(run_timestamp)
ORDER BY check_date DESC, table_name;


-- ---------------------------------------------------------------
-- 8. Tables below quality threshold (score < 90)
-- ---------------------------------------------------------------
SELECT
    table_name,
    overall_score,
    grade,
    failed,
    warned,
    run_timestamp
FROM quality_score_history
WHERE overall_score < 90
  AND run_timestamp >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY overall_score ASC;
