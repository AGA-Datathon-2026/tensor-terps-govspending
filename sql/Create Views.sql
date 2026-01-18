-- File: 02_create_views.sql
USE GovSpendingDB;
GO

/*========================================================
  02) ANALYTICS VIEWS FOR TABLEAU
  - Drops then recreates views (safe re-run)
  - Built for Tableau: wide, clear columns, stable names
========================================================*/

-- =========================================
-- Drop views (reverse dependency order)
-- =========================================
DROP VIEW IF EXISTS dbo.vw_budget_function_share_by_year;
DROP VIEW IF EXISTS dbo.vw_top_states_latest_year;
DROP VIEW IF EXISTS dbo.vw_state_spend_by_year;
DROP VIEW IF EXISTS dbo.vw_awardtype_share_latest_year;
DROP VIEW IF EXISTS dbo.vw_awardtype_share_by_year;
DROP VIEW IF EXISTS dbo.vw_global_yoy_extremes;
DROP VIEW IF EXISTS dbo.vw_agency_yoy_extremes;
DROP VIEW IF EXISTS dbo.vw_top_agencies_latest_year_ranked;
DROP VIEW IF EXISTS dbo.vw_top_agencies_by_year_ranked;
DROP VIEW IF EXISTS dbo.vw_agency_share_of_total;
DROP VIEW IF EXISTS dbo.vw_agency_yoy_growth;
DROP VIEW IF EXISTS dbo.vw_agency_spend_by_year;
DROP VIEW IF EXISTS dbo.vw_total_spending_by_year;
GO


-- =========================================
-- 1) Total Spending by Year
-- =========================================
CREATE VIEW dbo.vw_total_spending_by_year AS
SELECT
    f.year,
    SUM(f.amount) AS total_spending
FROM dbo.fact_spend_agency_year f
GROUP BY f.year;
GO


-- =========================================
-- 2) Agency Spending by Year
-- =========================================
CREATE VIEW dbo.vw_agency_spend_by_year AS
SELECT
    a.agency_id,
    a.agency_name,
    a.toptier_code,
    f.year,
    SUM(f.amount) AS total_spending
FROM dbo.fact_spend_agency_year f
JOIN dbo.dim_agency a
    ON a.agency_id = f.agency_id
GROUP BY
    a.agency_id, a.agency_name, a.toptier_code, f.year;
GO


-- =========================================
-- 3) Agency YoY Growth
-- =========================================
CREATE VIEW dbo.vw_agency_yoy_growth AS
WITH base AS (
    SELECT
        agency_id,
        agency_name,
        year,
        total_spending,
        LAG(total_spending) OVER (PARTITION BY agency_id ORDER BY year) AS prev_year_spending
    FROM dbo.vw_agency_spend_by_year
)
SELECT
    agency_id,
    agency_name,
    year,
    total_spending,
    prev_year_spending,
    CASE
        WHEN prev_year_spending IS NULL THEN NULL
        WHEN prev_year_spending = 0 THEN NULL
        ELSE (total_spending - prev_year_spending) / NULLIF(prev_year_spending, 0)
    END AS yoy_growth_rate
FROM base;
GO


-- =========================================
-- 4) Agency Share of Total
-- =========================================
CREATE VIEW dbo.vw_agency_share_of_total AS
WITH totals AS (
    SELECT year, SUM(total_spending) AS total_spending_all
    FROM dbo.vw_agency_spend_by_year
    GROUP BY year
)
SELECT
    a.agency_id,
    a.agency_name,
    a.year,
    a.total_spending,
    t.total_spending_all,
    a.total_spending * 1.0 / NULLIF(t.total_spending_all, 0) AS share_of_total
FROM dbo.vw_agency_spend_by_year a
JOIN totals t
    ON a.year = t.year;
GO


-- =========================================
-- 5) Top Agencies by Year (Ranked)
-- =========================================
CREATE VIEW dbo.vw_top_agencies_by_year_ranked AS
SELECT
    year,
    agency_id,
    agency_name,
    total_spending,
    DENSE_RANK() OVER (PARTITION BY year ORDER BY total_spending DESC) AS spending_rank
FROM dbo.vw_agency_spend_by_year;
GO


-- =========================================
-- 6) Top Agencies - Latest Year
-- =========================================
CREATE VIEW dbo.vw_top_agencies_latest_year_ranked AS
WITH ly AS (SELECT MAX(year) AS max_year FROM dbo.dim_time)
SELECT *
FROM dbo.vw_top_agencies_by_year_ranked
WHERE year = (SELECT max_year FROM ly);
GO


-- =========================================
-- 7) YoY Extremes per Agency
-- =========================================
CREATE VIEW dbo.vw_agency_yoy_extremes AS
WITH base AS (
    SELECT *
    FROM dbo.vw_agency_yoy_growth
    WHERE yoy_growth_rate IS NOT NULL
)
SELECT
    agency_id,
    agency_name,
    MAX(yoy_growth_rate) AS max_yoy_growth_rate,
    MIN(yoy_growth_rate) AS min_yoy_growth_rate
FROM base
GROUP BY agency_id, agency_name;
GO


-- =========================================
-- 8) Global YoY Extremes (Max Increase / Decrease)
-- =========================================
CREATE VIEW dbo.vw_global_yoy_extremes AS
WITH base AS (
    SELECT *
    FROM dbo.vw_agency_yoy_growth
    WHERE yoy_growth_rate IS NOT NULL
)
SELECT TOP 1
    'Max Increase' AS extreme_type,
    agency_id,
    agency_name,
    year,
    yoy_growth_rate
FROM base
ORDER BY yoy_growth_rate DESC

UNION ALL

SELECT TOP 1
    'Max Decrease' AS extreme_type,
    agency_id,
    agency_name,
    year,
    yoy_growth_rate
FROM base
ORDER BY yoy_growth_rate ASC;
GO


-- =========================================
-- 9) Award Type Share by Agency-Year
-- =========================================
CREATE VIEW dbo.vw_awardtype_share_by_year AS
WITH base AS (
    SELECT
        a.agency_id,
        a.agency_name,
        f.year,
        at.award_type_name,
        SUM(f.amount) AS total_spending
    FROM dbo.fact_spend_agency_awardtype_year f
    JOIN dbo.dim_agency a ON a.agency_id = f.agency_id
    JOIN dbo.dim_award_type at ON at.award_type_id = f.award_type_id
    GROUP BY a.agency_id, a.agency_name, f.year, at.award_type_name
),
totals AS (
    SELECT agency_id, year, SUM(total_spending) AS year_total
    FROM base
    GROUP BY agency_id, year
)
SELECT
    b.agency_id,
    b.agency_name,
    b.year,
    b.award_type_name,
    b.total_spending,
    t.year_total,
    b.total_spending * 1.0 / NULLIF(t.year_total, 0) AS share_of_agency_year
FROM base b
JOIN totals t
  ON b.agency_id = t.agency_id AND b.year = t.year;
GO


-- =========================================
-- 10) Award Type Share - Latest Year
-- =========================================
CREATE VIEW dbo.vw_awardtype_share_latest_year AS
WITH ly AS (SELECT MAX(year) AS max_year FROM dbo.dim_time)
SELECT *
FROM dbo.vw_awardtype_share_by_year
WHERE year = (SELECT max_year FROM ly);
GO


-- =========================================
-- 11) State Spend by Agency-Year
-- =========================================
CREATE VIEW dbo.vw_state_spend_by_year AS
SELECT
    a.agency_id,
    a.agency_name,
    f.year,
    s.state_code,
    s.state_name,
    SUM(f.amount) AS total_spending
FROM dbo.fact_spend_agency_state_year f
JOIN dbo.dim_agency a ON a.agency_id = f.agency_id
JOIN dbo.dim_state s ON s.state_code = f.state_code
GROUP BY a.agency_id, a.agency_name, f.year, s.state_code, s.state_name;
GO


-- =========================================
-- 12) Top States - Latest Year (Ranked per Agency)
-- =========================================
CREATE VIEW dbo.vw_top_states_latest_year AS
WITH ly AS (SELECT MAX(year) AS max_year FROM dbo.dim_time),
base AS (
    SELECT *
    FROM dbo.vw_state_spend_by_year
    WHERE year = (SELECT max_year FROM ly)
)
SELECT
    agency_id,
    agency_name,
    year,
    state_code,
    state_name,
    total_spending,
    DENSE_RANK() OVER (PARTITION BY agency_id ORDER BY total_spending DESC) AS state_rank
FROM base;
GO


-- =========================================
-- 13) Budget Function Share by Year
-- =========================================
CREATE VIEW dbo.vw_budget_function_share_by_year AS
WITH totals AS (
    SELECT year, SUM(amount) AS year_total
    FROM dbo.fact_spend_budget_function_year
    GROUP BY year
)
SELECT
    f.year,
    f.budget_function_code,
    f.budget_function_name,
    f.amount,
    t.year_total,
    f.amount * 1.0 / NULLIF(t.year_total, 0) AS share_of_total
FROM dbo.fact_spend_budget_function_year f
JOIN totals t
  ON f.year = t.year;
GO
