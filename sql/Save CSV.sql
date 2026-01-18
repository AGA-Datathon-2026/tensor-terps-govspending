-- File: 03_tableau_extract_queries.sql
USE GovSpendingDB;
GO

/*========================================================
  03) TABLEAU EXTRACT / QA QUERIES
  - These are the SELECT statements you run to export CSVs
  - Also includes the optional YoY ranking view for states
========================================================*/

-- =========================================
-- Optional: State Total YoY + Rank (all agencies combined)
-- This creates a Tableau-friendly view for "state YoY winners"
-- =========================================
CREATE OR ALTER VIEW dbo.vw_state_total_spend_yoy_ranked AS
WITH state_year AS (
    SELECT
        state_code,
        state_name,
        [year],
        SUM(total_spending) AS total_spending
    FROM dbo.vw_state_spend_by_year
    GROUP BY state_code, state_name, [year]
),
yoy AS (
    SELECT
        state_code,
        state_name,
        [year],
        total_spending,
        LAG(total_spending) OVER (PARTITION BY state_code ORDER BY [year]) AS prev_year_spending,
        (total_spending - LAG(total_spending) OVER (PARTITION BY state_code ORDER BY [year])) AS yoy_change_amount,
        CASE
            WHEN LAG(total_spending) OVER (PARTITION BY state_code ORDER BY [year]) IS NULL
              OR LAG(total_spending) OVER (PARTITION BY state_code ORDER BY [year]) = 0
            THEN NULL
            ELSE
                (total_spending - LAG(total_spending) OVER (PARTITION BY state_code ORDER BY [year]))
                * 100.0
                / LAG(total_spending) OVER (PARTITION BY state_code ORDER BY [year])
        END AS yoy_growth_pct
    FROM state_year
)
SELECT
    *,
    DENSE_RANK() OVER (
        PARTITION BY [year]
        ORDER BY yoy_growth_pct DESC
    ) AS yoy_growth_rank_in_year
FROM yoy;
GO


-- =========================================
-- 01) Total Spending by Year
-- =========================================
SELECT *
FROM dbo.vw_total_spending_by_year
ORDER BY year;


-- =========================================
-- 02) Agency Spending by Year
-- =========================================
SELECT *
FROM dbo.vw_agency_spend_by_year
ORDER BY agency_name, year;


-- =========================================
-- 03) Agency YoY Growth
-- =========================================
SELECT *
FROM dbo.vw_agency_yoy_growth
ORDER BY agency_name, year;


-- =========================================
-- 04) Agency Share of Total Spending
-- =========================================
SELECT *
FROM dbo.vw_agency_share_of_total
ORDER BY year, share_of_total DESC;


-- =========================================
-- 05) Top Agencies by Year (Ranked)
-- =========================================
SELECT *
FROM dbo.vw_top_agencies_by_year_ranked
ORDER BY year, spending_rank;


-- =========================================
-- 06) Top Agencies - Latest Year
-- =========================================
SELECT *
FROM dbo.vw_top_agencies_latest_year_ranked
ORDER BY spending_rank;


-- =========================================
-- 07) YoY Extremes per Agency
-- =========================================
SELECT *
FROM dbo.vw_agency_yoy_extremes
ORDER BY max_yoy_growth_rate DESC;


-- =========================================
-- 08) Global YoY Extremes (Max Increase / Decrease)
-- =========================================
SELECT *
FROM dbo.vw_global_yoy_extremes;


-- =========================================
-- 09) Award Type Share by Agency-Year
-- =========================================
SELECT *
FROM dbo.vw_awardtype_share_by_year
ORDER BY agency_name, year;


-- =========================================
-- 10) Award Type Share - Latest Year
-- =========================================
SELECT *
FROM dbo.vw_awardtype_share_latest_year
ORDER BY agency_name, award_type_name;


-- =========================================
-- 11) State Spend by Agency-Year
-- =========================================
SELECT *
FROM dbo.vw_state_spend_by_year
ORDER BY year DESC, total_spending DESC;


-- =========================================
-- 12) Top States - Latest Year (per Agency)
-- =========================================
SELECT *
FROM dbo.vw_top_states_latest_year
ORDER BY agency_name, state_rank;


-- =========================================
-- 13) State Total YoY + Rank (combined)
-- =========================================
SELECT *
FROM dbo.vw_state_total_spend_yoy_ranked
ORDER BY [year] DESC, yoy_growth_rank_in_year ASC;
GO
