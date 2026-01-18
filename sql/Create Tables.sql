-- File: 01_create_tables.sql
USE GovSpendingDB;
GO

/*========================================================
  01) CORE DIMENSIONS + FACT TABLES
  - Idempotent: creates only if not exists
  - Seeds: time dimension, award types, states (+ territories)
========================================================*/

-- =========================
-- dim_time
-- =========================
IF OBJECT_ID('dbo.dim_time', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.dim_time (
        year INT PRIMARY KEY
    );
END
GO

-- Seed years (edit if you extend fiscal years later)
;WITH years AS (
    SELECT v.year
    FROM (VALUES (2018),(2019),(2020),(2021),(2022),(2023),(2024),(2025)) v(year)
)
INSERT INTO dbo.dim_time(year)
SELECT y.year
FROM years y
WHERE NOT EXISTS (SELECT 1 FROM dbo.dim_time t WHERE t.year = y.year);
GO


-- =========================
-- dim_agency
-- =========================
IF OBJECT_ID('dbo.dim_agency', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.dim_agency (
        agency_id INT IDENTITY(1,1) PRIMARY KEY,
        toptier_code VARCHAR(10) NOT NULL UNIQUE,
        agency_name VARCHAR(255) NOT NULL
    );
END
GO


-- =========================
-- fact_spend_agency_year
-- =========================
IF OBJECT_ID('dbo.fact_spend_agency_year', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.fact_spend_agency_year (
        year INT NOT NULL,
        agency_id INT NOT NULL,
        amount DECIMAL(20,2) NOT NULL,

        CONSTRAINT PK_fact_spend_agency_year PRIMARY KEY (year, agency_id),
        CONSTRAINT FK_fact_year FOREIGN KEY (year) REFERENCES dbo.dim_time(year),
        CONSTRAINT FK_fact_agency FOREIGN KEY (agency_id) REFERENCES dbo.dim_agency(agency_id)
    );
END
GO


-- =========================
-- dim_award_type
-- =========================
IF OBJECT_ID('dbo.dim_award_type', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.dim_award_type (
        award_type_id INT IDENTITY(1,1) PRIMARY KEY,
        award_type_name VARCHAR(50) NOT NULL UNIQUE
    );
END
GO

-- Seed award types (idempotent)
IF NOT EXISTS (SELECT 1 FROM dbo.dim_award_type WHERE award_type_name = 'Contract')
    INSERT INTO dbo.dim_award_type (award_type_name) VALUES ('Contract');

IF NOT EXISTS (SELECT 1 FROM dbo.dim_award_type WHERE award_type_name = 'Assistance')
    INSERT INTO dbo.dim_award_type (award_type_name) VALUES ('Assistance');
GO


-- =========================
-- fact_spend_agency_awardtype_year
-- =========================
IF OBJECT_ID('dbo.fact_spend_agency_awardtype_year', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.fact_spend_agency_awardtype_year (
        year INT NOT NULL,
        agency_id INT NOT NULL,
        award_type_id INT NOT NULL,
        amount DECIMAL(20,2) NOT NULL,

        CONSTRAINT PK_fact_agency_awardtype_year PRIMARY KEY (year, agency_id, award_type_id),
        CONSTRAINT FK_awardtype_year FOREIGN KEY (year) REFERENCES dbo.dim_time(year),
        CONSTRAINT FK_awardtype_agency FOREIGN KEY (agency_id) REFERENCES dbo.dim_agency(agency_id),
        CONSTRAINT FK_awardtype_dim FOREIGN KEY (award_type_id) REFERENCES dbo.dim_award_type(award_type_id)
    );
END
GO


-- =========================
-- dim_state
-- =========================
IF OBJECT_ID('dbo.dim_state', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.dim_state (
        state_code CHAR(2) NOT NULL PRIMARY KEY,
        state_name VARCHAR(100) NOT NULL
    );
END
GO

-- Seed states (50 + DC)
MERGE dbo.dim_state AS target
USING (VALUES
('AL','Alabama'),
('AK','Alaska'),
('AZ','Arizona'),
('AR','Arkansas'),
('CA','California'),
('CO','Colorado'),
('CT','Connecticut'),
('DE','Delaware'),
('DC','District of Columbia'),
('FL','Florida'),
('GA','Georgia'),
('HI','Hawaii'),
('ID','Idaho'),
('IL','Illinois'),
('IN','Indiana'),
('IA','Iowa'),
('KS','Kansas'),
('KY','Kentucky'),
('LA','Louisiana'),
('ME','Maine'),
('MD','Maryland'),
('MA','Massachusetts'),
('MI','Michigan'),
('MN','Minnesota'),
('MS','Mississippi'),
('MO','Missouri'),
('MT','Montana'),
('NE','Nebraska'),
('NV','Nevada'),
('NH','New Hampshire'),
('NJ','New Jersey'),
('NM','New Mexico'),
('NY','New York'),
('NC','North Carolina'),
('ND','North Dakota'),
('OH','Ohio'),
('OK','Oklahoma'),
('OR','Oregon'),
('PA','Pennsylvania'),
('RI','Rhode Island'),
('SC','South Carolina'),
('SD','South Dakota'),
('TN','Tennessee'),
('TX','Texas'),
('UT','Utah'),
('VT','Vermont'),
('VA','Virginia'),
('WA','Washington'),
('WV','West Virginia'),
('WI','Wisconsin'),
('WY','Wyoming')
) AS source(state_code, state_name)
ON target.state_code = source.state_code
WHEN MATCHED AND target.state_name <> source.state_name THEN
    UPDATE SET state_name = source.state_name
WHEN NOT MATCHED THEN
    INSERT (state_code, state_name) VALUES (source.state_code, source.state_name);
GO

-- Add common territories + military codes
MERGE dbo.dim_state AS target
USING (VALUES
('PR','Puerto Rico'),
('GU','Guam'),
('VI','U.S. Virgin Islands'),
('AS','American Samoa'),
('MP','Northern Mariana Islands'),
('AA','Armed Forces Americas'),
('AE','Armed Forces Europe'),
('AP','Armed Forces Pacific')
) AS source(state_code, state_name)
ON target.state_code = source.state_code
WHEN MATCHED AND target.state_name <> source.state_name THEN
    UPDATE SET state_name = source.state_name
WHEN NOT MATCHED THEN
    INSERT (state_code, state_name) VALUES (source.state_code, source.state_name);
GO


-- =========================
-- fact_spend_agency_state_year
-- =========================
IF OBJECT_ID('dbo.fact_spend_agency_state_year', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.fact_spend_agency_state_year (
        year INT NOT NULL,
        agency_id INT NOT NULL,
        state_code CHAR(2) NOT NULL,
        amount DECIMAL(20,2) NOT NULL,

        CONSTRAINT PK_fact_agency_state_year PRIMARY KEY (year, agency_id, state_code),
        CONSTRAINT FK_state_year FOREIGN KEY (year) REFERENCES dbo.dim_time(year),
        CONSTRAINT FK_state_agency FOREIGN KEY (agency_id) REFERENCES dbo.dim_agency(agency_id),
        CONSTRAINT FK_state_dim FOREIGN KEY (state_code) REFERENCES dbo.dim_state(state_code)
    );
END
GO


-- =========================
-- fact_spend_budget_function_year
-- =========================
IF OBJECT_ID('dbo.fact_spend_budget_function_year', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.fact_spend_budget_function_year (
        year INT NOT NULL,
        budget_function_code VARCHAR(10) NOT NULL,
        budget_function_name VARCHAR(255) NOT NULL,
        amount DECIMAL(20,2) NOT NULL,

        CONSTRAINT PK_fact_spend_budget_function_year
            PRIMARY KEY (year, budget_function_code),
        CONSTRAINT FK_fact_budget_function_year
            FOREIGN KEY (year) REFERENCES dbo.dim_time(year)
    );
END
GO
