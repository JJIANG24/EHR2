-- Step 3: ELT - Extract, Load, Transform

-- Extract data from external sources or other databases.

-- Step 3a: Extract data from external sources

-- Example: Extract patient data from CSV file into staging table
CREATE TABLE ehr_financial_data.staging_patients (
    patient_id INT64 PRIMARY KEY,
    name STRING,
    dob DATE,
    gender STRING,
    address STRING,
    insurance_provider STRING,
    admission_date DATE,
    discharge_date DATE,
    diagnosis_code STRING
);

-- Load the extracted data into the staging tables.

-- Step 3b: Load data into staging tables

-- Example: Load patient data from CSV file into staging table
COPY ehr_financial_data.staging_patients FROM 'patient_data.csv' CSV HEADER;

-- Step 3c: Transform and clean the extracted data as needed.

-- Example: Transform and clean patient data from staging table
-- For demonstration purposes, let's assume we need to clean the data by removing any rows with missing values
CREATE TABLE ehr_financial_data.patients_cleaned AS
SELECT * FROM ehr_financial_data.staging_patients WHERE dob IS NOT NULL AND gender IS NOT NULL;

-- Step 3d: Load the transformed data into the final database tables.

-- Example: Load cleaned patient data into final patient table
INSERT INTO ehr_financial_data.patients (patient_id, name, dob, gender, address, insurance_provider, admission_date, discharge_date, diagnosis_code)
SELECT * FROM ehr_financial_data.patients_cleaned;

-- Step 4: Data Cleaning and Preparation

-- Clean the data by removing duplicates, handling missing values, and ensuring data consistency.

-- Step 4a: Data cleaning

-- Example: Remove duplicates from the transactions table
DELETE FROM ehr_financial_data.transactions WHERE transaction_id IN (
    SELECT transaction_id FROM (
        SELECT transaction_id, ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_id) AS row_num
        FROM ehr_financial_data.transactions
    ) AS duplicates
    WHERE row_num > 1
);

-- Step 4b: Data preparation

-- Example: Calculate age from date of birth and admission date
ALTER TABLE ehr_financial_data.patients ADD COLUMN age INT;
UPDATE ehr_financial_data.patients SET age = EXTRACT(YEAR FROM admission_date) - EXTRACT(YEAR FROM dob);

-- Step 5: Perform Statistical Analysis

-- Conduct advanced statistical analyses to extract meaningful insights from the data.

-- Example 1: Calculate total revenue by insurance provider
WITH insurance_revenue AS (
    SELECT
        p.insurance_provider,
        SUM(t.amount) AS total_revenue
    FROM
        ehr_financial_data.patients p
    JOIN
        ehr_financial_data.transactions t
    ON
        p.patient_id = t.patient_id
    GROUP BY
        p.insurance_provider
)
SELECT
    insurance_provider,
    total_revenue
FROM
    insurance_revenue
ORDER BY
    total_revenue DESC;

-- Example 2: Identify patients with high procedure costs
SELECT
    patient_id,
    SUM(procedure_cost) AS total_procedure_cost
FROM
    ehr_financial_data.procedures
GROUP BY
    patient_id
HAVING
    total_procedure_cost > (SELECT AVG(procedure_cost) * 2 FROM ehr_financial_data.procedures)
ORDER BY
    total_procedure_cost DESC;

-- Step 6: Advanced SQL Techniques

-- Utilize advanced SQL techniques to perform complex analyses and optimize query performance.

-- Example 4: Calculate moving average of transaction amounts
SELECT
    transaction_date,
    amount,
    AVG(amount) OVER (ORDER BY transaction_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS moving_avg_amount
FROM
    ehr_financial_data.transactions
ORDER BY
    transaction_date;

-- Example 5: Use recursive CTE to generate patient treatment history
WITH RECURSIVE patient_history AS (
    SELECT
        patient_id,
        procedure_date,
        procedure_code,
        procedure_description,
        procedure_cost,
        performing_doctor_id
    FROM
        ehr_financial_data.procedures
    UNION ALL
    SELECT
        p.patient_id,
        p.procedure_date,
        p.procedure_code,
        p.procedure_description,
        p.procedure_cost,
        p.performing_doctor_id
    FROM
        ehr_financial_data.procedures p
    JOIN
        patient_history ph
    ON
        p.patient_id = ph.patient_id
)
SELECT
    patient_id,
    procedure_date,
    procedure_code,
    procedure_description,
    procedure_cost,
    performing_doctor_id
FROM
    patient_history;

-- Step 7: Optimization

-- Optimize query performance through indexing, partitioning, and materialized views.

-- Example 6: Create indexes for frequently queried columns
CREATE INDEX idx_patient_id ON ehr_financial_data.transactions(patient_id);
CREATE INDEX idx_patient_id ON ehr_financial_data.procedures(patient_id);

-- Example 7: Partition large tables for improved query speed
CREATE TABLE ehr_financial_data.transactions_partitioned
PARTITION BY patient_id
AS
SELECT
    *
FROM
    ehr_financial_data.transactions;

-- Example 8: Create materialized views for complex queries
CREATE MATERIALIZED VIEW ehr_financial_data.patient_summary
AS
SELECT
    patient_id,
    COUNT(transaction_id) AS transaction_count,
    SUM(amount) AS total_transaction_amount
FROM
    ehr_financial_data.transactions
GROUP BY
    patient_id;

-- Step 8: Documentation and Reporting

-- Document project methodologies, findings, and insights for knowledge sharing and future reference.

-- End of Project
