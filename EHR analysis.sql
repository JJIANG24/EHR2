-- Project: Comprehensive EHR and Financial Data Analysis

-- Step 1: Create Schema

-- The schema defines the structure of our database, providing a blueprint for organizing data efficiently.

CREATE SCHEMA ehr_financial_data;

-- Step 2: Create Tables

-- We create tables to store patient information, diagnosis codes, medical procedures, financial transactions, insurance details, healthcare provider information, and more.

CREATE TABLE ehr_financial_data.patients (
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

CREATE TABLE ehr_financial_data.diagnosis_codes (
    code STRING PRIMARY KEY,
    description STRING
);

CREATE TABLE ehr_financial_data.procedures (
    procedure_id INT64 PRIMARY KEY,
    patient_id INT64,
    procedure_date DATE,
    procedure_code STRING,
    procedure_description STRING,
    procedure_cost FLOAT64,
    performing_doctor_id INT64,
    FOREIGN KEY (patient_id) REFERENCES ehr_financial_data.patients(patient_id)
);

CREATE TABLE ehr_financial_data.transactions (
    transaction_id INT64 PRIMARY KEY,
    patient_id INT64,
    transaction_date DATE,
    amount FLOAT64,
    transaction_type STRING,
    FOREIGN KEY (patient_id) REFERENCES ehr_financial_data.patients(patient_id)
);

CREATE TABLE ehr_financial_data.insurance_plans (
    plan_id INT64 PRIMARY KEY,
    plan_name STRING,
    coverage_type STRING,
    coverage_amount FLOAT64
);

CREATE TABLE ehr_financial_data.healthcare_providers (
    provider_id INT64 PRIMARY KEY,
    provider_name STRING,
    specialty STRING,
    address STRING,
    contact_number STRING
);

CREATE TABLE ehr_financial_data.lab_results (
    result_id INT64 PRIMARY KEY,
    patient_id INT64,
    test_date DATE,
    test_type STRING,
    result_value FLOAT64,
    unit STRING,
    FOREIGN KEY (patient_id) REFERENCES ehr_financial_data.patients(patient_id)
);

CREATE TABLE ehr_financial_data.admissions (
    admission_id INT64 PRIMARY KEY,
    patient_id INT64,
    admission_date DATE,
    discharge_date DATE,
    admission_type STRING,
    discharge_type STRING,
    FOREIGN KEY (patient_id) REFERENCES ehr_financial_data.patients(patient_id)
);

CREATE TABLE ehr_financial_data.medical_staff (
    staff_id INT64 PRIMARY KEY,
    name STRING,
    role STRING,
    specialty STRING
);

-- Step 3: ETL - Extract, Transform, Load

-- Extract data from external sources or other databases.

-- Step 3a: Extract data from staging tables

-- Transform and clean the extracted data as needed.

-- Step 3b: Transform and clean the extracted data as needed.

-- Load the transformed data into the final database tables.

-- Step 3c: Load data into final tables

-- Example: Load patient data from staging table into final patient table
INSERT INTO ehr_financial_data.patients (patient_id, name, dob, gender, address, insurance_provider, admission_date, discharge_date, diagnosis_code)
SELECT * FROM ehr_financial_data.staging_patients;

-- Step 4: Data Cleaning and Preparation

-- Clean the data by removing duplicates, handling missing values, and ensuring data consistency.

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

-- Example 3: Calculate average transaction amount by gender and insurance type
SELECT
    p.gender,
    p.insurance_provider,
    AVG(t.amount) AS avg_transaction_amount
FROM
    ehr_financial_data.patients p
JOIN
    ehr_financial_data.transactions t
ON
    p.patient_id = t.patient_id
GROUP BY
    p.gender, p.insurance_provider;

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
