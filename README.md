# ❄️ Insurance - Snowflake Project ❄️

A complete end-to-end Snowflake data pipeline demonstrating ingestion, transformation, and analytics with insurance domain related data.

## Tech Stacks used in this project

<img width="320" height="77" alt="Snowflake_Logo" src="https://github.com/user-attachments/assets/88baf960-dab7-421a-8ca3-0d3f56e7be69" />

<a href="https://www.snowflake.com/">Snowflake</a> is a cloud-native data platform built for fast, scalable data warehousing and analytics. It separates compute and storage, supports both structured and semi-structured data, and runs on AWS, Azure, and Google Cloud. Snowflake offers secure data sharing, near-zero maintenance, and high performance.

## 📘 Table of Contents
- [Overview](#overview)
- [Architecture Diagram](#architecture-diagram)
- [Setup Instructions](#setup-instructions)
- [SQL Scripts](#sql-scripts)
- [Contact](#contact)

## 🧩 Overview
This project demonstrates how to build a scalable **data pipeline in Snowflake** using:
- Snowflake external stages (via AWS S3)  
- Snowpipe for automated ingestion (auto ingestion)
- Transformations using Snowflake Tasks & Streams (with SCD 2 implementaion)

## 🏗️ Architecture Diagram
![Snowflake Data Pipeline Diagram](images/snowflake_architecture.png)
*(Place your image under the `/images` folder in the repo.)*

## 🧰 Step-by-step guide

### To create an end to end data flow follow the given steps:
1. [Project setup](##project-setup) - Creation of required database objects in Snowflake
3. [Setting up datalake for file loading](##datalake-setup) - To load source files into AWS s3 bucket
4. [Copying data from external stage](##copy-data-setup) - To copy data from s3 bucket to Snowflake tables
5. To clean the data with proper structure and data type
6. To load the data to the stage layer and perform transformations
7. To load the data into base tables and implement SCD type 2 in dimension tables
   
<details>
<summary><b>1️⃣ Project setup - Create Database, Schema and other required database objects</b> <i>(click to expand)</i> </summary>

Here, we use Account Admin role for object creation,
```sql
USE ROLE ACCOUNTADMIN;
```
Database creation,
```sql
CREATE DATABASE Insurance_project;
```
Schema creation,
```sql
--Setting context to appropriate database

USE Insurance_project;

CREATE SCHEMA config;
CREATE SCHEMA Raw;
CREATE SCHEMA Stg;
CREATE SCHEMA Insurance;
CREATE SCHEMA Reporting;
```
Creation of raw data tables (RAW_US_CENTRAL,RAW_US_OTHERS,RAW_US_MERGED),

```sql
--Setting context and creating required tables

USE Insurance_project.raw;

/*###############################################    RAW_US_CENTRAL   ###############################################*/

CREATE OR REPLACE TABLE RAW_US_CENTRAL (
    "Customer ID" NUMBER,
    "Customer Title" VARCHAR,
    "Customer First Name" VARCHAR,
    "Customer Middle Name" VARCHAR,
    "Customer Last Name" VARCHAR,
    "Customer_Segment" VARCHAR,
    "Maritial_Status" VARCHAR,
    "Gender" VARCHAR,
    "DOB" VARCHAR,
    "Effective_Start_Dt" VARCHAR,
    "Effective_End_Dt" VARCHAR,
    "Policy_Type_Id" NUMBER,
    "Policy_Type" VARCHAR,
    "Policy_Type_Desc" VARCHAR,
    "Policy_Id" VARCHAR,
    "Policy_Name" VARCHAR,
    "Premium_Amt" NUMBER,
    "Policy_Term" VARCHAR,
    "Policy_Start_Dt" VARCHAR,
    "Policy_End_Dt" VARCHAR,
    "Next_Premium_Dt" VARCHAR,
    "Actual_Premium_Paid_Dt" VARCHAR,
    "Country" VARCHAR,
    "Region" VARCHAR,
    "State or Province" VARCHAR,
    "City" VARCHAR,
    "Postal Code" NUMBER,
    "Total_Policy_Amt" NUMBER,
    "Premium_Amt_Paid_TillDate" NUMBER
);

/*###############################################    RAW_US_OTHERS   ###############################################*/

CREATE OR REPLACE TABLE RAW_US_OTHERS (
    "Customer ID" NUMBER,
    "Customer Name" VARCHAR,
    "Customer_Segment" VARCHAR,
    "Maritial_Status" VARCHAR,
    "Gender" VARCHAR,
    "DOB" VARCHAR,
    "Effective_Start_Dt" VARCHAR,
    "Effective_End_Dt" VARCHAR,
    "Policy_Type_Id" VARCHAR,
    "Policy_Type" VARCHAR,
    "Policy_Type_Desc" VARCHAR,
    "Policy_Id" VARCHAR,
    "Policy_Name" VARCHAR,
    "Premium_Amt" NUMBER,
    "Policy_Term" VARCHAR,
    "Policy_Start_Dt" VARCHAR,
    "Policy_End_Dt" VARCHAR,
    "Next_Premium_Dt" VARCHAR,
    "Actual_Premium_Paid_Dt" VARCHAR,
    "Country" VARCHAR,
    "Region" VARCHAR,
    "State or Province" VARCHAR,
    "City" VARCHAR,
    "Postal Code" NUMBER,
    "Total_Policy_Amt" NUMBER,
    "Premium_Amt_Paid_TillDate" NUMBER
);

/*###############################################    RAW_US_MERGED   ###############################################*/

CREATE OR REPLACE TABLE RAW_US_MERGED (
    "Customer ID" NUMBER,
    "Customer Name" VARCHAR(500),
    "Customer Title" VARCHAR(500),
    "Customer First Name" VARCHAR(500),
    "Customer Middle Name" VARCHAR(500),
    "Customer Last Name" VARCHAR(500),
    "Customer_Segment" VARCHAR(500),
    "Maritial_Status" VARCHAR(500),
    "Gender" VARCHAR(500),
    "DOB" DATE,
    "Effective_Start_Dt" DATE,
    "Effective_End_Dt" DATE,
    "Policy_Type_Id" NUMBER,
    "Policy_Type" VARCHAR(500),
    "Policy_Type_Desc" VARCHAR(500),
    "Policy_Id" VARCHAR(500),
    "Policy_Name" VARCHAR(500),
    "Premium_Amt" NUMBER,
    "Policy_Term" VARCHAR(500),
    "Policy_Start_Dt" DATE,
    "Policy_End_Dt" DATE,
    "Next_Premium_Dt" DATE,
    "Actual_Premium_Paid_Dt" DATE,
    "Country" VARCHAR(500),
    "Region" VARCHAR(500),
    "State or Province" VARCHAR(500),
    "City" VARCHAR(500),
    "Postal Code" NUMBER,
    "Total_Policy_Amt" NUMBER,
    "Premium_Amt_Paid_TillDate" NUMBER
);
```

Creation of stage tables (STG_CUSTOMER_D,STG_POLICY_D,STG_ADDRESS_D,STG_TRANSACTION_F),

```sql
--Setting context and creating required tables

USE Insurance_project.stg;

/*###############################################    STG_CUSTOMER_D   ###############################################*/

CREATE OR REPLACE TABLE STG_CUSTOMER_D (
    "Customer_ID" NUMBER,
    "Customer_Name" VARCHAR(500),
    "Customer_Segment" VARCHAR(500),
    "Marital_Status" VARCHAR(500),
    "Gender" VARCHAR(500),
    "Date_of_Birth" DATE
);

/*###############################################    STG_POLICY_D   ###############################################*/

CREATE OR REPLACE TABLE STG_POLICY_D (
    "Policy_Id" NUMBER,
    "Policy_Code" NUMBER,
    "Policy_Name" VARCHAR(500),
    "Policy_Type" VARCHAR(500),
    "Policy_Type_Description" VARCHAR (500),
    "Policy_Term" VARCHAR (500),
    "Policy_Start_Date" DATE,
    "Policy_Completion_Date" DATE
);

/*###############################################    STG_ADDRESS_D   ###############################################*/

CREATE OR REPLACE TABLE STG_ADDRESS_D (
    "Country" VARCHAR(500),
    "Region" VARCHAR(500),
    "State" VARCHAR(500),
    "City" VARCHAR(500),
    "Postal_Code" NUMBER
);

/*###############################################    STG_TRANSACTION_F  ###############################################*/

CREATE OR REPLACE TABLE STG_TRANSACTION_F (
    "Customer_ID" NUMBER,
    "Customer_Name" VARCHAR(500),
    "Customer_Segment" VARCHAR(500),
    "Policy_Id" NUMBER,
    "Policy_Name" VARCHAR(500),
    "Policy_Code" NUMBER,
    "Policy_Type" VARCHAR(500),
    "Policy_Type_Description" VARCHAR (500),
    "Policy_Term" VARCHAR (500),
    "Policy_Start_Date" DATE,
    "Policy_Completion_Date" DATE,
    "Total_policy_Amount" NUMBER(38, 2),
    "Premium_Amount" NUMBER(38, 2),
    "Premium_Amount_Paid_till_Date" NUMBER(38, 2),
    "Country" VARCHAR(500),
    "Region" VARCHAR(500),
    "State" VARCHAR(500),
    "City" VARCHAR(500),
    "Postal Code" NUMBER
);
```
Creation of streams,

```sql
--Setting context

USE SCHEMA STG;

CREATE OR REPLACE STREAM CUSTOMER_STREAM_INSERT ON TABLE INSURANCE_PROJECT.STG.STG_CUSTOMER_D;

CREATE OR REPLACE STREAM POLICY_STREAM_INSERT ON TABLE INSURANCE_PROJECT.STG.STG_POLICY_D;

CREATE OR REPLACE STREAM ADDRESS_STREAM_INSERT ON TABLE INSURANCE_PROJECT.STG.STG_ADDRESS_D;

CREATE OR REPLACE STREAM CUSTOMER_STREAM_UPDATE ON TABLE INSURANCE_PROJECT.STG.STG_CUSTOMER_D;

CREATE OR REPLACE STREAM POLICY_STREAM_UPDATE ON TABLE INSURANCE_PROJECT.STG.STG_POLICY_D;

CREATE OR REPLACE STREAM ADDRESS_STREAM_UPDATE ON TABLE INSURANCE_PROJECT.STG.STG_ADDRESS_D;
```

Creation of sequence generators,

```sql
--Setting context

USE Insurance_project.INSURANCE;

--creating sequence generator

CREATE OR REPLACE SEQUENCE INSURANCE_PROJECT.INSURANCE.CUSTOMER_SEQ START = 000000 INCREMENT = 1 ORDER;

CREATE OR REPLACE SEQUENCE INSURANCE_PROJECT.INSURANCE.POLICY_SEQ START = 000000 INCREMENT = 1 ORDER;

CREATE OR REPLACE SEQUENCE INSURANCE_PROJECT.INSURANCE.ADDRESS_SEQ START = 000000 INCREMENT = 1 ORDER;
```

Creation of base tables with constraints and auto incremental columns,

```sql
--Setting context and creating required tables

USE Insurance_project.INSURANCE;

/*###############################################    CUSTOMER_D   ###############################################*/

CREATE OR REPLACE TABLE CUSTOMER_D (
    "SCD_ID" INT DEFAULT INSURANCE_PROJECT.INSURANCE.CUSTOMER_SEQ.NEXTVAL PRIMARY KEY,
    "Customer_ID" NUMBER,
    "Customer_Name" VARCHAR(500),
    "Customer_Segment" VARCHAR(500),
    "Marital_Status" VARCHAR(500),
    "Gender" VARCHAR(500),
    "Date_of_Birth" DATE,
    "CURRENT_FLG" VARCHAR(1),
    "LAST_INSERT_DT" TIMESTAMP,
    "LAST_UPDATE_DT" TIMESTAMP,
    "X_SCD_ID" INT
);

/*###############################################    POLICY_D   ###############################################*/

CREATE OR REPLACE TABLE POLICY_D (
    "SCD_ID" INT DEFAULT INSURANCE_PROJECT.INSURANCE.POLICY_SEQ.NEXTVAL PRIMARY KEY,
    "Policy_Id" NUMBER,
    "Policy_Code" NUMBER,
    "Policy_Name" VARCHAR(500),
    "Policy_Type" VARCHAR(500),
    "Policy_Type_Description" VARCHAR (500),
    "Policy_Term" VARCHAR (500),
    "Policy_Start_Date" DATE,
    "Policy_Completion_Date" DATE,
    "X_SCD_ID" INT
);

/*###############################################    ADDRESS_D   ###############################################*/

CREATE OR REPLACE TABLE ADDRESS_D (
    "SCD_ID" INT DEFAULT INSURANCE_PROJECT.INSURANCE.ADDRESS_SEQ.NEXTVAL PRIMARY KEY,
    "Country" VARCHAR(500),
    "Region" VARCHAR(500),
    "State" VARCHAR(500),
    "City" VARCHAR(500),
    "Postal_Code" NUMBER,
    "X_SCD_ID" INT
);

/*###############################################    TRANSACTION_F  ###############################################*/

CREATE OR REPLACE TABLE TRANSACTION_F (
    "CUS_SCD_ID" INT,
    "POL_SCD_ID" INT,
    "ADD_SCD_ID" INT,
    "Total_policy_Amount" NUMBER(38, 2),
    "Premium_Amount" NUMBER(38, 2),
    "Premium_Amount_Paid_till_Date" NUMBER(38, 2),
    FOREIGN KEY ("CUS_SCD_ID") REFERENCES INSURANCE_PROJECT.INSURANCE.CUSTOMER_D("SCD_ID"),
    FOREIGN KEY ("POL_SCD_ID") REFERENCES INSURANCE_PROJECT.INSURANCE.POLICY_D ("SCD_ID"),
    FOREIGN KEY ("ADD_SCD_ID") REFERENCES INSURANCE_PROJECT.INSURANCE.ADDRESS_D ("SCD_ID")
);
```
</details>

<a id = "datalake-setup"></a>
## 2. Setting up datalake for file loading:

<a id = "copy-data-setup"></a>
## 3. Copying data from external stage:

Creation of required database objects in snowflake to copy data from AWS s3 bucket (external stage),

Here, we use Account Admin role for object creation,

Setting up the context,
```
--Set ACCOUNTADMIN role
USE ROLE ACCOUNTADMIN;

--Setting context
USE DATABASE Insurance_project;
USE SCHEMA raw;

--Checking the region
SELECT CURRENT_REGION(); --AWS_US_EAST_1
```
Creating storage integration to connect snowflake with AWS s3 bucket,
```
--Storage integration creation
CREATE STORAGE INTEGRATION insurance_s3_full_access_storage_integration
TYPE = EXTERNAL_STAGE STORAGE_PROVIDER = 'S3'
ENABLED = TRUE STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::866018955955:role/insurance_sf_full_access_role' --AWS role arn
STORAGE_ALLOWED_LOCATIONS = ('*');

DESC INTEGRATION insurance_s3_full_access_storage_integration;

--Required information from storage integration to update the AWS role trust policy
    --STORAGE_AWS_IAM_USER_ARN = arn:aws:iam::285177568129:user/znb51000-s
    --STORAGE_AWS_EXTERNAL_ID = GDC17730_SFCRole=7_xQFrQez9wTzDoOvXQjafzxh6F6g=
```
Creating file format and external stage on AWS s3 bucket to access the files,
```
--File format creation
CREATE OR REPLACE FILE FORMAT csv_ff SKIP_HEADER = 1 FIELD_DELIMITER = ',' RECORD_DELIMITER = '\n' FIELD_OPTIONALLY_ENCLOSED_BY = '"'
--MULTI_LINE = TRUE
TYPE = CSV;

--External stage creation
CREATE OR REPLACE STAGE raw_data_full_access STORAGE_INTEGRATION = insurance_s3_full_access_storage_integration URL = 's3://insurance-project-raw/CSV-files/' --S3 URI from S3 bucket
FILE_FORMAT = csv_ff;
'''
Creating auto ingestion via snow pipe,
'''
--Auto ingestion of raw data using SQS event
 --Creation of snowpipe
CREATE OR REPLACE PIPE raw_data_load_east_003
AUTO_INGEST = TRUE AS COPY INTO INSURANCE_PROJECT.RAW.RAW_US_OTHERS
FROM @raw_data_full_access FILE_FORMAT = csv_ff PATTERN = '.*East.*\.csv';

--Description of snowpipe
DESC PIPE raw_data_load_east_003;
--notification_channel => arn:aws:sqs:us-east-1:285177568129:sf-snowpipe-AIDAUEZPILOAZQHSIOWQQ-drKBia09OM3SdoL626Kbyg
```
Operation and control on snow pipe,
```
--Status of snowpipe
SELECT SYSTEM$PIPE_STATUS('raw_data_load_east_003');

--snowpipe operation
ALTER PIPE raw_data_load_east_003
SET PIPE_EXECUTION_PAUSED = FALSE;

ALTER PIPE raw_data_load_east_003
SET PIPE_EXECUTION_PAUSED = TRUE;

ALTER PIPE raw_data_load_east_003 REFRESH;
```

## 📧 Contact

Author: Henrie A

LinkedIn: linkedin.com/in/henriea

Email: ahenrie08@gmail.com
