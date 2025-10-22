# ❄️ Insurance - Snowflake Project ❄️

A complete end-to-end Snowflake data pipeline demonstrating ingestion, transformation, and analytics with insurance domain related data.

## Tech Stacks used in this project

<img width="256" height="61" alt="Snowflake_Logo" src="https://github.com/user-attachments/assets/88baf960-dab7-421a-8ca3-0d3f56e7be69" />

<a href="https://www.snowflake.com/">Snowflake</a> is a cloud-native data platform built for fast, scalable data warehousing and analytics. It separates compute and storage, supports both structured and semi-structured data, and runs on AWS, Azure, and Google Cloud. Snowflake offers secure data sharing, near-zero maintenance, and high performance.


<img width="211" height="126" alt="Amazon_Web_Services_Logo" src="https://github.com/user-attachments/assets/ac24f187-e82d-4bfe-ba02-4fc03a4c4b5d" />

<a href="https://aws.amazon.com/s3/">AWS S3 (Simple Storage Service)</a> is a highly scalable, durable, and secure object storage service for storing and retrieving any amount of data from anywhere. It’s widely used for static website hosting, backups, big data analytics, and cloud-native application storage.

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

<img width="3191" height="1642" alt="Architecture-insurance-snowflake-project" src="https://github.com/user-attachments/assets/446f2a13-18cc-42e8-9bfc-3ec6a2ec8f52" />

## 🧰 Step-by-step guide of implementation
 
<details>
<summary><b> 1️⃣ <code>Project setup</code> - Create Database, Schema and other required database objects</b> <i>(click to expand)</i> </summary>

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
    "Address_ID" NUMBER,
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

CREATE OR REPLACE STREAM CUSTOMER_STREAM ON TABLE INSURANCE_PROJECT.STG.STG_CUSTOMER_D;

CREATE OR REPLACE STREAM POLICY_STREAM ON TABLE INSURANCE_PROJECT.STG.STG_POLICY_D;

CREATE OR REPLACE STREAM ADDRESS_STREAM ON TABLE INSURANCE_PROJECT.STG.STG_ADDRESS_D;
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
    "LAST_INSERT_DT" TIMESTAMP,
    "LAST_UPDATE_DT" TIMESTAMP,
    "X_SCD_ID" INT
);

/*###############################################    ADDRESS_D   ###############################################*/

CREATE OR REPLACE TABLE ADDRESS_D (
    "SCD_ID" INT DEFAULT INSURANCE_PROJECT.INSURANCE.ADDRESS_SEQ.NEXTVAL PRIMARY KEY,
    "Address_ID" NUMBER,
    "Country" VARCHAR(500),
    "Region" VARCHAR(500),
    "State" VARCHAR(500),
    "City" VARCHAR(500),
    "Postal_Code" NUMBER,
    "LAST_INSERT_DT" TIMESTAMP,
    "LAST_UPDATE_DT" TIMESTAMP,
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
    "LAST_INSERT_DT" TIMESTAMP,
    FOREIGN KEY ("CUS_SCD_ID") REFERENCES INSURANCE_PROJECT.INSURANCE.CUSTOMER_D("SCD_ID"),
    FOREIGN KEY ("POL_SCD_ID") REFERENCES INSURANCE_PROJECT.INSURANCE.POLICY_D ("SCD_ID"),
    FOREIGN KEY ("ADD_SCD_ID") REFERENCES INSURANCE_PROJECT.INSURANCE.ADDRESS_D ("SCD_ID")
);
```
</details>

<details>
<summary><b> 2️⃣ <code>Datalake setup</code> - Raw file loading in AWS S3 bucket</b> <i>(click to expand)</i> </summary><br>
1. Create an <a href ="https://aws.amazon.com/free/?all-free-tier.sort-by=item.additionalFields.SortRank&all-free-tier.sort-order=asc&awsf.Free%20Tier%20Types=*all&awsf.Free%20Tier%20Categories=categories%23compute&trk=007a06de-ab77-4a65-8196-aa4e3e97204c&sc_channel=ps&ef_id=Cj0KCQjw9czHBhCyARIsAFZlN8QNa0D1aZ8XCqqaN-seuMhSMlEWWmqif4j1cc1yfEC0LsYB55ETT_4aAnjaEALw_wcB:G:s&s_kwcid=AL!4422!3!476942607514!p!!g!!amazon%20web%20services%20cloud%20service!11542865500!116152064567&gad_campaignid=11542865500&gbraid=0AAAAADjHtp_tp0aqLn8sm-f_L_i0qLB4Z&gclid=Cj0KCQjw9czHBhCyARIsAFZlN8QNa0D1aZ8XCqqaN-seuMhSMlEWWmqif4j1cc1yfEC0LsYB55ETT_4aAnjaEALw_wcB"> AWS free tier account</a>.<br>
2. Create a <a href ="https://docs.aws.amazon.com/quickstarts/latest/s3backup/step-1-create-bucket.html"> S3 bucket</a> with unique global name as shown below,<br>

   <img width="1252" height="386" alt="image" src="https://github.com/user-attachments/assets/7f2fd90e-8ace-4afb-92de-8fabbbd9f570" /><br>
3. Create a folder for storing the raw csv files and upload the files in S3 bucket from data folder as shown below,<br>

   <img width="1896" height="432" alt="image" src="https://github.com/user-attachments/assets/a9b3c9ab-cbc0-47b8-bb2f-cc9e65cfde84" /><br>

   <img width="1897" height="541" alt="image" src="https://github.com/user-attachments/assets/1213a752-c406-4ca9-9726-9d0547275ff0" /><br>
   
</details>

<details>
<summary><b> 3️⃣ <code>Ingestion</code> - Copy data from external stage to Snowflake tables</b> <i>(click to expand)</i> </summary><br>
   Setting up the connection between Snowflake and AWS s3 bucket for data ingestion. Refer official documentation <a href="https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration">here</a> for more details<br>
 1. Creation of IAM policy for created S3 bucket in AWS with <code>s3:GetBucketLocation</code> <code>s3:GetObject</code> <code>s3:GetObjectVersion</code> <code>s3:ListBucket</code> permissions.<br>
 <img width="1897" height="732" alt="image" src="https://github.com/user-attachments/assets/ec5401c4-80bd-42d3-a4c2-5496bcaf1c40" /><br>
 2. Create an IAM role in AWS as shown below,<br>
 <img width="962" height="1587" alt="image" src="https://github.com/user-attachments/assets/9cc18edf-a64b-4ea2-b534-dbab321b326e" /><br>
 Add temporary Account ID and Require external ID with place holder and replace it with parameters from storage integration as shown above.<br>
 <img width="962" height="1587" alt="image" src="https://github.com/user-attachments/assets/5939ae2b-be96-4d3c-b255-c34a833a7cdc" /><br>
 Add the created policy in permissions as shown above.<br>
 3. Create storage integration in snowflake,<br>

 Here, we use Account Admin role for object creation,<br>

Setting up the context,<br>
```sql
--Set ACCOUNTADMIN role
USE ROLE ACCOUNTADMIN;

--Setting context
USE DATABASE Insurance_project;
USE SCHEMA raw;

--Checking the region
SELECT CURRENT_REGION(); --AWS_US_EAST_1
```
Creating storage integration to connect snowflake with AWS s3 bucket,
```sql
--Storage integration creation
CREATE STORAGE INTEGRATION insurance_s3_full_access_storage_integration
TYPE = EXTERNAL_STAGE STORAGE_PROVIDER = 'S3'
ENABLED = TRUE STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::############:role/insurance_sf_full_access_role' --replace the AWS S3 role arn
STORAGE_ALLOWED_LOCATIONS = ('*');
 ```
Replace the <code>STORAGE_AWS_ROLE_ARN</code> with ARN from created AWS role,<br>
<img width="1886" height="726" alt="image" src="https://github.com/user-attachments/assets/fe84bcc4-5dcd-4cc8-a3c6-7997a660ee32" /><br>

4. Describe storage integration<br>
```sql
DESC INTEGRATION insurance_s3_full_access_storage_integration;
```
 <img width="1810" height="407" alt="image" src="https://github.com/user-attachments/assets/204ee03a-a47b-4ba3-bd44-b6814a9e8535" /><br>
Replace the <code>STORAGE_AWS_IAM_USER_ARN</code> and <code>STORAGE_AWS_EXTERNAL_ID</code> from storage integration in AWS policy as shown below,<br>

<img width="712" height="466" alt="image" src="https://github.com/user-attachments/assets/c5286ddb-c0e5-425f-a76b-8963c4268dae" /><br>
<img width="1902" height="748" alt="image" src="https://github.com/user-attachments/assets/7b7224bb-127f-4648-be08-82bc2faa46d6" />
<br>
5. Creating file format and external stage on AWS s3 bucket to access the files,<br>
```sql
--File format creation
CREATE OR REPLACE FILE FORMAT csv_ff
SKIP_HEADER = 1
FIELD_DELIMITER = ','
RECORD_DELIMITER = '\n'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
--MULTI_LINE = TRUE
TYPE = CSV;

--External stage creation
CREATE OR REPLACE STAGE raw_data_full_access
STORAGE_INTEGRATION = insurance_s3_full_access_storage_integration
URL = 's3://##########/########/' --S3 URI from S3 bucket
FILE_FORMAT = csv_ff;
```
Replace <code>URL</code> with URI from S3 bucket,<br>
<img width="1229" height="361" alt="image" src="https://github.com/user-attachments/assets/e14fe6f7-de41-4cb2-a62a-38fb2db7c6e1" /><br>

6. Creating auto ingestion via snow pipe,<br>
```sql

 --Creation of snowpipe
CREATE OR REPLACE PIPE p_raw_data_load_central
AUTO_INGEST = TRUE AS COPY INTO INSURANCE_PROJECT.RAW.RAW_US_CENTRAL
FROM @raw_data_full_access FILE_FORMAT = csv_ff PATTERN = '.*Central.*\.csv';

CREATE OR REPLACE PIPE p_raw_data_load_east
AUTO_INGEST = TRUE AS COPY INTO INSURANCE_PROJECT.RAW.RAW_US_OTHERS
FROM @raw_data_full_access FILE_FORMAT = csv_ff PATTERN = '.*East.*\.csv';

CREATE OR REPLACE PIPE p_raw_data_load_west
AUTO_INGEST = TRUE AS COPY INTO INSURANCE_PROJECT.RAW.RAW_US_OTHERS
FROM @raw_data_full_access FILE_FORMAT = csv_ff PATTERN = '.*West.*\.csv';

CREATE OR REPLACE PIPE p_raw_data_load_south
AUTO_INGEST = TRUE AS COPY INTO INSURANCE_PROJECT.RAW.RAW_US_OTHERS
FROM @raw_data_full_access FILE_FORMAT = csv_ff PATTERN = '.*South.*\.csv';
```
7. Enabling auto ingestion via SQS event AWS<br>
```sql
--Description of snowpipe
DESC PIPE p_raw_data_load_central;
```
Get <code>notification_channel</code> and add it in AWS S3 bucket - SQS Queue (ARN) to create event notification with SQS queue and below settings as shown below,<br>

<img width="1500" height="658" alt="image" src="https://github.com/user-attachments/assets/9f4f1976-fb74-4ad8-a78e-38d7a1f7ebe0" /><br>

<img width="1877" height="742" alt="image" src="https://github.com/user-attachments/assets/fb193f87-a4f9-4dcf-bf17-38b866b87884" /><br>

<img width="1887" height="272" alt="image" src="https://github.com/user-attachments/assets/5c8aa48f-e6a0-40d5-b806-8e91fc1cf29f" /><br>

Operation and control on snow pipe,<br>
```sql
--Status of snowpipe
SELECT SYSTEM$PIPE_STATUS('p_raw_data_load_central');

--snowpipe operation
ALTER PIPE p_raw_data_load_central
SET PIPE_EXECUTION_PAUSED = FALSE;

ALTER PIPE p_raw_data_load_central
SET PIPE_EXECUTION_PAUSED = TRUE;

ALTER PIPE p_raw_data_load_central REFRESH;
```
</details>
<details>
<summary><b> 4️⃣ <code>Cleaning</code> - Cleaning and restructuring the raw data</b> <i>(click to expand)</i> </summary>
  With auto - ingestion data are refreshed in <code>INSURANCE_PROJECT.RAW.RAW_US_CENTRAL</code> and <code>INSURANCE_PROJECT.RAW.RAW_US_OTHERS</code> (raw data from East,West and South).<br>
 Restructing the data and loading into <code>INSURANCE_PROJECT.RAW.RAW_US_MERGED</code> using below queries,
 
```sql
 --Set ACCOUNTADMIN role
USE role accountadmin;
--Setting context
USE database insurance_project;
USE SCHEMA raw;
--Creating streams for raw tables
CREATE OR REPLACE STREAM RAW_US_CENTRAL_STREAM ON TABLE INSURANCE_PROJECT.RAW.RAW_US_CENTRAL;

CREATE OR REPLACE STREAM RAW_US_OTHERS_STREAM ON TABLE INSURANCE_PROJECT.RAW.RAW_US_OTHERS;
```
Creation of stored procedure,
```sql
--Creating stored procedure to execute truncate and insert
CREATE OR REPLACE PROCEDURE SP_RAW_US_MERGED()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
truncate_query VARCHAR;
insert_query VARCHAR;
BEGIN
--Truncate the merged raw table
truncate_query := 'TRUNCATE insurance_project.raw.raw_us_merged;';
--Load the data into merged table (without duplicates,type casted and restructured)
insert_query:= 'INSERT INTO insurance_project.raw.raw_us_merged
            (
                   select "Customer ID",
                                 concat( "Customer Title", '' '', "Customer First Name", '' '', "Customer Last Name" ) AS "Customer Name",
                          "Customer Title",
                          "Customer First Name",
                          "Customer Middle Name",
                          "Customer Last Name",
                          "Customer_Segment",
                          "Maritial_Status",
                          "Gender",
                          to_date(replace("DOB", ''/'', ''-''), ''MM-DD-YYYY'')                  AS "DOB",
                          to_date( replace("Effective_Start_Dt", ''/'', ''-''), ''MM-DD-YYYY'' ) AS "Effective_Start_Dt",
                          to_date( replace("Effective_End_Dt", ''/'', ''-''), ''MM-DD-YYYY'' )   AS "Effective_End_Dt",
                          "Policy_Type_Id",
                          "Policy_Type",
                          "Policy_Type_Desc",
                          "Policy_Id",
                          "Policy_Name",
                          "Premium_Amt",
                          "Policy_Term",
                          to_date(replace("Policy_Start_Dt", ''/'', ''-''), ''MM-DD-YYYY'')          AS "Policy_Start_Dt",
                          to_date(replace("Policy_End_Dt", ''/'', ''-''), ''MM-DD-YYYY'')            AS "Policy_End_Dt",
                          to_date(replace("Next_Premium_Dt", ''/'', ''-''), ''MM-DD-YYYY'')          AS "Next_Premium_Dt",
                          to_date( replace("Actual_Premium_Paid_Dt", ''/'', ''-''), ''MM-DD-YYYY'' ) AS "Actual_Premium_Paid_Dt",
                          ''USA''                                                                AS "Country",
                          "Region",
                          "State or Province",
                          "City",
                          "Postal Code",
                          "Total_Policy_Amt",
                          "Premium_Amt_Paid_TillDate"
                   FROM   insurance_project.raw.RAW_US_CENTRAL_STREAM
            )
     UNION
           (
                  SELECT * exclude("Customer_split")
                  FROM   (
                                SELECT "Customer ID",
                                       "Customer Name",
                                       (split("Customer Name", '' '')) AS "Customer_split",
                                       to_char("Customer_split" [0]) AS "Customer Title",
                                       to_char("Customer_split" [1]) AS "Customer First Name",
                                       --Logic to split middle and last name as few names dowsn''t have middle name
                                       CASE
                                              WHEN array_size("Customer_split") = 4 THEN to_char("Customer_split" [2])
                                              ELSE NULL
                                       END AS "Customer Middle Name",
                                       CASE
                                              WHEN array_size("Customer_split") = 3 THEN to_char("Customer_split" [2])
                                              WHEN array_size("Customer_split") = 4 THEN to_char("Customer_split" [3])
                                              ELSE NULL
                                       END AS "Customer Last Name",
                                       "Customer_Segment",
                                       "Maritial_Status",
                                       "Gender",
                                       to_date(replace("DOB", ''/'', ''-''), ''MM-DD-YYYY'')                  AS "DOB",
                                       to_date( replace("Effective_Start_Dt", ''/'', ''-''), ''MM-DD-YYYY'' ) AS "Effective_Start_Dt",
                                       to_date( replace("Effective_End_Dt", ''/'', ''-''), ''MM-DD-YYYY'' )   AS "Effective_End_Dt",
                                       "Policy_Type_Id",
                                       "Policy_Type",
                                       "Policy_Type_Desc",
                                       "Policy_Id",
                                       "Policy_Name",
                                       "Premium_Amt",
                                       "Policy_Term",
                                       to_date(replace("Policy_Start_Dt", ''/'', ''-''), ''MM-DD-YYYY'')          AS "Policy_Start_Dt",
                                       to_date(replace("Policy_End_Dt", ''/'', ''-''), ''MM-DD-YYYY'')            AS "Policy_End_Dt",
                                       to_date(replace("Next_Premium_Dt", ''/'', ''-''), ''MM-DD-YYYY'')          AS "Next_Premium_Dt",
                                       to_date( replace("Actual_Premium_Paid_Dt", ''/'', ''-''), ''MM-DD-YYYY'' ) AS "Actual_Premium_Paid_Dt",
                                       ''USA''                                                                AS "Country",
                                       "Region",
                                       "State or Province",
                                       "City",
                                       "Postal Code",
                                       "Total_Policy_Amt",
                                       "Premium_Amt_Paid_TillDate"
                                FROM   insurance_project.raw.RAW_US_OTHERS_STREAM ) );';
EXECUTE IMMEDIATE truncate_query;
EXECUTE IMMEDIATE insert_query;
return 'inserted successfully';
END;
$$;

CALL SP_RAW_US_MERGED();
```
Creation of task to auto refresh merged table,
```sql
--Creating task to auto refresh merged table
CREATE OR REPLACE TASK INSURANCE_PROJECT.RAW.T_RAW_US_MERGED
WAREHOUSE = COMPUTE_WH
WHEN SYSTEM$STREAM_HAS_DATA('insurance_project.raw.RAW_US_CENTRAL_STREAM')
OR
SYSTEM$STREAM_HAS_DATA('insurance_project.raw.RAW_US_OTHERS_STREAM')
AS
CALL SP_RAW_US_MERGED();

--To resume the task
ALTER TASK INSURANCE_PROJECT.RAW.T_RAW_US_MERGED RESUME;
```
</details>

<details>
<summary><b> 5️⃣ <code>Stage load</code> - Cast and load data to stage layer</b> <i>(click to expand)</i> </summary>
Setting context,
 
 ```sql
 --Set ACCOUNTADMIN role
USE ROLE ACCOUNTADMIN;
--Setting context
USE DATABASE Insurance_project;
USE SCHEMA stg;
```
Loading stage tables with appropriate data types,<br>
Customer dimension stage table loading,	

```sql
--Creating stored procedure to execute truncate and insert
CREATE OR REPLACE PROCEDURE SP_STG_CUSTOMER_D()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
truncate_query VARCHAR;
insert_query VARCHAR;
BEGIN
--Truncate the merged raw table
truncate_query:='TRUNCATE INSURANCE_PROJECT.STG.STG_CUSTOMER_D;';
insert_query:='INSERT INTO
    INSURANCE_PROJECT.STG.STG_CUSTOMER_D
SELECT
    DISTINCT "Customer ID" AS "Customer_ID",
    INITCAP("Customer Name") AS "Customer_Name",
    "Customer_Segment" AS "Customer_Segment",
    "Maritial_Status" AS "Marital_Status",
    "Gender" AS "Gender",
    "DOB" AS "Date_of_Birth"
FROM
    INSURANCE_PROJECT.RAW.RAW_US_MERGED;';
EXECUTE IMMEDIATE truncate_query;
EXECUTE IMMEDIATE insert_query;
return 'inserted successfully';
END;
$$;

--Creating task to auto refresh stage table
CREATE OR REPLACE TASK INSURANCE_PROJECT.STG.T_STG_CUSTOMER_D
WAREHOUSE = COMPUTE_WH
AFTER INSURANCE_PROJECT.RAW.T_RAW_US_MERGED
AS
CALL SP_STG_CUSTOMER_D();
```

Policy dimension stage table loading,

```sql
--Creating stored procedure to execute truncate and insert
CREATE OR REPLACE PROCEDURE SP_STG_POLICY_D()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
truncate_query VARCHAR;
insert_query VARCHAR;
BEGIN
--Truncate the merged raw table
truncate_query:='TRUNCATE INSURANCE_PROJECT.STG.STG_POLICY_D;';
insert_query:='INSERT INTO
    INSURANCE_PROJECT.STG.STG_POLICY_D
SELECT
    DISTINCT "Policy_Type_Id" AS "Policy_Id",
    (SUBSTR("Policy_Id", POSITION('_', "Policy_Id") + 1)) AS "Policy_Code",
    "Policy_Name" AS "Policy_Name",
    "Policy_Type" AS "Policy_Type",
    "Policy_Type_Desc" AS "Policy_Type_Description",
    "Policy_Term" AS "Policy_Term",
    "Policy_Start_Dt" AS "Policy_Start_Date",
    "Policy_End_Dt" AS "Policy_Completion_Date"
FROM
    INSURANCE_PROJECT.RAW.RAW_US_MERGED;';
EXECUTE IMMEDIATE truncate_query;
EXECUTE IMMEDIATE insert_query;
return 'inserted successfully';
END;
$$;

--Creating task to auto refresh stage table
CREATE OR REPLACE TASK INSURANCE_PROJECT.STG.T_STG_POLICY_D
WAREHOUSE = COMPUTE_WH
AFTER INSURANCE_PROJECT.RAW.T_RAW_US_MERGED
AS
CALL SP_STG_POLICY_D();
```
Address dimension stage table loading,

```sql
--Creating stored procedure to execute truncate and insert
CREATE OR REPLACE PROCEDURE SP_STG_ADDRESS_D()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
truncate_query VARCHAR;
insert_query VARCHAR;
BEGIN
--Truncate the merged raw table
truncate_query:='TRUNCATE INSURANCE_PROJECT.STG.STG_ADDRESS_D;';
insert_query:='INSERT INTO
    INSURANCE_PROJECT.STG.STG_ADDRESS_D
SELECT
    DISTINCT
    "Customer_ID" AS "Address_ID",
    "Country" AS "Country",
    "Region" AS "Region",
    "State or Province" AS "State",
    "City" AS "City",
    "Postal Code" AS "Postal_Code"
FROM
    INSURANCE_PROJECT.RAW.RAW_US_MERGED;';
EXECUTE IMMEDIATE truncate_query;
EXECUTE IMMEDIATE insert_query;
return 'inserted successfully';
END;
$$;

--Creating task to auto refresh stage table
CREATE OR REPLACE TASK INSURANCE_PROJECT.STG.T_STG_ADDRESS_D
WAREHOUSE = COMPUTE_WH
AFTER INSURANCE_PROJECT.RAW.T_RAW_US_MERGED
AS
CALL SP_STG_ADDRESS_D();
```

Transaction fact stage table loading,

```sql
--Creating stored procedure to execute truncate and insert
CREATE OR REPLACE PROCEDURE SP_STG_TRANSACTION_F()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
truncate_query VARCHAR;
insert_query VARCHAR;
BEGIN
--Truncate the merged raw table
truncate_query:='TRUNCATE INSURANCE_PROJECT.STG.STG_TRANSACTION_F;';
insert_query:='INSERT INTO
    INSURANCE_PROJECT.STG.STG_TRANSACTION_F
SELECT
    DISTINCT "Customer ID" AS "Customer_ID",
    INITCAP("Customer Name") AS "Customer_Name",
    "Total_Policy_Amt" AS "Total_policy_Amount",
    "Premium_Amt" AS "Premium_Amount",
    "Premium_Amt_Paid_TillDate" AS "Premium_Amount_Paid_till_Date",
    "Country" AS "Country",
    "Region" AS "Region",
    "State or Province" AS "State",
    "City" AS "City",
    "Postal Code" AS "Postal_Code"
FROM
    INSURANCE_PROJECT.RAW.RAW_US_MERGED;';
EXECUTE IMMEDIATE truncate_query;
EXECUTE IMMEDIATE insert_query;
return 'inserted successfully';
END;
$$;

--Creating task to auto refresh stage table
CREATE OR REPLACE TASK INSURANCE_PROJECT.STG.T_STG_TRANSACTION_F
WAREHOUSE = COMPUTE_WH
AFTER INSURANCE_PROJECT.RAW.T_RAW_US_MERGED
AS
CALL SP_STG_TRANSACTION_F();
```
</details>

<details>
<summary><b> 6️⃣ <code>Transformation</code> - Load data to base tables and implement SCD type 2 in dimension tables</b> <i>(click to expand)</i> </summary>
SCD 2 implementation to handle new inserts and updates via stored procedure<br>
Customer dimension table loading,
 
```sql

 --Creation of stored procedure
CREATE OR REPLACE PROCEDURE SP_CUSTOMER_D_SCD2()
    RETURNS VARCHAR
    LANGUAGE SQL
    AS 
    DECLARE 
    V_MERGE_QUERY VARCHAR;
    V_INSERT_QUERY VARCHAR;
    V_XSCD_QUERY VARCHAR;
    RES RESULTSET;
BEGIN 
 V_MERGE_QUERY := 'MERGE INTO INSURANCE_PROJECT.INSURANCE.CUSTOMER_D T
    USING INSURANCE_PROJECT.STG.CUSTOMER_STREAM_INSERT S
    ON T."Customer_ID" = S."Customer_ID" 
    AND T."CURRENT_FLG" = ''Y''
    
    --Updates the existing record changes (CURRENT_FLG to ''N'')
    
    WHEN MATCHED AND (
    S."Customer_Name" <> T."Customer_Name"
    OR S."Customer_Segment" <> T."Customer_Segment"
    OR S."Marital_Status" <> T."Marital_Status"
    OR S."Gender" <> T."Gender"
    OR S."Date_of_Birth" <> T."Date_of_Birth"
    AND S.METADATA$ACTION = ''INSERT'' 
    AND  S.METADATA$ISUPDATE = ''FALSE'' ) -- If stage table is persistent incremental table, then S.METADATA$ACTION = ''DELETE'' AND S.METADATA$ISUPDATE = ''TRUE''
    THEN
    UPDATE SET T."LAST_UPDATE_DT" = CURRENT_TIMESTAMP(), T."CURRENT_FLG" = ''N''
    
    --Inserts new records 
    
    WHEN NOT MATCHED THEN
    INSERT ("Customer_ID",
    "Customer_Name",
    "Customer_Segment",
    "Marital_Status", 
    "Gender", 
    "Date_of_Birth",
    "CURRENT_FLG",
    "LAST_INSERT_DT",
    "LAST_UPDATE_DT")
    VALUES (S."Customer_ID",
    S."Customer_Name", 
    S."Customer_Segment", 
    S."Marital_Status", 
    S."Gender", 
    S."Date_of_Birth",
    ''Y'',
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP());';
V_INSERT_QUERY := '-- Insert new versions for updated records (with CURRENT_FLG to ''Y'')
    
    INSERT INTO INSURANCE_PROJECT.INSURANCE.CUSTOMER_D (
    "Customer_ID",
    "Customer_Name",
    "Customer_Segment",
    "Marital_Status",
    "Gender",
    "Date_of_Birth",
    "CURRENT_FLG",
    "LAST_INSERT_DT",
    "LAST_UPDATE_DT")
    SELECT
    S."Customer_ID",
    S."Customer_Name", 
    S."Customer_Segment", 
    S."Marital_Status", 
    S."Gender", 
    S."Date_of_Birth",
    ''Y'',
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
    FROM INSURANCE_PROJECT.STG.CUSTOMER_STREAM_UPDATE S
    JOIN INSURANCE_PROJECT.INSURANCE.CUSTOMER_D T 
    ON T."Customer_ID" = S."Customer_ID" 
    AND T."CURRENT_FLG" = ''N'' 
    WHERE S.METADATA$ACTION = ''INSERT'' 
    AND S.METADATA$ISUPDATE = ''FALSE''; -- If stage table is persistent incremental table, then S.METADATA$ACTION = ''INSERT'' AND S.METADATA$ISUPDATE = ''TRUE'';';

V_XSCD_QUERY := 'UPDATE
    INSURANCE_PROJECT.INSURANCE.CUSTOMER_D AS D
SET
    D."X_SCD_ID" = X."X_SCD_ID",
    D."LAST_UPDATE_DT" = CURRENT_TIMESTAMP()
FROM
    ( SELECT
            "Customer_ID",
            MIN(SCD_ID) AS "X_SCD_ID"
        FROM INSURANCE_PROJECT.INSURANCE.CUSTOMER_D D, CUSTOMER_STREAM S
		WHERE D."Customer_ID" = S."Customer_ID"
        GROUP BY
            "Customer_ID"
        ORDER BY
            "Customer_ID") AS X
WHERE
    D."Customer_ID" = X."Customer_ID";';
RES := (EXECUTE IMMEDIATE V_MERGE_QUERY);
RES := (EXECUTE IMMEDIATE V_INSERT_QUERY);
RES := (EXECUTE IMMEDIATE V_XSCD_QUERY);
RETURN 'SCD 2 implemented';
END;
--Creating task to auto refresh base table
CREATE OR REPLACE TASK INSURANCE_PROJECT.INSURANCE.T_CUSTOMER_D
WAREHOUSE = COMPUTE_WH
AFTER INSURANCE_PROJECT.STG.T_STG_CUSTOMER_D
AS
CALL SP_CUSTOMER_D_SCD2();
 ```
Address dimension table loading,
```sql

 --Creation of stored procedure
CREATE OR REPLACE PROCEDURE SP_ADDRESS_D_SCD2()
    RETURNS VARCHAR
    LANGUAGE SQL
    AS 
    DECLARE 
    V_MERGE_QUERY VARCHAR;
    V_INSERT_QUERY VARCHAR;
    V_XSCD_QUERY VARCHAR;
    RES RESULTSET;
BEGIN 
 V_MERGE_QUERY := 'MERGE INTO INSURANCE_PROJECT.INSURANCE.ADDRESS_D T
    USING INSURANCE_PROJECT.STG.ADDRESS_STREAM_INSERT S
    ON T."Address_ID" = S."Address_ID" 
    AND T."CURRENT_FLG" = ''Y''
    
    --Updates the existing record changes (CURRENT_FLG to ''N'')
    
    WHEN MATCHED AND (
    S."Country" <> T."Country"
    OR S."Region" <> T."Region"
    OR S."Marital_Status" <> T."Marital_Status"
    OR S."State" <> T."State"
    OR S."City" <> T."City"
	OR S."Postal_Code"<>T."Postal_Code"
    AND S.METADATA$ACTION = ''INSERT'' 
    AND  S.METADATA$ISUPDATE = ''FALSE'' ) -- If stage table is persistent incremental table, then S.METADATA$ACTION = ''DELETE'' AND S.METADATA$ISUPDATE = ''TRUE''
    THEN
    UPDATE SET T."LAST_UPDATE_DT" = CURRENT_TIMESTAMP(), T."CURRENT_FLG" = ''N''
    
    --Inserts new records 
    
    WHEN NOT MATCHED THEN
    INSERT (
	"Address_ID",
    "Country",
    "Region",
    "State",
    "City",
    "Postal_Code",
    "CURRENT_FLG",
    "LAST_INSERT_DT",
    "LAST_UPDATE_DT")
    VALUES (
	S."Address_ID",
    S."Country",
    S."Region",
    S."State",
    S."City",
    S."Postal_Code",
    ''Y'',
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP());';
V_INSERT_QUERY := '-- Insert new versions for updated records (with CURRENT_FLG to ''Y'')
    
    INSERT INTO INSURANCE_PROJECT.INSURANCE.ADDRESS_D (
    "Address_ID",
    "Country",
    "Region",
    "State",
    "City",
    "Postal_Code",
    "CURRENT_FLG",
    "LAST_INSERT_DT",
    "LAST_UPDATE_DT")
    SELECT
	S."Address_ID",
    S."Country",
    S."Region",
    S."State",
    S."City",
    S."Postal_Code",
    ''Y'',
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
    FROM INSURANCE_PROJECT.STG.ADDRESS_STREAM_UPDATE S
    JOIN INSURANCE_PROJECT.INSURANCE.CUSTOMER_D T 
    ON T."Address_ID" = S."Address_ID" 
    AND T."CURRENT_FLG" = ''N'' 
    WHERE S.METADATA$ACTION = ''INSERT'' 
    AND S.METADATA$ISUPDATE = ''FALSE''; -- If stage table is persistent incremental table, then S.METADATA$ACTION = ''INSERT'' AND S.METADATA$ISUPDATE = ''TRUE'';';

V_XSCD_QUERY := 'UPDATE
    INSURANCE_PROJECT.INSURANCE.ADDRESS_D AS D
SET
    D."X_SCD_ID" = X."X_SCD_ID",
    D."LAST_UPDATE_DT" = CURRENT_TIMESTAMP()
FROM
    ( SELECT
            "ADDRESS_ID",
            MIN(SCD_ID) AS "X_SCD_ID"
        FROM INSURANCE_PROJECT.INSURANCE.ADDRESS_D D, ADDRESS_STREAM S
		WHERE D."Address_ID" = S."Address_ID"
        GROUP BY
            "Address_ID"
        ORDER BY
            "Address_ID") AS X
WHERE
    D."Address_ID" = X."Address_ID";';
RES := (EXECUTE IMMEDIATE V_MERGE_QUERY);
RES := (EXECUTE IMMEDIATE V_INSERT_QUERY);
RES := (EXECUTE IMMEDIATE V_XSCD_QUERY);
RETURN 'SCD 2 implemented';
END;
--Creating task to auto refresh base table
CREATE OR REPLACE TASK INSURANCE_PROJECT.INSURANCE.T_ADDRESS_D
WAREHOUSE = COMPUTE_WH
AFTER INSURANCE_PROJECT.STG.T_STG_ADDRESS_D
AS
CALL SP_ADDRESS_D_SCD2();

```
</details>

## 📧 Contact

Author: Henrie A

LinkedIn: linkedin.com/in/henriea

Email: ahenrie08@gmail.com
