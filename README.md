# Insurance Snowflake Project
### To create an end to end data flow to achieve the following process:
1. [Project setup](##project-setup) - Creation of required database objects in Snowflake
2. [Setting up datalake for file loading](##datalake-setup) - To load source files into AWS s3 bucket
3. [Copying data from external storage](##copy-data-setup) - To copy data from s3 bucket to Snowflake tables
4. To clean the data with proper structure and data type
5. To load the data to the stage layer and perform transformations
6. To load the data into base tables and implement SCD type 2 in dimension tables

<a id = "project-setup"></a>
## 1. Project setup:
Creation of required database objects,

Here, we use Account Admin role for object creation,

```
USE ROLE ACCOUNTADMIN;
```
Database creation,
```
CREATE DATABASE Insurance_project;
```
Schema creation,
```
--Setting context to appropriate database

USE Insurance_project;

CREATE SCHEMA config;
CREATE SCHEMA Raw;
CREATE SCHEMA Stg;
CREATE SCHEMA Insurance;
CREATE SCHEMA Reporting;
```
Creation of raw data tables (RAW_US_CENTRAL,RAW_US_OTHERS,RAW_US_MERGED),

```
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

```
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

```
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

```
--Setting context

USE Insurance_project.INSURANCE;

--creating sequence generator

CREATE OR REPLACE SEQUENCE INSURANCE_PROJECT.INSURANCE.CUSTOMER_SEQ START = 000000 INCREMENT = 1 ORDER;

CREATE OR REPLACE SEQUENCE INSURANCE_PROJECT.INSURANCE.POLICY_SEQ START = 000000 INCREMENT = 1 ORDER;

CREATE OR REPLACE SEQUENCE INSURANCE_PROJECT.INSURANCE.ADDRESS_SEQ START = 000000 INCREMENT = 1 ORDER;
```

Creation of base tables with constraints and auto incremental columns,

```
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
<a id = "datalake-setup"></a>
## 2. Setting up datalake for file loading:

<a id = "copy-data-setup"></a>
## 3. Copying data from external storage:

Creation of required database objects in snowflake to copy data from AWS s3 bucket (external stage),

Here, we use Account Admin role for object creation,

```
--Set ACCOUNTADMIN role
USE ROLE ACCOUNTADMIN;

--Setting context
USE DATABASE Insurance_project;
USE SCHEMA raw;

--Checking the region
SELECT CURRENT_REGION(); --AWS_US_EAST_1

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
