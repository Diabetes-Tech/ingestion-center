---
workflowPaths:
  inProcess:
    home: >-
      /home/vinod/projects/Diabetes Research Hub
      DRH_DOLT/INGESTION_GITHUB/personal_vinod.wst/ingestion-center/support/assurance/ingestion-center-elt/drh/results-test-e2e
  egress:
    home: >-
      /home/vinod/projects/Diabetes Research Hub
      DRH_DOLT/INGESTION_GITHUB/personal_vinod.wst/ingestion-center/support/assurance/ingestion-center-elt/drh/results-test-e2e
walkRootPaths:
  - support/assurance/ingestion-center-elt/drh/synthetic-content
referenceDataHome: >-
  /home/vinod/projects/Diabetes Research Hub
  DRH_DOLT/INGESTION_GITHUB/personal_vinod.wst/ingestion-center/src/ingestion-center-elt/reference-data
sources:
  - uri: >-
      support/assurance/ingestion-center-elt/drh/synthetic-content/LIBRE_20240307.csv
    nature: CSV
    tableName: libre_20240307
    ingestionIssues: 0
  - uri: >-
      support/assurance/ingestion-center-elt/drh/synthetic-content/synthetic-fail-excel-01.xlsx
    nature: ERROR
    tableName: ERROR
    ingestionIssues: 1
  - uri: >-
      support/assurance/ingestion-center-elt/drh/synthetic-content/synthetic-fail.csv
    nature: ERROR
    tableName: ERROR
    ingestionIssues: 1
  - uri: >-
      support/assurance/ingestion-center-elt/drh/synthetic-content/GLUCOSE_20240307.csv
    nature: CSV
    tableName: glucose_20240307
    ingestionIssues: 0
  - uri: >-
      support/assurance/ingestion-center-elt/drh/synthetic-content/Libre_data_240402.xlsx
    nature: Excel Workbook Sheet
    tableName: libre_data_240402_m_y_b004_libre_data
    ingestionIssues: 0
  - uri: >-
      support/assurance/ingestion-center-elt/drh/synthetic-content/Libre_data_240402.xlsx
    nature: Excel Workbook Sheet
    tableName: libre_data_240402_m_y_b001_libre_data
    ingestionIssues: 0
  - uri: >-
      support/assurance/ingestion-center-elt/drh/synthetic-content/Libre_data_240402.xlsx
    nature: Excel Workbook Sheet
    tableName: libre_data_240402_m_y_b006_libre_data
    ingestionIssues: 0
  - uri: >-
      support/assurance/ingestion-center-elt/drh/synthetic-content/vitalsigns_20240307.csv
    nature: CSV
    tableName: vitalsigns_20240307
    ingestionIssues: 0
  - uri: >-
      /home/vinod/projects/Diabetes Research Hub
      DRH_DOLT/INGESTION_GITHUB/personal_vinod.wst/ingestion-center/src/ingestion-center-elt/reference-data/screening-status-code-reference.csv
    nature: CSV
    tableName: screening_status_code_reference
    ingestionIssues: 0
  - uri: >-
      /home/vinod/projects/Diabetes Research Hub
      DRH_DOLT/INGESTION_GITHUB/personal_vinod.wst/ingestion-center/src/ingestion-center-elt/reference-data/business-rules.csv
    nature: CSV
    tableName: business_rules
    ingestionIssues: 0
---
# Orchestration Diagnostics
## Contents

- [init](#init)
- [ingest](#ingest)
  - [`ingest` STDOUT (status: `1`)](#ingest-stdout-status-1-)
  - [`ingest` STDERR](#ingest-stderr)
- [ensureContent](#ensurecontent)
- [emitResources](#emitresources)
- [jsonResult_4](#jsonresult-4)
  - [`jsonResult_4` STDOUT (status: `0`)](#jsonresult-4-stdout-status-0-)
- [emitDiagnostics](#emitdiagnostics)


## init

```sql
-- preambleSQL
SET autoinstall_known_extensions=true;
SET autoload_known_extensions=true;
-- end preambleSQL
-- no before-init SQL found
CREATE TABLE IF NOT EXISTS "device" (
    "device_id" TEXT PRIMARY KEY NOT NULL,
    "name" TEXT NOT NULL,
    "state" TEXT NOT NULL,
    "boundary" TEXT NOT NULL,
    "segmentation" TEXT,
    "state_sysinfo" TEXT,
    "elaboration" TEXT,
    UNIQUE("name", "state", "boundary")
);
CREATE TABLE IF NOT EXISTS "orch_session" (
    "orch_session_id" TEXT PRIMARY KEY NOT NULL,
    "device_id" TEXT NOT NULL,
    "version" TEXT NOT NULL,
    "orch_started_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    "orch_finished_at" TIMESTAMP,
    "elaboration" TEXT,
    "args_json" TEXT,
    "diagnostics_json" TEXT,
    "diagnostics_md" TEXT,
    FOREIGN KEY("device_id") REFERENCES "device"("device_id")
);
CREATE TABLE IF NOT EXISTS "orch_session_entry" (
    "orch_session_entry_id" TEXT PRIMARY KEY NOT NULL,
    "session_id" TEXT NOT NULL,
    "ingest_src" TEXT NOT NULL,
    "ingest_table_name" TEXT,
    "elaboration" TEXT,
    FOREIGN KEY("session_id") REFERENCES "orch_session"("orch_session_id")
);
CREATE TABLE IF NOT EXISTS "orch_session_state" (
    "orch_session_state_id" TEXT PRIMARY KEY NOT NULL,
    "session_id" TEXT NOT NULL,
    "session_entry_id" TEXT,
    "from_state" TEXT NOT NULL,
    "to_state" TEXT NOT NULL,
    "transition_result" TEXT,
    "transition_reason" TEXT,
    "transitioned_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    "elaboration" TEXT,
    FOREIGN KEY("session_id") REFERENCES "orch_session"("orch_session_id"),
    FOREIGN KEY("session_entry_id") REFERENCES "orch_session_entry"("orch_session_entry_id"),
    UNIQUE("orch_session_state_id", "from_state", "to_state")
);
CREATE TABLE IF NOT EXISTS "orch_session_exec" (
    "orch_session_exec_id" TEXT PRIMARY KEY NOT NULL,
    "exec_nature" TEXT NOT NULL,
    "session_id" TEXT NOT NULL,
    "session_entry_id" TEXT,
    "parent_exec_id" TEXT,
    "namespace" TEXT,
    "exec_identity" TEXT,
    "exec_code" TEXT NOT NULL,
    "exec_status" INTEGER NOT NULL,
    "input_text" TEXT,
    "exec_error_text" TEXT,
    "output_text" TEXT,
    "output_nature" TEXT,
    "narrative_md" TEXT,
    "elaboration" TEXT,
    FOREIGN KEY("session_id") REFERENCES "orch_session"("orch_session_id"),
    FOREIGN KEY("session_entry_id") REFERENCES "orch_session_entry"("orch_session_entry_id"),
    FOREIGN KEY("parent_exec_id") REFERENCES "orch_session_exec"("orch_session_exec_id")
);
CREATE TABLE IF NOT EXISTS "orch_session_issue" (
    "orch_session_issue_id" TEXT PRIMARY KEY NOT NULL,
    "session_id" TEXT NOT NULL,
    "session_entry_id" TEXT,
    "issue_type" TEXT NOT NULL,
    "issue_message" TEXT NOT NULL,
    "issue_row" INTEGER,
    "issue_column" TEXT,
    "invalid_value" TEXT,
    "remediation" TEXT,
    "elaboration" TEXT,
    FOREIGN KEY("session_id") REFERENCES "orch_session"("orch_session_id"),
    FOREIGN KEY("session_entry_id") REFERENCES "orch_session_entry"("orch_session_entry_id")
);
CREATE INDEX IF NOT EXISTS "idx_device__name__state" ON "device"("name", "state");

DROP VIEW IF EXISTS "orch_session_diagnostic_text";
CREATE VIEW IF NOT EXISTS "orch_session_diagnostic_text" AS
    SELECT
        -- Including all other columns from 'orch_session'
        ises.* EXCLUDE (orch_started_at, orch_finished_at),
        -- TODO: Casting known timestamp columns to text so emit to Excel works with GDAL (spatial)
           -- strftime(timestamptz orch_started_at, '%Y-%m-%d %H:%M:%S') AS orch_started_at,
           -- strftime(timestamptz orch_finished_at, '%Y-%m-%d %H:%M:%S') AS orch_finished_at,
    
        -- Including all columns from 'orch_session_entry'
        isee.* EXCLUDE (session_id),
    
        -- Including all other columns from 'orch_session_issue'
        isi.* EXCLUDE (session_id, session_entry_id)
    FROM orch_session AS ises
    JOIN orch_session_entry AS isee ON ises.orch_session_id = isee.session_id
    LEFT JOIN orch_session_issue AS isi ON isee.orch_session_entry_id = isi.session_entry_id;

-- register the current device and session and use the identifiers for all logging
INSERT INTO "device" ("device_id", "name", "state", "boundary", "segmentation", "state_sysinfo", "elaboration") VALUES ('7bab389e-54af-5a13-a39f-079abdc73a48', 'vinod-OptiPlex', 'SINGLETON', 'UNKNOWN', NULL, '{"os-arch":"x64","os-platform":"linux"}', NULL) ON CONFLICT DO NOTHING;
INSERT INTO "orch_session" ("orch_session_id", "device_id", "version", "orch_started_at", "orch_finished_at", "elaboration", "args_json", "diagnostics_json", "diagnostics_md") VALUES ('05269d28-15ae-5bd6-bd88-f949ccfa52d7', '7bab389e-54af-5a13-a39f-079abdc73a48', '0.12.0', ('2024-04-16T08:48:44.333Z'), NULL, NULL, NULL, NULL, 'Session 05269d28-15ae-5bd6-bd88-f949ccfa52d7 markdown diagnostics not provided (not completed?)');

-- Load Reference data from csvs

-- no after-init SQL found
```
No STDOUT emitted by `init` (status: `0`).

No STDERR emitted by `init`.

    

## ingest

```sql
-- preambleSQL
SET autoinstall_known_extensions=true;
SET autoload_known_extensions=true;
-- end preambleSQL
-- ingest support/assurance/ingestion-center-elt/drh/synthetic-content/LIBRE_20240307.csv (libre_20240307)
-- required by IngestEngine, setup the ingestion entry for logging
INSERT INTO "orch_session_entry" ("orch_session_entry_id", "session_id", "ingest_src", "ingest_table_name", "elaboration") VALUES ('8b7c669c-1795-5f6b-8f3a-3e502b74c628', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'support/assurance/ingestion-center-elt/drh/synthetic-content/LIBRE_20240307.csv', 'libre_20240307', NULL);

-- state management diagnostics
INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('7b979b68-7227-53fd-b689-e4fe153afb76', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', '8b7c669c-1795-5f6b-8f3a-3e502b74c628', 'ENTER(ingest)', 'ATTEMPT_CSV_INGEST', NULL, 'ScreeningCsvFileIngestSource.ingestSQL', (CURRENT_TIMESTAMP), NULL);


-- be sure to add src_file_row_number and session_id columns to each row
-- because assurance CTEs require them
CREATE TABLE libre_20240307 AS
  SELECT *, row_number() OVER () as src_file_row_number, '05269d28-15ae-5bd6-bd88-f949ccfa52d7' as session_id, '8b7c669c-1795-5f6b-8f3a-3e502b74c628' as session_entry_id
    FROM read_csv_auto('support/assurance/ingestion-center-elt/drh/synthetic-content/LIBRE_20240307.csv', types={'device': 'VARCHAR', 'serial number': 'VARCHAR'});

WITH required_column_names_in_src AS (
    SELECT column_name
      FROM (VALUES ('device'), ('serial number'), ('time stamp'), ('glucose value')) AS required(column_name)
     WHERE required.column_name NOT IN (
         SELECT column_name
           FROM information_schema.columns
          WHERE table_name = 'libre_20240307')
)
INSERT INTO orch_session_issue (orch_session_issue_id, session_id, session_entry_id, issue_type, issue_message, remediation)
    SELECT uuid(),
           '05269d28-15ae-5bd6-bd88-f949ccfa52d7',
           '8b7c669c-1795-5f6b-8f3a-3e502b74c628',
           'Missing Column',
           'Required column ' || column_name || ' is missing in libre_20240307.',
           'Ensure libre_20240307 contains the column "' || column_name || '"'
      FROM required_column_names_in_src;

INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('abf5c680-a135-5d89-b871-fa5b9b99aed6', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', '8b7c669c-1795-5f6b-8f3a-3e502b74c628', 'ATTEMPT_CSV_INGEST', 'INGESTED_CSV', NULL, 'ScreeningCsvFileIngestSource.ingestSQL', (CURRENT_TIMESTAMP), NULL);
    
-- ingest support/assurance/ingestion-center-elt/drh/synthetic-content/synthetic-fail-excel-01.xlsx (ERROR)
-- required by IngestEngine, setup the ingestion entry for logging
INSERT INTO "orch_session_entry" ("orch_session_entry_id", "session_id", "ingest_src", "ingest_table_name", "elaboration") VALUES ('641dff51-97fd-56b3-8443-c1ed568a6d66', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'support/assurance/ingestion-center-elt/drh/synthetic-content/synthetic-fail-excel-01.xlsx', 'ERROR', NULL);
INSERT INTO "orch_session_issue" ("orch_session_issue_id", "session_id", "session_entry_id", "issue_type", "issue_message", "issue_row", "issue_column", "invalid_value", "remediation", "elaboration") VALUES ('d70a4700-6b40-52fc-a7a2-69ef0d7f69ff', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', '641dff51-97fd-56b3-8443-c1ed568a6d66', 'Sheet Missing', 'Excel workbook sheet ''MYB004_Libre_data'' not found in ''synthetic-fail-excel-01.xlsx'' (available: Sheet1)', NULL, NULL, 'support/assurance/ingestion-center-elt/drh/synthetic-content/synthetic-fail-excel-01.xlsx', NULL, NULL);
-- ingest support/assurance/ingestion-center-elt/drh/synthetic-content/synthetic-fail-excel-01.xlsx (ERROR)
-- required by IngestEngine, setup the ingestion entry for logging
INSERT INTO "orch_session_entry" ("orch_session_entry_id", "session_id", "ingest_src", "ingest_table_name", "elaboration") VALUES ('47277588-99e8-59f5-8384-b24344a86073', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'support/assurance/ingestion-center-elt/drh/synthetic-content/synthetic-fail-excel-01.xlsx', 'ERROR', NULL);
INSERT INTO "orch_session_issue" ("orch_session_issue_id", "session_id", "session_entry_id", "issue_type", "issue_message", "issue_row", "issue_column", "invalid_value", "remediation", "elaboration") VALUES ('58b22e99-5854-53bf-adbe-08e67df99b85', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', '47277588-99e8-59f5-8384-b24344a86073', 'Sheet Missing', 'Excel workbook sheet ''MYB001_Libre_data'' not found in ''synthetic-fail-excel-01.xlsx'' (available: Sheet1)', NULL, NULL, 'support/assurance/ingestion-center-elt/drh/synthetic-content/synthetic-fail-excel-01.xlsx', NULL, NULL);
-- ingest support/assurance/ingestion-center-elt/drh/synthetic-content/synthetic-fail-excel-01.xlsx (ERROR)
-- required by IngestEngine, setup the ingestion entry for logging
INSERT INTO "orch_session_entry" ("orch_session_entry_id", "session_id", "ingest_src", "ingest_table_name", "elaboration") VALUES ('a26ce332-3ced-5623-861d-23a2ef78e4a9', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'support/assurance/ingestion-center-elt/drh/synthetic-content/synthetic-fail-excel-01.xlsx', 'ERROR', NULL);
INSERT INTO "orch_session_issue" ("orch_session_issue_id", "session_id", "session_entry_id", "issue_type", "issue_message", "issue_row", "issue_column", "invalid_value", "remediation", "elaboration") VALUES ('bc0c03b5-d1ba-5301-850f-5e4c42c1bf09', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'a26ce332-3ced-5623-861d-23a2ef78e4a9', 'Sheet Missing', 'Excel workbook sheet ''MYB006_Libre_data'' not found in ''synthetic-fail-excel-01.xlsx'' (available: Sheet1)', NULL, NULL, 'support/assurance/ingestion-center-elt/drh/synthetic-content/synthetic-fail-excel-01.xlsx', NULL, NULL);
-- ingest support/assurance/ingestion-center-elt/drh/synthetic-content/synthetic-fail.csv (ERROR)
-- required by IngestEngine, setup the ingestion entry for logging
INSERT INTO "orch_session_entry" ("orch_session_entry_id", "session_id", "ingest_src", "ingest_table_name", "elaboration") VALUES ('ae477ba1-c7f1-5f34-847a-50bddb7130aa', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'support/assurance/ingestion-center-elt/drh/synthetic-content/synthetic-fail.csv', 'ERROR', NULL);
INSERT INTO "orch_session_issue" ("orch_session_issue_id", "session_id", "session_entry_id", "issue_type", "issue_message", "issue_row", "issue_column", "invalid_value", "remediation", "elaboration") VALUES ('8aad9cfa-b1a2-5fb1-a6ab-613a79a7e839', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'ae477ba1-c7f1-5f34-847a-50bddb7130aa', 'Unknown CSV File Type', 'CSV file ''synthetic-fail.csv'' not found in ''synthetic-fail.csv''', NULL, NULL, 'support/assurance/ingestion-center-elt/drh/synthetic-content/synthetic-fail.csv', NULL, NULL);
-- ingest support/assurance/ingestion-center-elt/drh/synthetic-content/GLUCOSE_20240307.csv (glucose_20240307)
-- required by IngestEngine, setup the ingestion entry for logging
INSERT INTO "orch_session_entry" ("orch_session_entry_id", "session_id", "ingest_src", "ingest_table_name", "elaboration") VALUES ('b41ccd27-9a4f-5cc8-9c5d-b55242d90fb0', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'support/assurance/ingestion-center-elt/drh/synthetic-content/GLUCOSE_20240307.csv', 'glucose_20240307', NULL);

-- state management diagnostics
INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('7ef8bdeb-fd56-5eb9-a09b-ef15ce18dc49', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'b41ccd27-9a4f-5cc8-9c5d-b55242d90fb0', 'ENTER(ingest)', 'ATTEMPT_CSV_INGEST', NULL, 'QeAdminDataCsvFileIngestSource.ingestSQL', (CURRENT_TIMESTAMP), NULL);

-- be sure to add src_file_row_number and session_id columns to each row
-- because assurance CTEs require them
CREATE TABLE glucose_20240307 AS
  SELECT *, row_number() OVER () as src_file_row_number, '05269d28-15ae-5bd6-bd88-f949ccfa52d7' as session_id, 'b41ccd27-9a4f-5cc8-9c5d-b55242d90fb0' as session_entry_id
    FROM read_csv_auto('support/assurance/ingestion-center-elt/drh/synthetic-content/GLUCOSE_20240307.csv');

WITH required_column_names_in_src AS (
    SELECT column_name
      FROM (VALUES ) AS required(column_name)
     WHERE required.column_name NOT IN (
         SELECT column_name
           FROM information_schema.columns
          WHERE table_name = 'glucose_20240307')
)
INSERT INTO orch_session_issue (orch_session_issue_id, session_id, session_entry_id, issue_type, issue_message, remediation)
    SELECT uuid(),
           '05269d28-15ae-5bd6-bd88-f949ccfa52d7',
           'b41ccd27-9a4f-5cc8-9c5d-b55242d90fb0',
           'Missing Column',
           'Required column ' || column_name || ' is missing in glucose_20240307.',
           'Ensure glucose_20240307 contains the column "' || column_name || '"'
      FROM required_column_names_in_src;

INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('591191c7-f693-5957-8734-ac87151ca981', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'b41ccd27-9a4f-5cc8-9c5d-b55242d90fb0', 'ATTEMPT_CSV_INGEST', 'INGESTED_CSV', NULL, 'QeAdminDataCsvFileIngestSource.ingestSQL', (CURRENT_TIMESTAMP), NULL);
    
-- ingest support/assurance/ingestion-center-elt/drh/synthetic-content/Libre_data_240402.xlsx (libre_data_240402_m_y_b004_libre_data)
-- required by IngestEngine, setup the ingestion entry for logging
INSERT INTO "orch_session_entry" ("orch_session_entry_id", "session_id", "ingest_src", "ingest_table_name", "elaboration") VALUES ('3b4eb0e5-6239-537a-8e67-e50e172e72a2', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'support/assurance/ingestion-center-elt/drh/synthetic-content/Libre_data_240402.xlsx', 'libre_data_240402_m_y_b004_libre_data', NULL);
     
-- state management diagnostics 
INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('071f8fe1-4899-5c71-9c86-7d7377661d45', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', '3b4eb0e5-6239-537a-8e67-e50e172e72a2', 'ENTER(ingest)', 'ATTEMPT_EXCEL_INGEST', NULL, 'AdminDemographicExcelSheetIngestSource.ingestSQL', (CURRENT_TIMESTAMP), NULL);

-- ingest Excel workbook sheet 'MYB004_Libre_data' into libre_data_240402_m_y_b004_libre_data using spatial plugin
INSTALL spatial; LOAD spatial;

-- be sure to add src_file_row_number and session_id columns to each row
-- because assurance CTEs require them
CREATE TABLE libre_data_240402_m_y_b004_libre_data AS
  SELECT *, row_number() OVER () as src_file_row_number, '05269d28-15ae-5bd6-bd88-f949ccfa52d7' as session_id, '3b4eb0e5-6239-537a-8e67-e50e172e72a2' as session_entry_id
    FROM st_read('support/assurance/ingestion-center-elt/drh/synthetic-content/Libre_data_240402.xlsx', layer='MYB004_Libre_data', open_options=['HEADERS=FORCE', 'FIELD_TYPES=AUTO']);          

WITH required_column_names_in_src AS (
    SELECT column_name
      FROM (VALUES ('device'), ('serial number'), ('time stamp'), ('glucose value')) AS required(column_name)
     WHERE required.column_name NOT IN (
         SELECT column_name
           FROM information_schema.columns
          WHERE table_name = 'libre_data_240402_m_y_b004_libre_data')
)
INSERT INTO orch_session_issue (orch_session_issue_id, session_id, session_entry_id, issue_type, issue_message, remediation)
    SELECT uuid(),
           '05269d28-15ae-5bd6-bd88-f949ccfa52d7',
           '3b4eb0e5-6239-537a-8e67-e50e172e72a2',
           'Missing Column',
           'Required column ' || column_name || ' is missing in libre_data_240402_m_y_b004_libre_data.',
           'Ensure libre_data_240402_m_y_b004_libre_data contains the column "' || column_name || '"'
      FROM required_column_names_in_src;
INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('86b4a49e-7378-5159-9f41-b005208c31bc', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', '3b4eb0e5-6239-537a-8e67-e50e172e72a2', 'ATTEMPT_EXCEL_INGEST', 'INGESTED_EXCEL_WORKBOOK_SHEET', NULL, 'AdminDemographicExcelSheetIngestSource.ingestSQL', (CURRENT_TIMESTAMP), NULL);

-- ingest support/assurance/ingestion-center-elt/drh/synthetic-content/Libre_data_240402.xlsx (libre_data_240402_m_y_b001_libre_data)
-- required by IngestEngine, setup the ingestion entry for logging
INSERT INTO "orch_session_entry" ("orch_session_entry_id", "session_id", "ingest_src", "ingest_table_name", "elaboration") VALUES ('a530fe1b-57ef-5a90-8bea-835ece2483da', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'support/assurance/ingestion-center-elt/drh/synthetic-content/Libre_data_240402.xlsx', 'libre_data_240402_m_y_b001_libre_data', NULL);
     
-- state management diagnostics 
INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('a3fe7098-8ae8-5612-81ac-cbe10780c19b', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'a530fe1b-57ef-5a90-8bea-835ece2483da', 'ENTER(ingest)', 'ATTEMPT_EXCEL_INGEST', NULL, 'ScreeningExcelSheetIngestSource.ingestSQL', (CURRENT_TIMESTAMP), NULL);

-- ingest Excel workbook sheet 'MYB001_Libre_data' into libre_data_240402_m_y_b001_libre_data using spatial plugin
INSTALL spatial; LOAD spatial;

-- be sure to add src_file_row_number and session_id columns to each row
-- because assurance CTEs require them
CREATE TABLE libre_data_240402_m_y_b001_libre_data AS
  SELECT *, row_number() OVER () as src_file_row_number, '05269d28-15ae-5bd6-bd88-f949ccfa52d7' as session_id, 'a530fe1b-57ef-5a90-8bea-835ece2483da' as session_entry_id
    FROM st_read('support/assurance/ingestion-center-elt/drh/synthetic-content/Libre_data_240402.xlsx', layer='MYB001_Libre_data', open_options=['HEADERS=FORCE', 'FIELD_TYPES=AUTO']);          

WITH required_column_names_in_src AS (
    SELECT column_name
      FROM (VALUES ('device'), ('serial number'), ('time stamp'), ('glucose value')) AS required(column_name)
     WHERE required.column_name NOT IN (
         SELECT column_name
           FROM information_schema.columns
          WHERE table_name = 'libre_data_240402_m_y_b001_libre_data')
)
INSERT INTO orch_session_issue (orch_session_issue_id, session_id, session_entry_id, issue_type, issue_message, remediation)
    SELECT uuid(),
           '05269d28-15ae-5bd6-bd88-f949ccfa52d7',
           'a530fe1b-57ef-5a90-8bea-835ece2483da',
           'Missing Column',
           'Required column ' || column_name || ' is missing in libre_data_240402_m_y_b001_libre_data.',
           'Ensure libre_data_240402_m_y_b001_libre_data contains the column "' || column_name || '"'
      FROM required_column_names_in_src;
INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('99e72a60-96ab-5ef1-a3af-3e7759777664', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'a530fe1b-57ef-5a90-8bea-835ece2483da', 'ATTEMPT_EXCEL_INGEST', 'INGESTED_EXCEL_WORKBOOK_SHEET', NULL, 'ScreeningExcelSheetIngestSource.ingestSQL', (CURRENT_TIMESTAMP), NULL);

-- ingest support/assurance/ingestion-center-elt/drh/synthetic-content/Libre_data_240402.xlsx (libre_data_240402_m_y_b006_libre_data)
-- required by IngestEngine, setup the ingestion entry for logging
INSERT INTO "orch_session_entry" ("orch_session_entry_id", "session_id", "ingest_src", "ingest_table_name", "elaboration") VALUES ('e36daa69-3c63-5384-b6a7-03fa3b00641d', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'support/assurance/ingestion-center-elt/drh/synthetic-content/Libre_data_240402.xlsx', 'libre_data_240402_m_y_b006_libre_data', NULL);
     
-- state management diagnostics 
INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('89f7ec04-277a-5799-afaa-a70d0f2a8ed5', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'e36daa69-3c63-5384-b6a7-03fa3b00641d', 'ENTER(ingest)', 'ATTEMPT_EXCEL_INGEST', NULL, 'QeAdminDataExcelSheetIngestSource.ingestSQL', (CURRENT_TIMESTAMP), NULL);

-- ingest Excel workbook sheet 'MYB006_Libre_data' into libre_data_240402_m_y_b006_libre_data using spatial plugin
INSTALL spatial; LOAD spatial;

-- be sure to add src_file_row_number and session_id columns to each row
-- because assurance CTEs require them
CREATE TABLE libre_data_240402_m_y_b006_libre_data AS
  SELECT *, row_number() OVER () as src_file_row_number, '05269d28-15ae-5bd6-bd88-f949ccfa52d7' as session_id, 'e36daa69-3c63-5384-b6a7-03fa3b00641d' as session_entry_id
    FROM st_read('support/assurance/ingestion-center-elt/drh/synthetic-content/Libre_data_240402.xlsx', layer='MYB006_Libre_data', open_options=['HEADERS=FORCE', 'FIELD_TYPES=AUTO']);          

WITH required_column_names_in_src AS (
    SELECT column_name
      FROM (VALUES ('device'), ('serial number'), ('time stamp'), ('glucose value')) AS required(column_name)
     WHERE required.column_name NOT IN (
         SELECT column_name
           FROM information_schema.columns
          WHERE table_name = 'libre_data_240402_m_y_b006_libre_data')
)
INSERT INTO orch_session_issue (orch_session_issue_id, session_id, session_entry_id, issue_type, issue_message, remediation)
    SELECT uuid(),
           '05269d28-15ae-5bd6-bd88-f949ccfa52d7',
           'e36daa69-3c63-5384-b6a7-03fa3b00641d',
           'Missing Column',
           'Required column ' || column_name || ' is missing in libre_data_240402_m_y_b006_libre_data.',
           'Ensure libre_data_240402_m_y_b006_libre_data contains the column "' || column_name || '"'
      FROM required_column_names_in_src;
INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('c60cf3db-b1bf-5103-b278-b0c128ce924a', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'e36daa69-3c63-5384-b6a7-03fa3b00641d', 'ATTEMPT_EXCEL_INGEST', 'INGESTED_EXCEL_WORKBOOK_SHEET', NULL, 'QeAdminDataExcelSheetIngestSource.ingestSQL', (CURRENT_TIMESTAMP), NULL);

-- ingest support/assurance/ingestion-center-elt/drh/synthetic-content/vitalsigns_20240307.csv (vitalsigns_20240307)
-- required by IngestEngine, setup the ingestion entry for logging
INSERT INTO "orch_session_entry" ("orch_session_entry_id", "session_id", "ingest_src", "ingest_table_name", "elaboration") VALUES ('b2a7c7e8-5ffe-5f28-8112-4eb7abb6397f', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'support/assurance/ingestion-center-elt/drh/synthetic-content/vitalsigns_20240307.csv', 'vitalsigns_20240307', NULL);

-- state management diagnostics
INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('b10e248d-8c94-59ec-83fc-a1249dd3b111', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'b2a7c7e8-5ffe-5f28-8112-4eb7abb6397f', 'ENTER(ingest)', 'ATTEMPT_CSV_INGEST', NULL, 'AdminDemographicCsvFileIngestSource.ingestSQL', (CURRENT_TIMESTAMP), NULL);

-- be sure to add src_file_row_number and session_id columns to each row
-- because assurance CTEs require them
CREATE TABLE vitalsigns_20240307 AS
  SELECT *, row_number() OVER () as src_file_row_number, '05269d28-15ae-5bd6-bd88-f949ccfa52d7' as session_id, 'b2a7c7e8-5ffe-5f28-8112-4eb7abb6397f' as session_entry_id
    FROM read_csv_auto('support/assurance/ingestion-center-elt/drh/synthetic-content/vitalsigns_20240307.csv', types={'dataset_id': 'VARCHAR'});

WITH required_column_names_in_src AS (
    SELECT column_name
      FROM (VALUES ) AS required(column_name)
     WHERE required.column_name NOT IN (
         SELECT column_name
           FROM information_schema.columns
          WHERE table_name = 'vitalsigns_20240307')
)
INSERT INTO orch_session_issue (orch_session_issue_id, session_id, session_entry_id, issue_type, issue_message, remediation)
    SELECT uuid(),
           '05269d28-15ae-5bd6-bd88-f949ccfa52d7',
           'b2a7c7e8-5ffe-5f28-8112-4eb7abb6397f',
           'Missing Column',
           'Required column ' || column_name || ' is missing in vitalsigns_20240307.',
           'Ensure vitalsigns_20240307 contains the column "' || column_name || '"'
      FROM required_column_names_in_src;

INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('5222b730-9add-5b52-b0c9-6f2506b0af9d', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'b2a7c7e8-5ffe-5f28-8112-4eb7abb6397f', 'ATTEMPT_CSV_INGEST', 'INGESTED_CSV', NULL, 'AdminDemographicCsvFileIngestSource.ingestSQL', (CURRENT_TIMESTAMP), NULL);
    
-- ingest /home/vinod/projects/Diabetes Research Hub DRH_DOLT/INGESTION_GITHUB/personal_vinod.wst/ingestion-center/src/ingestion-center-elt/reference-data/screening-status-code-reference.csv (screening_status_code_reference)
-- required by IngestEngine, setup the ingestion entry for logging
INSERT INTO "orch_session_entry" ("orch_session_entry_id", "session_id", "ingest_src", "ingest_table_name", "elaboration") VALUES ('fa7874f6-f848-572b-a9ab-9db4c8d5e959', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', '/home/vinod/projects/Diabetes Research Hub DRH_DOLT/INGESTION_GITHUB/personal_vinod.wst/ingestion-center/src/ingestion-center-elt/reference-data/screening-status-code-reference.csv', 'screening_status_code_reference', NULL);

-- state management diagnostics
INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('c302047e-21cf-5059-a32c-e81a9bd3a9b9', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'fa7874f6-f848-572b-a9ab-9db4c8d5e959', 'ENTER(ingest)', 'ATTEMPT_CSV_INGEST', NULL, 'ScreeningStatusCodeReferenceCsvFileIngestSource.ingestSQL', (CURRENT_TIMESTAMP), NULL);

-- be sure to add src_file_row_number and session_id columns to each row
-- because assurance CTEs require them
CREATE TABLE screening_status_code_reference AS
  SELECT *, row_number() OVER () as src_file_row_number, '05269d28-15ae-5bd6-bd88-f949ccfa52d7' as session_id, 'fa7874f6-f848-572b-a9ab-9db4c8d5e959' as session_entry_id
    FROM read_csv_auto('/home/vinod/projects/Diabetes Research Hub DRH_DOLT/INGESTION_GITHUB/personal_vinod.wst/ingestion-center/src/ingestion-center-elt/reference-data/screening-status-code-reference.csv',
      header = true
    );

WITH required_column_names_in_src AS (
    SELECT column_name
      FROM (VALUES ('Lvl'), ('Code'), ('Display'), ('Definition')) AS required(column_name)
     WHERE required.column_name NOT IN (
         SELECT column_name
           FROM information_schema.columns
          WHERE table_name = 'screening_status_code_reference')
)
INSERT INTO orch_session_issue (orch_session_issue_id, session_id, session_entry_id, issue_type, issue_message, remediation)
    SELECT uuid(),
           '05269d28-15ae-5bd6-bd88-f949ccfa52d7',
           'fa7874f6-f848-572b-a9ab-9db4c8d5e959',
           'Missing Column',
           'Required column ' || column_name || ' is missing in screening_status_code_reference.',
           'Ensure screening_status_code_reference contains the column "' || column_name || '"'
      FROM required_column_names_in_src;

INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('3252fee6-3a9a-5f4c-81c6-739201046d79', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'fa7874f6-f848-572b-a9ab-9db4c8d5e959', 'ATTEMPT_CSV_INGEST', 'INGESTED_CSV', NULL, 'ScreeningStatusCodeReferenceCsvFileIngestSource.ingestSQL', (CURRENT_TIMESTAMP), NULL);
      
-- ingest /home/vinod/projects/Diabetes Research Hub DRH_DOLT/INGESTION_GITHUB/personal_vinod.wst/ingestion-center/src/ingestion-center-elt/reference-data/business-rules.csv (business_rules)
-- required by IngestEngine, setup the ingestion entry for logging
INSERT INTO "orch_session_entry" ("orch_session_entry_id", "session_id", "ingest_src", "ingest_table_name", "elaboration") VALUES ('78d6a904-035e-54ae-8ac2-ca5cdf3f75f7', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', '/home/vinod/projects/Diabetes Research Hub DRH_DOLT/INGESTION_GITHUB/personal_vinod.wst/ingestion-center/src/ingestion-center-elt/reference-data/business-rules.csv', 'business_rules', NULL);

-- state management diagnostics
INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('9860873a-c387-5d98-9930-4ff296eb7192', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', '78d6a904-035e-54ae-8ac2-ca5cdf3f75f7', 'ENTER(ingest)', 'ATTEMPT_CSV_INGEST', NULL, 'BusinessRulesReferenceCsvFileIngestSource.ingestSQL', (CURRENT_TIMESTAMP), NULL);

-- be sure to add src_file_row_number and session_id columns to each row
-- because assurance CTEs require them
CREATE TABLE business_rules AS
  SELECT *, row_number() OVER () as src_file_row_number, '05269d28-15ae-5bd6-bd88-f949ccfa52d7' as session_id, '78d6a904-035e-54ae-8ac2-ca5cdf3f75f7' as session_entry_id
    FROM read_csv_auto('/home/vinod/projects/Diabetes Research Hub DRH_DOLT/INGESTION_GITHUB/personal_vinod.wst/ingestion-center/src/ingestion-center-elt/reference-data/business-rules.csv',
      header = true
    );

WITH required_column_names_in_src AS (
    SELECT column_name
      FROM (VALUES ('Worksheet'), ('Field'), ('Required'), ('Permissible Values'), ('True Rejection'), ('Warning Layer'), ('Resolved by QE/QCS')) AS required(column_name)
     WHERE required.column_name NOT IN (
         SELECT column_name
           FROM information_schema.columns
          WHERE table_name = 'business_rules')
)
INSERT INTO orch_session_issue (orch_session_issue_id, session_id, session_entry_id, issue_type, issue_message, remediation)
    SELECT uuid(),
           '05269d28-15ae-5bd6-bd88-f949ccfa52d7',
           '78d6a904-035e-54ae-8ac2-ca5cdf3f75f7',
           'Missing Column',
           'Required column ' || column_name || ' is missing in business_rules.',
           'Ensure business_rules contains the column "' || column_name || '"'
      FROM required_column_names_in_src;

INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('d5d6e25d-81b4-5f98-8b91-ea2dbc155a9c', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', '78d6a904-035e-54ae-8ac2-ca5cdf3f75f7', 'ATTEMPT_CSV_INGEST', 'INGESTED_CSV', NULL, 'BusinessRulesReferenceCsvFileIngestSource.ingestSQL', (CURRENT_TIMESTAMP), NULL);
      
WITH check_all_tables_are_ingested_in_a_group AS (
  WITH required_tables AS (
      SELECT 'libre_20240307'
        AS table_name,
        'LIBRE' AS table_name_suffix
      UNION ALL
      SELECT 'vitalsigns_20240307'
        AS table_name,
        'VITALSIGNS' AS table_name_suffix
      UNION ALL
      SELECT 'glucose_20240307'
        AS table_name,
        'GLUCOSE' AS table_name_suffix
  )
  SELECT rt.table_name as table_name, rt.table_name_suffix as table_name_suffix
  FROM required_tables rt
  LEFT JOIN information_schema.tables ist ON rt.table_name = ist.table_name
  WHERE
    'libre_20240307' IN (
      'libre_20240307',
      'vitalsigns_20240307',
      'glucose_20240307'
      )
  AND ist.table_name IS NULL
)
INSERT INTO orch_session_issue (orch_session_issue_id, session_id, session_entry_id, issue_type, issue_row, issue_column, invalid_value, issue_message, remediation)
    SELECT uuid(),
           '05269d28-15ae-5bd6-bd88-f949ccfa52d7',
           '8b7c669c-1795-5f6b-8f3a-3e502b74c628',
           'CSV File Missing',
           NULL,
           NULL,
           table_name,
           'CSV file ' || table_name_suffix || '_20240307 not found under the group (_20240307)',
           NULL
      FROM check_all_tables_are_ingested_in_a_group
    ;
WITH check_all_tables_are_ingested_in_a_group AS (
  WITH required_tables AS (
      SELECT 'libre_20240307'
        AS table_name,
        'LIBRE' AS table_name_suffix
      UNION ALL
      SELECT 'vitalsigns_20240307'
        AS table_name,
        'VITALSIGNS' AS table_name_suffix
      UNION ALL
      SELECT 'glucose_20240307'
        AS table_name,
        'GLUCOSE' AS table_name_suffix
  )
  SELECT rt.table_name as table_name, rt.table_name_suffix as table_name_suffix
  FROM required_tables rt
  LEFT JOIN information_schema.tables ist ON rt.table_name = ist.table_name
  WHERE
    'ERROR' IN (
      'libre_20240307',
      'vitalsigns_20240307',
      'glucose_20240307'
      )
  AND ist.table_name IS NULL
)
INSERT INTO orch_session_issue (orch_session_issue_id, session_id, session_entry_id, issue_type, issue_row, issue_column, invalid_value, issue_message, remediation)
    SELECT uuid(),
           '05269d28-15ae-5bd6-bd88-f949ccfa52d7',
           '641dff51-97fd-56b3-8443-c1ed568a6d66',
           'CSV File Missing',
           NULL,
           NULL,
           table_name,
           'CSV file ' || table_name_suffix || '_20240307 not found under the group (_20240307)',
           NULL
      FROM check_all_tables_are_ingested_in_a_group
    ;
WITH check_all_tables_are_ingested_in_a_group AS (
  WITH required_tables AS (
      SELECT 'libre_20240307'
        AS table_name,
        'LIBRE' AS table_name_suffix
      UNION ALL
      SELECT 'vitalsigns_20240307'
        AS table_name,
        'VITALSIGNS' AS table_name_suffix
      UNION ALL
      SELECT 'glucose_20240307'
        AS table_name,
        'GLUCOSE' AS table_name_suffix
  )
  SELECT rt.table_name as table_name, rt.table_name_suffix as table_name_suffix
  FROM required_tables rt
  LEFT JOIN information_schema.tables ist ON rt.table_name = ist.table_name
  WHERE
    'ERROR' IN (
      'libre_20240307',
      'vitalsigns_20240307',
      'glucose_20240307'
      )
  AND ist.table_name IS NULL
)
INSERT INTO orch_session_issue (orch_session_issue_id, session_id, session_entry_id, issue_type, issue_row, issue_column, invalid_value, issue_message, remediation)
    SELECT uuid(),
           '05269d28-15ae-5bd6-bd88-f949ccfa52d7',
           '47277588-99e8-59f5-8384-b24344a86073',
           'CSV File Missing',
           NULL,
           NULL,
           table_name,
           'CSV file ' || table_name_suffix || '_20240307 not found under the group (_20240307)',
           NULL
      FROM check_all_tables_are_ingested_in_a_group
    ;
WITH check_all_tables_are_ingested_in_a_group AS (
  WITH required_tables AS (
      SELECT 'libre_20240307'
        AS table_name,
        'LIBRE' AS table_name_suffix
      UNION ALL
      SELECT 'vitalsigns_20240307'
        AS table_name,
        'VITALSIGNS' AS table_name_suffix
      UNION ALL
      SELECT 'glucose_20240307'
        AS table_name,
        'GLUCOSE' AS table_name_suffix
  )
  SELECT rt.table_name as table_name, rt.table_name_suffix as table_name_suffix
  FROM required_tables rt
  LEFT JOIN information_schema.tables ist ON rt.table_name = ist.table_name
  WHERE
    'ERROR' IN (
      'libre_20240307',
      'vitalsigns_20240307',
      'glucose_20240307'
      )
  AND ist.table_name IS NULL
)
INSERT INTO orch_session_issue (orch_session_issue_id, session_id, session_entry_id, issue_type, issue_row, issue_column, invalid_value, issue_message, remediation)
    SELECT uuid(),
           '05269d28-15ae-5bd6-bd88-f949ccfa52d7',
           'a26ce332-3ced-5623-861d-23a2ef78e4a9',
           'CSV File Missing',
           NULL,
           NULL,
           table_name,
           'CSV file ' || table_name_suffix || '_20240307 not found under the group (_20240307)',
           NULL
      FROM check_all_tables_are_ingested_in_a_group
    ;
WITH check_all_tables_are_ingested_in_a_group AS (
  WITH required_tables AS (
      SELECT 'libre_20240307'
        AS table_name,
        'LIBRE' AS table_name_suffix
      UNION ALL
      SELECT 'vitalsigns_20240307'
        AS table_name,
        'VITALSIGNS' AS table_name_suffix
      UNION ALL
      SELECT 'glucose_20240307'
        AS table_name,
        'GLUCOSE' AS table_name_suffix
  )
  SELECT rt.table_name as table_name, rt.table_name_suffix as table_name_suffix
  FROM required_tables rt
  LEFT JOIN information_schema.tables ist ON rt.table_name = ist.table_name
  WHERE
    'ERROR' IN (
      'libre_20240307',
      'vitalsigns_20240307',
      'glucose_20240307'
      )
  AND ist.table_name IS NULL
)
INSERT INTO orch_session_issue (orch_session_issue_id, session_id, session_entry_id, issue_type, issue_row, issue_column, invalid_value, issue_message, remediation)
    SELECT uuid(),
           '05269d28-15ae-5bd6-bd88-f949ccfa52d7',
           'ae477ba1-c7f1-5f34-847a-50bddb7130aa',
           'CSV File Missing',
           NULL,
           NULL,
           table_name,
           'CSV file ' || table_name_suffix || '_20240307 not found under the group (_20240307)',
           NULL
      FROM check_all_tables_are_ingested_in_a_group
    ;
WITH check_all_tables_are_ingested_in_a_group AS (
  WITH required_tables AS (
      SELECT 'libre_20240307'
        AS table_name,
        'LIBRE' AS table_name_suffix
      UNION ALL
      SELECT 'vitalsigns_20240307'
        AS table_name,
        'VITALSIGNS' AS table_name_suffix
      UNION ALL
      SELECT 'glucose_20240307'
        AS table_name,
        'GLUCOSE' AS table_name_suffix
  )
  SELECT rt.table_name as table_name, rt.table_name_suffix as table_name_suffix
  FROM required_tables rt
  LEFT JOIN information_schema.tables ist ON rt.table_name = ist.table_name
  WHERE
    'glucose_20240307' IN (
      'libre_20240307',
      'vitalsigns_20240307',
      'glucose_20240307'
      )
  AND ist.table_name IS NULL
)
INSERT INTO orch_session_issue (orch_session_issue_id, session_id, session_entry_id, issue_type, issue_row, issue_column, invalid_value, issue_message, remediation)
    SELECT uuid(),
           '05269d28-15ae-5bd6-bd88-f949ccfa52d7',
           'b41ccd27-9a4f-5cc8-9c5d-b55242d90fb0',
           'CSV File Missing',
           NULL,
           NULL,
           table_name,
           'CSV file ' || table_name_suffix || '_20240307 not found under the group (_20240307)',
           NULL
      FROM check_all_tables_are_ingested_in_a_group
    ;
WITH check_all_tables_are_ingested_in_a_group AS (
  WITH required_tables AS (
      SELECT 'libre_20240307'
        AS table_name,
        'LIBRE' AS table_name_suffix
      UNION ALL
      SELECT 'vitalsigns_20240307'
        AS table_name,
        'VITALSIGNS' AS table_name_suffix
      UNION ALL
      SELECT 'glucose_20240307'
        AS table_name,
        'GLUCOSE' AS table_name_suffix
  )
  SELECT rt.table_name as table_name, rt.table_name_suffix as table_name_suffix
  FROM required_tables rt
  LEFT JOIN information_schema.tables ist ON rt.table_name = ist.table_name
  WHERE
    'libre_data_240402_m_y_b004_libre_data' IN (
      'libre_20240307',
      'vitalsigns_20240307',
      'glucose_20240307'
      )
  AND ist.table_name IS NULL
)
INSERT INTO orch_session_issue (orch_session_issue_id, session_id, session_entry_id, issue_type, issue_row, issue_column, invalid_value, issue_message, remediation)
    SELECT uuid(),
           '05269d28-15ae-5bd6-bd88-f949ccfa52d7',
           '3b4eb0e5-6239-537a-8e67-e50e172e72a2',
           'CSV File Missing',
           NULL,
           NULL,
           table_name,
           'CSV file ' || table_name_suffix || '_20240307 not found under the group (_20240307)',
           NULL
      FROM check_all_tables_are_ingested_in_a_group
    ;
WITH check_all_tables_are_ingested_in_a_group AS (
  WITH required_tables AS (
      SELECT 'libre_20240307'
        AS table_name,
        'LIBRE' AS table_name_suffix
      UNION ALL
      SELECT 'vitalsigns_20240307'
        AS table_name,
        'VITALSIGNS' AS table_name_suffix
      UNION ALL
      SELECT 'glucose_20240307'
        AS table_name,
        'GLUCOSE' AS table_name_suffix
  )
  SELECT rt.table_name as table_name, rt.table_name_suffix as table_name_suffix
  FROM required_tables rt
  LEFT JOIN information_schema.tables ist ON rt.table_name = ist.table_name
  WHERE
    'libre_data_240402_m_y_b001_libre_data' IN (
      'libre_20240307',
      'vitalsigns_20240307',
      'glucose_20240307'
      )
  AND ist.table_name IS NULL
)
INSERT INTO orch_session_issue (orch_session_issue_id, session_id, session_entry_id, issue_type, issue_row, issue_column, invalid_value, issue_message, remediation)
    SELECT uuid(),
           '05269d28-15ae-5bd6-bd88-f949ccfa52d7',
           'a530fe1b-57ef-5a90-8bea-835ece2483da',
           'CSV File Missing',
           NULL,
           NULL,
           table_name,
           'CSV file ' || table_name_suffix || '_20240307 not found under the group (_20240307)',
           NULL
      FROM check_all_tables_are_ingested_in_a_group
    ;
WITH check_all_tables_are_ingested_in_a_group AS (
  WITH required_tables AS (
      SELECT 'libre_20240307'
        AS table_name,
        'LIBRE' AS table_name_suffix
      UNION ALL
      SELECT 'vitalsigns_20240307'
        AS table_name,
        'VITALSIGNS' AS table_name_suffix
      UNION ALL
      SELECT 'glucose_20240307'
        AS table_name,
        'GLUCOSE' AS table_name_suffix
  )
  SELECT rt.table_name as table_name, rt.table_name_suffix as table_name_suffix
  FROM required_tables rt
  LEFT JOIN information_schema.tables ist ON rt.table_name = ist.table_name
  WHERE
    'libre_data_240402_m_y_b006_libre_data' IN (
      'libre_20240307',
      'vitalsigns_20240307',
      'glucose_20240307'
      )
  AND ist.table_name IS NULL
)
INSERT INTO orch_session_issue (orch_session_issue_id, session_id, session_entry_id, issue_type, issue_row, issue_column, invalid_value, issue_message, remediation)
    SELECT uuid(),
           '05269d28-15ae-5bd6-bd88-f949ccfa52d7',
           'e36daa69-3c63-5384-b6a7-03fa3b00641d',
           'CSV File Missing',
           NULL,
           NULL,
           table_name,
           'CSV file ' || table_name_suffix || '_20240307 not found under the group (_20240307)',
           NULL
      FROM check_all_tables_are_ingested_in_a_group
    ;
WITH check_all_tables_are_ingested_in_a_group AS (
  WITH required_tables AS (
      SELECT 'libre_20240307'
        AS table_name,
        'LIBRE' AS table_name_suffix
      UNION ALL
      SELECT 'vitalsigns_20240307'
        AS table_name,
        'VITALSIGNS' AS table_name_suffix
      UNION ALL
      SELECT 'glucose_20240307'
        AS table_name,
        'GLUCOSE' AS table_name_suffix
  )
  SELECT rt.table_name as table_name, rt.table_name_suffix as table_name_suffix
  FROM required_tables rt
  LEFT JOIN information_schema.tables ist ON rt.table_name = ist.table_name
  WHERE
    'vitalsigns_20240307' IN (
      'libre_20240307',
      'vitalsigns_20240307',
      'glucose_20240307'
      )
  AND ist.table_name IS NULL
)
INSERT INTO orch_session_issue (orch_session_issue_id, session_id, session_entry_id, issue_type, issue_row, issue_column, invalid_value, issue_message, remediation)
    SELECT uuid(),
           '05269d28-15ae-5bd6-bd88-f949ccfa52d7',
           'b2a7c7e8-5ffe-5f28-8112-4eb7abb6397f',
           'CSV File Missing',
           NULL,
           NULL,
           table_name,
           'CSV file ' || table_name_suffix || '_20240307 not found under the group (_20240307)',
           NULL
      FROM check_all_tables_are_ingested_in_a_group
    ;
WITH check_all_tables_are_ingested_in_a_group AS (
  WITH required_tables AS (
      SELECT 'libre_20240307'
        AS table_name,
        'LIBRE' AS table_name_suffix
      UNION ALL
      SELECT 'vitalsigns_20240307'
        AS table_name,
        'VITALSIGNS' AS table_name_suffix
      UNION ALL
      SELECT 'glucose_20240307'
        AS table_name,
        'GLUCOSE' AS table_name_suffix
  )
  SELECT rt.table_name as table_name, rt.table_name_suffix as table_name_suffix
  FROM required_tables rt
  LEFT JOIN information_schema.tables ist ON rt.table_name = ist.table_name
  WHERE
    'screening_status_code_reference' IN (
      'libre_20240307',
      'vitalsigns_20240307',
      'glucose_20240307'
      )
  AND ist.table_name IS NULL
)
INSERT INTO orch_session_issue (orch_session_issue_id, session_id, session_entry_id, issue_type, issue_row, issue_column, invalid_value, issue_message, remediation)
    SELECT uuid(),
           '05269d28-15ae-5bd6-bd88-f949ccfa52d7',
           'fa7874f6-f848-572b-a9ab-9db4c8d5e959',
           'CSV File Missing',
           NULL,
           NULL,
           table_name,
           'CSV file ' || table_name_suffix || '_20240307 not found under the group (_20240307)',
           NULL
      FROM check_all_tables_are_ingested_in_a_group
    ;
WITH check_all_tables_are_ingested_in_a_group AS (
  WITH required_tables AS (
      SELECT 'libre_20240307'
        AS table_name,
        'LIBRE' AS table_name_suffix
      UNION ALL
      SELECT 'vitalsigns_20240307'
        AS table_name,
        'VITALSIGNS' AS table_name_suffix
      UNION ALL
      SELECT 'glucose_20240307'
        AS table_name,
        'GLUCOSE' AS table_name_suffix
  )
  SELECT rt.table_name as table_name, rt.table_name_suffix as table_name_suffix
  FROM required_tables rt
  LEFT JOIN information_schema.tables ist ON rt.table_name = ist.table_name
  WHERE
    'business_rules' IN (
      'libre_20240307',
      'vitalsigns_20240307',
      'glucose_20240307'
      )
  AND ist.table_name IS NULL
)
INSERT INTO orch_session_issue (orch_session_issue_id, session_id, session_entry_id, issue_type, issue_row, issue_column, invalid_value, issue_message, remediation)
    SELECT uuid(),
           '05269d28-15ae-5bd6-bd88-f949ccfa52d7',
           '78d6a904-035e-54ae-8ac2-ca5cdf3f75f7',
           'CSV File Missing',
           NULL,
           NULL,
           table_name,
           'CSV file ' || table_name_suffix || '_20240307 not found under the group (_20240307)',
           NULL
      FROM check_all_tables_are_ingested_in_a_group
    ;
SELECT session_entry_id, orch_session_issue_id, issue_type, issue_message, invalid_value FROM orch_session_issue WHERE session_id = '05269d28-15ae-5bd6-bd88-f949ccfa52d7'
```
### `ingest` STDOUT (status: `1`)
```json
[{"session_entry_id":"641dff51-97fd-56b3-8443-c1ed568a6d66","orch_session_issue_id":"d70a4700-6b40-52fc-a7a2-69ef0d7f69ff","issue_type":"Sheet Missing","issue_message":"Excel workbook sheet 'MYB004_Libre_data' not found in 'synthetic-fail-excel-01.xlsx' (available: Sheet1)","invalid_value":"support/assurance/ingestion-center-elt/drh/synthetic-content/synthetic-fail-excel-01.xlsx"},
{"session_entry_id":"47277588-99e8-59f5-8384-b24344a86073","orch_session_issue_id":"58b22e99-5854-53bf-adbe-08e67df99b85","issue_type":"Sheet Missing","issue_message":"Excel workbook sheet 'MYB001_Libre_data' not found in 'synthetic-fail-excel-01.xlsx' (available: Sheet1)","invalid_value":"support/assurance/ingestion-center-elt/drh/synthetic-content/synthetic-fail-excel-01.xlsx"},
{"session_entry_id":"a26ce332-3ced-5623-861d-23a2ef78e4a9","orch_session_issue_id":"bc0c03b5-d1ba-5301-850f-5e4c42c1bf09","issue_type":"Sheet Missing","issue_message":"Excel workbook sheet 'MYB006_Libre_data' not found in 'synthetic-fail-excel-01.xlsx' (available: Sheet1)","invalid_value":"support/assurance/ingestion-center-elt/drh/synthetic-content/synthetic-fail-excel-01.xlsx"},
{"session_entry_id":"ae477ba1-c7f1-5f34-847a-50bddb7130aa","orch_session_issue_id":"8aad9cfa-b1a2-5fb1-a6ab-613a79a7e839","issue_type":"Unknown CSV File Type","issue_message":"CSV file 'synthetic-fail.csv' not found in 'synthetic-fail.csv'","invalid_value":"support/assurance/ingestion-center-elt/drh/synthetic-content/synthetic-fail.csv"}]

```
### `ingest` STDERR
```sh
Error: near line 67: Parser Error: syntax error at or near ")"
LINE 3:       FROM (VALUES ) AS required(column_name)
                           ^
Error: near line 201: Parser Error: syntax error at or near ")"
LINE 3:       FROM (VALUES ) AS required(column_name)
                           ^

```
    

## ensureContent

```sql
-- preambleSQL
SET autoinstall_known_extensions=true;
SET autoload_known_extensions=true;
-- end preambleSQL
INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('c2c0cbca-70cb-54f6-9dc7-66b47c4f3157', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', '8b7c669c-1795-5f6b-8f3a-3e502b74c628', 'INGESTED_CSV', 'ATTEMPT_CSV_ASSURANCE', NULL, 'ScreeningCsvFileIngestSource.assuranceSQL', (CURRENT_TIMESTAMP), NULL);

WITH mandatory_value AS (
    SELECT 'device' AS issue_column,
           "device" AS invalid_value,
           src_file_row_number AS issue_row
      FROM "libre_20240307"
     WHERE "device" IS NULL
        OR TRIM("device") = ''
)
INSERT INTO orch_session_issue (orch_session_issue_id, session_id, session_entry_id, issue_type, issue_row, issue_column, invalid_value, issue_message, remediation)
    SELECT uuid(),
           '05269d28-15ae-5bd6-bd88-f949ccfa52d7',
           '8b7c669c-1795-5f6b-8f3a-3e502b74c628',
           'Missing Mandatory Value',
           issue_row,
           issue_column,
           invalid_value,
           'Mandatory field ' || issue_column || ' is empty',
           'Provide a value for ' || issue_column
      FROM mandatory_value;
WITH mandatory_value AS (
    SELECT 'serial number' AS issue_column,
           "serial number" AS invalid_value,
           src_file_row_number AS issue_row
      FROM "libre_20240307"
     WHERE "serial number" IS NULL
        OR TRIM("serial number") = ''
)
INSERT INTO orch_session_issue (orch_session_issue_id, session_id, session_entry_id, issue_type, issue_row, issue_column, invalid_value, issue_message, remediation)
    SELECT uuid(),
           '05269d28-15ae-5bd6-bd88-f949ccfa52d7',
           '8b7c669c-1795-5f6b-8f3a-3e502b74c628',
           'Missing Mandatory Value',
           issue_row,
           issue_column,
           invalid_value,
           'Mandatory field ' || issue_column || ' is empty',
           'Provide a value for ' || issue_column
      FROM mandatory_value;
WITH mandatory_value AS (
    SELECT 'time stamp' AS issue_column,
           "time stamp" AS invalid_value,
           src_file_row_number AS issue_row
      FROM "libre_20240307"
     WHERE "time stamp" IS NULL
        OR TRIM("time stamp") = ''
)
INSERT INTO orch_session_issue (orch_session_issue_id, session_id, session_entry_id, issue_type, issue_row, issue_column, invalid_value, issue_message, remediation)
    SELECT uuid(),
           '05269d28-15ae-5bd6-bd88-f949ccfa52d7',
           '8b7c669c-1795-5f6b-8f3a-3e502b74c628',
           'Missing Mandatory Value',
           issue_row,
           issue_column,
           invalid_value,
           'Mandatory field ' || issue_column || ' is empty',
           'Provide a value for ' || issue_column
      FROM mandatory_value;
WITH mandatory_value AS (
    SELECT 'glucose value' AS issue_column,
           "glucose value" AS invalid_value,
           src_file_row_number AS issue_row
      FROM "libre_20240307"
     WHERE "glucose value" IS NULL
        OR TRIM("glucose value") = ''
)
INSERT INTO orch_session_issue (orch_session_issue_id, session_id, session_entry_id, issue_type, issue_row, issue_column, invalid_value, issue_message, remediation)
    SELECT uuid(),
           '05269d28-15ae-5bd6-bd88-f949ccfa52d7',
           '8b7c669c-1795-5f6b-8f3a-3e502b74c628',
           'Missing Mandatory Value',
           issue_row,
           issue_column,
           invalid_value,
           'Mandatory field ' || issue_column || ' is empty',
           'Provide a value for ' || issue_column
      FROM mandatory_value;

INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('e8b3dab4-5058-5c79-8088-45b423119149', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', '8b7c669c-1795-5f6b-8f3a-3e502b74c628', 'ATTEMPT_CSV_ASSURANCE', 'ASSURED_CSV', NULL, 'ScreeningCsvFileIngestSource.assuranceSQL', (CURRENT_TIMESTAMP), NULL);
    
INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('8640a4b5-53ef-506e-bcde-83f00315d4b2', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'b41ccd27-9a4f-5cc8-9c5d-b55242d90fb0', 'INGESTED_CSV', 'ATTEMPT_CSV_ASSURANCE', NULL, 'QeAdminDataCsvFileIngestSource.assuranceSQL', (CURRENT_TIMESTAMP), NULL);

INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('9dabd022-4a26-55f2-98f4-e534e7704b23', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'b41ccd27-9a4f-5cc8-9c5d-b55242d90fb0', 'ATTEMPT_CSV_ASSURANCE', 'ASSURED_CSV', NULL, 'QeAdminDataCsvFileIngestSource.assuranceSQL', (CURRENT_TIMESTAMP), NULL);
    
INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('544998d3-58c5-5f65-9dc8-9f998508495f', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', '3b4eb0e5-6239-537a-8e67-e50e172e72a2', 'INGESTED_EXCEL_WORKBOOK_SHEET', 'ATTEMPT_EXCEL_WORKBOOK_SHEET_ASSURANCE', NULL, 'AdminDemographicExcelSheetIngestSource.assuranceSQL', (CURRENT_TIMESTAMP), NULL);

WITH mandatory_value AS (
    SELECT 'device' AS issue_column,
           "device" AS invalid_value,
           src_file_row_number AS issue_row
      FROM "libre_data_240402_m_y_b004_libre_data"
     WHERE "device" IS NULL
        OR TRIM("device") = ''
)
INSERT INTO orch_session_issue (orch_session_issue_id, session_id, session_entry_id, issue_type, issue_row, issue_column, invalid_value, issue_message, remediation)
    SELECT uuid(),
           '05269d28-15ae-5bd6-bd88-f949ccfa52d7',
           '3b4eb0e5-6239-537a-8e67-e50e172e72a2',
           'Missing Mandatory Value',
           issue_row,
           issue_column,
           invalid_value,
           'Mandatory field ' || issue_column || ' is empty',
           'Provide a value for ' || issue_column
      FROM mandatory_value;
WITH mandatory_value AS (
    SELECT 'serial number' AS issue_column,
           "serial number" AS invalid_value,
           src_file_row_number AS issue_row
      FROM "libre_data_240402_m_y_b004_libre_data"
     WHERE "serial number" IS NULL
        OR TRIM("serial number") = ''
)
INSERT INTO orch_session_issue (orch_session_issue_id, session_id, session_entry_id, issue_type, issue_row, issue_column, invalid_value, issue_message, remediation)
    SELECT uuid(),
           '05269d28-15ae-5bd6-bd88-f949ccfa52d7',
           '3b4eb0e5-6239-537a-8e67-e50e172e72a2',
           'Missing Mandatory Value',
           issue_row,
           issue_column,
           invalid_value,
           'Mandatory field ' || issue_column || ' is empty',
           'Provide a value for ' || issue_column
      FROM mandatory_value;
WITH mandatory_value AS (
    SELECT 'time stamp' AS issue_column,
           "time stamp" AS invalid_value,
           src_file_row_number AS issue_row
      FROM "libre_data_240402_m_y_b004_libre_data"
     WHERE "time stamp" IS NULL
        OR TRIM("time stamp") = ''
)
INSERT INTO orch_session_issue (orch_session_issue_id, session_id, session_entry_id, issue_type, issue_row, issue_column, invalid_value, issue_message, remediation)
    SELECT uuid(),
           '05269d28-15ae-5bd6-bd88-f949ccfa52d7',
           '3b4eb0e5-6239-537a-8e67-e50e172e72a2',
           'Missing Mandatory Value',
           issue_row,
           issue_column,
           invalid_value,
           'Mandatory field ' || issue_column || ' is empty',
           'Provide a value for ' || issue_column
      FROM mandatory_value;
WITH mandatory_value AS (
    SELECT 'glucose value' AS issue_column,
           "glucose value" AS invalid_value,
           src_file_row_number AS issue_row
      FROM "libre_data_240402_m_y_b004_libre_data"
     WHERE "glucose value" IS NULL
        OR TRIM("glucose value") = ''
)
INSERT INTO orch_session_issue (orch_session_issue_id, session_id, session_entry_id, issue_type, issue_row, issue_column, invalid_value, issue_message, remediation)
    SELECT uuid(),
           '05269d28-15ae-5bd6-bd88-f949ccfa52d7',
           '3b4eb0e5-6239-537a-8e67-e50e172e72a2',
           'Missing Mandatory Value',
           issue_row,
           issue_column,
           invalid_value,
           'Mandatory field ' || issue_column || ' is empty',
           'Provide a value for ' || issue_column
      FROM mandatory_value;
    
INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('80af4eff-d697-565b-9e3f-a587e322b1da', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', '3b4eb0e5-6239-537a-8e67-e50e172e72a2', 'ATTEMPT_EXCEL_WORKBOOK_SHEET_ASSURANCE', 'ASSURED_EXCEL_WORKBOOK_SHEET', NULL, 'AdminDemographicExcelSheetIngestSource.assuranceSQL', (CURRENT_TIMESTAMP), NULL);
    
INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('0adb81bc-3df2-5f86-99cc-2d20e1dd5efd', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'a530fe1b-57ef-5a90-8bea-835ece2483da', 'INGESTED_EXCEL_WORKBOOK_SHEET', 'ATTEMPT_EXCEL_WORKBOOK_SHEET_ASSURANCE', NULL, 'ScreeningExcelSheetIngestSource.assuranceSQL', (CURRENT_TIMESTAMP), NULL);

WITH mandatory_value AS (
    SELECT 'device' AS issue_column,
           "device" AS invalid_value,
           src_file_row_number AS issue_row
      FROM "libre_data_240402_m_y_b001_libre_data"
     WHERE "device" IS NULL
        OR TRIM("device") = ''
)
INSERT INTO orch_session_issue (orch_session_issue_id, session_id, session_entry_id, issue_type, issue_row, issue_column, invalid_value, issue_message, remediation)
    SELECT uuid(),
           '05269d28-15ae-5bd6-bd88-f949ccfa52d7',
           'a530fe1b-57ef-5a90-8bea-835ece2483da',
           'Missing Mandatory Value',
           issue_row,
           issue_column,
           invalid_value,
           'Mandatory field ' || issue_column || ' is empty',
           'Provide a value for ' || issue_column
      FROM mandatory_value;
WITH mandatory_value AS (
    SELECT 'serial number' AS issue_column,
           "serial number" AS invalid_value,
           src_file_row_number AS issue_row
      FROM "libre_data_240402_m_y_b001_libre_data"
     WHERE "serial number" IS NULL
        OR TRIM("serial number") = ''
)
INSERT INTO orch_session_issue (orch_session_issue_id, session_id, session_entry_id, issue_type, issue_row, issue_column, invalid_value, issue_message, remediation)
    SELECT uuid(),
           '05269d28-15ae-5bd6-bd88-f949ccfa52d7',
           'a530fe1b-57ef-5a90-8bea-835ece2483da',
           'Missing Mandatory Value',
           issue_row,
           issue_column,
           invalid_value,
           'Mandatory field ' || issue_column || ' is empty',
           'Provide a value for ' || issue_column
      FROM mandatory_value;
WITH mandatory_value AS (
    SELECT 'time stamp' AS issue_column,
           "time stamp" AS invalid_value,
           src_file_row_number AS issue_row
      FROM "libre_data_240402_m_y_b001_libre_data"
     WHERE "time stamp" IS NULL
        OR TRIM("time stamp") = ''
)
INSERT INTO orch_session_issue (orch_session_issue_id, session_id, session_entry_id, issue_type, issue_row, issue_column, invalid_value, issue_message, remediation)
    SELECT uuid(),
           '05269d28-15ae-5bd6-bd88-f949ccfa52d7',
           'a530fe1b-57ef-5a90-8bea-835ece2483da',
           'Missing Mandatory Value',
           issue_row,
           issue_column,
           invalid_value,
           'Mandatory field ' || issue_column || ' is empty',
           'Provide a value for ' || issue_column
      FROM mandatory_value;
WITH mandatory_value AS (
    SELECT 'glucose value' AS issue_column,
           "glucose value" AS invalid_value,
           src_file_row_number AS issue_row
      FROM "libre_data_240402_m_y_b001_libre_data"
     WHERE "glucose value" IS NULL
        OR TRIM("glucose value") = ''
)
INSERT INTO orch_session_issue (orch_session_issue_id, session_id, session_entry_id, issue_type, issue_row, issue_column, invalid_value, issue_message, remediation)
    SELECT uuid(),
           '05269d28-15ae-5bd6-bd88-f949ccfa52d7',
           'a530fe1b-57ef-5a90-8bea-835ece2483da',
           'Missing Mandatory Value',
           issue_row,
           issue_column,
           invalid_value,
           'Mandatory field ' || issue_column || ' is empty',
           'Provide a value for ' || issue_column
      FROM mandatory_value;
        
INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('6fcd9df5-34cf-5c09-8fb5-e73617e28d73', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'a530fe1b-57ef-5a90-8bea-835ece2483da', 'ATTEMPT_EXCEL_WORKBOOK_SHEET_ASSURANCE', 'ASSURED_EXCEL_WORKBOOK_SHEET', NULL, 'ScreeningExcelSheetIngestSource.assuranceSQL', (CURRENT_TIMESTAMP), NULL);
    
INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('10d0290c-b2eb-581e-b627-b5b8fcbb830f', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'e36daa69-3c63-5384-b6a7-03fa3b00641d', 'INGESTED_EXCEL_WORKBOOK_SHEET', 'ATTEMPT_EXCEL_WORKBOOK_SHEET_ASSURANCE', NULL, 'QeAdminDataExcelSheetIngestSource.assuranceSQL', (CURRENT_TIMESTAMP), NULL);

WITH mandatory_value AS (
    SELECT 'device' AS issue_column,
           "device" AS invalid_value,
           src_file_row_number AS issue_row
      FROM "libre_data_240402_m_y_b006_libre_data"
     WHERE "device" IS NULL
        OR TRIM("device") = ''
)
INSERT INTO orch_session_issue (orch_session_issue_id, session_id, session_entry_id, issue_type, issue_row, issue_column, invalid_value, issue_message, remediation)
    SELECT uuid(),
           '05269d28-15ae-5bd6-bd88-f949ccfa52d7',
           'e36daa69-3c63-5384-b6a7-03fa3b00641d',
           'Missing Mandatory Value',
           issue_row,
           issue_column,
           invalid_value,
           'Mandatory field ' || issue_column || ' is empty',
           'Provide a value for ' || issue_column
      FROM mandatory_value;
WITH mandatory_value AS (
    SELECT 'serial number' AS issue_column,
           "serial number" AS invalid_value,
           src_file_row_number AS issue_row
      FROM "libre_data_240402_m_y_b006_libre_data"
     WHERE "serial number" IS NULL
        OR TRIM("serial number") = ''
)
INSERT INTO orch_session_issue (orch_session_issue_id, session_id, session_entry_id, issue_type, issue_row, issue_column, invalid_value, issue_message, remediation)
    SELECT uuid(),
           '05269d28-15ae-5bd6-bd88-f949ccfa52d7',
           'e36daa69-3c63-5384-b6a7-03fa3b00641d',
           'Missing Mandatory Value',
           issue_row,
           issue_column,
           invalid_value,
           'Mandatory field ' || issue_column || ' is empty',
           'Provide a value for ' || issue_column
      FROM mandatory_value;
WITH mandatory_value AS (
    SELECT 'time stamp' AS issue_column,
           "time stamp" AS invalid_value,
           src_file_row_number AS issue_row
      FROM "libre_data_240402_m_y_b006_libre_data"
     WHERE "time stamp" IS NULL
        OR TRIM("time stamp") = ''
)
INSERT INTO orch_session_issue (orch_session_issue_id, session_id, session_entry_id, issue_type, issue_row, issue_column, invalid_value, issue_message, remediation)
    SELECT uuid(),
           '05269d28-15ae-5bd6-bd88-f949ccfa52d7',
           'e36daa69-3c63-5384-b6a7-03fa3b00641d',
           'Missing Mandatory Value',
           issue_row,
           issue_column,
           invalid_value,
           'Mandatory field ' || issue_column || ' is empty',
           'Provide a value for ' || issue_column
      FROM mandatory_value;
WITH mandatory_value AS (
    SELECT 'glucose value' AS issue_column,
           "glucose value" AS invalid_value,
           src_file_row_number AS issue_row
      FROM "libre_data_240402_m_y_b006_libre_data"
     WHERE "glucose value" IS NULL
        OR TRIM("glucose value") = ''
)
INSERT INTO orch_session_issue (orch_session_issue_id, session_id, session_entry_id, issue_type, issue_row, issue_column, invalid_value, issue_message, remediation)
    SELECT uuid(),
           '05269d28-15ae-5bd6-bd88-f949ccfa52d7',
           'e36daa69-3c63-5384-b6a7-03fa3b00641d',
           'Missing Mandatory Value',
           issue_row,
           issue_column,
           invalid_value,
           'Mandatory field ' || issue_column || ' is empty',
           'Provide a value for ' || issue_column
      FROM mandatory_value;
     
INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('7e65e3a7-4415-55f4-866b-3b0cc4e85fc6', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'e36daa69-3c63-5384-b6a7-03fa3b00641d', 'ATTEMPT_EXCEL_WORKBOOK_SHEET_ASSURANCE', 'ASSURED_EXCEL_WORKBOOK_SHEET', NULL, 'QeAdminDataExcelSheetIngestSource.assuranceSQL', (CURRENT_TIMESTAMP), NULL);
    
INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('e6951d0b-be59-58c3-8a04-01181208c601', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'b2a7c7e8-5ffe-5f28-8112-4eb7abb6397f', 'INGESTED_CSV', 'ATTEMPT_CSV_ASSURANCE', NULL, 'AdminDemographicCsvFileIngestSource.assuranceSQL', (CURRENT_TIMESTAMP), NULL);

     
INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('5b77d127-e62a-50a9-acee-bea63ff64dd5', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'b2a7c7e8-5ffe-5f28-8112-4eb7abb6397f', 'ATTEMPT_CSV_ASSURANCE', 'ASSURED_CSV', NULL, 'AdminDemographicCsvFileIngestSource.assuranceSQL', (CURRENT_TIMESTAMP), NULL);
    
INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('2afb3098-bcfd-5a54-8ebb-4d65d399c55e', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'fa7874f6-f848-572b-a9ab-9db4c8d5e959', 'INGESTED_CSV', 'ATTEMPT_CSV_ASSURANCE', NULL, 'ScreeningStatusCodeReferenceCsvFileIngestSource.assuranceSQL', (CURRENT_TIMESTAMP), NULL);

-- add field validation

INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('a92a6466-6fe4-58d7-8948-e2e09dc2fec2', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'fa7874f6-f848-572b-a9ab-9db4c8d5e959', 'ATTEMPT_CSV_ASSURANCE', 'ASSURED_CSV', NULL, 'ScreeningStatusCodeReferenceCsvFileIngestSource.assuranceSQL', (CURRENT_TIMESTAMP), NULL);
      
INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('0e074bf2-f1fe-55d4-bd44-a88cbed79aeb', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', '78d6a904-035e-54ae-8ac2-ca5cdf3f75f7', 'INGESTED_CSV', 'ATTEMPT_CSV_ASSURANCE', NULL, 'BusinessRulesReferenceCsvFileIngestSource.assuranceSQL', (CURRENT_TIMESTAMP), NULL);

-- add field validation

INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('34e90086-3d06-5b10-972d-7d0b40a02289', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', '78d6a904-035e-54ae-8ac2-ca5cdf3f75f7', 'ATTEMPT_CSV_ASSURANCE', 'ASSURED_CSV', NULL, 'BusinessRulesReferenceCsvFileIngestSource.assuranceSQL', (CURRENT_TIMESTAMP), NULL);
      
```
No STDOUT emitted by `ensureContent` (status: `0`).

No STDERR emitted by `ensureContent`.

    

## emitResources

```sql
-- preambleSQL
SET autoinstall_known_extensions=true;
SET autoload_known_extensions=true;
-- end preambleSQL
INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('05e8feaa-0bed-5909-a817-39812494b361', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', NULL, 'NONE', 'ENTER(prepareInit)', NULL, 'rsEE.beforeCell', ('2024-04-16T08:48:45.697Z'), NULL);
INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('8f460419-7b80-516d-8919-84520950f612', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', NULL, 'EXIT(prepareInit)', 'ENTER(init)', NULL, 'rsEE.afterCell', ('2024-04-16T08:48:45.697Z'), NULL);
INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('1931dfcc-e8fc-597d-b1bc-65b4287e6fdf', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', NULL, 'EXIT(init)', 'ENTER(ingest)', NULL, 'rsEE.afterCell', ('2024-04-16T08:48:45.697Z'), NULL);
INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('4971a2f5-06a3-5898-823d-364145d3b9a5', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', NULL, 'EXIT(ingest)', 'ENTER(ensureContent)', NULL, 'rsEE.afterCell', ('2024-04-16T08:48:45.697Z'), NULL);
INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('4f7e4436-c5f6-5ba1-9793-580ab66789fb', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', NULL, 'EXIT(ensureContent)', 'ENTER(emitResources)', NULL, 'rsEE.afterCell', ('2024-04-16T08:48:45.697Z'), NULL);

-- removed SQLPage and execution diagnostics SQL DML from diagnostics Markdown

CREATE VIEW orch_session_issue_classification AS
WITH cte_business_rule AS (
  SELECT worksheet as worksheet,
      field as field,
      required as required,
      "Resolved by QE/QCS" as resolved_by_qe_qcs,
      CONCAT(
          CASE WHEN UPPER("True Rejection") = 'YES' THEN 'REJECTION' ELSE '' END,
          CASE WHEN UPPER("Warning Layer") = 'YES' THEN 'WARNING' ELSE '' END
      ) AS record_action
  FROM
      "ingestion-center".main.business_rules
)
--select * from cte_business_rule

SELECT
  -- Including all other columns from 'orch_session'
  ises.* EXCLUDE (orch_started_at, orch_finished_at),
  -- TODO: Casting known timestamp columns to text so emit to Excel works with GDAL (spatial)
    -- strftime(timestamptz orch_started_at, '%Y-%m-%d %H:%M:%S') AS orch_started_at,
    -- strftime(timestamptz orch_finished_at, '%Y-%m-%d %H:%M:%S') AS orch_finished_at,
  -- Including all columns from 'orch_session_entry'
  isee.* EXCLUDE (session_id),
  -- Including all other columns from 'orch_session_issue'
  isi.* EXCLUDE (session_id, session_entry_id),
  CASE
    WHEN
        UPPER(isi.issue_type) = 'MISSING COLUMN'
      THEN
        'STRUCTURAL ISSUE'
      ELSE
        br.record_action
    END
  AS disposition,
  case when UPPER(br.resolved_by_qe_qcs) = 'YES' then 'Resolved By QE/QCS' else null end AS remediation
  FROM orch_session AS ises
  JOIN orch_session_entry AS isee ON ises.orch_session_id = isee.session_id
  LEFT JOIN orch_session_issue AS isi ON isee.orch_session_entry_id = isi.session_entry_id
  --LEFT JOIN business_rules br ON isi.issue_column = br.FIELD
  LEFT OUTER JOIN cte_business_rule br ON br.field = isi.issue_column
  WHERE isi.orch_session_issue_id IS NOT NULL
;

ATTACH '/home/vinod/projects/Diabetes Research Hub DRH_DOLT/INGESTION_GITHUB/personal_vinod.wst/ingestion-center/support/assurance/ingestion-center-elt/drh/results-test-e2e/resource.sqlite.db' AS resource_db (TYPE SQLITE);

-- copy relevant orchestration engine admin tables into the the attached database
CREATE TABLE resource_db.device AS SELECT * FROM device;
CREATE TABLE resource_db.orch_session AS SELECT * FROM orch_session;
CREATE TABLE resource_db.orch_session_entry AS SELECT * FROM orch_session_entry;
CREATE TABLE resource_db.orch_session_state AS SELECT * FROM orch_session_state;
CREATE TABLE resource_db.orch_session_exec AS SELECT * FROM orch_session_exec;
CREATE TABLE resource_db.orch_session_issue AS SELECT * FROM orch_session_issue;
CREATE TABLE resource_db.sqlpage_files AS SELECT * FROM sqlpage_files;

-- export content tables from DuckDb into the attached database (nature-dependent)
INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('86ff3ab6-900d-5474-b63c-cbcac3c66f1a', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', '8b7c669c-1795-5f6b-8f3a-3e502b74c628', 'ASSURED_CSV', 'EXIT(ScreeningCsvFileIngestSource)', NULL, 'ScreeningCsvFileIngestSource.exportResourceSQL', (CURRENT_TIMESTAMP), NULL);

  INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('413ec5cd-eee9-5c62-90a5-6670f8b9ddff', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', '8b7c669c-1795-5f6b-8f3a-3e502b74c628', 'ATTEMPT_CSV_EXPORT', 'EXIT(ScreeningCsvFileIngestSource)', NULL, 'ScreeningCsvFileIngestSource.exportResourceSQL', (CURRENT_TIMESTAMP), NULL);

CREATE TABLE IF NOT EXISTS libre AS SELECT * FROM libre_20240307 WHERE 0=1;
INSERT INTO libre SELECT * FROM libre_20240307;

CREATE TABLE resource_db.libre_20240307 AS SELECT * FROM libre_20240307;

CREATE TABLE IF NOT EXISTS resource_db.libre AS SELECT * FROM libre_20240307 WHERE 0=1;
INSERT INTO resource_db.libre SELECT * FROM libre_20240307;



  
INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('f6d4aff4-4b71-5662-8f57-00ee247dc57c', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'b41ccd27-9a4f-5cc8-9c5d-b55242d90fb0', 'ASSURED_CSV', 'EXIT(QeAdminDataCsvFileIngestSource)', NULL, 'QeAdminDataCsvFileIngestSource.exportResourceSQL', (CURRENT_TIMESTAMP), NULL);

CREATE TABLE IF NOT EXISTS glucose AS SELECT * FROM glucose_20240307 WHERE 0=1;
INSERT INTO glucose SELECT * FROM glucose_20240307;

CREATE TABLE resource_db.glucose_20240307 AS SELECT * FROM glucose_20240307;

CREATE TABLE IF NOT EXISTS resource_db.glucose AS SELECT * FROM glucose_20240307 WHERE 0=1;
INSERT INTO resource_db.glucose SELECT * FROM glucose_20240307;

  INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('6c48996f-0dd4-572f-b087-e5913926cd4b', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'b41ccd27-9a4f-5cc8-9c5d-b55242d90fb0', 'ATTEMPT_CSV_EXPORT', 'EXIT(ScreeningCsvFileIngestSource)', NULL, 'QeAdminDataCsvFileIngestSource.exportResourceSQL', (CURRENT_TIMESTAMP), NULL);
  
INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('6202ec4a-f3d5-5302-9ed6-9cb59a5b2818', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', '3b4eb0e5-6239-537a-8e67-e50e172e72a2', 'ASSURED_EXCEL_WORKBOOK_SHEET', 'ATTEMPT_EXCEL_WORKBOOK_SHEET_EXPORT', NULL, 'AdminDemographicExcelSheetIngestSource.exportResourceSQL', (CURRENT_TIMESTAMP), NULL);

CREATE TABLE resource_db.libre_data_240402_m_y_b004_libre_data AS SELECT * FROM libre_data_240402_m_y_b004_libre_data;

INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('bebf797d-855b-5e76-93d2-2a802febd5a2', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', '3b4eb0e5-6239-537a-8e67-e50e172e72a2', 'ATTEMPT_EXCEL_WORKBOOK_SHEET_EXPORT', 'EXIT(AdminDemographicExcelSheetIngestSource)', NULL, 'AdminDemographicExcelSheetIngestSource.exportResourceSQL', (CURRENT_TIMESTAMP), NULL);
    
INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('4b7537b2-9d60-59f3-8c61-fa2ff4265c02', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'a530fe1b-57ef-5a90-8bea-835ece2483da', 'ASSURED_EXCEL_WORKBOOK_SHEET', 'ATTEMPT_EXCEL_WORKBOOK_SHEET_EXPORT', NULL, 'ScreeningExcelSheetIngestSource.exportResourceSQL', (CURRENT_TIMESTAMP), NULL);

CREATE TABLE resource_db.libre_data_240402_m_y_b001_libre_data AS SELECT * FROM libre_data_240402_m_y_b001_libre_data;

INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('17cedd6e-e794-5b45-9790-c4ba2483cc1e', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'a530fe1b-57ef-5a90-8bea-835ece2483da', 'ATTEMPT_EXCEL_WORKBOOK_SHEET_EXPORT', 'EXIT(ScreeningExcelSheetIngestSource)', NULL, 'ScreeningExcelSheetIngestSource.exportResourceSQL', (CURRENT_TIMESTAMP), NULL);
    
INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('657d6337-8d24-5b67-b139-87db6a228264', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'e36daa69-3c63-5384-b6a7-03fa3b00641d', 'ASSURED_EXCEL_WORKBOOK_SHEET', 'ATTEMPT_EXCEL_WORKBOOK_SHEET_EXPORT', NULL, 'QeAdminDataExcelSheetIngestSource.exportResourceSQL', (CURRENT_TIMESTAMP), NULL);

CREATE TABLE resource_db.libre_data_240402_m_y_b006_libre_data AS SELECT * FROM libre_data_240402_m_y_b006_libre_data;

INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('c40829eb-7f91-583a-8af8-06de851777a0', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'e36daa69-3c63-5384-b6a7-03fa3b00641d', 'ATTEMPT_EXCEL_WORKBOOK_SHEET_EXPORT', 'EXIT(AdminDemographicExcelSheetIngestSource)', NULL, 'QeAdminDataExcelSheetIngestSource.exportResourceSQL', (CURRENT_TIMESTAMP), NULL);
    
INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('9f13dd7d-9ff8-509d-b716-cde856c5f0f0', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'b2a7c7e8-5ffe-5f28-8112-4eb7abb6397f', 'ASSURED_CSV', 'EXIT(AdminDemographicCsvFileIngestSource)', NULL, 'AdminDemographicCsvFileIngestSource.exportResourceSQL', (CURRENT_TIMESTAMP), NULL);

CREATE TABLE IF NOT EXISTS vitalsigns AS SELECT * FROM vitalsigns_20240307 WHERE 0=1;
INSERT INTO vitalsigns SELECT * FROM vitalsigns_20240307;

CREATE TABLE resource_db.vitalsigns_20240307 AS SELECT * FROM vitalsigns_20240307;

CREATE TABLE IF NOT EXISTS resource_db.vitalsigns AS SELECT * FROM vitalsigns_20240307 WHERE 0=1;
INSERT INTO resource_db.vitalsigns SELECT * FROM vitalsigns_20240307;



  INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('a8ec8b43-9e16-5eeb-9683-bc14288971f1', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'b2a7c7e8-5ffe-5f28-8112-4eb7abb6397f', 'ATTEMPT_CSV_EXPORT', 'EXIT(ScreeningCsvFileIngestSource)', NULL, 'AdminDemographicCsvFileIngestSource.exportResourceSQL', (CURRENT_TIMESTAMP), NULL);
  
INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('9c0d34d3-bf09-527a-aef5-85004a400be5', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'fa7874f6-f848-572b-a9ab-9db4c8d5e959', 'ASSURED_CSV', 'EXIT(ScreeningStatusCodeReferenceCsvFileIngestSource)', NULL, 'ScreeningStatusCodeReferenceCsvFileIngestSource.exportResourceSQL', (CURRENT_TIMESTAMP), NULL);

CREATE TABLE IF NOT EXISTS resource_db.screening_status_code_reference AS SELECT * FROM screening_status_code_reference WHERE 0=1;
INSERT INTO resource_db.screening_status_code_reference SELECT * FROM screening_status_code_reference;

  INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('35c62034-5b20-5891-8d38-3e9b051dec6e', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', 'fa7874f6-f848-572b-a9ab-9db4c8d5e959', 'ATTEMPT_CSV_EXPORT', 'EXIT(ScreeningStatusCodeReferenceCsvFileIngestSource)', NULL, 'ScreeningStatusCodeReferenceCsvFileIngestSource.exportResourceSQL', (CURRENT_TIMESTAMP), NULL);
  
INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('e2816d61-4406-5073-ac60-f129a107d938', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', '78d6a904-035e-54ae-8ac2-ca5cdf3f75f7', 'ASSURED_CSV', 'EXIT(BusinessRulesReferenceCsvFileIngestSource)', NULL, 'BusinessRulesReferenceCsvFileIngestSource.exportResourceSQL', (CURRENT_TIMESTAMP), NULL);

CREATE TABLE IF NOT EXISTS resource_db.business_rules AS SELECT * FROM business_rules WHERE 0=1;
INSERT INTO resource_db.business_rules SELECT * FROM business_rules;

  INSERT INTO "orch_session_state" ("orch_session_state_id", "session_id", "session_entry_id", "from_state", "to_state", "transition_result", "transition_reason", "transitioned_at", "elaboration") VALUES ('aa8b8d1a-c8cc-5a9b-b5aa-34a6fc85e11a', '05269d28-15ae-5bd6-bd88-f949ccfa52d7', '78d6a904-035e-54ae-8ac2-ca5cdf3f75f7', 'ATTEMPT_CSV_EXPORT', 'EXIT(BusinessRulesReferenceCsvFileIngestSource)', NULL, 'BusinessRulesReferenceCsvFileIngestSource.exportResourceSQL', (CURRENT_TIMESTAMP), NULL);
  ;


DETACH DATABASE resource_db;

-- no after-finalize SQL provided
```
No STDOUT emitted by `emitResources` (status: `0`).

No STDERR emitted by `emitResources`.

    

## jsonResult_4

```sql
-- preambleSQL
SET autoinstall_known_extensions=true;
SET autoload_known_extensions=true;
-- end preambleSQL
SELECT COUNT(*) AS table_count FROM information_schema.tables WHERE table_name IN ('libre', 'vitalsigns', 'glucose');
      
```
### `jsonResult_4` STDOUT (status: `0`)
```json
[{"table_count":3}]

```
No STDERR emitted by `jsonResult_4`.

    

## emitDiagnostics

```sql
-- preambleSQL
SET autoinstall_known_extensions=true;
SET autoload_known_extensions=true;
-- end preambleSQL
INSTALL spatial; LOAD spatial;
-- TODO: join with orch_session table to give all the results in one sheet
COPY (SELECT * FROM orch_session_issue_classification) TO '/home/vinod/projects/Diabetes Research Hub DRH_DOLT/INGESTION_GITHUB/personal_vinod.wst/ingestion-center/support/assurance/ingestion-center-elt/drh/results-test-e2e/diagnostics.xlsx' WITH (FORMAT GDAL, DRIVER 'xlsx');
```
No STDOUT emitted by `emitDiagnostics` (status: `0`).

No STDERR emitted by `emitDiagnostics`.

    