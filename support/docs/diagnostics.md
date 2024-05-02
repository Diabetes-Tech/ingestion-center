# Overview

This document provides an overview of the files and folders present in the
diagnostics package. The diagnostics package includes files and folders in the
`/egress/<SESSION_ID>` file path. The following are the main files and folders
available inside each package:

- `/.consumed` folder.
- `/.workflow` folder.
- `session.json` file.
- `resource.sqlite.db` database file.
- `diagnostics.md` file.
- `diagnostics.xlsx` file.
- `diagnostics.json` file.

# Session Data (/egress/<SESSION_ID>/session.json)

This JSON file contains metadata and configuration details about the diagnostics
session. It includes information about ingress paths, initialization timestamp,
session ID, source files being processed,  json validated results and more.
We need to verify these data first for getting the progress of the session and
to know the result of each stages. Below are the fields in the session.

## Fields

- `ingressPaths`: Paths for ingressing data into the system.
  - `home`: The home directory for ingressing data.
- `initAt`: The timestamp when the session was initialized.
- `sessionID`: The unique identifier for the session.
- `src`: An array of source files being processed in the session.
  - `fsPath`: The filesystem path of the source file.
  - `watchPath`: Information about the watch path for the file.
    - `pathID`: The identifier of the path.
    - `rootPath`: The root path being watched.
  - `draining`: Indicates if the file is being drained (processed).
- `version`: The version of the application used for the session.
- `consumed`: An array of activities performed on the consumed files.
  - `activity`: The type of activity performed (e.g., move).
  - `fsPath`: The filesystem path of the file on which the activity was
    performed.
- `stdErrsEncountered`: Information about any standard errors encountered during
  the session.
- `diagsMarkdown`: The path to the Markdown file containing diagnostics
  information.
- `duckDb`: Information about the DuckDB database containing raw ingested
  content and validation tables.
- `sqliteDB`: Information about the SQLite database containing aggregated
  content and validation tables.
- `referenceDataHome`: The path to the home directory for reference data.
- `publishJSONURL`: The URL for publishing JSON data.
- `publishJSONResult`: An array of results from publishing JSON data.
  - `response`: The response from the JSON publishing endpoint.
  - `JSONJsonStructValid`: Indicates if the JSON JSON structure is valid.
- `finalizeAt`: The timestamp when the session was finalized.

# Diagnostics .xlsx (/egress/<SESSION_ID>/diagnostics.xlsx)

An overview of the issues, validations and remediations from the session is
recorded in the `diagnostics.xlsx` file, where we can investigate the content &
structural errors especially with the csv files ingested in the session.
Following are the fields which are present in the file.

## Fields

- `orch_session_id`: The unique identifier for the orchestration session.
- `device_id`: The identifier of the device from which the data was collected.
- `version`: The version of the 1115-hub application used to collect the data.
- `elaboration`: A numerical value representing the level of detail in the
  diagnostics data.
- `args_json`: JSON-encoded arguments passed to the diagnostics tool.
- `diagnostics_json`: JSON-encoded diagnostics data.
- `diagnostics_md`: Markdown-formatted diagnostics data for human-readable
  presentation.
- `orch_session_entry_id`: The unique identifier for the entry in the
  orchestration session.
- `ingest_src`: The source from which the diagnostics data was ingested.
- `ingest_table_name`: The name of the table into which the diagnostics data was
  ingested.
- `elaboration:1`: A numerical value representing an additional level of detail
  in the diagnostics data.
- `orch_session_issue_id`: The unique identifier for an issue detected in the
  orchestration session.
- `issue_type`: The type of issue detected.
- `issue_message`: A message describing the issue.
- `issue_row`: The row number in the data CSV where the issue was detected.
- `issue_column`: The column name in the data where the issue was detected.
- `invalid_value`: The value that was identified as invalid during diagnostics.
- `remediation`: Suggested remediation steps for resolving the issue.
- `elaboration:2`: Another numerical value representing an additional level of
  detail in the diagnostics data.
- `disposition`: The final disposition of the diagnostics data (e.g., warning,
  rejection).
- `remediation:1`: Additional suggested remediation steps for resolving the
  issue needed from QE/QCS side.

# SQLite Database (/egress/<SESSION_ID>/resource.sqlite.db)

The `resource.sqlite.db` file is a SQLite database that contains the content and
admin tables. It is used to store and manage data processed during the
diagnostics session.

## Tables

The database contains various tables, each serving a specific purpose in the
context of the diagnostics package. Common tables include:

- `Admin Tables`: These tables contain information related to the diagnostics
  session, validation of the data processed during the session. They include
  details about any issues or errors encountered and their respective
  remediation steps.
- `Content Tables`: These tables store individual and aggregated data derived
  from the raw data ingested during the diagnostics session. They provide a
  consolidated view of the data for further analysis and processing.

# Diagnostics .md (/egress/<SESSION_ID>/diagnostics.md)

The `diagnostics.md` file is a Markdown file that contains detailed information
about the diagnostics session. It includes various sections that outline the
steps and actions taken during the session, as well as any issues or errors
encountered. This file can be used to review and understand the steps taken
during a diagnostics session, as well as to troubleshoot any issues that may
have arisen.

## Contents

- `workflowPaths`: Paths used during the diagnostics session for ingressing,
  processing, and egressing data.
- `walkRootPaths`: Paths that were walked during the session to collect data.
- `referenceDataHome`: The path to the directory containing reference data used
  during the session.
- `sources`: A list of source files processed during the session, including
  their URIs, nature (e.g., CSV), table names, and any ingestion issues.
- `init`: SQL statements and other actions taken during the initialization phase
  of the session.
- `ingest`: Details about the ingestion of data, including SQL statements and
  state management diagnostics.
- `ensureContent`: Actions taken to ensure the content is as expected.
- `emitResources`: Details about the emission of resources during the session.
- `emitDiagnostics`: Information about the diagnostics emitted during the
  session.
- `jsonResult_5`: Example of a JSON result from a specific step in the session,
  including STDOUT and status.

# Diagnostics .json (/egress/<SESSION_ID>/diagnostics.json)

The `diagnostics.json` file contains detailed information about the diagnostics
session in JSON format. It includes data about the workflow paths, sources
processed, and diagnostics details for each step of the session.

## Fields

- `args`: Arguments and configuration settings used during the diagnostics
  session.
  - `workflowPaths`: Paths used for ingressing, processing, and egressing data.
  - `walkRootPaths`: Paths that were walked during the session to collect data.
  - `referenceDataHome`: The path to the directory containing reference data.
  - `sources`: A list of source files processed during the session, including
    their URIs, nature (CSV, JSON, etc.), table names, and any ingestion issues.

- `diags`: An array of diagnostics entries, each containing information about a
  specific step in the session.
  - `insertable`: An object containing details about the execution of a step,
    such as the identity, code, SQL statements, session ID, and any narrative or
    error messages.

# .consumed folder (/egress/<SESSION_ID>/.consumed)

This folder contains all the files that were put into the /ingress folders and
were successfully ingested. There could be a single or multiple sets of .csv
data files.

# .workflow folder (/egress/<SESSION_ID>/.workflow)

This folder contains the duckdb file `ingestion-center.duckdb`. The
`ingestion-center.duckdb` is a DuckDB database that contains the content and
admin tables. It is used to store and manage data processed during the
diagnostics session.
