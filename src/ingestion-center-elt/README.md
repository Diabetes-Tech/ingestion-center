# DRH Assurance and ETL

This module is a Deno TypeScript library for building type-safe, reliable and
scalable procedures for extracting Accountable Health Communities (AHC)
Health-Related Social Needs (HRSN) drh data, validating ("assuring") it,
transforming it, enriching it and loading it in a relational or other database.

## Layout

```
/
├── src/
│   └── ingestion-center-elt/                  # code for AHC HRSN functionality
│       └── drh/                 # 
│           ├── csv.ts                 # module which helps build DuckDB SQL for CSV ingestion
│           ├── deps.ts                # all external dependencies used by this module
│           ├── excel.ts               # module which helps build DuckDB SQL for Excel workbook ingestion
│           ├── governance.ts          # business rules and other "governance" code  
│           ├── orchestrate.ts         # orchestration workflow (notebook cells) code
│           ├── sqlpage.ts             # SQLPage server diagnostics code notebook 
│           ├── mod.ts                 # Deno module entrypoint when building your own code
│           └── test-e2e.ts            # code executed when `deno task ahc-hrsn-drh-test-e2e` is run
|
└── support/                           # scripts, libraries, and modules which support this project for development or deployment
    ├── assurance/                     # quality assurance code and utilities
    │   └── ingestion-center-elt/              # 
    │       └── drh/             # 
    │           ├── results-test-e2e/  # location where src/ingestion-center-elt/drh/test-e2e.ts emits results
    │           └── synthetic-content/ # location where src/ingestion-center-elt/drh/test-e2e.ts sources content
    │
    ├── bin/                           # doctor.ts and other supporting binaries
    │
    └── docs/                          # documentation and related artifacts
```
