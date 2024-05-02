import { SQLa_orch as o, SQLa_orch_duckdb as ddbo } from "./deps.ts";
import * as sg from "./governance.ts";


const screeningStatusCodeReferenceColumnNames = [
  "Lvl",
  "Code",
  "Display",
  "Definition",
] as const;

type ScreeningStatusCodeReferenceColumnName =
  (typeof screeningStatusCodeReferenceColumnNames)[number];

const SCREENING_STATUS_CODE_TERMINAL_STATE =
  "EXIT(ScreeningStatusCodeReferenceCsvFileIngestSource)" as const;

export class ScreeningStatusCodeReferenceStructureRules<
  TableName extends string,
> extends ddbo.DuckDbOrchTableAssuranceRules<
  TableName,
  ScreeningStatusCodeReferenceColumnName
> {
  requiredColumnNames() {
    return this.tableRules.requiredColumnNamesStrict([
      ...screeningStatusCodeReferenceColumnNames,
    ]);
  }
}
export class ScreeningStatusCodeReferenceCsvFileIngestSource<
  TableName extends "screening_status_code_reference",
  InitState extends o.State,
> implements
  o.CsvFileIngestSource<
    TableName,
    ddbo.DuckDbOrchGovernance,
    InitState,
    typeof SCREENING_STATUS_CODE_TERMINAL_STATE,
    ddbo.DuckDbOrchEmitContext
  > {
  readonly nature = "CSV";
  constructor(
    readonly dataHome: string,
    readonly govn: ddbo.DuckDbOrchGovernance,
    readonly tableName = "screening_status_code_reference" as TableName,
    readonly uri = `${dataHome}/screening-status-code-reference.csv`,
  ) {
  }

  // deno-lint-ignore require-await
  async workflow(
    session: o.OrchSession<
      ddbo.DuckDbOrchGovernance,
      ddbo.DuckDbOrchEmitContext
    >,
    sessionEntryID: string,
  ): ReturnType<
    o.CsvFileIngestSource<
      TableName,
      ddbo.DuckDbOrchGovernance,
      InitState,
      typeof SCREENING_STATUS_CODE_TERMINAL_STATE,
      ddbo.DuckDbOrchEmitContext
    >["workflow"]
  > {
    const ssr = new ScreeningStatusCodeReferenceStructureRules(
      this.tableName,
      session.sessionID,
      sessionEntryID,
      this.govn,
    );
    const sar = new sg.ScreeningStatusCodeReferenceAssuranceRules(
      this.tableName,
      session.sessionID,
      sessionEntryID,
      this.govn,
    );

    return {
      ingestSQL: async (issac) =>
        await this.ingestSQL(session, issac, ssr, sar),
      assuranceSQL: async () => await this.assuranceSQL(session, sar),
      exportResourceSQL: async (targetSchema) =>
        await this.exportResourceSQL(session, sar.sessionEntryID, targetSchema),
      terminalState: () => SCREENING_STATUS_CODE_TERMINAL_STATE,
    };
  }

  async ingestSQL(
    session: o.OrchSession<
      ddbo.DuckDbOrchGovernance,
      ddbo.DuckDbOrchEmitContext
    >,
    issac: o.IngestSourceStructAssuranceContext<
      InitState,
      ddbo.DuckDbOrchEmitContext
    >,
    ssr: ScreeningStatusCodeReferenceStructureRules<TableName>,
    sar: sg.ScreeningStatusCodeReferenceAssuranceRules<
      TableName,
      ScreeningStatusCodeReferenceColumnName
    >,
  ) {
    const { tableName, uri } = this;
    const { sessionID, sessionEntryID } = sar;

    // deno-fmt-ignore
    return this.govn.SQL`
        -- required by IngestEngine, setup the ingestion entry for logging
        ${await issac.sessionEntryInsertDML()}

        -- state management diagnostics
        ${await session.entryStateDML(
          sessionEntryID,
          issac.initState(),
          "ATTEMPT_CSV_INGEST",
          "ScreeningStatusCodeReferenceCsvFileIngestSource.ingestSQL",
          this.govn.emitCtx.sqlEngineNow
        )}

        -- be sure to add src_file_row_number and session_id columns to each row
        -- because assurance CTEs require them
        CREATE TABLE ${tableName} AS
          SELECT *, row_number() OVER () as src_file_row_number, '${sessionID}' as session_id, '${sessionEntryID}' as session_entry_id
            FROM read_csv_auto('${uri}',
              header = true
            );

        ${ssr.requiredColumnNames()}

        ${await session.entryStateDML(
          sessionEntryID,
          "ATTEMPT_CSV_INGEST",
          "INGESTED_CSV",
          "ScreeningStatusCodeReferenceCsvFileIngestSource.ingestSQL",
          this.govn.emitCtx.sqlEngineNow
        )}
      `;
  }

  async assuranceSQL(
    session: o.OrchSession<
      ddbo.DuckDbOrchGovernance,
      ddbo.DuckDbOrchEmitContext
    >,
    sar: sg.ScreeningStatusCodeReferenceAssuranceRules<
      TableName,
      ScreeningStatusCodeReferenceColumnName
    >,
  ) {
    const { govn } = this;
    const { sessionEntryID, tableRules: tr } = sar;

    // deno-fmt-ignore
    return govn.SQL`
        ${await session.entryStateDML(
          sessionEntryID,
          "INGESTED_CSV",
          "ATTEMPT_CSV_ASSURANCE",
          "ScreeningStatusCodeReferenceCsvFileIngestSource.assuranceSQL",
          this.govn.emitCtx.sqlEngineNow
        )}

        -- add field validation

        ${await session.entryStateDML(
          sessionEntryID,
          "ATTEMPT_CSV_ASSURANCE",
          "ASSURED_CSV",
          "ScreeningStatusCodeReferenceCsvFileIngestSource.assuranceSQL",
          this.govn.emitCtx.sqlEngineNow
        )}
      `;
  }

  async exportResourceSQL(
    session: o.OrchSession<
      ddbo.DuckDbOrchGovernance,
      ddbo.DuckDbOrchEmitContext
    >,
    sessionEntryID: string,
    targetSchema: string,
  ) {
    const {
      govn: { SQL },
      tableName,
    } = this;
    // deno-fmt-ignore
    return SQL`
        ${await session.entryStateDML(
          sessionEntryID,
          "ASSURED_CSV",
          "EXIT(ScreeningStatusCodeReferenceCsvFileIngestSource)",
          "ScreeningStatusCodeReferenceCsvFileIngestSource.exportResourceSQL",
          this.govn.emitCtx.sqlEngineNow
        )}

        CREATE TABLE IF NOT EXISTS ${targetSchema}.${tableName} AS SELECT * FROM ${tableName} WHERE 0=1;
        INSERT INTO ${targetSchema}.${tableName} SELECT * FROM ${tableName};

          ${await session.entryStateDML(
            sessionEntryID,
            "ATTEMPT_CSV_EXPORT",
            SCREENING_STATUS_CODE_TERMINAL_STATE,
            "ScreeningStatusCodeReferenceCsvFileIngestSource.exportResourceSQL",
            this.govn.emitCtx.sqlEngineNow
          )}
          `;
  }
}




const businessRulesReferenceColumnNames = [
  "Worksheet",
  "Field",
  "Required",
  "Permissible Values",
  "True Rejection",
  "Warning Layer",
  "Resolved by QE/QCS",
] as const;

type BusinessRulesReferenceColumnName =
  (typeof businessRulesReferenceColumnNames)[number];

const BUSINESS_RULES_TERMINAL_STATE =
  "EXIT(BusinessRulesReferenceCsvFileIngestSource)" as const;

export class BusinessRulesReferenceStructureRules<
  TableName extends string,
> extends ddbo.DuckDbOrchTableAssuranceRules<
  TableName,
  BusinessRulesReferenceColumnName
> {
  requiredColumnNames() {
    return this.tableRules.requiredColumnNamesStrict([
      ...businessRulesReferenceColumnNames,
    ]);
  }
}
export class BusinessRulesReferenceCsvFileIngestSource<
  TableName extends "business_rules",
  InitState extends o.State,
> implements
  o.CsvFileIngestSource<
    TableName,
    ddbo.DuckDbOrchGovernance,
    InitState,
    typeof BUSINESS_RULES_TERMINAL_STATE,
    ddbo.DuckDbOrchEmitContext
  > {
  readonly nature = "CSV";
  constructor(
    readonly dataHome: string,
    readonly govn: ddbo.DuckDbOrchGovernance,
    readonly tableName = "business_rules" as TableName,
    readonly uri = `${dataHome}/business-rules.csv`,
  ) {
  }

  // deno-lint-ignore require-await
  async workflow(
    session: o.OrchSession<
      ddbo.DuckDbOrchGovernance,
      ddbo.DuckDbOrchEmitContext
    >,
    sessionEntryID: string,
  ): ReturnType<
    o.CsvFileIngestSource<
      TableName,
      ddbo.DuckDbOrchGovernance,
      InitState,
      typeof BUSINESS_RULES_TERMINAL_STATE,
      ddbo.DuckDbOrchEmitContext
    >["workflow"]
  > {
    const ssr = new BusinessRulesReferenceStructureRules(
      this.tableName,
      session.sessionID,
      sessionEntryID,
      this.govn,
    );
    const sar = new sg.BusinessRulesReferenceAssuranceRules(
      this.tableName,
      session.sessionID,
      sessionEntryID,
      this.govn,
    );

    return {
      ingestSQL: async (issac) =>
        await this.ingestSQL(session, issac, ssr, sar),
      assuranceSQL: async () => await this.assuranceSQL(session, sar),
      exportResourceSQL: async (targetSchema) =>
        await this.exportResourceSQL(session, sar.sessionEntryID, targetSchema),
      terminalState: () => BUSINESS_RULES_TERMINAL_STATE,
    };
  }

  async ingestSQL(
    session: o.OrchSession<
      ddbo.DuckDbOrchGovernance,
      ddbo.DuckDbOrchEmitContext
    >,
    issac: o.IngestSourceStructAssuranceContext<
      InitState,
      ddbo.DuckDbOrchEmitContext
    >,
    ssr: BusinessRulesReferenceStructureRules<TableName>,
    sar: sg.BusinessRulesReferenceAssuranceRules<
      TableName,
      BusinessRulesReferenceColumnName
    >,
  ) {
    const { tableName, uri } = this;
    const { sessionID, sessionEntryID } = sar;

    // deno-fmt-ignore
    return this.govn.SQL`
        -- required by IngestEngine, setup the ingestion entry for logging
        ${await issac.sessionEntryInsertDML()}

        -- state management diagnostics
        ${await session.entryStateDML(
          sessionEntryID,
          issac.initState(),
          "ATTEMPT_CSV_INGEST",
          "BusinessRulesReferenceCsvFileIngestSource.ingestSQL",
          this.govn.emitCtx.sqlEngineNow
        )}

        -- be sure to add src_file_row_number and session_id columns to each row
        -- because assurance CTEs require them
        CREATE TABLE ${tableName} AS
          SELECT *, row_number() OVER () as src_file_row_number, '${sessionID}' as session_id, '${sessionEntryID}' as session_entry_id
            FROM read_csv_auto('${uri}',
              header = true
            );

        ${ssr.requiredColumnNames()}

        ${await session.entryStateDML(
          sessionEntryID,
          "ATTEMPT_CSV_INGEST",
          "INGESTED_CSV",
          "BusinessRulesReferenceCsvFileIngestSource.ingestSQL",
          this.govn.emitCtx.sqlEngineNow
        )}
      `;
  }

  async assuranceSQL(
    session: o.OrchSession<
      ddbo.DuckDbOrchGovernance,
      ddbo.DuckDbOrchEmitContext
    >,
    sar: sg.BusinessRulesReferenceAssuranceRules<
      TableName,
      BusinessRulesReferenceColumnName
    >,
  ) {
    const { govn } = this;
    const { sessionEntryID, tableRules: tr } = sar;

    // deno-fmt-ignore
    return govn.SQL`
        ${await session.entryStateDML(
          sessionEntryID,
          "INGESTED_CSV",
          "ATTEMPT_CSV_ASSURANCE",
          "BusinessRulesReferenceCsvFileIngestSource.assuranceSQL",
          this.govn.emitCtx.sqlEngineNow
        )}

        -- add field validation

        ${await session.entryStateDML(
          sessionEntryID,
          "ATTEMPT_CSV_ASSURANCE",
          "ASSURED_CSV",
          "BusinessRulesReferenceCsvFileIngestSource.assuranceSQL",
          this.govn.emitCtx.sqlEngineNow
        )}
      `;
  }

  async exportResourceSQL(
    session: o.OrchSession<
      ddbo.DuckDbOrchGovernance,
      ddbo.DuckDbOrchEmitContext
    >,
    sessionEntryID: string,
    targetSchema: string,
  ) {
    const {
      govn: { SQL },
      tableName,
    } = this;
    // deno-fmt-ignore
    return SQL`
        ${await session.entryStateDML(
          sessionEntryID,
          "ASSURED_CSV",
          "EXIT(BusinessRulesReferenceCsvFileIngestSource)",
          "BusinessRulesReferenceCsvFileIngestSource.exportResourceSQL",
          this.govn.emitCtx.sqlEngineNow
        )}

        CREATE TABLE IF NOT EXISTS ${targetSchema}.${tableName} AS SELECT * FROM ${tableName} WHERE 0=1;
        INSERT INTO ${targetSchema}.${tableName} SELECT * FROM ${tableName};

          ${await session.entryStateDML(
            sessionEntryID,
            "ATTEMPT_CSV_EXPORT",
            BUSINESS_RULES_TERMINAL_STATE,
            "BusinessRulesReferenceCsvFileIngestSource.exportResourceSQL",
            this.govn.emitCtx.sqlEngineNow
          )}
          `;
  }
}




