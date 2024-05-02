import { path, SQLa_orch as o, SQLa_orch_duckdb as ddbo } from "./deps.ts";
import * as sg from "./governance.ts";

// @deno-types="https://cdn.sheetjs.com/xlsx-0.20.1/package/types/index.d.ts"
import * as xlsx from "https://cdn.sheetjs.com/xlsx-0.20.1/package/xlsx.mjs";

const libreColumnNames = [
  "device",
  "serial number",
  "time stamp",
  "glucose value",
] as const;

const glucoseColumnNames = [
  "device",
  "serial number",
  "time stamp",
  "glucose value",
] as const;

const vitalSignsDataColumnNames = [
  "device",
  "serial number",
  "time stamp",
  "glucose value",
] as const;

type LibreColumnName = (typeof libreColumnNames)[number];

type GlucoseColumnName = (typeof glucoseColumnNames)[number];

type VitalSignsDataColumnName = (typeof vitalSignsDataColumnNames)[number];

class LibreStructureRules<
  TableName extends string
> extends ddbo.DuckDbOrchTableAssuranceRules<TableName, LibreColumnName> {
  requiredColumnNames() {
    return this.tableRules.requiredColumnNamesStrict([...libreColumnNames]);
  }
}

class GlucoseStructureRules<
  TableName extends string
> extends ddbo.DuckDbOrchTableAssuranceRules<TableName, GlucoseColumnName> {
  requiredColumnNames() {
    return this.tableRules.requiredColumnNamesStrict([...glucoseColumnNames]);
  }
}

class VitalSignsDataStructureRules<
  TableName extends string
> extends ddbo.DuckDbOrchTableAssuranceRules<
  TableName,
  VitalSignsDataColumnName
> {
  requiredColumnNames() {
    return this.tableRules.requiredColumnNamesStrict([
      ...vitalSignsDataColumnNames,
    ]);
  }
}

export const excelWorkbookSheetNames = [
  "MYB001_Libre_data",
  "MYB004_Libre_data",
  "MYB006_Libre_data",
] as const;
export type ExcelWorkbookSheetName = (typeof excelWorkbookSheetNames)[number];

const TODO_SHEET_TERMINAL_STATE = "EXIT(ExcelSheetTodoIngestSource)" as const;

export class ExcelSheetTodoIngestSource<
  SheetName extends string,
  InitState extends o.State
> implements
    o.ExcelSheetIngestSource<
      SheetName,
      string,
      ddbo.DuckDbOrchGovernance,
      InitState,
      typeof TODO_SHEET_TERMINAL_STATE,
      ddbo.DuckDbOrchEmitContext
    >
{
  readonly nature = "Excel Workbook Sheet";
  readonly tableName: string;
  constructor(
    readonly uri: string,
    readonly sheetName: SheetName,
    readonly govn: ddbo.DuckDbOrchGovernance
  ) {
    this.tableName = govn.toSnakeCase(
      path.basename(uri, ".xlsx") + "_" + sheetName
    );
  }

  // deno-lint-ignore require-await
  async workflow(): ReturnType<
    o.ExcelSheetIngestSource<
      string,
      string,
      ddbo.DuckDbOrchGovernance,
      InitState,
      typeof TODO_SHEET_TERMINAL_STATE,
      ddbo.DuckDbOrchEmitContext
    >["workflow"]
  > {
    return {
      ingestSQL: async (issac) =>
        // deno-fmt-ignore
        this.govn.SQL`
          -- required by IngestEngine, setup the ingestion entry for logging
          ${await issac.sessionEntryInsertDML()}
        
          ${await issac.issueInsertDML(
            `Excel workbook '${path.basename(this.uri)}' sheet '${
              this.sheetName
            }' has not been implemented yet.`,
            "TODO"
          )}`,

      assuranceSQL: () =>
        this.govn.SQL`
          -- Sheet '${this.sheetName}' ingestion not implemented.
        `,

      exportResourceSQL: (targetSchema: string) =>
        this.govn.SQL`
          --  Sheet '${this.sheetName}' exportResourceSQL(${targetSchema})
        `,

      terminalState: () => TODO_SHEET_TERMINAL_STATE,
    };
  }
}

const LIBRE_SHEET_TERMINAL_STATE = "EXIT(LibreExcelSheetIngestSource)" as const;

/**
 * The class LibreExcelSheetIngestSource that implements the o.ExcelSheetIngestSource interface.
 * The purpose of this class is to handle the ingestion of data from an Excel sheet into DuckDb database, specifically for the "MYB001_Libre_data" sheet.
 *
 * The class is defined with generic type parameters for the table name (TableName) and initial state (InitState).
 * It implements the o.ExcelSheetIngestSource interface with specific types.
 */
export class LibreExcelSheetIngestSource<
  TableName extends string,
  InitState extends o.State
> implements
    o.ExcelSheetIngestSource<
      "MYB001_Libre_data",
      TableName,
      ddbo.DuckDbOrchGovernance,
      InitState,
      typeof LIBRE_SHEET_TERMINAL_STATE,
      ddbo.DuckDbOrchEmitContext
    >
{
  readonly nature = "Excel Workbook Sheet";
  readonly sheetName = "MYB001_Libre_data";
  readonly tableName: TableName;
  constructor(readonly uri: string, readonly govn: ddbo.DuckDbOrchGovernance) {
    this.tableName = govn.toSnakeCase(
      path.basename(uri, ".xlsx") + "_" + this.sheetName
    ) as TableName;
  }

  // deno-lint-ignore require-await
  async workflow(
    session: o.OrchSession<
      ddbo.DuckDbOrchGovernance,
      ddbo.DuckDbOrchEmitContext
    >,
    sessionEntryID: string
  ): ReturnType<
    o.ExcelSheetIngestSource<
      "MYB001_Libre_data",
      TableName,
      ddbo.DuckDbOrchGovernance,
      InitState,
      typeof LIBRE_SHEET_TERMINAL_STATE,
      ddbo.DuckDbOrchEmitContext
    >["workflow"]
  > {
    const ssr = new LibreStructureRules(
      this.tableName,
      session.sessionID,
      sessionEntryID,
      this.govn
    );
    const sar = new sg.LibreAssuranceRules(
      this.tableName,
      session.sessionID,
      sessionEntryID,
      this.govn
    );

    return {
      ingestSQL: async (issac) =>
        await this.ingestSQL(session, issac, ssr, sar),
      assuranceSQL: async () => await this.assuranceSQL(session, sar),
      exportResourceSQL: async (targetSchema) =>
        await this.exportResourceSQL(session, sar.sessionEntryID, targetSchema),
      terminalState: () => LIBRE_SHEET_TERMINAL_STATE,
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
    ssr: LibreStructureRules<TableName>,
    sar: sg.LibreAssuranceRules<TableName, LibreColumnName>
  ) {
    const { sheetName, tableName, uri } = this;
    const { sessionID, sessionEntryID } = sar;

    // deno-fmt-ignore
    return this.govn.SQL`
      -- required by IngestEngine, setup the ingestion entry for logging
      ${await issac.sessionEntryInsertDML()}
     
      -- state management diagnostics 
      ${await session.entryStateDML(
        sessionEntryID,
        issac.initState(),
        "ATTEMPT_EXCEL_INGEST",
        "ScreeningExcelSheetIngestSource.ingestSQL",
        this.govn.emitCtx.sqlEngineNow
      )}

      -- ingest Excel workbook sheet '${sheetName}' into ${tableName} using spatial plugin
      INSTALL spatial; LOAD spatial;

      -- be sure to add src_file_row_number and session_id columns to each row
      -- because assurance CTEs require them
      CREATE TABLE ${tableName} AS
        SELECT *, row_number() OVER () as src_file_row_number, '${sessionID}' as session_id, '${sessionEntryID}' as session_entry_id
          FROM st_read('${uri}', layer='${sheetName}', open_options=['HEADERS=FORCE', 'FIELD_TYPES=AUTO']);          
      
      ${ssr.requiredColumnNames()}
      ${await session.entryStateDML(
        sessionEntryID,
        "ATTEMPT_EXCEL_INGEST",
        "INGESTED_EXCEL_WORKBOOK_SHEET",
        "ScreeningExcelSheetIngestSource.ingestSQL",
        this.govn.emitCtx.sqlEngineNow
      )}
      `;
  }

  async assuranceSQL(
    session: o.OrchSession<
      ddbo.DuckDbOrchGovernance,
      ddbo.DuckDbOrchEmitContext
    >,
    sar: sg.LibreAssuranceRules<TableName, LibreColumnName>
  ) {
    const { sessionEntryID, tableRules: tr } = sar;

    // deno-fmt-ignore
    return this.govn.SQL`
      ${await session.entryStateDML(
        sessionEntryID,
        "INGESTED_EXCEL_WORKBOOK_SHEET",
        "ATTEMPT_EXCEL_WORKBOOK_SHEET_ASSURANCE",
        "ScreeningExcelSheetIngestSource.assuranceSQL",
        this.govn.emitCtx.sqlEngineNow
      )}

      ${tr.mandatoryValueInAllRows("device")}
      ${tr.mandatoryValueInAllRows("serial number")}      
      ${tr.mandatoryValueInAllRows("time stamp")}
      ${tr.mandatoryValueInAllRows("glucose value")}
              
      ${await session.entryStateDML(
        sessionEntryID,
        "ATTEMPT_EXCEL_WORKBOOK_SHEET_ASSURANCE",
        "ASSURED_EXCEL_WORKBOOK_SHEET",
        "ScreeningExcelSheetIngestSource.assuranceSQL",
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
    targetSchema: string
  ) {
    const { govn, tableName } = this;

    // deno-fmt-ignore
    return govn.SQL`
      ${await session.entryStateDML(
        sessionEntryID,
        "ASSURED_EXCEL_WORKBOOK_SHEET",
        "ATTEMPT_EXCEL_WORKBOOK_SHEET_EXPORT",
        "ScreeningExcelSheetIngestSource.exportResourceSQL",
        this.govn.emitCtx.sqlEngineNow
      )}

      CREATE TABLE ${targetSchema}.${tableName} AS SELECT * FROM ${tableName};

      ${await session.entryStateDML(
        sessionEntryID,
        "ATTEMPT_EXCEL_WORKBOOK_SHEET_EXPORT",
        LIBRE_SHEET_TERMINAL_STATE,
        "ScreeningExcelSheetIngestSource.exportResourceSQL",
        this.govn.emitCtx.sqlEngineNow
      )}
    `;
  }
}

const GLUCOSE_SHEET_TERMINAL_STATE =
  "EXIT(GlucoseExcelSheetIngestSource)" as const;

/**
 * The class GlucoseExcelSheetIngestSource that implements the o.ExcelSheetIngestSource interface.
 * The purpose of this class is to handle the ingestion of data from an Excel sheet into DuckDb database, specifically for the "MYB004_Libre_data" sheet.
 *
 * The class is defined with generic type parameters for the table name (TableName) and initial state (InitState).
 * It implements the o.ExcelSheetIngestSource interface with specific types.
 */
export class GlucoseExcelSheetIngestSource<
  TableName extends string,
  InitState extends o.State
> implements
    o.ExcelSheetIngestSource<
      "MYB004_Libre_data",
      TableName,
      ddbo.DuckDbOrchGovernance,
      InitState,
      typeof GLUCOSE_SHEET_TERMINAL_STATE,
      ddbo.DuckDbOrchEmitContext
    >
{
  readonly nature = "Excel Workbook Sheet";
  readonly sheetName = "MYB004_Libre_data";
  readonly tableName: TableName;
  constructor(readonly uri: string, readonly govn: ddbo.DuckDbOrchGovernance) {
    this.tableName = govn.toSnakeCase(
      path.basename(uri, ".xlsx") + "_" + this.sheetName
    ) as TableName;
  }

  // deno-lint-ignore require-await
  async workflow(
    session: o.OrchSession<
      ddbo.DuckDbOrchGovernance,
      ddbo.DuckDbOrchEmitContext
    >,
    sessionEntryID: string
  ): ReturnType<
    o.ExcelSheetIngestSource<
      "MYB001_Libre_data",
      TableName,
      ddbo.DuckDbOrchGovernance,
      InitState,
      typeof GLUCOSE_SHEET_TERMINAL_STATE,
      ddbo.DuckDbOrchEmitContext
    >["workflow"]
  > {
    const ssr = new GlucoseStructureRules(
      this.tableName,
      session.sessionID,
      sessionEntryID,
      this.govn
    );
    const sar = new sg.GlucoseAssuranceRules(
      this.tableName,
      session.sessionID,
      sessionEntryID,
      this.govn
    );

    return {
      ingestSQL: async (issac) =>
        await this.ingestSQL(session, issac, ssr, sar),
      assuranceSQL: async () => await this.assuranceSQL(session, sar),
      exportResourceSQL: async (targetSchema) =>
        await this.exportResourceSQL(session, sar.sessionEntryID, targetSchema),
      terminalState: () => GLUCOSE_SHEET_TERMINAL_STATE,
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
    ssr: GlucoseStructureRules<TableName>,
    sar: sg.GlucoseAssuranceRules<TableName, GlucoseColumnName>
  ) {
    const { sheetName, tableName, uri } = this;
    const { sessionID, sessionEntryID } = sar;

    // deno-fmt-ignore
    return this.govn.SQL`
      -- required by IngestEngine, setup the ingestion entry for logging
      ${await issac.sessionEntryInsertDML()}
     
      -- state management diagnostics 
      ${await session.entryStateDML(
        sessionEntryID,
        issac.initState(),
        "ATTEMPT_EXCEL_INGEST",
        "AdminDemographicExcelSheetIngestSource.ingestSQL",
        this.govn.emitCtx.sqlEngineNow
      )}

      -- ingest Excel workbook sheet '${sheetName}' into ${tableName} using spatial plugin
      INSTALL spatial; LOAD spatial;

      -- be sure to add src_file_row_number and session_id columns to each row
      -- because assurance CTEs require them
      CREATE TABLE ${tableName} AS
        SELECT *, row_number() OVER () as src_file_row_number, '${sessionID}' as session_id, '${sessionEntryID}' as session_entry_id
          FROM st_read('${uri}', layer='${sheetName}', open_options=['HEADERS=FORCE', 'FIELD_TYPES=AUTO']);          
      
      ${ssr.requiredColumnNames()}
      ${await session.entryStateDML(
        sessionEntryID,
        "ATTEMPT_EXCEL_INGEST",
        "INGESTED_EXCEL_WORKBOOK_SHEET",
        "AdminDemographicExcelSheetIngestSource.ingestSQL",
        this.govn.emitCtx.sqlEngineNow
      )}
      `;
  }

  async assuranceSQL(
    session: o.OrchSession<
      ddbo.DuckDbOrchGovernance,
      ddbo.DuckDbOrchEmitContext
    >,
    adar: sg.GlucoseAssuranceRules<TableName, GlucoseColumnName>
  ) {
    const { sessionEntryID, tableRules: tr } = adar;

    // deno-fmt-ignore
    return this.govn.SQL`
      ${await session.entryStateDML(
        sessionEntryID,
        "INGESTED_EXCEL_WORKBOOK_SHEET",
        "ATTEMPT_EXCEL_WORKBOOK_SHEET_ASSURANCE",
        "AdminDemographicExcelSheetIngestSource.assuranceSQL",
        this.govn.emitCtx.sqlEngineNow
      )}

      ${tr.mandatoryValueInAllRows("device")}
      ${tr.mandatoryValueInAllRows("serial number")}      
      ${tr.mandatoryValueInAllRows("time stamp")}
      ${tr.mandatoryValueInAllRows("glucose value")}
          
      ${await session.entryStateDML(
        sessionEntryID,
        "ATTEMPT_EXCEL_WORKBOOK_SHEET_ASSURANCE",
        "ASSURED_EXCEL_WORKBOOK_SHEET",
        "AdminDemographicExcelSheetIngestSource.assuranceSQL",
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
    targetSchema: string
  ) {
    const { govn, tableName } = this;

    // deno-fmt-ignore
    return govn.SQL`
      ${await session.entryStateDML(
        sessionEntryID,
        "ASSURED_EXCEL_WORKBOOK_SHEET",
        "ATTEMPT_EXCEL_WORKBOOK_SHEET_EXPORT",
        "AdminDemographicExcelSheetIngestSource.exportResourceSQL",
        this.govn.emitCtx.sqlEngineNow
      )}

      CREATE TABLE ${targetSchema}.${tableName} AS SELECT * FROM ${tableName};

      ${await session.entryStateDML(
        sessionEntryID,
        "ATTEMPT_EXCEL_WORKBOOK_SHEET_EXPORT",
        GLUCOSE_SHEET_TERMINAL_STATE,
        "AdminDemographicExcelSheetIngestSource.exportResourceSQL",
        this.govn.emitCtx.sqlEngineNow
      )}
    `;
  }
}

const VITAL_SIGNS_DATA_SHEET_TERMINAL_STATE =
  "EXIT(VitalSignsDataExcelSheetIngestSource)" as const;

/**
 * The class VitalSignsDataExcelSheetIngestSource that implements the o.ExcelSheetIngestSource interface.
 * The purpose of this class is to handle the ingestion of data from an Excel sheet into DuckDb database, specifically for the "MYB006_Libre_data" sheet.
 *
 * The class is defined with generic type parameters for the table name (TableName) and initial state (InitState).
 * It implements the o.ExcelSheetIngestSource interface with specific types.
 */
export class VitalSignsDataExcelSheetIngestSource<
  TableName extends string,
  InitState extends o.State
> implements
    o.ExcelSheetIngestSource<
      "MYB006_Libre_data",
      TableName,
      ddbo.DuckDbOrchGovernance,
      InitState,
      typeof VITAL_SIGNS_DATA_SHEET_TERMINAL_STATE,
      ddbo.DuckDbOrchEmitContext
    >
{
  readonly nature = "Excel Workbook Sheet";
  readonly sheetName = "MYB006_Libre_data";
  readonly tableName: TableName;
  constructor(readonly uri: string, readonly govn: ddbo.DuckDbOrchGovernance) {
    this.tableName = govn.toSnakeCase(
      path.basename(uri, ".xlsx") + "_" + this.sheetName
    ) as TableName;
  }

  // deno-lint-ignore require-await
  async workflow(
    session: o.OrchSession<
      ddbo.DuckDbOrchGovernance,
      ddbo.DuckDbOrchEmitContext
    >,
    sessionEntryID: string
  ): ReturnType<
    o.ExcelSheetIngestSource<
      "MYB006_Libre_data",
      TableName,
      ddbo.DuckDbOrchGovernance,
      InitState,
      typeof VITAL_SIGNS_DATA_SHEET_TERMINAL_STATE,
      ddbo.DuckDbOrchEmitContext
    >["workflow"]
  > {
    const ssr = new VitalSignsDataStructureRules(
      this.tableName,
      session.sessionID,
      sessionEntryID,
      this.govn
    );
    const sar = new sg.VitalSignsDataAssuranceRules(
      this.tableName,
      session.sessionID,
      sessionEntryID,
      this.govn
    );

    return {
      ingestSQL: async (issac) =>
        await this.ingestSQL(session, issac, ssr, sar),
      assuranceSQL: async () => await this.assuranceSQL(session, sar),
      exportResourceSQL: async (targetSchema) =>
        await this.exportResourceSQL(session, sar.sessionEntryID, targetSchema),
      terminalState: () => VITAL_SIGNS_DATA_SHEET_TERMINAL_STATE,
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
    ssr: VitalSignsDataStructureRules<TableName>,
    sar: sg.VitalSignsDataAssuranceRules<TableName, VitalSignsDataColumnName>
  ) {
    const { sheetName, tableName, uri } = this;
    const { sessionID, sessionEntryID } = sar;

    // deno-fmt-ignore
    return this.govn.SQL`
      -- required by IngestEngine, setup the ingestion entry for logging
      ${await issac.sessionEntryInsertDML()}
     
      -- state management diagnostics 
      ${await session.entryStateDML(
        sessionEntryID,
        issac.initState(),
        "ATTEMPT_EXCEL_INGEST",
        "QeAdminDataExcelSheetIngestSource.ingestSQL",
        this.govn.emitCtx.sqlEngineNow
      )}

      -- ingest Excel workbook sheet '${sheetName}' into ${tableName} using spatial plugin
      INSTALL spatial; LOAD spatial;

      -- be sure to add src_file_row_number and session_id columns to each row
      -- because assurance CTEs require them
      CREATE TABLE ${tableName} AS
        SELECT *, row_number() OVER () as src_file_row_number, '${sessionID}' as session_id, '${sessionEntryID}' as session_entry_id
          FROM st_read('${uri}', layer='${sheetName}', open_options=['HEADERS=FORCE', 'FIELD_TYPES=AUTO']);          
      
      ${ssr.requiredColumnNames()}
      ${await session.entryStateDML(
        sessionEntryID,
        "ATTEMPT_EXCEL_INGEST",
        "INGESTED_EXCEL_WORKBOOK_SHEET",
        "QeAdminDataExcelSheetIngestSource.ingestSQL",
        this.govn.emitCtx.sqlEngineNow
      )}
      `;
  }

  async assuranceSQL(
    session: o.OrchSession<
      ddbo.DuckDbOrchGovernance,
      ddbo.DuckDbOrchEmitContext
    >,
    qedar: sg.VitalSignsDataAssuranceRules<TableName, VitalSignsDataColumnName>
  ) {
    const { sessionEntryID, tableRules: tr } = qedar;

    // deno-fmt-ignore
    return this.govn.SQL`
      ${await session.entryStateDML(
        sessionEntryID,
        "INGESTED_EXCEL_WORKBOOK_SHEET",
        "ATTEMPT_EXCEL_WORKBOOK_SHEET_ASSURANCE",
        "QeAdminDataExcelSheetIngestSource.assuranceSQL",
        this.govn.emitCtx.sqlEngineNow
      )}

      ${tr.mandatoryValueInAllRows("device")}
      ${tr.mandatoryValueInAllRows("serial number")}      
      ${tr.mandatoryValueInAllRows("time stamp")}
      ${tr.mandatoryValueInAllRows("glucose value")}
           
      ${await session.entryStateDML(
        sessionEntryID,
        "ATTEMPT_EXCEL_WORKBOOK_SHEET_ASSURANCE",
        "ASSURED_EXCEL_WORKBOOK_SHEET",
        "QeAdminDataExcelSheetIngestSource.assuranceSQL",
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
    targetSchema: string
  ) {
    const { govn, tableName } = this;

    // deno-fmt-ignore
    return govn.SQL`
      ${await session.entryStateDML(
        sessionEntryID,
        "ASSURED_EXCEL_WORKBOOK_SHEET",
        "ATTEMPT_EXCEL_WORKBOOK_SHEET_EXPORT",
        "QeAdminDataExcelSheetIngestSource.exportResourceSQL",
        this.govn.emitCtx.sqlEngineNow
      )}

      CREATE TABLE ${targetSchema}.${tableName} AS SELECT * FROM ${tableName};

      ${await session.entryStateDML(
        sessionEntryID,
        "ATTEMPT_EXCEL_WORKBOOK_SHEET_EXPORT",
        GLUCOSE_SHEET_TERMINAL_STATE,
        "QeAdminDataExcelSheetIngestSource.exportResourceSQL",
        this.govn.emitCtx.sqlEngineNow
      )}
    `;
  }
}

export function ingestExcelSourcesSupplier(
  govn: ddbo.DuckDbOrchGovernance
): o.IngestFsPatternSourcesSupplier<
  | LibreExcelSheetIngestSource<string, o.State>
  | GlucoseExcelSheetIngestSource<string, o.State>
  | VitalSignsDataExcelSheetIngestSource<string, o.State>
  | ExcelSheetTodoIngestSource<string, o.State>
  | o.ErrorIngestSource<
      ddbo.DuckDbOrchGovernance,
      o.State,
      ddbo.DuckDbOrchEmitContext
    >
> {
  return {
    pattern: path.globToRegExp("**/*.xlsx", {
      extended: true,
      globstar: true,
    }),
    sources: (entry) => {
      const uri = String(entry.path);
      const sources: (
        | LibreExcelSheetIngestSource<string, o.State>
        | GlucoseExcelSheetIngestSource<string, o.State>
        | VitalSignsDataExcelSheetIngestSource<string, o.State>
        | ExcelSheetTodoIngestSource<string, o.State>
        | o.ErrorIngestSource<
            ddbo.DuckDbOrchGovernance,
            o.State,
            ddbo.DuckDbOrchEmitContext
          >
      )[] = [];

      const sheetsExpected: Record<
        ExcelWorkbookSheetName,
        () =>
          | ExcelSheetTodoIngestSource<string, o.State>
          | LibreExcelSheetIngestSource<string, o.State>
          | GlucoseExcelSheetIngestSource<string, o.State>
          | VitalSignsDataExcelSheetIngestSource<string, o.State>
      > = {
        MYB004_Libre_data: () => new GlucoseExcelSheetIngestSource(uri, govn),
        MYB001_Libre_data: () => new LibreExcelSheetIngestSource(uri, govn),
        MYB006_Libre_data: () =>
          new VitalSignsDataExcelSheetIngestSource(uri, govn),
      };

      try {
        const wb = xlsx.readFile(uri);

        // deno-fmt-ignore
        const sheetNotFound = (name: string) =>
          Error(
            `Excel workbook sheet '${name}' not found in '${path.basename(
              uri
            )}' (available: ${wb.SheetNames.join(", ")})`
          );

        let sheetsFound = 0;
        const expectedSheetNames = Object.keys(sheetsExpected);
        for (const expectedSN of expectedSheetNames) {
          if (wb.SheetNames.find((sn) => sn == expectedSN)) {
            sheetsFound++;
          } else {
            sources.push(
              new o.ErrorIngestSource(
                uri,
                sheetNotFound(expectedSN),
                "Sheet Missing",
                govn
              )
            );
          }
        }

        if (expectedSheetNames.length == sheetsFound) {
          for (const newSourceInstance of Object.values(sheetsExpected)) {
            sources.push(newSourceInstance());
          }
        }
      } catch (err) {
        sources.push(new o.ErrorIngestSource(uri, err, "ERROR", govn));
      }
      return sources;
    },
  };
}
