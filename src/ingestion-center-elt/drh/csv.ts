import { path, SQLa_orch as o, SQLa_orch_duckdb as ddbo } from "./deps.ts";
import * as sg from "./governance.ts";

export const csvFileNames = ["LIBRE", "GLUCOSE", "vitalsigns"] as const;
export type CsvFileName = (typeof csvFileNames)[number];

export const csvTableNames = ["libre", "glucose", "vitalsigns"] as const;

export const [aggrLibreTableName, aggrGlucose, aggrVitalSignsTableName] =
  csvTableNames;

// device,serial number,time stamp,glucose value
const libreCsvColumnNames = [
  "device",
  "serial number",
  "time stamp",
  "glucose value",
] as const;

const glucoseCsvColumnNames = [] as const;

const vitalSignsDataCsvColumnNames = [] as const;

type LibreCsvColumnName = (typeof libreCsvColumnNames)[number];

type GlucoseCsvColumnName = (typeof glucoseCsvColumnNames)[number];

type VitalSignsDataCsvColumnName =
  (typeof vitalSignsDataCsvColumnNames)[number];

export class LibreCsvStructureRules<
  TableName extends string
> extends ddbo.DuckDbOrchTableAssuranceRules<TableName, LibreCsvColumnName> {
  requiredColumnNames() {
    return this.tableRules.requiredColumnNamesStrict([...libreCsvColumnNames]);
  }
}

export class GlucoseCsvStructureRules<
  TableName extends string
> extends ddbo.DuckDbOrchTableAssuranceRules<TableName, GlucoseCsvColumnName> {
  requiredColumnNames() {
    return this.tableRules.requiredColumnNamesStrict([
      ...glucoseCsvColumnNames,
    ]);
  }
}

export class VitalSignsDataCsvStructureRules<
  TableName extends string
> extends ddbo.DuckDbOrchTableAssuranceRules<
  TableName,
  VitalSignsDataCsvColumnName
> {
  requiredColumnNames() {
    return this.tableRules.requiredColumnNamesStrict([
      ...vitalSignsDataCsvColumnNames,
    ]);
  }
}

const TERMINAL_STATE = "EXIT(LibreCsvFileIngestSource)" as const;

export class LibreCsvFileIngestSource<
  TableName extends string,
  InitState extends o.State,
  GroupName extends string
> implements
    o.CsvFileIngestSource<
      TableName,
      ddbo.DuckDbOrchGovernance,
      InitState,
      typeof TERMINAL_STATE,
      ddbo.DuckDbOrchEmitContext
    >
{
  readonly nature = "CSV";
  constructor(
    readonly uri: string,
    readonly tableName: TableName,
    readonly groupName: GroupName,
    readonly relatedTableNames: {
      readonly glucoseTableName: string;
      readonly vitalSignsDataTableName: string;
    },
    readonly govn: ddbo.DuckDbOrchGovernance
  ) {}

  // deno-lint-ignore require-await
  async workflow(
    session: o.OrchSession<
      ddbo.DuckDbOrchGovernance,
      ddbo.DuckDbOrchEmitContext
    >,
    sessionEntryID: string
  ): ReturnType<
    o.CsvFileIngestSource<
      TableName,
      ddbo.DuckDbOrchGovernance,
      InitState,
      typeof TERMINAL_STATE,
      ddbo.DuckDbOrchEmitContext
    >["workflow"]
  > {
    const ssr = new LibreCsvStructureRules(
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
      terminalState: () => TERMINAL_STATE,
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
    ssr: LibreCsvStructureRules<TableName>,
    sar: sg.LibreAssuranceRules<TableName, LibreCsvColumnName>
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
        "LibreCsvFileIngestSource.ingestSQL",
        this.govn.emitCtx.sqlEngineNow
      )}


      -- be sure to add src_file_row_number and session_id columns to each row
      -- because assurance CTEs require them
      CREATE TABLE ${tableName} AS
        SELECT *, row_number() OVER () as src_file_row_number, '${sessionID}' as session_id, '${sessionEntryID}' as session_entry_id
          FROM read_csv_auto('${uri}', types={'device': 'VARCHAR', 'serial number': 'VARCHAR'});

      ${ssr.requiredColumnNames()}

      ${await session.entryStateDML(
        sessionEntryID,
        "ATTEMPT_CSV_INGEST",
        "INGESTED_CSV",
        "LibreCsvFileIngestSource.ingestSQL",
        this.govn.emitCtx.sqlEngineNow
      )}
    `;
  }

  async assuranceSQL(
    session: o.OrchSession<
      ddbo.DuckDbOrchGovernance,
      ddbo.DuckDbOrchEmitContext
    >,
    sar: sg.LibreAssuranceRules<TableName, LibreCsvColumnName>
  ) {
    const { govn, relatedTableNames } = this;
    const { sessionEntryID, tableRules: tr } = sar;

    // deno-fmt-ignore
    return govn.SQL`
      ${await session.entryStateDML(
        sessionEntryID,
        "INGESTED_CSV",
        "ATTEMPT_CSV_ASSURANCE",
        "LibreCsvFileIngestSource.assuranceSQL",
        this.govn.emitCtx.sqlEngineNow
      )}

      ${tr.mandatoryValueInAllRows("device")}
      ${tr.mandatoryValueInAllRows("serial number")}
      ${tr.mandatoryValueInAllRows("time stamp")}
      ${tr.mandatoryValueInAllRows("glucose value")}
      
      ${await session.entryStateDML(
        sessionEntryID,
        "ATTEMPT_CSV_ASSURANCE",
        "ASSURED_CSV",
        "LibreCsvFileIngestSource.assuranceSQL",
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
    const {
      govn: { SQL },
      tableName,
      relatedTableNames,
    } = this;

    // deno-fmt-ignore
    return SQL`
      ${await session.entryStateDML(
        sessionEntryID,
        "ASSURED_CSV",
        "EXIT(LibreCsvFileIngestSource)",
        "LibreCsvFileIngestSource.exportResourceSQL",
        this.govn.emitCtx.sqlEngineNow
      )}

        ${await session.entryStateDML(
          sessionEntryID,
          "ATTEMPT_CSV_EXPORT",
          TERMINAL_STATE,
          "LibreCsvFileIngestSource.exportResourceSQL",
          this.govn.emitCtx.sqlEngineNow
        )}

      CREATE TABLE IF NOT EXISTS ${aggrLibreTableName} AS SELECT * FROM ${tableName} WHERE 0=1;
      INSERT INTO ${aggrLibreTableName} SELECT * FROM ${tableName};

      CREATE TABLE ${targetSchema}.${tableName} AS SELECT * FROM ${tableName};

      CREATE TABLE IF NOT EXISTS ${targetSchema}.${aggrLibreTableName} AS SELECT * FROM ${tableName} WHERE 0=1;
      INSERT INTO ${targetSchema}.${aggrLibreTableName} SELECT * FROM ${tableName};



        `;
  }
}

const GLUCOSE_CSV_TERMINAL_STATE = "EXIT(GlucoseCsvFileIngestSource)" as const;

export class GlucoseCsvFileIngestSource<
  TableName extends string,
  InitState extends o.State,
  GroupName extends string
> implements
    o.CsvFileIngestSource<
      TableName,
      ddbo.DuckDbOrchGovernance,
      InitState,
      typeof GLUCOSE_CSV_TERMINAL_STATE,
      ddbo.DuckDbOrchEmitContext
    >
{
  readonly nature = "CSV";
  constructor(
    readonly uri: string,
    readonly tableName: TableName,
    readonly groupName: GroupName,
    readonly relatedTableNames: {
      readonly libreTableName: string;
      readonly vitalSignsDataTableName: string;
    },
    readonly govn: ddbo.DuckDbOrchGovernance
  ) {}

  // deno-lint-ignore require-await
  async workflow(
    session: o.OrchSession<
      ddbo.DuckDbOrchGovernance,
      ddbo.DuckDbOrchEmitContext
    >,
    sessionEntryID: string
  ): ReturnType<
    o.CsvFileIngestSource<
      TableName,
      ddbo.DuckDbOrchGovernance,
      InitState,
      typeof GLUCOSE_CSV_TERMINAL_STATE,
      ddbo.DuckDbOrchEmitContext
    >["workflow"]
  > {
    const ssr = new GlucoseCsvStructureRules(
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
      terminalState: () => GLUCOSE_CSV_TERMINAL_STATE,
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
    ssr: GlucoseCsvStructureRules<TableName>,
    sar: sg.GlucoseAssuranceRules<TableName, GlucoseCsvColumnName>
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
        "GlucoseCsvFileIngestSource.ingestSQL",
        this.govn.emitCtx.sqlEngineNow
      )}

      -- be sure to add src_file_row_number and session_id columns to each row
      -- because assurance CTEs require them
      CREATE TABLE ${tableName} AS
        SELECT *, row_number() OVER () as src_file_row_number, '${sessionID}' as session_id, '${sessionEntryID}' as session_entry_id
          FROM read_csv_auto('${uri}', types={'dataset_id': 'VARCHAR'});

      ${ssr.requiredColumnNames()}

      ${await session.entryStateDML(
        sessionEntryID,
        "ATTEMPT_CSV_INGEST",
        "INGESTED_CSV",
        "GlucoseCsvFileIngestSource.ingestSQL",
        this.govn.emitCtx.sqlEngineNow
      )}
    `;
  }

  async assuranceSQL(
    session: o.OrchSession<
      ddbo.DuckDbOrchGovernance,
      ddbo.DuckDbOrchEmitContext
    >,
    adar: sg.GlucoseAssuranceRules<TableName, GlucoseCsvColumnName>
  ) {
    const { govn } = this;
    const { sessionEntryID, tableRules: tr } = adar;

    // deno-fmt-ignore
    return govn.SQL`
      ${await session.entryStateDML(
        sessionEntryID,
        "INGESTED_CSV",
        "ATTEMPT_CSV_ASSURANCE",
        "GlucoseCsvFileIngestSource.assuranceSQL",
        this.govn.emitCtx.sqlEngineNow
      )}

     
      ${await session.entryStateDML(
        sessionEntryID,
        "ATTEMPT_CSV_ASSURANCE",
        "ASSURED_CSV",
        "GlucoseCsvFileIngestSource.assuranceSQL",
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
    const {
      govn: { SQL },
      tableName,
    } = this;

    // deno-fmt-ignore
    return SQL`
      ${await session.entryStateDML(
        sessionEntryID,
        "ASSURED_CSV",
        "EXIT(GlucoseCsvFileIngestSource)",
        "GlucoseCsvFileIngestSource.exportResourceSQL",
        this.govn.emitCtx.sqlEngineNow
      )}

      CREATE TABLE IF NOT EXISTS ${aggrVitalSignsTableName} AS SELECT * FROM ${tableName} WHERE 0=1;
      INSERT INTO ${aggrVitalSignsTableName} SELECT * FROM ${tableName};

      CREATE TABLE ${targetSchema}.${tableName} AS SELECT * FROM ${tableName};

      CREATE TABLE IF NOT EXISTS ${targetSchema}.${aggrVitalSignsTableName} AS SELECT * FROM ${tableName} WHERE 0=1;
      INSERT INTO ${targetSchema}.${aggrVitalSignsTableName} SELECT * FROM ${tableName};



        ${await session.entryStateDML(
          sessionEntryID,
          "ATTEMPT_CSV_EXPORT",
          TERMINAL_STATE,
          "GlucoseCsvFileIngestSource.exportResourceSQL",
          this.govn.emitCtx.sqlEngineNow
        )}
        `;
  }
}

const VITAL_SIGNS_DATA_SHEET_TERMINAL_STATE =
  "EXIT(VitalSignsDataCsvFileIngestSource)" as const;

export class VitalSignsDataCsvFileIngestSource<
  TableName extends string,
  InitState extends o.State,
  GroupName extends string
> implements
    o.CsvFileIngestSource<
      TableName,
      ddbo.DuckDbOrchGovernance,
      InitState,
      typeof VITAL_SIGNS_DATA_SHEET_TERMINAL_STATE,
      ddbo.DuckDbOrchEmitContext
    >
{
  readonly nature = "CSV";
  constructor(
    readonly uri: string,
    readonly tableName: TableName,
    readonly groupName: GroupName,
    readonly relatedTableNames: {
      readonly glucoseTableName: string;
      readonly libreTableName: string;
    },
    readonly govn: ddbo.DuckDbOrchGovernance
  ) {}

  // deno-lint-ignore require-await
  async workflow(
    session: o.OrchSession<
      ddbo.DuckDbOrchGovernance,
      ddbo.DuckDbOrchEmitContext
    >,
    sessionEntryID: string
  ): ReturnType<
    o.CsvFileIngestSource<
      TableName,
      ddbo.DuckDbOrchGovernance,
      InitState,
      typeof VITAL_SIGNS_DATA_SHEET_TERMINAL_STATE,
      ddbo.DuckDbOrchEmitContext
    >["workflow"]
  > {
    const ssr = new VitalSignsDataCsvStructureRules(
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
    ssr: VitalSignsDataCsvStructureRules<TableName>,
    sar: sg.VitalSignsDataAssuranceRules<TableName, VitalSignsDataCsvColumnName>
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
        "VitalSignsDataCsvFileIngestSource.ingestSQL",
        this.govn.emitCtx.sqlEngineNow
      )}

      -- be sure to add src_file_row_number and session_id columns to each row
      -- because assurance CTEs require them
      CREATE TABLE ${tableName} AS
        SELECT *, row_number() OVER () as src_file_row_number, '${sessionID}' as session_id, '${sessionEntryID}' as session_entry_id
          FROM read_csv_auto('${uri}');

      ${ssr.requiredColumnNames()}

      ${await session.entryStateDML(
        sessionEntryID,
        "ATTEMPT_CSV_INGEST",
        "INGESTED_CSV",
        "VitalSignsDataCsvFileIngestSource.ingestSQL",
        this.govn.emitCtx.sqlEngineNow
      )}
    `;
  }

  async assuranceSQL(
    session: o.OrchSession<
      ddbo.DuckDbOrchGovernance,
      ddbo.DuckDbOrchEmitContext
    >,
    qedar: sg.VitalSignsDataAssuranceRules<
      TableName,
      VitalSignsDataCsvColumnName
    >
  ) {
    const { govn } = this;
    const { sessionEntryID, tableRules: tr } = qedar;

    // deno-fmt-ignore
    return govn.SQL`
      ${await session.entryStateDML(
        sessionEntryID,
        "INGESTED_CSV",
        "ATTEMPT_CSV_ASSURANCE",
        "VitalSignsDataCsvFileIngestSource.assuranceSQL",
        this.govn.emitCtx.sqlEngineNow
      )}

      ${await session.entryStateDML(
        sessionEntryID,
        "ATTEMPT_CSV_ASSURANCE",
        "ASSURED_CSV",
        "VitalSignsDataCsvFileIngestSource.assuranceSQL",
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
    const {
      govn: { SQL },
      tableName,
    } = this;

    // deno-fmt-ignore
    return SQL`
      ${await session.entryStateDML(
        sessionEntryID,
        "ASSURED_CSV",
        "EXIT(VitalSignsDataCsvFileIngestSource)",
        "VitalSignsDataCsvFileIngestSource.exportResourceSQL",
        this.govn.emitCtx.sqlEngineNow
      )}

      CREATE TABLE IF NOT EXISTS ${aggrGlucose} AS SELECT * FROM ${tableName} WHERE 0=1;
      INSERT INTO ${aggrGlucose} SELECT * FROM ${tableName};

      CREATE TABLE ${targetSchema}.${tableName} AS SELECT * FROM ${tableName};

      CREATE TABLE IF NOT EXISTS ${targetSchema}.${aggrGlucose} AS SELECT * FROM ${tableName} WHERE 0=1;
      INSERT INTO ${targetSchema}.${aggrGlucose} SELECT * FROM ${tableName};

        ${await session.entryStateDML(
          sessionEntryID,
          "ATTEMPT_CSV_EXPORT",
          TERMINAL_STATE,
          "VitalSignsDataCsvFileIngestSource.exportResourceSQL",
          this.govn.emitCtx.sqlEngineNow
        )}
        `;
  }
}

export function ingestCsvFilesSourcesSupplier(
  govn: ddbo.DuckDbOrchGovernance
): o.IngestFsPatternSourcesSupplier<
  | LibreCsvFileIngestSource<string, o.State, string>
  | GlucoseCsvFileIngestSource<string, o.State, string>
  | VitalSignsDataCsvFileIngestSource<string, o.State, string>
  | o.ErrorIngestSource<
      ddbo.DuckDbOrchGovernance,
      o.State,
      ddbo.DuckDbOrchEmitContext
    >
> {
  return {
    pattern: path.globToRegExp("**/*.csv", {
      extended: true,
      globstar: true,
    }),
    sources: (entry) => {
      const filePath = String(entry.path);
      const sources: (
        | LibreCsvFileIngestSource<string, o.State, string>
        | GlucoseCsvFileIngestSource<string, o.State, string>
        | VitalSignsDataCsvFileIngestSource<string, o.State, string>
        | o.ErrorIngestSource<
            ddbo.DuckDbOrchGovernance,
            o.State,
            ddbo.DuckDbOrchEmitContext
          >
      )[] = [];
      const fileName = path.basename(filePath);
      const patterns = /.*(LIBRE|GLUCOSE|vitalsigns)(.*)?.csv/i;
      const groupMatch = filePath.match(patterns);

      if (groupMatch) {
        const suffix = groupMatch[2];
        const libreTableName = govn.toSnakeCase(`libre${suffix}`.toLowerCase());
        const vitalSignsDataTableName = govn.toSnakeCase(
          `vitalsigns${suffix}`.toLowerCase()
        );
        const glucoseTableName = govn.toSnakeCase(
          `glucose${suffix}`.toLowerCase()
        );

        const csvFileName: CsvFileName = groupMatch[1] as CsvFileName;
        const csvExpected: Record<
          CsvFileName,
          () =>
            | LibreCsvFileIngestSource<string, o.State, string>
            | GlucoseCsvFileIngestSource<string, o.State, string>
            | VitalSignsDataCsvFileIngestSource<string, o.State, string>
        > = {
          LIBRE: () =>
            new LibreCsvFileIngestSource(
              String(entry.path),
              libreTableName,
              suffix,
              {
                vitalSignsDataTableName,
                glucoseTableName,
              },
              govn
            ),
          GLUCOSE: () =>
            new VitalSignsDataCsvFileIngestSource(
              String(entry.path),
              vitalSignsDataTableName,
              suffix,
              {
                glucoseTableName,
                libreTableName,
              },
              govn
            ),
          vitalsigns: () =>
            new GlucoseCsvFileIngestSource(
              String(entry.path),
              glucoseTableName,
              suffix,
              {
                vitalSignsDataTableName,
                libreTableName,
              },
              govn
            ),
        };
        if (csvFileName in csvExpected) {
          sources.push(csvExpected?.[csvFileName]());
        }
      } else {
        sources.push(
          new o.ErrorIngestSource(
            filePath,
            Error(
              `CSV file '${fileName}' not found in '${path.basename(filePath)}'`
            ),
            "Unknown CSV File Type",
            govn
          )
        );
      }
      return sources;
    },
  };
}
