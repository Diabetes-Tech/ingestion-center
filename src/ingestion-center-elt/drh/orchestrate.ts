import {
  array,
  chainNB,
  dax,
  fs,
  path,
  safety,
  SQLa_orch as o,
  SQLa_orch_duckdb as ddbo,
  uuid,
  ws,
  yaml,
  colors,
  mysql,
  env,
} from "./deps.ts";
import * as sp from "./sqlpage.ts";
import * as ref from "./reference.ts";
import * as csv from "./csv.ts";
import * as excel from "./excel.ts";
import * as gov from "./governance.ts";

export const ORCHESTRATE_VERSION = "0.12.0";

export interface FhirRecord {
  PAT_MRN_ID: string;
  ENCOUNTER_ID: string;
  FHIR: object;
}
export interface TableCountRecord {
  table_count: number;
}
export let fhirGeneratorCheck = false;

const loadEnv = await env.load();
export const dbConstants = {
  mysql_host: loadEnv["MYSQL_HOST"],
  mysql_user: loadEnv["MYSQL_USER"],
  mysql_port: loadEnv["MYSQL_PORT"],
  mysql_database: loadEnv["MYSQL_DATABASE"],
  addChanges: "call dolt_add('-A');",
  pushChanges: "call dolt_push;",
  checkForChangesQuery: "select * from dolt_status;",
  commitMessage: function (tableNames) {
    return `-am 'add ${tableNames} to ${this.mysql_database}'`;
  },
  commitChanges: function (tableNames) {
    return `call dolt_commit(${this.commitMessage(tableNames)});`;
  },
  doltQuery: function (tableNames) {
    return [this.addChanges, this.commitChanges(tableNames), this.pushChanges];
  },
  get mysql_con() {
    return `host=${this.mysql_host} user=${this.mysql_user} port=${this.mysql_port} database=${this.mysql_database}`;
  },
  get useDBQuery() {
    return `use ${this.mysql_database};`;
  },
};
export default dbConstants;

export type PotentialIngestSource =
  | excel.LibreExcelSheetIngestSource<string, o.State>
  | excel.GlucoseExcelSheetIngestSource<string, o.State>
  | excel.VitalSignsDataExcelSheetIngestSource<string, o.State>
  | excel.ExcelSheetTodoIngestSource<string, o.State>
  | csv.LibreCsvFileIngestSource<string, o.State, string>
  | csv.GlucoseCsvFileIngestSource<string, o.State, string>
  | csv.VitalSignsDataCsvFileIngestSource<string, o.State, string>
  | ref.ScreeningStatusCodeReferenceCsvFileIngestSource<
      "screening_status_code_reference",
      o.State
    >
  | ref.BusinessRulesReferenceCsvFileIngestSource<"business_rules", o.State>
  | o.ErrorIngestSource<
      ddbo.DuckDbOrchGovernance,
      o.State,
      ddbo.DuckDbOrchEmitContext
    >;

export function removeNullsFromObject(
  obj: Record<string, unknown>
): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  Object.entries(obj).forEach(([key, value]) => {
    if (Array.isArray(value)) {
      const cleanedArray = removeNullsFromArray(value);
      if (cleanedArray.length > 0) {
        result[key] = cleanedArray;
      }
    } else if (value && typeof value === "object") {
      const cleanedValue = removeNullsFromObject(
        value as Record<string, unknown>
      );
      if (Object.keys(cleanedValue).length > 0) {
        result[key] = cleanedValue;
      }
    } else if (value !== null) {
      result[key] = value;
    }
  });
  return result;
}

export function removeNullsFromArray(arr: unknown[]): unknown[] {
  return arr
    .map((item) => {
      if (Array.isArray(item)) {
        return removeNullsFromArray(item);
      } else if (item && typeof item === "object") {
        return removeNullsFromObject(item as Record<string, unknown>);
      } else if (item !== null) {
        return item;
      }
    })
    .filter((item) => item !== undefined && item !== null);
}

export function removeNulls(value: unknown): unknown {
  if (typeof value === "string") {
    value = JSON.parse(value);
  }

  if (Array.isArray(value)) {
    return removeNullsFromArray(value);
  } else if (value && typeof value === "object") {
    return removeNullsFromObject(value as Record<string, unknown>);
  }
  return value;
}

export function walkFsPatternIngestSourcesSupplier(
  govn: ddbo.DuckDbOrchGovernance
): o.IngestSourcesSupplier<PotentialIngestSource, [string[] | undefined]> {
  return {
    sources: async (suggestedRootPaths?: string[]) => {
      const sources: PotentialIngestSource[] = [];
      const iss = [
        csv.ingestCsvFilesSourcesSupplier(govn),
        excel.ingestExcelSourcesSupplier(govn),
      ];
      const rootPaths = suggestedRootPaths ?? [Deno.cwd()];

      // loop through all the root paths and find patterns such as **/*.csv,
      // **/*.xlsx, etc. supplied in the `iss` array. For each file that
      // matches, obtain the source and put it into the result
      for (const rp of rootPaths) {
        for await (const entry of fs.walk(rp)) {
          if (entry.isFile) {
            for (const p of iss.filter((p) => p.pattern.test(entry.path))) {
              for (const s of await p.sources(entry)) {
                sources.push(s);
              }
            }
          }
        }
      }

      return sources;
    },
  };
}

export type ScreeningIngressGroup = {
  readonly groupID: string;
  readonly component: string;
  readonly entries: o.IngressEntry<string, string>[];
  readonly onIngress: (group: ScreeningIngressGroup) => Promise<void> | void;
};

export const isScreeningIngressGroup = safety.typeGuard<ScreeningIngressGroup>(
  "groupID",
  "component"
);

export class ScreeningIngressGroups {
  // entries are like `screening-<groupID>_admin.csv`, `screening-<groupID>_questions.csv`, etc.
  readonly pattern = /.*(screening)-([^_])_(.*)?.csv/i;
  readonly groups = new Map<string, ScreeningIngressGroup>();

  constructor(readonly onIngress: ScreeningIngressGroup["onIngress"]) {}

  potential(
    entry: o.IngressEntry<string, string>,
    onIngress?: ScreeningIngressGroup["onIngress"]
  ) {
    const groupMatch = entry.fsPath.match(this.pattern);
    if (groupMatch) {
      const [, _screening, groupID, component] = groupMatch;
      let group = this.groups.get(groupID);
      if (!group) {
        group = {
          groupID,
          component,
          entries: [entry],
          onIngress: onIngress ?? this.onIngress,
        };
        this.groups.set(groupID, group);
      } else {
        group.entries.push(entry);
      }
      if (group.entries.length == 3) {
        group.onIngress(group);
      }
    }
    return undefined;
  }
}

export function watchFsPatternIngestSourcesSupplier(
  govn: ddbo.DuckDbOrchGovernance,
  src:
    | ScreeningIngressGroup
    | o.IngressEntry<string, string>
    | o.IngressEntry<string, string>[]
): o.IngestSourcesSupplier<PotentialIngestSource, [string[] | undefined]> {
  return {
    sources: async () => {
      const sources: PotentialIngestSource[] = [];
      const iss = [
        csv.ingestCsvFilesSourcesSupplier(govn),
        excel.ingestExcelSourcesSupplier(govn),
      ];

      const collect = async (path: string | URL) => {
        for (const p of iss.filter((p) => p.pattern.test(String(path)))) {
          for (const s of await p.sources({ path })) {
            sources.push(s);
          }
        }
      };

      if (isScreeningIngressGroup(src)) {
        src.entries.forEach(async (entry) => await collect(entry.fsPath));
      } else {
        if (Array.isArray(src)) {
          src.forEach(async (entry) => await collect(entry.fsPath));
        } else {
          await collect(src.fsPath);
        }
      }

      return sources;
    },
  };
}

export type OrchStep = chainNB.NotebookCell<
  OrchEngine,
  chainNB.NotebookCellID<OrchEngine>
>;
export type OrchStepContext = chainNB.NotebookCellContext<OrchEngine, OrchStep>;
export const oeDescr = new chainNB.NotebookDescriptor<OrchEngine, OrchStep>();

export type OrchEnginePath = { readonly home: string } & o.OrchPathSupplier &
  o.OrchPathMutator;

export type OrchEngineStorablePath = OrchEnginePath & o.OrchPathStore;

export interface OrchEngineIngressPaths {
  readonly ingress: OrchEnginePath;
  readonly initializePaths?: () => Promise<void>;
  readonly finalizePaths?: () => Promise<void>;
}

export interface OrchEngineWorkflowPaths {
  readonly ingressArchive?: OrchEngineStorablePath;
  readonly inProcess: OrchEngineStorablePath & {
    readonly duckDbFsPathSupplier: () => string;
  };
  readonly egress: OrchEngineStorablePath & {
    readonly resourceDbSupplier?: () => string;
    readonly diagsJsonSupplier?: () => string;
    readonly diagsXlsxSupplier?: () => string;
    readonly diagsMdSupplier?: () => string;
    readonly fhirJsonSupplier?: (pat_mrn_id: string) => string;
    readonly fhirHttpSupplier?: () => string;
  };

  readonly initializePaths?: () => Promise<void>;
  readonly finalizePaths?: () => Promise<void>;
}

export function orchEngineIngressPaths(
  home: string,
  init?: Partial<OrchEngineIngressPaths>
): OrchEngineIngressPaths {
  const ingress = (): OrchEnginePath => {
    const resolvedPath = (child: string) => path.join(home, child);

    return {
      home,
      resolvedPath,
      movedPath: async (path, dest) => {
        const movedToPath = dest.resolvedPath(path);
        await Deno.rename(path, movedToPath);
        return movedToPath;
      },
    };
  };

  return { ingress: ingress(), ...init };
}

export function orchEngineWorkflowPaths(
  rootPath: string,
  sessionID: string
): OrchEngineWorkflowPaths {
  const oePath = (childPath: string): OrchEnginePath => {
    const home = path.join(rootPath, childPath);
    const resolvedPath = (child: string) => path.join(home, child);

    return {
      home,
      resolvedPath,
      movedPath: async (path, dest) => {
        const movedToPath = dest.resolvedPath(path);
        await Deno.rename(path, movedToPath);
        return movedToPath;
      },
    };
  };

  const oeStorablePath = (childPath: string): OrchEngineStorablePath => {
    const oep = oePath(childPath);
    return {
      ...oep,
      storedContent: async (path, content) => {
        const dest = oep.resolvedPath(path);
        await Deno.writeTextFile(dest, content);
        return dest;
      },
    };
  };

  const egress: OrchEngineWorkflowPaths["egress"] = {
    ...oeStorablePath(path.join("egress", sessionID)),
    diagsJsonSupplier: () => egress.resolvedPath("diagnostics.json"),
    diagsMdSupplier: () => egress.resolvedPath("diagnostics.md"),
    diagsXlsxSupplier: () => egress.resolvedPath("diagnostics.xlsx"),
    resourceDbSupplier: () => egress.resolvedPath("resource.sqlite.db"),
    fhirJsonSupplier: (id: string) => {
      return egress.resolvedPath("fhir-" + id + ".json");
    },
    fhirHttpSupplier: () => egress.resolvedPath("fhir.http"),
  };
  const inProcess: OrchEngineWorkflowPaths["inProcess"] = {
    ...oeStorablePath(path.join("egress", sessionID, ".workflow")),
    duckDbFsPathSupplier: () =>
      inProcess.resolvedPath("ingestion-center.duckdb"),
  };
  const ingressArchive: OrchEngineWorkflowPaths["ingressArchive"] =
    oeStorablePath(path.join("egress", sessionID, ".consumed"));

  return {
    ingressArchive,
    inProcess,
    egress,

    initializePaths: async () => {
      await Deno.mkdir(ingressArchive.home, { recursive: true });
      await Deno.mkdir(inProcess.home, { recursive: true });
      await Deno.mkdir(egress.home, { recursive: true });
    },
  };
}

export interface OrchEngineArgs
  extends o.OrchArgs<
    ddbo.DuckDbOrchGovernance,
    OrchEngine,
    ddbo.DuckDbOrchEmitContext
  > {
  readonly workflowPaths: OrchEngineWorkflowPaths;
  readonly walkRootPaths?: string[];
  readonly referenceDataHome?: string;
}

/**
 * Use OrchEngine to prepare SQL for orchestration steps and execute them
 * using DuckDB CLI engine. Each method that does not have a @ieDescr.disregard()
 * attribute is considered a "step" and each step is executed in the order it is
 * declared. As each step is executed, its error or results are passed to the
 * next method.
 *
 * This Engine assumes that the Kernel observer will abort on Errors. If you want
 * to continue after an error, throw a OrchResumableError and use the second
 * cell argument (result) to test for it.
 *
 * This class is introspected and run using SQLa's Notebook infrastructure.
 * See: https://github.com/netspective-labs/sql-aide/tree/main/lib/notebook
 */
export class OrchEngine {
  protected potentialSources?: PotentialIngestSource[];
  protected ingestables?: {
    readonly psIndex: number; // the index in #potentialSources
    readonly source: PotentialIngestSource;
    readonly workflow: Awaited<ReturnType<PotentialIngestSource["workflow"]>>;
    readonly sessionEntryID: string;
    readonly sql: string;
    readonly issues: {
      readonly session_entry_id: string;
      readonly orch_session_issue_id: string;
      readonly issue_type: string;
      readonly issue_message: string;
      readonly invalid_value: string;
    }[];
  }[];
  readonly duckdb: ddbo.DuckDbShell;
  readonly sqlPageNB: ReturnType<typeof sp.SQLPageNotebook.create>;

  constructor(
    readonly iss: o.IngestSourcesSupplier<
      PotentialIngestSource,
      [string[] | undefined] // optional root paths
    >,
    readonly govn: ddbo.DuckDbOrchGovernance,
    readonly args: OrchEngineArgs
  ) {
    this.duckdb = new ddbo.DuckDbShell(args.session, {
      duckdbCmd: "duckdb",
      dbDestFsPathSupplier: args.workflowPaths.inProcess.duckDbFsPathSupplier,
      preambleSQL: () =>
        `-- preambleSQL\nSET autoinstall_known_extensions=true;\nSET autoload_known_extensions=true;\n-- end preambleSQL\n`,
    });
    this.sqlPageNB = sp.SQLPageNotebook.create(this.govn);
  }

  /**
   * Prepare the DuckDB path/database for initialization. Typically this gives
   * a chance for the path to the database to be created or removing the existing
   * database in case we want to initialize from scratch.
   * @param osc the type-safe notebook cell context for diagnostics or business rules
   */
  async prepareInit(_osc: OrchStepContext) {
    await this.args.workflowPaths.initializePaths?.();
  }

  /**
   * Initialize the DuckDB database by ensuring the admin tables such as tracking
   * orchestration events (states), activities (which files are being loaded), ingest
   * issues (errors, etc.), and related  entities are created. If there are any
   * errors during this process all other processing should stop and no other steps
   * are executed.
   * @param osc the type-safe notebook cell context for diagnostics or business rules
   */
  async init(osc: OrchStepContext) {
    const {
      govn,
      govn: { informationSchema: is },
      args: { session },
    } = this;
    const beforeInit = Array.from(
      session.sqlCatalogSqlSuppliers("before-init")
    );
    const afterInit = Array.from(session.sqlCatalogSqlSuppliers("after-init"));

    const initDDL = govn.SQL`
      ${beforeInit.length > 0 ? beforeInit : "-- no before-init SQL found"}
      ${is.adminTables}
      ${is.adminTableIndexes}

      ${session.diagnosticsView()}

      -- register the current device and session and use the identifiers for all logging
      ${await session.deviceSqlDML()}
      ${await session.orchSessionSqlDML()}

      -- Load Reference data from csvs

      ${afterInit.length > 0 ? afterInit : "-- no after-init SQL found"}`.SQL(
      this.govn.emitCtx
    );

    const execResult = await this.duckdb.execute(initDDL, osc.current.nbCellID);
    if (execResult.status.code != 0) {
      const diagsTmpFile = await this.duckdb.writeDiagnosticsSqlMD(
        {
          exec_code: String(execResult.status.code),
          sql: initDDL,
          exec_identity: `initDDL`,
        },
        execResult.status
      );
      // the kernel stops processing if it's not a OrchResumableError instance
      throw new Error(
        `duckdb.execute status in ${osc.current.nbCellID}() did not return zero, see ${diagsTmpFile}`
      );
    }
  }

  /**
   * Walk the root paths, find all types of files we can handle, generate
   * ingestion SQL ("loading" part of ELT/ETL) and execute the SQL in a single
   * DuckDB call. Then, for each successful execution (any ingestions that do
   * not create issues in the issue table) prepare the list of subsequent steps
   * for further cleansing, validation, transformations, etc.
   *
   * The reason why we separate initial ingestion and structural validation from
   * content validation, cleansing, and transformations is because content
   * SQL relies on table names and table column names in CTEs and other SQL
   * which must exist otherwise they cause syntax errors. Once we validate the
   * structure and columns of ingested data, then the remainder of the SQL for
   * cleansing, validation, or transformations will not cause syntax errors.
   *
   * @param osc the type-safe notebook cell context for diagnostics or business rules
   * @returns list of "assurables" that did not generate any ingestion issues
   */
  async ingest(osc: OrchStepContext) {
    const {
      govn,
      govn: { emitCtx: ctx },
      args: {
        session,
        referenceDataHome = "https://github.com/vinodkalpaka01/ingestion-center/tree/main/src/ingestion-center-elt/reference-data",
      },
    } = this;
    const { sessionID } = session;
    const tableGroupCheckSql: string[] = [];
    let psIndex = 0;
    this.potentialSources = Array.from(
      await this.iss.sources(this.args.walkRootPaths)
    );

    const referenceIngestSources = [
      new ref.ScreeningStatusCodeReferenceCsvFileIngestSource(
        referenceDataHome,
        govn
      ),
      new ref.BusinessRulesReferenceCsvFileIngestSource(
        referenceDataHome,
        govn
      ),
    ];

    this.potentialSources.push(...referenceIngestSources);

    this.ingestables = [];
    const uniqueGroups = new Set<string>();
    for (const ps of this.potentialSources) {
      const { uri, tableName } = ps;
      if ("groupName" in ps) {
        uniqueGroups.add(ps.groupName);
      }
      const sessionEntryID = await govn.emitCtx.newUUID(govn.deterministicPKs);
      const workflow = await ps.workflow(session, sessionEntryID);
      const checkStruct = await workflow.ingestSQL({
        initState: () => "ENTER(ingest)",
        sessionEntryInsertDML: () => {
          return govn.orchSessionEntryCRF.insertDML({
            orch_session_entry_id: sessionEntryID,
            session_id: sessionID,
            ingest_src: uri,
            ingest_table_name: tableName,
          });
        },
        issueInsertDML: async (message, type = "Structural") => {
          return govn.orchSessionIssueCRF.insertDML({
            orch_session_issue_id: await govn.emitCtx.newUUID(
              govn.deterministicPKs
            ),
            session_id: sessionID,
            session_entry_id: sessionEntryID,
            issue_type: type,
            issue_message: message,
            invalid_value: uri,
          });
        },
      });

      uniqueGroups.forEach((value) => {
        const gCsvStr = new gov.GroupCsvStructureRules(
          "information_schema.tables",
          sessionID,
          sessionEntryID,
          govn
        );
        tableGroupCheckSql.push(
          gCsvStr.checkAllTablesAreIngestedInAGroup(value, tableName).SQL(ctx) +
            ";"
        );
      });

      this.ingestables.push({
        psIndex,
        sessionEntryID,
        sql:
          `-- ${osc.current.nbCellID} ${uri} (${tableName})\n` +
          checkStruct.SQL(ctx),
        source: ps,
        workflow,
        issues: [],
      });
      psIndex++;
    }

    // run the SQL and then emit the errors to STDOUT in JSON
    const ingestSQL = this.ingestables.map((ic) => ic.sql);
    ingestSQL.push(tableGroupCheckSql.join("\n"));
    ingestSQL.push(
      `SELECT session_entry_id, orch_session_issue_id, issue_type, issue_message, invalid_value FROM orch_session_issue WHERE session_id = '${sessionID}'`
    );
    const ingestResult = await this.duckdb.jsonResult<
      (typeof this.ingestables)[number]["issues"][number]
    >(ingestSQL.join("\n"), osc.current.nbCellID);
    if (ingestResult.json) {
      // if errors were found, put the problems into the proper ingestable issues
      for (const row of ingestResult.json) {
        const ingestable = this.ingestables.find(
          (i) => i.sessionEntryID == row.session_entry_id
        );
        if (ingestable) ingestable.issues.push(row);
      }
    }

    // for the next step (ensureContent) we only want to pursue remainder of
    // ingestion for those ingestables that didn't have errors during construction
    return this.ingestables.filter((i) => i.issues.length == 0);
  }

  /**
   * For all ingestions from the previous step that did not create any issues
   * (meaning they were successfully ingested), prepare all cleansing,
   * validation, transformation and other SQL and then execute entire SQL as a
   * single DuckDB instance call.
   *
   * The content SQL is separated from structural SQL to avoid syntax errors
   * when a table or table column does not exist (which can happen if the format
   * or structure of an ingested CSV, Excel, Parquet, or other source does not
   * match our expectations).
   *
   * @param osc the type-safe notebook cell context for diagnostics or business rules
   * @param ingestResult the list of successful ingestions from the previous step
   */
  async ensureContent(
    osc: OrchStepContext,
    ingestResult: Awaited<ReturnType<typeof OrchEngine.prototype.ingest>>
  ) {
    const {
      govn: { emitCtx: ctx },
    } = this;

    // any ingestions that did not produce structural errors will have SQL
    const assurableSQL = await Promise.all(
      ingestResult.map(async (sr) =>
        (await sr.workflow.assuranceSQL()).SQL(ctx)
      )
    );

    await this.duckdb.execute(assurableSQL.join("\n"), osc.current.nbCellID);
    return ingestResult;
  }

  async emitResources(
    isc: OrchStepContext,
    ensureResult: Awaited<ReturnType<typeof OrchEngine.prototype.ensureContent>>
  ) {
    const {
      args: {
        workflowPaths: { egress },
        session,
      },
      govn: { emitCtx: ctx },
    } = this;
    if (egress.resourceDbSupplier) {
      const resourceDb = egress.resourceDbSupplier();
      try {
        Deno.removeSync(resourceDb);
      } catch (_err) {
        // ignore errors if file does not exist
      }

      const adminTables = [
        this.govn.device,
        this.govn.orchSession,
        this.govn.orchSessionEntry,
        this.govn.orchSessionState,
        this.govn.orchSessionExec,
        this.govn.orchSessionIssue,
        this.sqlPageNB.table,
      ];

      const beforeFinalize = session.sqlCatalogSqlText("before-finalize");
      const afterFinalize = session.sqlCatalogSqlText("after-finalize");
      const rdbSchemaName = "resource_db";

      const exportsSQL = await Promise.all(
        ensureResult.map(async (sr) =>
          (await sr.workflow.exportResourceSQL(rdbSchemaName)).SQL(ctx)
        )
      );

      // `beforeFinalize` SQL includes state management SQL that log all the
      // state changes between this notebooks' cells; however, the "exit" state
      // for this method will not be stored since this is the last step in the
      // process and the exit state will not be encountered before writing to
      // the database.

      // Everything with the `diagnostics-md-ignore-start` and `*-finish` will be
      // executed by the orchestration engine but the SQL won't be store in the
      // diagnostics log because it's noisy and mostly infrastructure.
      const sqlPageCells = await this.sqlPageNB.sqlCells();
      await this.duckdb.execute(
        // deno-fmt-ignore
        this.govn.SQL`
          ${
            beforeFinalize.length > 0
              ? beforeFinalize.join(";\n") + ";"
              : "-- no before-finalize SQL provided"
          }

          -- diagnostics-md-ignore-start "SQLPage and execution diagnostics SQL DML"
          -- emit all the SQLPage content
          ${sqlPageCells};

          -- emit all the execution diagnostics
          ${this.duckdb.diagnostics};
          -- diagnostics-md-ignore-finish "SQLPage and execution diagnostics SQL DML"

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

          ATTACH '${
            dbConstants.mysql_con
          }' AS ${rdbSchemaName}  (TYPE mysql_scanner);

          -- copy relevant orchestration engine admin tables into the the attached database
          ${adminTables.map((t) => ({
            SQL: () =>
              `CREATE TABLE ${rdbSchemaName}.${t.tableName} AS SELECT * FROM ${t.tableName}`,
          }))}

          -- export content tables from DuckDb into the attached database (nature-dependent)
          ${exportsSQL};


          DETACH DATABASE ${rdbSchemaName};

          ${
            afterFinalize.length > 0
              ? afterFinalize.join(";\n") + ";"
              : "-- no after-finalize SQL provided"
          }`.SQL(this.govn.emitCtx),
        isc.current.nbCellID
      );
      //createFhirViewQuery

      const tableNames = Array.from(
        await this.iss.sources(this.args.walkRootPaths)
      )
        .filter((ps: any) => Boolean(ps.tableName))
        .map((ps: any) => ps.tableName)
        .join(", ");

      const client = await new mysql.Client().connect({
        hostname: dbConstants.mysql_host,
        username: dbConstants.mysql_user,
        db: dbConstants.mysql_database,
        port: dbConstants.mysql_port,
      });
      let branch: any[] = [];
      const [_, changes] = await Promise.all(
        [dbConstants.useDBQuery, dbConstants.checkForChangesQuery].map(
          async (query) => await client.execute(query)
        )
      );
      if ("rows" in changes && changes.rows.length) {
        console.log("rows: " + changes.rows);
        branch = await Promise.all(
          dbConstants
            .doltQuery(tableNames)
            .map(async (query) => await client.execute(query))
        );
        await client.close();
      }
      if (branch.length && "rows" in branch[branch.length - 1]) {
        console.info(
          "  💡",
          colors.green(
            `commit message: ${dbConstants.commitMessage(tableNames)} `
          )
        );
        console.info(
          "  🆗",
          colors.green(
            `committed and pushed changes ${
              branch[branch.length - 1].rows[0].message
            } `
          )
        );
      }
    }
  }

  async checkRequiredTables(): Promise<number> {
    const resultsCheck = await this.duckdb.jsonResult(
      this.govn.SQL`
        SELECT COUNT(*) AS table_count FROM information_schema.tables WHERE table_name IN ('${csv.aggrLibreTableName}', '${csv.aggrVitalSignsTableName}', '${csv.aggrGlucose}');
      `.SQL(this.govn.emitCtx)
    );

    if (resultsCheck.json) {
      for (const row of resultsCheck.json as TableCountRecord[]) {
        return row.table_count;
      }
    }
    return 0;
  }

  createDerivedFromCte(): string {
    // Return the SQL string for the derived_from_cte common table expression
    return `derived_from_cte AS (
      SELECT
          parent_question_code,
          parent_question_sl_no,
          json_group_array(json_object('reference', derived_reference)) AS derived_from_references
      FROM (
          SELECT
              acw.QUESTION_CODE AS parent_question_code,
              acw.QUESTION_SLNO AS parent_question_sl_no,
              CONCAT('Observation/ObservationResponseQuestion_', acw_sub.QUESTION_SLNO) AS derived_reference
          FROM
              ahc_cross_walk acw
          INNER JOIN ahc_cross_walk acw_sub ON acw_sub."QUESTION_SLNO_REFERENCE" = acw.QUESTION_SLNO
          GROUP BY
              acw.QUESTION_CODE,
              acw.QUESTION_SLNO,
              acw_sub.QUESTION_SLNO
      ) AS distinct_references
      GROUP BY
          parent_question_code,
          parent_question_sl_no
    )`;
  }

  // `finalize` means always run this even if errors abort the above methods
  @oeDescr.finalize()
  async emitDiagnostics() {
    const {
      workflowPaths: { egress },
    } = this.args;
    if (egress.diagsXlsxSupplier) {
      const diagsXlsx = egress.diagsXlsxSupplier();
      // if Excel workbook already exists, GDAL xlsx driver will error
      try {
        Deno.removeSync(diagsXlsx);
      } catch (_err) {
        // ignore errors if file does not exist
      }

      // deno-fmt-ignore
      await this.duckdb.execute(
        ws.unindentWhitespace(`
          INSTALL spatial; LOAD spatial;
          -- TODO: join with orch_session table to give all the results in one sheet
          COPY (SELECT * FROM orch_session_issue_classification) TO '${diagsXlsx}' WITH (FORMAT GDAL, DRIVER 'xlsx');`),
        "emitDiagnostics"
      );
    }

    const stringifiableArgs = JSON.parse(
      JSON.stringify(
        {
          ...this.args,
          sources: this.potentialSources
            ? array.distinctEntries(
                this.potentialSources.map((ps, psIndex) => ({
                  uri: ps.uri,
                  nature: ps.nature,
                  tableName: ps.tableName,
                  ingestionIssues: this.ingestables?.find(
                    (i) => i.psIndex == psIndex
                  )?.issues.length,
                }))
              )
            : undefined,
        },
        // deep copy only string-frienly properties
        (key, value) => (key == "session" ? undefined : value),
        "  "
      )
    );

    if (egress.diagsJsonSupplier) {
      const diagsJson = egress.diagsJsonSupplier();
      await Deno.writeTextFile(
        diagsJson,
        JSON.stringify(
          { args: stringifiableArgs, diags: this.duckdb.diagnostics },
          null,
          "  "
        )
      );
    }

    if (egress.fhirJsonSupplier && fhirGeneratorCheck) {
      const results = await this.duckdb.jsonResult(
        this.govn.SQL`
          SELECT PAT_MRN_ID, ENCOUNTER_ID, FHIR_Bundle as FHIR FROM fhir_bundle
        `.SQL(this.govn.emitCtx)
      );

      if (results.json) {
        try {
          let fhirHttpContent = "";
          for (const row of results.json as FhirRecord[]) {
            const fhirJson = egress.fhirJsonSupplier(
              row.PAT_MRN_ID + "-" + row.ENCOUNTER_ID
            );
            const refinedFhir = removeNulls(row.FHIR);
            await Deno.writeTextFile(fhirJson, JSON.stringify(refinedFhir));
            if (egress.fhirHttpSupplier) {
              fhirHttpContent =
                fhirHttpContent + "### Submit FHIR Resource Bundle\n\n";
              fhirHttpContent =
                fhirHttpContent +
                "POST https://{{host}}/{{path}}?processingAgent=QE HTTP/1.1\n";
              fhirHttpContent =
                fhirHttpContent + "content-type: application/json\n\n";
              fhirHttpContent = fhirHttpContent + "< " + fhirJson + "\n\n";
            }
          }
          if (egress.fhirHttpSupplier) {
            const fhirHttp = egress.fhirHttpSupplier();
            await Deno.writeTextFile(fhirHttp, fhirHttpContent);
          }
        } catch (error) {
          console.log(error);
        }
      }
    }

    const markdown = this.duckdb.diagnosticsMarkdown();
    if (egress.diagsMdSupplier) {
      const diagsMd = egress.diagsMdSupplier();
      await Deno.writeTextFile(
        diagsMd,
        "---\n" +
          yaml.stringify(stringifiableArgs) +
          "---\n" +
          "# Orchestration Diagnostics\n" +
          markdown
      );
    }

    if (egress.resourceDbSupplier) {
      const {
        orchSession: sessTbl,
        orchSession: { columnNames: c },
        emitCtx: { sqlTextEmitOptions: steo },
      } = this.govn;
      const { sessionID } = this.args.session;
      const resourceDb = egress.resourceDbSupplier();
      await dax.$`sqlite3 ${resourceDb}`.stdinText(
        `UPDATE ${sessTbl.tableName} SET
            ${c.orch_finished_at} = CURRENT_TIMESTAMP,
            ${c.args_json} = ${
          steo.quotedLiteral(JSON.stringify(stringifiableArgs, null, "  "))[1]
        },
            ${c.diagnostics_json} = ${
          steo.quotedLiteral(
            JSON.stringify(this.duckdb.diagnostics, null, "  ")
          )[1]
        },
            ${c.diagnostics_md} = ${steo.quotedLiteral(markdown)[1]}
          WHERE ${c.orch_session_id} = '${sessionID}'`
      );
    }

    await this.args.workflowPaths.finalizePaths?.();
  }
}
