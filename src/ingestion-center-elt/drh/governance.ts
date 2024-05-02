import { SQLa_orch_duckdb as ddbo } from "./deps.ts";

export class GroupCsvStructureRules<
  TableName extends string,
  ColumnName extends string
> extends ddbo.DuckDbOrchTableAssuranceRules<TableName, ColumnName> {
  checkAllTablesAreIngestedInAGroup(groupName: string, tableName: string) {
    return this.govn.SQL`
      WITH check_all_tables_are_ingested_in_a_group AS (
        WITH required_tables AS (
            SELECT '${this.govn.toSnakeCase("libre" + groupName)}'
              AS table_name,
              'LIBRE' AS table_name_suffix
            UNION ALL
            SELECT '${this.govn.toSnakeCase("vitalsigns" + groupName)}'
              AS table_name,
              'VITALSIGNS' AS table_name_suffix
            UNION ALL
            SELECT '${this.govn.toSnakeCase("glucose" + groupName)}'
              AS table_name,
              'GLUCOSE' AS table_name_suffix
        )
        SELECT rt.table_name as table_name, rt.table_name_suffix as table_name_suffix
        FROM required_tables rt
        LEFT JOIN ${this.tableName} ist ON rt.table_name = ist.table_name
        WHERE
          '${tableName}' IN (
            '${this.govn.toSnakeCase("libre" + groupName)}',
            '${this.govn.toSnakeCase("vitalsigns" + groupName)}',
            '${this.govn.toSnakeCase("glucose" + groupName)}'
            )
        AND ist.table_name IS NULL
      )
      ${this.insertRowValueIssueCtePartial(
        "check_all_tables_are_ingested_in_a_group",
        `CSV File Missing`,
        `NULL`,
        `NULL`,
        "table_name",
        `'CSV file ' || table_name_suffix || '${groupName} not found under the group (${groupName})'`
      )}
    `;
  }
}
/**
 * CommonAssuranceRules class provides common assurance rules applicable for all classes.
 * It extends DuckDbOrchTableAssuranceRules for additional functionality.
 */
export class CommonAssuranceRules<
  TableName extends string,
  ColumnName extends string
> extends ddbo.DuckDbOrchTableAssuranceRules<TableName, ColumnName> {
  // if there are any custom business logic rules put them here and if they can
  // be further generalized we can move them into the upstream SQLa library

  // any rules defined here will be available as car.rule() in the
  onlyAllowValidZipInAllRows(columnName: ColumnName) {
    return this.tableRules.patternValueInAllRows(
      columnName,
      "^\\d{5}(\\d{4})?$"
    );
  }

  onlyAllowAlphabetsAndNumbersInAllRows(columnName: ColumnName) {
    return this.tableRules.patternValueInAllRows(columnName, "^[a-zA-Z0-9]+$");
  }

  onlyAllowAlphabetsWithSpacesInAllRows(columnName: ColumnName) {
    return this.tableRules.patternValueInAllRows(columnName, "^[a-zA-Z\\s]+$");
  }

  onlyAllowAlphabetsAndNumbersWithSpaceInAllRows(columnName: ColumnName) {
    return this.tableRules.patternValueInAllRows(
      columnName,
      "^[a-zA-Z0-9\\s]+$"
    );
  }

  onlyAllowValidMedicaidCinFormatInAllRows(columnName: ColumnName) {
    return this.tableRules.patternValueInAllRows(
      columnName,
      "^[A-Za-z]{2}\\d{5}[A-Za-z]$"
    );
  }

  onlyAllowValidIntegerAlphaNumericStringInAllRows(columnName: ColumnName) {
    const cteName = "valid_integer_alphanumeric_string_in_all_rows";

    // Construct the SQL query using tagged template literals
    return this.govn.SQL`
      WITH ${cteName} AS (
        SELECT '${columnName}' AS issue_column,
          t."${columnName}" AS invalid_value,
          t.src_file_row_number AS issue_row
        FROM ${this.tableName} t
        WHERE t."${columnName}" SIMILAR TO '[0-9]+'
      )
      ${this.insertRowValueIssueCtePartial(
        cteName,
        `Data Type Mismatch`,
        "issue_row",
        "issue_column",
        "invalid_value",
        `'Invalid value "' || invalid_value || '" found in ' || issue_column`,
        `'Invalid string of numbers found'`
      )}`;
  }

  onlyAllowValidFieldCombinationsInAllRows(
    columnName1: ColumnName,
    columnName2: ColumnName
  ) {
    const cteName = "valid_field_combination_in_all_rows";
    const columnReference = {
      ENCOUNTER_CLASS_CODE: {
        referenceTableName: "encounter_class_reference",
        referenceFieldName: "Code",
      },
    };
    // Construct the SQL query using tagged template literals
    return this.govn.SQL`
      WITH ${cteName} AS (
        SELECT 	'${columnName1}' AS issue_column,
            tbl."${columnName1}" AS invalid_value,
            tbl."${columnName2}" AS dependent_value,
            tbl.src_file_row_number AS issue_row
        FROM ${this.tableName}  tbl
        WHERE tbl."${columnName1}" is not null
        and tbl."${columnName2}" is not null
        and NOT EXISTS ( SELECT "${
          columnReference[columnName2 as keyof typeof columnReference]
            .referenceFieldName
        }" FROM ${
      columnReference[columnName1 as keyof typeof columnReference]
        .referenceTableName
    } WHERE UPPER(tbl."${columnName2}") = UPPER("${
      columnReference[columnName2 as keyof typeof columnReference]
        .referenceFieldName
    }") AND UPPER(tbl."${columnName1}") = UPPER("${
      columnReference[columnName1 as keyof typeof columnReference]
        .referenceFieldName
    }"))
      )
      ${this.insertRowValueIssueCtePartial(
        cteName,
        `Combination Not Matching`,
        "issue_row",
        "issue_column",
        "invalid_value",
        `'Invalid value "' || invalid_value || '" found in ' || issue_column`,
        `'The ${columnName1} "' || invalid_value || '" of ${columnName2} "' || dependent_value || '" is not matching with the ${columnName1} of ${columnName2} in reference data'`
      )}`;
  }
}

/**
 * Represents assurance rules specific to screening data, extending from DuckDbOrchTableAssuranceRules.
 * Provides methods for enforcing screening-specific business logic.
 */

export class AhcCrossWalkAssuranceRules<
  TableName extends string,
  ColumnName extends string
> extends ddbo.DuckDbOrchTableAssuranceRules<TableName, ColumnName> {
  readonly car: CommonAssuranceRules<TableName, ColumnName>;

  constructor(
    readonly tableName: TableName,
    readonly sessionID: string,
    readonly sessionEntryID: string,
    readonly govn: ddbo.DuckDbOrchGovernance
  ) {
    super(tableName, sessionID, sessionEntryID, govn);
    this.car = new CommonAssuranceRules<TableName, ColumnName>(
      tableName,
      sessionID,
      sessionEntryID,
      govn
    );
  }
}

export class PreferredLanguageReferenceAssuranceRules<
  TableName extends string,
  ColumnName extends string
> extends ddbo.DuckDbOrchTableAssuranceRules<TableName, ColumnName> {
  readonly car: CommonAssuranceRules<TableName, ColumnName>;

  constructor(
    readonly tableName: TableName,
    readonly sessionID: string,
    readonly sessionEntryID: string,
    readonly govn: ddbo.DuckDbOrchGovernance
  ) {
    super(tableName, sessionID, sessionEntryID, govn);
    this.car = new CommonAssuranceRules<TableName, ColumnName>(
      tableName,
      sessionID,
      sessionEntryID,
      govn
    );
  }
}

export class SdohDomainReferenceAssuranceRules<
  TableName extends string,
  ColumnName extends string
> extends ddbo.DuckDbOrchTableAssuranceRules<TableName, ColumnName> {
  readonly car: CommonAssuranceRules<TableName, ColumnName>;

  constructor(
    readonly tableName: TableName,
    readonly sessionID: string,
    readonly sessionEntryID: string,
    readonly govn: ddbo.DuckDbOrchGovernance
  ) {
    super(tableName, sessionID, sessionEntryID, govn);
    this.car = new CommonAssuranceRules<TableName, ColumnName>(
      tableName,
      sessionID,
      sessionEntryID,
      govn
    );
  }
}

export class EncounterClassReferenceAssuranceRules<
  TableName extends string,
  ColumnName extends string
> extends ddbo.DuckDbOrchTableAssuranceRules<TableName, ColumnName> {
  readonly car: CommonAssuranceRules<TableName, ColumnName>;

  constructor(
    readonly tableName: TableName,
    readonly sessionID: string,
    readonly sessionEntryID: string,
    readonly govn: ddbo.DuckDbOrchGovernance
  ) {
    super(tableName, sessionID, sessionEntryID, govn);
    this.car = new CommonAssuranceRules<TableName, ColumnName>(
      tableName,
      sessionID,
      sessionEntryID,
      govn
    );
  }
}

export class EncounterStatusCodeReferenceAssuranceRules<
  TableName extends string,
  ColumnName extends string
> extends ddbo.DuckDbOrchTableAssuranceRules<TableName, ColumnName> {
  readonly car: CommonAssuranceRules<TableName, ColumnName>;

  constructor(
    readonly tableName: TableName,
    readonly sessionID: string,
    readonly sessionEntryID: string,
    readonly govn: ddbo.DuckDbOrchGovernance
  ) {
    super(tableName, sessionID, sessionEntryID, govn);
    this.car = new CommonAssuranceRules<TableName, ColumnName>(
      tableName,
      sessionID,
      sessionEntryID,
      govn
    );
  }
}

export class EncounterTypeCodeReferenceAssuranceRules<
  TableName extends string,
  ColumnName extends string
> extends ddbo.DuckDbOrchTableAssuranceRules<TableName, ColumnName> {
  readonly car: CommonAssuranceRules<TableName, ColumnName>;

  constructor(
    readonly tableName: TableName,
    readonly sessionID: string,
    readonly sessionEntryID: string,
    readonly govn: ddbo.DuckDbOrchGovernance
  ) {
    super(tableName, sessionID, sessionEntryID, govn);
    this.car = new CommonAssuranceRules<TableName, ColumnName>(
      tableName,
      sessionID,
      sessionEntryID,
      govn
    );
  }
}

export class ScreeningStatusCodeReferenceAssuranceRules<
  TableName extends string,
  ColumnName extends string
> extends ddbo.DuckDbOrchTableAssuranceRules<TableName, ColumnName> {
  readonly car: CommonAssuranceRules<TableName, ColumnName>;

  constructor(
    readonly tableName: TableName,
    readonly sessionID: string,
    readonly sessionEntryID: string,
    readonly govn: ddbo.DuckDbOrchGovernance
  ) {
    super(tableName, sessionID, sessionEntryID, govn);
    this.car = new CommonAssuranceRules<TableName, ColumnName>(
      tableName,
      sessionID,
      sessionEntryID,
      govn
    );
  }
}

export class AdministrativeSexReferenceAssuranceRules<
  TableName extends string,
  ColumnName extends string
> extends ddbo.DuckDbOrchTableAssuranceRules<TableName, ColumnName> {
  readonly car: CommonAssuranceRules<TableName, ColumnName>;

  constructor(
    readonly tableName: TableName,
    readonly sessionID: string,
    readonly sessionEntryID: string,
    readonly govn: ddbo.DuckDbOrchGovernance
  ) {
    super(tableName, sessionID, sessionEntryID, govn);
    this.car = new CommonAssuranceRules<TableName, ColumnName>(
      tableName,
      sessionID,
      sessionEntryID,
      govn
    );
  }
}

export class GenderIdentityReferenceAssuranceRules<
  TableName extends string,
  ColumnName extends string
> extends ddbo.DuckDbOrchTableAssuranceRules<TableName, ColumnName> {
  readonly car: CommonAssuranceRules<TableName, ColumnName>;

  constructor(
    readonly tableName: TableName,
    readonly sessionID: string,
    readonly sessionEntryID: string,
    readonly govn: ddbo.DuckDbOrchGovernance
  ) {
    super(tableName, sessionID, sessionEntryID, govn);
    this.car = new CommonAssuranceRules<TableName, ColumnName>(
      tableName,
      sessionID,
      sessionEntryID,
      govn
    );
  }
}

export class SexAtBirthReferenceAssuranceRules<
  TableName extends string,
  ColumnName extends string
> extends ddbo.DuckDbOrchTableAssuranceRules<TableName, ColumnName> {
  readonly car: CommonAssuranceRules<TableName, ColumnName>;

  constructor(
    readonly tableName: TableName,
    readonly sessionID: string,
    readonly sessionEntryID: string,
    readonly govn: ddbo.DuckDbOrchGovernance
  ) {
    super(tableName, sessionID, sessionEntryID, govn);
    this.car = new CommonAssuranceRules<TableName, ColumnName>(
      tableName,
      sessionID,
      sessionEntryID,
      govn
    );
  }
}

export class SexualOrientationReferenceAssuranceRules<
  TableName extends string,
  ColumnName extends string
> extends ddbo.DuckDbOrchTableAssuranceRules<TableName, ColumnName> {
  readonly car: CommonAssuranceRules<TableName, ColumnName>;

  constructor(
    readonly tableName: TableName,
    readonly sessionID: string,
    readonly sessionEntryID: string,
    readonly govn: ddbo.DuckDbOrchGovernance
  ) {
    super(tableName, sessionID, sessionEntryID, govn);
    this.car = new CommonAssuranceRules<TableName, ColumnName>(
      tableName,
      sessionID,
      sessionEntryID,
      govn
    );
  }
}

export class BusinessRulesReferenceAssuranceRules<
  TableName extends string,
  ColumnName extends string
> extends ddbo.DuckDbOrchTableAssuranceRules<TableName, ColumnName> {
  readonly car: CommonAssuranceRules<TableName, ColumnName>;

  constructor(
    readonly tableName: TableName,
    readonly sessionID: string,
    readonly sessionEntryID: string,
    readonly govn: ddbo.DuckDbOrchGovernance
  ) {
    super(tableName, sessionID, sessionEntryID, govn);
    this.car = new CommonAssuranceRules<TableName, ColumnName>(
      tableName,
      sessionID,
      sessionEntryID,
      govn
    );
  }
}

export class RaceReferenceAssuranceRules<
  TableName extends string,
  ColumnName extends string
> extends ddbo.DuckDbOrchTableAssuranceRules<TableName, ColumnName> {
  readonly car: CommonAssuranceRules<TableName, ColumnName>;

  constructor(
    readonly tableName: TableName,
    readonly sessionID: string,
    readonly sessionEntryID: string,
    readonly govn: ddbo.DuckDbOrchGovernance
  ) {
    super(tableName, sessionID, sessionEntryID, govn);
    this.car = new CommonAssuranceRules<TableName, ColumnName>(
      tableName,
      sessionID,
      sessionEntryID,
      govn
    );
  }
}

export class EthnicityReferenceAssuranceRules<
  TableName extends string,
  ColumnName extends string
> extends ddbo.DuckDbOrchTableAssuranceRules<TableName, ColumnName> {
  readonly car: CommonAssuranceRules<TableName, ColumnName>;

  constructor(
    readonly tableName: TableName,
    readonly sessionID: string,
    readonly sessionEntryID: string,
    readonly govn: ddbo.DuckDbOrchGovernance
  ) {
    super(tableName, sessionID, sessionEntryID, govn);
    this.car = new CommonAssuranceRules<TableName, ColumnName>(
      tableName,
      sessionID,
      sessionEntryID,
      govn
    );
  }
}

export class LibreAssuranceRules<
  TableName extends string,
  ColumnName extends string
> extends ddbo.DuckDbOrchTableAssuranceRules<TableName, ColumnName> {
  readonly car: CommonAssuranceRules<TableName, ColumnName>;

  constructor(
    readonly tableName: TableName,
    readonly sessionID: string,
    readonly sessionEntryID: string,
    readonly govn: ddbo.DuckDbOrchGovernance
  ) {
    super(tableName, sessionID, sessionEntryID, govn);
    this.car = new CommonAssuranceRules<TableName, ColumnName>(
      tableName,
      sessionID,
      sessionEntryID,
      govn
    );
  }

  onlyAllowValidEncounterClassCodeInAllRows(columnName: ColumnName) {
    const cteName = "valid_encounter_class_in_all_rows";
    const encounterClassReferenceTable = "encounter_class_reference";
    // Construct the SQL query using tagged template literals
    return this.govn.SQL`
      WITH ${cteName} AS (
          SELECT '${columnName}' AS issue_column,
                 sr."${columnName}" AS invalid_value,
                 sr.src_file_row_number AS issue_row
            FROM ${this.tableName} sr
            LEFT JOIN ${encounterClassReferenceTable} ecr
            ON UPPER(sr.${columnName}) = UPPER(ecr.Code)
           WHERE sr.${columnName} IS NOT NULL
            AND ecr.Code IS NULL
      )
      ${this.insertRowValueIssueCtePartial(
        cteName,
        `Invalid ENCOUNTER CLASS CODE`,
        "issue_row",
        "issue_column",
        "invalid_value",
        `'Invalid ENCOUNTER CLASS CODE "' || invalid_value || '" found in ' || issue_column`,
        `'Validate ENCOUNTER CLASS CODE with encounter class reference data'`
      )}`;
  }

  onlyAllowValidRecordedTimeInAllRows(columnName: ColumnName) {
    // Construct the SQL query using tagged template literals
    const cteName = "valid_date_time_in_all_rows";

    // deno-fmt-ignore
    return this.govn.SQL`
      WITH ${cteName} AS (
              SELECT  '${columnName}' AS issue_column,
                    "${columnName}" AS invalid_value,
                    src_file_row_number AS issue_row
              FROM "${this.tableName}"
              WHERE "${columnName}" IS NOT NULL
              AND "${columnName}" NOT SIMILAR TO '([0-9]([0-9]([0-9][1-9]|[1-9]0)|[1-9]00)|[1-9]000)-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])T([01][0-9]|2[0-3]):[0-5][0-9]:([0-5][0-9]|60)(\\.[0-9]+)?(Z|(\\+|-)((0[0-9]|1[0-3]):[0-5][0-9]|14:00))$'
              OR TRY_CAST("${columnName}" AS TIMESTAMP) IS NULL
        )
      ${this.insertRowValueIssueCtePartial(
        cteName,
        "Invalid Date",
        "issue_row",
        "issue_column",
        "invalid_value",
        `'Invalid timestamp "' || invalid_value || '" found in ' || issue_column`,
        `'Please be sure to provide both a valid date and time.'`
      )}`;
  }

  matchesPatMrnIdAcrossScreeningQeAdminDemographics(
    patMrnIdcolumnName: ColumnName,
    facilityIdcolumnName: ColumnName,
    relatedTableNames: {
      adminDemographicsTableName: string;
      qeAdminDataTableName: string;
    }
  ) {
    const cteName = "valid_pat_mrn_id_across_all_three_tables";
    // Construct the SQL query using tagged template literals
    return this.govn.SQL`
      WITH ${cteName} AS (
        SELECT '${patMrnIdcolumnName}' AS issue_column, '${
      this.tableName
    }' AS issue_table_name, a.${patMrnIdcolumnName} AS invalid_pat_value, a.${facilityIdcolumnName} AS invalid_facility_value, a.src_file_row_number AS issue_row
        FROM ${this.tableName} a
        LEFT JOIN ${relatedTableNames.qeAdminDataTableName} b
        ON UPPER(a.${patMrnIdcolumnName}) = UPPER(b.${patMrnIdcolumnName})
        AND UPPER(a.${facilityIdcolumnName}) = UPPER(b.${facilityIdcolumnName})
        LEFT JOIN ${relatedTableNames.adminDemographicsTableName} c
        ON UPPER(a.${patMrnIdcolumnName}) = UPPER(c.${patMrnIdcolumnName})
        AND UPPER(a.${facilityIdcolumnName}) = UPPER(c.${facilityIdcolumnName})
        WHERE b.${patMrnIdcolumnName} IS NULL OR c.${patMrnIdcolumnName} IS NULL OR b.${facilityIdcolumnName} IS NULL OR c.${facilityIdcolumnName} IS NULL
        UNION
        SELECT '${patMrnIdcolumnName}' AS issue_column, '${
      relatedTableNames.qeAdminDataTableName
    }' AS issue_table_name, b.${patMrnIdcolumnName} AS invalid_pat_value, b.${facilityIdcolumnName} AS invalid_facility_value, b.src_file_row_number AS issue_row
        FROM ${relatedTableNames.qeAdminDataTableName} b
        LEFT JOIN ${this.tableName} a
        ON UPPER(a.${patMrnIdcolumnName}) = UPPER(b.${patMrnIdcolumnName})
        AND UPPER(a.${facilityIdcolumnName}) = UPPER(b.${facilityIdcolumnName})
        LEFT JOIN ${relatedTableNames.adminDemographicsTableName} c
        ON UPPER(b.${patMrnIdcolumnName}) = UPPER(c.${patMrnIdcolumnName})
        AND UPPER(b.${facilityIdcolumnName}) = UPPER(c.${facilityIdcolumnName})
        WHERE a.${patMrnIdcolumnName} IS NULL OR c.${patMrnIdcolumnName} IS NULL OR a.${facilityIdcolumnName} IS NULL OR c.${facilityIdcolumnName} IS NULL
        UNION
        SELECT '${patMrnIdcolumnName}' AS issue_column, '${
      relatedTableNames.adminDemographicsTableName
    }' AS issue_table_name, c.${patMrnIdcolumnName} AS invalid_pat_value, c.${facilityIdcolumnName} AS invalid_facility_value, c.src_file_row_number AS issue_row
        FROM ${relatedTableNames.adminDemographicsTableName} c
        LEFT JOIN ${this.tableName} a
        ON UPPER(a.${patMrnIdcolumnName}) = UPPER(c.${patMrnIdcolumnName})
        AND UPPER(a.${facilityIdcolumnName}) = UPPER(c.${facilityIdcolumnName})
        LEFT JOIN ${relatedTableNames.qeAdminDataTableName} b
        ON UPPER(c.${patMrnIdcolumnName}) = UPPER(b.${patMrnIdcolumnName})
        AND UPPER(c.${facilityIdcolumnName}) = UPPER(b.${facilityIdcolumnName})
        WHERE a.${patMrnIdcolumnName} IS NULL OR b.${patMrnIdcolumnName} IS NULL OR a.${facilityIdcolumnName} IS NULL OR b.${facilityIdcolumnName} IS NULL
      )
      ${this.insertRowValueIssueCtePartial(
        cteName,
        `${patMrnIdcolumnName} that does not match the ${facilityIdcolumnName}`,
        "issue_row",
        "issue_column",
        "invalid_pat_value",
        `'${patMrnIdcolumnName} ("' || invalid_pat_value || '") that does not match the ${facilityIdcolumnName} ("' || invalid_facility_value || '") across the files was found in "' || issue_table_name || '".'`,
        `'Validate ${patMrnIdcolumnName} that maches the ${facilityIdcolumnName} across the files'`
      )}`;
  }
}

/**
 * GlucoseAssuranceRules class represents a set of assurance rules
 * specific to glucose, extending DuckDbOrchTableAssuranceRules.
 */
export class GlucoseAssuranceRules<
  TableName extends string,
  ColumnName extends string
> extends ddbo.DuckDbOrchTableAssuranceRules<TableName, ColumnName> {
  readonly car: CommonAssuranceRules<TableName, ColumnName>;

  constructor(
    readonly tableName: TableName,
    readonly sessionID: string,
    readonly sessionEntryID: string,
    readonly govn: ddbo.DuckDbOrchGovernance
  ) {
    super(tableName, sessionID, sessionEntryID, govn);
    this.car = new CommonAssuranceRules<TableName, ColumnName>(
      tableName,
      sessionID,
      sessionEntryID,
      govn
    );
  }

  onlyAllowUniqueMedicaidCinPerMrnInAllRows(columnName: ColumnName) {
    const cteName = "valid_unique_medicaid_cin_per_mrn_in_all_rows";
    return this.govn.SQL`
    WITH ${cteName} AS (
      SELECT '${columnName}' AS issue_column,
              "${columnName}" AS invalid_value,
              min(src_file_row_number) AS issue_row
        FROM ${this.tableName}
        WHERE MEDICAID_CIN IS NOT NULL
        GROUP BY pat_mrn_id, MEDICAID_CIN
        HAVING COUNT(*) > 1
    )
    ${this.insertRowValueIssueCtePartial(
      cteName,
      "Invalid Unique Medicaid Cin Per Mrn",
      "issue_row",
      "issue_column",
      "invalid_value",
      `'Invalid Unique Medicaid Cin Per Mrn "' || invalid_value || '" found in ' || issue_column`,
      `'Validate Unique Medicaid Cin Per Mrn'`
    )}`;
  }

  // if there are any admin-demographic-specific business logic rules put them here;
}

/**
 * VitalSignsDataAssuranceRules class represents a set of assurance rules
 * specific to VitalSigns, extending DuckDbOrchTableAssuranceRules.
 */
export class VitalSignsDataAssuranceRules<
  TableName extends string,
  ColumnName extends string
> extends ddbo.DuckDbOrchTableAssuranceRules<TableName, ColumnName> {
  readonly car: CommonAssuranceRules<TableName, ColumnName>;

  constructor(
    readonly tableName: TableName,
    readonly sessionID: string,
    readonly sessionEntryID: string,
    readonly govn: ddbo.DuckDbOrchGovernance
  ) {
    super(tableName, sessionID, sessionEntryID, govn);
    this.car = new CommonAssuranceRules<TableName, ColumnName>(
      tableName,
      sessionID,
      sessionEntryID,
      govn
    );
  }

  // if there are any admin-demographic-specific business logic rules put them here;
  onlyAllowValidUniqueFacilityAddress1PerFacilityInAllRows(
    columnName: ColumnName
  ) {
    const cteName = "valid_unique_facility_address1_per_facility_in_all_rows";
    // Construct the SQL query using tagged template literals
    return this.govn.SQL`
      WITH ${cteName} AS (
        SELECT '${columnName}' AS issue_column,
          ${columnName} AS invalid_value,
          min(src_file_row_number) AS issue_row
        FROM ${this.tableName}
        GROUP BY ${columnName}
        HAVING COUNT(DISTINCT FACILITY_LONG_NAME) > 1
      )
      ${this.insertRowValueIssueCtePartial(
        cteName,
        `Invalid ${columnName}`,
        "issue_row",
        "issue_column",
        "invalid_value",
        `'The unique field "' || issue_column || '" "' || invalid_value || '"is not unique per facility'`,
        `'${columnName} is not unique per facility.'`
      )}`;
  }
}
