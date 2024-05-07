import { SQLa, udm } from "./deps.ts";

const { gm } = udm;
const investigatorBuilder = SQLa.tableDefinition(
  "investigator",
  {
    investigator_id: gm.keys.ulidPrimaryKey(),
    investigator_name: udm.text(),
    email: udm.text(),
    created_date: udm.dateTime(),
    number_of_studies: udm.integer(),
    number_of_participants: udm.integer(),
    number_of_cgm_data_files: udm.integer(),
  },
);

const studyBuilder = SQLa.tableDefinition(
  "study",
  {
    study_id: gm.keys.ulidPrimaryKey(),
    study_name: udm.text(),
    total_participants_reported: udm.integer(),
    total_participants: udm.integer(),
    start_date: udm.dateTime(),
    end_date: udm.dateTime(),
    treatment_modalities: udm.text(),
    narrative_description: udm.text(),
    number_of_sites: udm.integer(),
    funding_source: udm.text(),
    nct: udm.integer().optional(),
  },
);

const siteBuilder = SQLa.tableDefinition(
  "site",
  {
    study_id: studyBuilder.belongsTo.study_id(),
    site_id: gm.keys.ulidPrimaryKey(),
    site_name: udm.text(),
    site_type: udm.text(),
    total_participants: udm.integer(),
  },
);

const participantBuilder = SQLa.tableDefinition(
  "participant",
  {
    subject_id: gm.keys.ulidPrimaryKey(),
    study_id: studyBuilder.belongsTo.study_id(),
    site_id: siteBuilder.belongsTo.site_id(),
    diagnosis: udm.text(), // Diagnosis (ICD-10)
    meds: udm.text(), // Meds (RxNORM)
    treatment_modality: udm.text(),
    sex: udm.text(), // Gender/Sex
    race_ethnicity: udm.text(), // Race Ethnicity (FHIR v.3 race)
    age: udm.integer(), // Age (in years)
    rest_of_OMOP_person_tables: udm.text(), // rest of OMOP person tables (future state)
  },
);

const personLevelCgmSummaryBuilder = SQLa.tableDefinition(
  "person_level_cgm_summary",
  {
    person_level_cgm_summary_id: gm.keys.ulidPrimaryKey(),
    number_of_tracings: udm.integer(),
    total_time_range_covered: udm.integer(),
    earliest_data_point: udm.text(),
    latest_data_point: udm.text(),
  },
);

const seedDDL = SQLa.SQL(SQLa.typicalSqlTextSupplierOptions())`
  ${investigatorBuilder}

  ${studyBuilder}

  ${siteBuilder}

  ${participantBuilder}

  ${personLevelCgmSummaryBuilder}
`;

export default seedDDL.SQL(SQLa.typicalSqlEmitContext());
console.log(seedDDL.SQL(SQLa.typicalSqlEmitContext()));
