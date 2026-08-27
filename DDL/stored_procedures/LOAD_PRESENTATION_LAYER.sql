CREATE OR REPLACE PROCEDURE {DB_NAME}.PMI_AGENT.LOAD_PRESENTATION_LAYER("P_STORY_KEY" VARCHAR, "P_TARGET_DB" VARCHAR DEFAULT 'T_IN_CAPG_BIONIC_PMI_POC', "P_SOURCE_DB" VARCHAR DEFAULT 'D_IN_CAPG_BIONIC_PMI_POC')
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'load_presentation'
EXECUTE AS CALLER
AS '
import json

def load_presentation(session, P_STORY_KEY, P_TARGET_DB, P_SOURCE_DB):
    results = []
    errors = []

    sttm_keys = session.sql(f"""
        SELECT DISTINCT f.key
        FROM {P_SOURCE_DB}.PMI_AGENT.PIPELINE_STTM_DATA,
        LATERAL FLATTEN(input => RAW_ROW) f
        WHERE ISSUE_KEY = ''{P_STORY_KEY}''
    """).collect()
    key_names = [r[0] for r in sttm_keys]

    is_bimo = P_STORY_KEY.startswith(''KAN'')
    tag_fqn = f"{P_SOURCE_DB}.PMI_AGENT.BIONIC_PMI_POC" if is_bimo else f"{P_SOURCE_DB}.PMI_AGENT.BIONIC_PMI_POC_IFP"

    has_integration = ''Integration.Schema'' in key_names or ''Integration.VIEW'' in key_names

    if is_bimo:
        staging_schema = ''SHR_SCHEMA''
        int_schema = ''BIMO_INT''
        pl_schema = ''PRESENTATION''
        dp_schema = ''DP_BIMO''
    else:
        staging_schema = ''SHR_SUPPLY_SCHEMA''
        int_schema = None
        pl_schema = ''PL_SUPPLY_LAYER''
        dp_schema = ''PL_SUPPLY_LAYER''

    def view_exists(schema, view_name):
        check = session.sql(f"""
            SELECT COUNT(*) FROM {P_TARGET_DB}.INFORMATION_SCHEMA.VIEWS
            WHERE TABLE_SCHEMA = ''{schema}'' AND TABLE_NAME = ''{view_name}''
        """).collect()
        return check[0][0] > 0

    if has_integration and is_bimo:
        staging_tables = session.sql(f"""
            SELECT TABLE_NAME FROM {P_TARGET_DB}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = ''{staging_schema}'' AND TABLE_TYPE = ''BASE TABLE''
            AND TABLE_NAME LIKE ANY (''CONSUMER_REGISTRATIONS_%'', ''DEVICE_REPLACEMENT_%'')
            AND TABLE_NAME NOT LIKE ''%_SV''
        """).collect()

        datasets = {}
        for row in staging_tables:
            tbl = row[0]
            parts = tbl.rsplit(''_'', 1)
            if len(parts) == 2 and len(parts[1]) == 2:
                base = parts[0]
                if base not in datasets:
                    datasets[base] = []
                datasets[base].append(tbl)
            else:
                if tbl not in datasets:
                    datasets[tbl] = [tbl]

        for dataset, tables in datasets.items():
            for tbl in tables:
                sv_name = f"{tbl}_SV"
                if view_exists(staging_schema, sv_name):
                    results.append({"object": f"{staging_schema}.{sv_name}", "type": "SECURE_VIEW", "status": "EXISTS_SKIPPED"})
                    continue
                country = tbl.rsplit(''_'', 1)[1] if ''_'' in tbl else ''''
                dataset_label = dataset.replace(''_'', '' '').title()
                comment = f"{P_STORY_KEY} staging secure view {dataset_label} {country}"
                try:
                    session.sql(f"""
                        CREATE SECURE VIEW {P_TARGET_DB}.{staging_schema}.{sv_name}
                        COMMENT = ''{comment}''
                        AS SELECT * FROM {P_TARGET_DB}.{staging_schema}.{tbl}
                    """).collect()
                    session.sql(f"ALTER VIEW {P_TARGET_DB}.{staging_schema}.{sv_name} SET TAG {tag_fqn} = ''{P_STORY_KEY}''").collect()
                    results.append({"object": f"{staging_schema}.{sv_name}", "type": "SECURE_VIEW", "status": "CREATED"})
                except Exception as e:
                    errors.append(f"SV {sv_name}: {str(e)[:80]}")

            int_view = f"{dataset}_VW_INT"
            if view_exists(int_schema, int_view):
                results.append({"object": f"{int_schema}.{int_view}", "type": "INTEGRATION_VIEW", "status": "EXISTS_SKIPPED"})
                continue
            dataset_label = dataset.replace(''_'', '' '').title()
            int_comment = f"{P_STORY_KEY} Integration UNION ALL {dataset_label}"
            sv_list = [f"SELECT * FROM {P_TARGET_DB}.{staging_schema}.{t}_SV" for t in tables]
            union_sql = " UNION ALL ".join(sv_list)
            try:
                session.sql(f"""
                    CREATE VIEW {P_TARGET_DB}.{int_schema}.{int_view}
                    COMMENT = ''{int_comment}''
                    AS {union_sql}
                """).collect()
                session.sql(f"ALTER VIEW {P_TARGET_DB}.{int_schema}.{int_view} SET TAG {tag_fqn} = ''{P_STORY_KEY}''").collect()
                results.append({"object": f"{int_schema}.{int_view}", "type": "INTEGRATION_VIEW", "status": "CREATED"})
            except Exception as e:
                errors.append(f"INT {int_view}: {str(e)[:80]}")

    pl_tables = session.sql(f"""
        SELECT TABLE_NAME FROM {P_TARGET_DB}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = ''{pl_schema}'' AND TABLE_TYPE = ''BASE TABLE''
    """).collect()

    etl_run_id_row = session.sql("SELECT ABS(HASH(CURRENT_TIMESTAMP()))::NUMBER(38,0) % 1000000000 AS RUN_ID").collect()
    etl_run_id = etl_run_id_row[0][0] if etl_run_id_row else 1

    for record in pl_tables:
        pl_table = record[0]
        try:
            base_dataset = pl_table.replace(''P_T_'', '''')
            if has_integration and is_bimo:
                merge_source = f"{P_TARGET_DB}.{int_schema}.{base_dataset}_VW_INT"
            else:
                merge_source = f"{P_TARGET_DB}.{staging_schema}.{base_dataset}"

            cols = session.sql(f"""
                SELECT COLUMN_NAME FROM {P_TARGET_DB}.INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ''{pl_schema}'' AND TABLE_NAME = ''{pl_table}''
                ORDER BY ORDINAL_POSITION
            """).collect()
            col_names = [c[0] for c in cols]

            if not col_names:
                continue

            bk_col = col_names[0]

            try:
                src_desc = session.sql(f"DESCRIBE VIEW {merge_source}").collect()
                src_col_names = [r[0] for r in src_desc]
            except:
                try:
                    src_desc = session.sql(f"DESCRIBE TABLE {merge_source}").collect()
                    src_col_names = [r[0] for r in src_desc]
                except:
                    src_col_names = []

            source_has_bk = bk_col in src_col_names

            if not source_has_bk:
                errors.append(f"PL {pl_table}: source missing BK column {bk_col}")
                continue

            new_bk_count = session.sql(f"""
                SELECT COUNT(*) FROM (
                    SELECT {bk_col} FROM {merge_source}
                    MINUS
                    SELECT {bk_col} FROM {P_TARGET_DB}.{pl_schema}.{pl_table}
                )
            """).collect()[0][0]

            audit_cols = [''RECORD_LOAD_DATETIME'', ''_ETL_RUN_ID'']
            data_cols = [c for c in col_names if c in src_col_names and c not in audit_cols]
            non_bk_data_cols = [c for c in data_cols if c != bk_col]

            changed_count = 0
            if non_bk_data_cols:
                compare_cols = ", ".join(non_bk_data_cols)
                changed_count = session.sql(f"""
                    SELECT COUNT(*) FROM {P_TARGET_DB}.{pl_schema}.{pl_table} t
                    INNER JOIN {merge_source} s ON t.{bk_col} = s.{bk_col}
                    WHERE MD5(CONCAT_WS(''|'', {'', ''.join([f''s.{c}'' for c in non_bk_data_cols])}))
                       != MD5(CONCAT_WS(''|'', {'', ''.join([f''t.{c}'' for c in non_bk_data_cols])}))
                """).collect()[0][0]

            if new_bk_count == 0 and changed_count == 0:
                existing_count = session.sql(f"SELECT COUNT(*) FROM {P_TARGET_DB}.{pl_schema}.{pl_table}").collect()[0][0]
                results.append({"table": f"{pl_schema}.{pl_table}", "rows": existing_count, "type": "PRESENTATION", "status": "NO_CHANGE_SKIPPED", "reason": "No new or changed rows"})
                continue

            dedup_source_sql = f"""
                SELECT * FROM {merge_source}
                QUALIFY ROW_NUMBER() OVER (PARTITION BY {bk_col} ORDER BY {bk_col}) = 1
            """

            update_set = ", ".join([f"t.{c} = s.{c}" for c in non_bk_data_cols])
            update_set += f", t.RECORD_LOAD_DATETIME = CURRENT_TIMESTAMP(), t._ETL_RUN_ID = {etl_run_id}"

            insert_cols = ", ".join(data_cols + [''RECORD_LOAD_DATETIME'', ''_ETL_RUN_ID''])
            insert_vals = ", ".join([f"s.{c}" for c in data_cols] + [''CURRENT_TIMESTAMP()'', str(etl_run_id)])

            session.sql(f"""
                MERGE INTO {P_TARGET_DB}.{pl_schema}.{pl_table} t
                USING ({dedup_source_sql}) s
                ON t.{bk_col} = s.{bk_col}
                WHEN MATCHED AND MD5(CONCAT_WS(''|'', {'', ''.join([f''s.{c}'' for c in non_bk_data_cols])}))
                              != MD5(CONCAT_WS(''|'', {'', ''.join([f''t.{c}'' for c in non_bk_data_cols])}))
                THEN UPDATE SET {update_set}
                WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
            """).collect()

            row_count = session.sql(f"SELECT COUNT(*) FROM {P_TARGET_DB}.{pl_schema}.{pl_table}").collect()[0][0]
            results.append({
                "table": f"{pl_schema}.{pl_table}",
                "rows": row_count,
                "type": "PRESENTATION",
                "status": "LOADED",
                "new_rows": new_bk_count,
                "updated_rows": changed_count
            })

        except Exception as e:
            errors.append(f"PL {pl_table}: {str(e)[:100]}")

    dp_views = session.sql(f"""
        SELECT TABLE_NAME, VIEW_DEFINITION FROM {P_SOURCE_DB}.INFORMATION_SCHEMA.VIEWS
        WHERE TABLE_SCHEMA = ''{dp_schema}'' AND COMMENT LIKE ''%{P_STORY_KEY}%''
    """).collect()

    for vw in dp_views:
        view_name = vw[0]
        view_def = vw[1]
        if view_name and view_def:
            if view_exists(dp_schema, view_name):
                results.append({"object": f"{dp_schema}.{view_name}", "type": "DP_VIEW", "status": "EXISTS_SKIPPED"})
                continue
            try:
                new_def = view_def.replace(P_SOURCE_DB, P_TARGET_DB)
                select_idx = new_def.upper().find(''SELECT'')
                if select_idx >= 0:
                    select_sql = new_def[select_idx:]
                else:
                    select_sql = f"SELECT * FROM {P_TARGET_DB}.{pl_schema}.{view_name}"

                dp_comment = f"{P_STORY_KEY} Data Product {view_name.replace(''DP_'', '''').replace(''BIMO_'', '''').replace(''_VW'', '''').replace(''_'', '' '').title()}"
                session.sql(f"""
                    CREATE VIEW {P_TARGET_DB}.{dp_schema}.{view_name}
                    COMMENT = ''{dp_comment}''
                    AS {select_sql}
                """).collect()
                session.sql(f"ALTER VIEW {P_TARGET_DB}.{dp_schema}.{view_name} SET TAG {tag_fqn} = ''{P_STORY_KEY}''").collect()
                results.append({"object": f"{dp_schema}.{view_name}", "type": "DP_VIEW", "status": "CREATED"})
            except Exception as e:
                errors.append(f"DP {view_name}: {str(e)[:80]}")

    return json.dumps({
        "status": "SUCCESS" if not errors else "PARTIAL",
        "story_key": P_STORY_KEY,
        "target_db": P_TARGET_DB,
        "project_type": "BIMO" if is_bimo else "IF&P",
        "etl_run_id": etl_run_id,
        "results": results,
        "errors": errors if errors else "None"
    }, indent=2)
';
