CREATE OR REPLACE PROCEDURE "LOAD_STAGING_LAYER"("P_STORY_KEY" VARCHAR, "P_TARGET_DB" VARCHAR DEFAULT '{DB_NAME}', "P_SOURCE_DB" VARCHAR DEFAULT '{DB_NAME}')
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python','openpyxl','pandas')
HANDLER = 'load_staging'
EXECUTE AS CALLER
AS '
import io, json, hashlib, pandas as pd

def load_staging(session, P_STORY_KEY, P_TARGET_DB, P_SOURCE_DB):
    results = []
    errors = []
    skipped = []

    # Get existing checksums FIRST
    existing_checksums = {}
    try:
        hist = session.sql(f"""
            SELECT OBJECT_NAME, CHECKSUM
            FROM {P_TARGET_DB}.PMI_AGENT.PIPELINE_LOAD_HISTORY
            WHERE STORY_KEY = ''{P_STORY_KEY}'' AND LAYER = ''STAGING_LOAD''
        """).collect()
        for h in hist:
            existing_checksums[h[0]] = str(h[1])
    except:
        pass

    # Get file-to-table mapping from STAGING history
    load_map = session.sql(f"""
        SELECT OBJECT_NAME, SOURCE_FILE
        FROM {P_SOURCE_DB}.PMI_AGENT.PIPELINE_LOAD_HISTORY
        WHERE STORY_KEY = ''{P_STORY_KEY}'' AND LAYER = ''STAGING''
    """).collect()

    if not load_map:
        return json.dumps({"status": "ERROR", "message": "No staging history found for this story"})

    # Detect staging schema from STTM
    sttm_keys = session.sql(f"""
        SELECT DISTINCT f.key FROM {P_SOURCE_DB}.PMI_AGENT.PIPELINE_STTM_DATA,
        LATERAL FLATTEN(input => RAW_ROW) f WHERE ISSUE_KEY = ''{P_STORY_KEY}''
    """).collect()
    key_names = [r[0] for r in sttm_keys]

    staging_schema = ''SHR_SCHEMA''
    if ''Source.Schema'' in key_names:
        schema_row = session.sql(f"""
            SELECT DISTINCT RAW_ROW:\"Source.Schema\"::VARCHAR AS S
            FROM {P_SOURCE_DB}.PMI_AGENT.PIPELINE_STTM_DATA
            WHERE ISSUE_KEY = ''{P_STORY_KEY}'' AND RAW_ROW:\"Source.Schema\" IS NOT NULL
        """).collect()
        if schema_row and schema_row[0][0]:
            staging_schema = schema_row[0][0]

    stage_path = f"@{P_SOURCE_DB}.SHR_SCHEMA.PMI_STAGE"
    is_bimo = P_STORY_KEY.startswith(''KAN'')
    tag_fqn = f"{P_SOURCE_DB}.PMI_AGENT.BIONIC_PMI_POC" if is_bimo else f"{P_SOURCE_DB}.PMI_AGENT.BIONIC_PMI_POC_IFP"

    # Build PK map from STTM
    pk_map = {}
    if ''Primary Key'' in key_names:
        try:
            pk_rows = session.sql(f"""
                SELECT DISTINCT
                    COALESCE(RAW_ROW:\"Source.Table/Staging Layer\"::VARCHAR, RAW_ROW:\"Source Table\"::VARCHAR) AS SRC_TABLE,
                    COALESCE(RAW_ROW:\"Source.Attribute/Coulmns\"::VARCHAR, RAW_ROW:\"Source Column\"::VARCHAR) AS COL_NAME
                FROM {P_SOURCE_DB}.PMI_AGENT.PIPELINE_STTM_DATA
                WHERE ISSUE_KEY = ''{P_STORY_KEY}'' AND RAW_ROW:\"Primary Key\"::VARCHAR = ''Y''
            """).collect()
            for row in pk_rows:
                src_table = row[0]
                col = row[1]
                if src_table and col:
                    clean_table = src_table.replace(''Source.'', '''').replace(''source.'', '''').strip()
                    if clean_table not in pk_map:
                        pk_map[clean_table] = []
                    pk_map[clean_table].append(col)
        except:
            pass

    # List stage files
    stage_files = session.sql(f"LIST {stage_path}").collect()

    # Process each file
    loaded_count = 0
    for record in load_map:
        table_name = record[0]
        source_file = record[1]

        # Find file on stage
        file_path = None
        for sf in stage_files:
            if source_file in sf[0]:
                raw_path = sf[0].replace(''pmi_stage/'', '''')
                file_path = f"{stage_path}/{raw_path}"
                break

        if not file_path:
            errors.append(f"{table_name}: file {source_file} not found")
            continue

        # Read file to compute checksum BEFORE loading
        try:
            input_stream = session.file.get_stream(file_path)
            file_bytes = input_stream.read()
        except:
            try:
                from snowflake.snowpark.files import SnowflakeFile
                clean_path = file_path.strip("''")
                with SnowflakeFile.open(clean_path, ''rb'', require_scoped_url=False) as f:
                    file_bytes = f.read()
            except Exception as e:
                errors.append(f"{table_name}: cannot read file - {str(e)[:80]}")
                continue

        file_checksum = hashlib.md5(file_bytes).hexdigest()

        # CHANGE DETECTION: Compare checksum with history
        prev_checksum = existing_checksums.get(table_name)
        if prev_checksum and prev_checksum == file_checksum:
            row_count = session.sql(f"SELECT COUNT(*) FROM {P_TARGET_DB}.{staging_schema}.{table_name}").collect()[0][0]
            skipped.append({"table": table_name, "file": source_file, "rows": row_count, "status": "UNCHANGED_SKIPPED", "reason": "File checksum unchanged"})
            continue

        ext = source_file.lower().rsplit(''.'', 1)[-1] if ''.'' in source_file else ''''

        if ext in (''xlsx'', ''xls''):
            import openpyxl
            wb = openpyxl.load_workbook(io.BytesIO(file_bytes), read_only=False, data_only=True)
            ws = wb.active
            if ws.merged_cells.ranges:
                for merge in list(ws.merged_cells.ranges):
                    ws.unmerge_cells(str(merge))
            headers = []
            header_row = None
            for row_idx in range(1, min(10, ws.max_row + 1)):
                row_vals = [ws.cell(row=row_idx, column=c).value for c in range(1, ws.max_column + 1) if ws.cell(row=row_idx, column=c).value is not None]
                if len(row_vals) >= 3:
                    header_row = row_idx
                    headers = [str(ws.cell(row=row_idx, column=c).value).strip().upper().replace('' '', ''_'') if ws.cell(row=row_idx, column=c).value else None for c in range(1, ws.max_column + 1)]
                    break
            if not header_row:
                wb.close()
                errors.append(f"{table_name}: no header row found")
                continue
            headers = [h for h in headers if h]
            rows = []
            for row_idx in range(header_row + 1, ws.max_row + 1):
                row_data = {}
                non_empty = 0
                for col_idx, header in enumerate(headers, 1):
                    val = ws.cell(row=row_idx, column=col_idx).value
                    if val is not None and str(val).strip() != '''':
                        non_empty += 1
                    row_data[header] = str(val).strip() if val is not None else None
                if non_empty >= 2:
                    rows.append(row_data)
            wb.close()
        elif ext == ''csv'':
            df = pd.read_csv(io.BytesIO(file_bytes))
            df.columns = [c.strip().upper().replace('' '', ''_'') for c in df.columns]
            headers = list(df.columns)
            rows = [{col: str(row[col]).strip() if pd.notna(row[col]) else None for col in headers} for _, row in df.iterrows()]
        else:
            errors.append(f"{table_name}: unsupported format .{ext}")
            continue

        if not rows:
            errors.append(f"{table_name}: no data rows")
            continue

        parts = table_name.rsplit(''_'', 1)
        dataset_name = parts[0] if len(parts) == 2 and len(parts[1]) == 2 else table_name

        pk_cols = pk_map.get(dataset_name, pk_map.get(table_name, []))
        bk_col = f"TDE_{dataset_name.upper().replace('' '', ''_'')}_BK"

        bk_set = set()
        for idx, row in enumerate(rows):
            pk_vals = []
            has_null = False
            for pk in pk_cols:
                val = row.get(pk, '''') or ''''
                if val == '''' or val == ''None'':
                    has_null = True
                pk_vals.append(val)
            if has_null:
                pk_vals.append(str(idx))
            bk = hashlib.md5(''|''.join(pk_vals).encode()).hexdigest()
            if bk in bk_set:
                bk = hashlib.md5((''|''.join(pk_vals) + f''|{idx}'').encode()).hexdigest()
            bk_set.add(bk)
            row[bk_col] = bk
            row[''SOURCE_FILE_NAME''] = source_file

        all_cols = headers + [bk_col, ''SOURCE_FILE_NAME'', ''SOURCE_LOADED_AT'']
        target_table = f"{P_TARGET_DB}.{staging_schema}.{table_name}"

        value_rows = []
        for row in rows:
            vals = []
            for col in all_cols:
                if col == ''SOURCE_LOADED_AT'':
                    vals.append("CURRENT_TIMESTAMP()")
                else:
                    v = row.get(col)
                    if v is None or v == ''None'' or v == '''':
                        vals.append("NULL")
                    else:
                        vals.append("''" + v.replace("''", "''''") + "''")
            value_rows.append("(" + ",".join(vals) + ")")

        values_sql = ",\\n".join(value_rows)
        col_aliases = ",".join([f"${i+1} AS {col}" for i, col in enumerate(all_cols)])
        non_bk_cols = [c for c in all_cols if c != bk_col]
        update_set = ",".join([f"tgt.{c} = src.{c}" for c in non_bk_cols])
        insert_cols = ",".join(all_cols)
        insert_vals = ",".join([f"src.{c}" for c in all_cols])

        merge_sql = f"""MERGE INTO {target_table} tgt
USING (SELECT {col_aliases} FROM VALUES {values_sql}) src
ON tgt.{bk_col} = src.{bk_col}
WHEN MATCHED THEN UPDATE SET {update_set}
WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"""

        result = session.sql(merge_sql).collect()
        merge_info = result[0].as_dict() if result else {}
        rows_inserted = merge_info.get(''number of rows inserted'', 0)
        rows_updated = merge_info.get(''number of rows updated'', 0)

        try:
            session.sql(f"ALTER TABLE {target_table} SET TAG {tag_fqn} = ''{P_STORY_KEY}''").collect()
        except:
            pass

        row_count = session.sql(f"SELECT COUNT(*) FROM {target_table}").collect()[0][0]

        try:
            session.sql(f"""
                MERGE INTO {P_TARGET_DB}.PMI_AGENT.PIPELINE_LOAD_HISTORY tgt
                USING (SELECT ''{P_STORY_KEY}'' AS SK, ''STAGING_LOAD'' AS LY, ''{table_name}'' AS OBJ) src
                ON tgt.STORY_KEY = src.SK AND tgt.LAYER = src.LY AND tgt.OBJECT_NAME = src.OBJ
                WHEN MATCHED THEN UPDATE SET ROWS_INSERTED = {rows_inserted}, ROWS_UPDATED = {rows_updated}, ROWS_TOTAL = {row_count}, SOURCE_FILE = ''{source_file}'', CHECKSUM = ''{file_checksum}'', STATUS = ''SUCCESS'', LOADED_AT = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT (STORY_KEY, LAYER, OBJECT_NAME, SOURCE_FILE, ROWS_INSERTED, ROWS_UPDATED, ROWS_TOTAL, CHECKSUM, STATUS, LOADED_AT)
                VALUES (''{P_STORY_KEY}'', ''STAGING_LOAD'', ''{table_name}'', ''{source_file}'', {rows_inserted}, {rows_updated}, {row_count}, ''{file_checksum}'', ''SUCCESS'', CURRENT_TIMESTAMP())
            """).collect()
        except:
            pass

        loaded_count += 1
        results.append({"table": table_name, "file": source_file, "rows": row_count, "rows_inserted": rows_inserted, "rows_updated": rows_updated, "status": "LOADED"})

    return json.dumps({
        "status": "SUCCESS" if not errors else "PARTIAL",
        "story_key": P_STORY_KEY,
        "target_db": P_TARGET_DB,
        "staging_schema": staging_schema,
        "tables_loaded": loaded_count,
        "tables_skipped": len(skipped),
        "results": results if results else skipped,
        "skipped": skipped if skipped else "None",
        "errors": errors if errors else "None"
    }, indent=2)
';