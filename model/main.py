import os
import pandas as pd
import logging
import configparser
from db_controller import get_db_engine, write_to_db, get_category_mapping, read_table_to_df
from ftp_controller import download_files_from_ftp, move_file, move_files_to_processing
from validator import ValidationReport, run_validations
from acc_aci_processor import (
    process_acc_data,
    process_aci_data,
    build_acc_aci_combined,
    robust_date_converter
)
from acc_aci_processor_monthly import (
    process_acc_data_monthly,
    process_aci_data_monthly,
    build_acc_aci_combined_monthly
)

# --- Configuration & Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
config = configparser.ConfigParser()
# 使用絕對路徑讀取設定檔
config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
if not os.path.exists(config_path):
    logging.critical(f"Configuration file not found at {config_path}")
    exit()
config.read(config_path)

# --- Helper Functions ---
def drop_empty_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Drop columns that are entirely empty (all NA or blank strings) and those with 'Unnamed' headers."""
    try:
        # Drop columns whose header starts with 'Unnamed'
        normalized_cols = [str(c).strip().lower() for c in df.columns]
        drop_cols = [orig for orig, norm in zip(df.columns, normalized_cols) if norm.startswith('unnamed')]
        if drop_cols:
            df = df.drop(columns=drop_cols)
            logging.info(f"Dropped columns with 'Unnamed' headers: {drop_cols}")

        # Treat blank strings as NA, then drop all-NA columns
        tmp = df.replace(r'^\s*$', pd.NA, regex=True)
        before_cols = set(tmp.columns)
        tmp = tmp.dropna(axis=1, how='all')
        dropped = list(before_cols - set(tmp.columns))
        if dropped:
            logging.info(f"Dropped entirely empty columns: {dropped}")
        return tmp
    except Exception as e:
        logging.warning(f"Failed during empty-column cleanup, skipping. Error: {e}")
        return df
    
def drop_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    try:
        tmp = df.replace(r"^\s*$", pd.NA, regex=True)
        before = len(tmp)
        tmp = tmp.dropna(axis=0, how='all')
        removed = before - len(tmp)
        if removed > 0:
            logging.info(f"Dropped {removed} entirely empty rows")
        return tmp
    except Exception as e:
        logging.warning(f"Failed during empty-row cleanup, skipping. Error: {e}")
        return df


# --- Processing Functions ---
def process_exchange_rate_ntd(df, type_value):
    """Processes the 'exchange_rate_ntd.xlsx' file."""
    logging.info("Processing exchange_rate_ntd.xlsx")
    df.index.name = 'period'
    df = df.reset_index()
    df_melted = df.melt(id_vars=['period'], var_name='currency', value_name='fx_rate')
    df_melted['type'] = type_value
    df_melted = df_melted.dropna(subset=['fx_rate'])
    final_df = df_melted[['type', 'period', 'currency', 'fx_rate']]
    final_df.columns = [col.lower() for col in final_df.columns]
    return final_df

def process_finkpdata(df, engine, product_line):
    """Processes the 'bsgp_finkpdata' files based on revised specs."""
    logging.info(f"Processing bsgp_finkpdata for product_line: {product_line}")
    df.columns = df.columns.str.strip()

    # --- Column Rename ---
    column_mapping = {
        'GL Date': 'gl_date', 'Order Type': 'order_type', 'Item no.': 'item_no',
        'Item Description': 'item_description', 'Quantity Shipped': 'quantity',
        'Net sale amount': 'net_sale_amount', 'Material Cost2': 'material_cost',
        'B&S Group': 'ag_no', '有無B&S Group': 'ag_no_check'
    }
    df.rename(columns=column_mapping, inplace=True)
    
    required_columns = list(column_mapping.values())
    present_columns = [col for col in required_columns if col in df.columns]
    df = df[present_columns]

    # --- Filtering Logic ---
    condition1 = df['ag_no_check'] != '無B&S Group'
    condition2 = df['material_cost'] != 0
    order_types_to_keep = ['B&S-RM EX-113', 'B&S-RM EX Book-113']
    condition3 = df['order_type'].isin(order_types_to_keep)
    df = df[condition1 & condition2 & condition3].copy()

    # --- Sequential Category Mapping Logic ---
    # Initialize the new 'category' column
    df['category'] = None 

    # 1. item_description contains "BRA" -> "BR"
    bra_mask = df['item_description'].str.contains('BRA', case=True, na=False)
    df.loc[bra_mask, 'category'] = 'BR'

    # 2. Map using item_code from the database where category is not yet set
    unmapped_mask = df['category'].isnull()
    if unmapped_mask.any():
        item_code_map_df = read_table_to_df('bsgp_itemcode', engine)
        item_code_map = item_code_map_df.set_index('item_code')['category'].to_dict()
        df['item_prefix'] = df['item_no'].str[:2]
        df.loc[unmapped_mask, 'category'] = df.loc[unmapped_mask, 'item_prefix'].map(item_code_map)

    # 3. Description starts with DDR/LPD/GDDR/LP -> "DDR"
    unmapped_mask = df['category'].isnull()
    if unmapped_mask.any():
        ddr_prefixes = ['DDR', 'LPD', 'GDDR', 'LP']
        ddr_mask = df.loc[unmapped_mask, 'item_description'].str.startswith(tuple(ddr_prefixes), na=False)
        df.loc[unmapped_mask & ddr_mask, 'category'] = 'DDR'

    # 4. Description starts with FLASH -> "FLASH"
    unmapped_mask = df['category'].isnull()
    if unmapped_mask.any():
        flash_mask = df.loc[unmapped_mask, 'item_description'].str.startswith('FLASH', na=False)
        df.loc[unmapped_mask & flash_mask, 'category'] = 'FLASH'

    # 5. All remaining are 'SSD' (avoid chained-assignment with inplace on Series)
    df['category'] = df['category'].fillna('SSD')

    # --- Final Calculations & Date Formatting ---
    df['cogs_price'] = 0
    df['equo_price'] = 0
    df['bs_gap'] = 0
    
    df['gl_date'] = df['gl_date'].apply(robust_date_converter)
    # Create 'period' column as yyyymm
    df['gl_yearmonth'] = df['gl_date'].dt.strftime('%Y%m')
    
    # Add the product_line column
    df['product_line'] = product_line

    # Clean up temporary columns
    if 'item_prefix' in df.columns:
        df.drop(columns=['item_prefix'], inplace=True)
        
    return df

def process_pac(df, product_line):
    """Processes the 'pac' files, adding a product_line column."""
    logging.info(f"Processing pac for product_line: {product_line}")
    df['product_line'] = product_line
    df.columns = [col.lower() for col in df.columns]
    return df

def process_shipunit(df, engine):
    """對 shipunit DataFrame 進行特殊處理"""
    logging.info("Processing shipunit...")
    try:
        # --- DEBUG: Log the columns found in the file ---
        logging.info(f"Columns found in shipunit.xlsx: {df.columns.tolist()}")

        country_group_df = read_table_to_df('countrygroup', engine)

        country_group_df.columns = [col.strip() for col in country_group_df.columns]
        df.columns = [col.strip() for col in df.columns]

        # 分離 NB 和 NR/NV
        nb_df = df[df['product_line'] == 'NB'].copy()
        nrnv_df = df[df['product_line'].isin(['NR', 'NV'])].copy()

        # 處理 NB 的 country
        nb_df = pd.merge(nb_df, country_group_df[['geo', 'country_code']], how='left', on='geo')
        nb_df.rename(columns={'country_code': 'country'}, inplace=True)

        # 處理 NR/NV 的 country
        nrnv_df = pd.merge(nrnv_df, country_group_df[['territory', 'country_code']], how='left', on='territory')
        nrnv_df.rename(columns={'country_code': 'country'}, inplace=True)
        
        # 合併 NB, NR, NV 的結果
        final_df = pd.concat([nb_df, nrnv_df], ignore_index=True)

        # --- 根據您的要求，只保留指定的欄位 ---
        final_columns = [
            'product_line',
            'ship_way_type',
            'unit_shipfee',
            'unit_storage_fee',
            'period',
            'freq',
            'country'
        ]
        # 篩選出實際存在的欄位，避免因原始檔缺少某些欄位而報錯
        existing_final_columns = [col for col in final_columns if col in final_df.columns]
        final_df = final_df[existing_final_columns]
        
        
        logging.info(f"Finished processing shipunit. Resulting shape: {final_df.shape}")
        return final_df
    except KeyError as e:
        logging.error(f"A column was not found during shipunit processing. This is likely due to a name mismatch in the Excel file. Missing column: {e}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred during shipunit processing: {e}")
        raise

def process_opp(df):
    """Extracts OPP data, limits to needed fields, and de-duplicates rows."""
    logging.info("Processing default_opp...")
    df = df.copy()
    df.columns = [col.strip().lower() for col in df.columns]

    required_columns = ['sku90pn', 'opp_rebate', 'period', 'freq', 'product_line']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing expected columns in default_opp data: {missing_columns}")

    filtered_df = df[required_columns].drop_duplicates()
    return filtered_df


def default_process(df):
    """Default processing for other xlsx files."""
    logging.info("Processing default_process with deduplication...")
    df = df.copy()
    
    # 過濾重複資料
    # 使用所有欄位來判斷重複，保留第一筆出現的資料
    df_deduplicated = df.drop_duplicates()
    
    logging.info(f"Removed {len(df) - len(df_deduplicated)} duplicate rows")
    
    return df_deduplicated

def process_hq_item_cost_model_cpu_common(df, table_name):
    """Process hq_item_cost_model_cpu files: GROUP BY and calculate AVG(item_cost_usd).
    
    Common processing for both hq_item_cost_model_cpu and hq_item_cost_model_cpu_ex.
    """
    logging.info(f"Processing {table_name}: grouping by sales_team, model_name, cpu, period and calculating AVG(item_cost_usd)...")
    df = df.copy()
    
    # Normalize column names
    df.columns = [col.strip().lower() for col in df.columns]
    
    # Required columns for grouping and aggregation
    group_columns = ['sales_team', 'model_name', 'cpu', 'period']
    agg_column = 'item_cost_usd'
    
    # Check if required columns exist
    missing_columns = [col for col in group_columns + [agg_column] if col not in df.columns]
    if missing_columns:
        logging.error(f"Missing required columns: {missing_columns}")
        raise ValueError(f"Missing required columns in {table_name}: {missing_columns}")
    
    # Group by and calculate average
    result_df = df.groupby(group_columns, as_index=False)[agg_column].mean()
    
    logging.info(f"Grouped from {len(df)} rows to {len(result_df)} rows")
    return result_df


def process_funding(df):
    """Process funding Excel: drop rows where funding == 0 (keep NULL/blank)."""
    logging.info("Processing funding: dropping rows with funding == 0 ...")
    df = df.copy()

    # Find the 'funding' column case-insensitively
    funding_col = None
    for c in df.columns:
        if str(c).strip().lower() == 'funding':
            funding_col = c
            break

    if funding_col is None:
        logging.warning("Funding column not found in funding Excel; skipping zero filter.")
        return df

    vals = pd.to_numeric(df[funding_col], errors='coerce')
    keep_mask = (vals != 0) | (vals.isna())
    before = len(df)
    df = df[keep_mask].copy()
    removed = before - len(df)
    logging.info(f"Funding zero filter removed {removed} rows; remaining {len(df)} rows.")

    return df

# --- Main Orchestrator ---
def process_files(engine):
    """Scans FTP, downloads, validates, processes, and uploads files to the DB."""
    ftp_config = config['FTP']
    
    # Define and create a local temporary directory
    local_temp_dir = os.path.join(os.path.dirname(__file__), 'tmp')
    # os.makedirs(local_temp_dir, exist_ok=True)
    logging.info(f"Using temporary directory: {local_temp_dir}")

    # Create a ValidationReport instance to collect all messages
    report = ValidationReport(config)
    local_files = []
    try:
        # First, move any newly uploaded files into Processing to avoid race conditions
        try:
            logging.info("Moving files from UPLOAD_DIR to PROCESSING_DIR before processing...")
            move_files_to_processing(ftp_config)
        except Exception as e:
            logging.error(f"Failed to move files to processing: {e}")
            # Proceeding anyway; download will handle current Processing contents

        # Download files from FTP to the local temp directory
        local_files = download_files_from_ftp(ftp_config, local_temp_dir)
        
        if not local_files:
            logging.info("No files found on FTP server to process.")
            return

        for local_filepath in local_files:
            filename = os.path.basename(local_filepath)
            try:
                logging.info("\n" + "="*80)
                # Default table_name is the filename without extension
                table_name = os.path.splitext(filename)[0]
                logging.info(f"Processing file: {filename}")

                processed_df = None
                if "exchange_rate_ntd" in filename.lower():
                    logging.info(f"-> Matched rule: 'exchange_rate_ntd'")
                    df_type = pd.read_excel(local_filepath, header=None, nrows=1, engine='openpyxl', keep_default_na=False)
                    type_value = df_type.iloc[0, 1]
                    df = pd.read_excel(local_filepath, header=1, index_col=0, engine='openpyxl', keep_default_na=False)
                    df = drop_empty_columns(df)
                    df = drop_empty_rows(df)
                    processed_df = process_exchange_rate_ntd(df, type_value)
                
                elif "bsgp_finkpdata" in filename.lower():
                    logging.info(f"-> Matched rule: 'bsgp_finkpdata'")
                    # Extract product_line from filename, e.g., '..._nb.xlsx' -> 'NB'
                    base_name = os.path.splitext(filename)[0] # bsgp_finkpdata_nb
                    product_line = base_name.split('_')[-1].upper() # NB
                    
                    # Set the target table name, removing the suffix
                    table_name = "bsgp_finkpdata"
                    logging.info(f"Identified product_line: {product_line}, redirecting to table: {table_name}")

                    df = pd.read_excel(local_filepath, header=0, engine='openpyxl', keep_default_na=False)
                    df = drop_empty_columns(df)
                    df = drop_empty_rows(df)
                    processed_df = process_finkpdata(df, engine, product_line)

                elif "pac_" in filename.lower():
                    logging.info(f"-> Matched rule: 'pac'")
                    base_name = os.path.splitext(filename)[0] # pac_nr
                    product_line = base_name.split('_')[-1].upper() # NR
                    
                    table_name = "pac"
                    logging.info(f"Identified product_line: {product_line}, redirecting to table: {table_name}")

                    df = pd.read_excel(local_filepath, header=0, engine='openpyxl', keep_default_na=False)
                    df = drop_empty_columns(df)
                    df = drop_empty_rows(df)
                    processed_df = process_pac(df, product_line)
                
                elif "shipunit" in filename.lower():
                    logging.info(f"-> Matched rule: 'shipunit'")
                    df = pd.read_excel(local_filepath, header=0, engine='openpyxl', keep_default_na=False)
                    df = drop_empty_columns(df)
                    df = drop_empty_rows(df)
                    processed_df = process_shipunit(df, engine)

                elif "opp" in filename.lower():
                    logging.info(f"-> Matched rule: 'process_opp'")
                    df = pd.read_excel(local_filepath, header=0, engine='openpyxl', keep_default_na=False)
                    df = drop_empty_columns(df)
                    df = drop_empty_rows(df)
                    processed_df = process_opp(df)

                elif "funding" in filename.lower():
                    logging.info(f"-> Matched rule: 'funding' (drop zeros before DB)")
                    table_name = "funding"
                    df = pd.read_excel(local_filepath, header=0, engine='openpyxl', keep_default_na=False)
                    df = drop_empty_columns(df)
                    df = drop_empty_rows(df)
                    processed_df = process_funding(df)

                elif "hq_item_cost_model_cpu_ex" in filename.lower():
                    logging.info(f"-> Matched rule: 'hq_item_cost_model_cpu_ex' (group by and average)")
                    table_name = "hq_item_cost_model_cpu_ex"
                    df = pd.read_excel(local_filepath, header=0, engine='openpyxl', keep_default_na=False)
                    df = drop_empty_columns(df)
                    df = drop_empty_rows(df)
                    processed_df = process_hq_item_cost_model_cpu_common(df, table_name)

                elif "hq_item_cost_model_cpu" in filename.lower():
                    logging.info(f"-> Matched rule: 'hq_item_cost_model_cpu' (group by and average)")
                    table_name = "hq_item_cost_model_cpu"
                    df = pd.read_excel(local_filepath, header=0, engine='openpyxl', keep_default_na=False)
                    df = drop_empty_columns(df)
                    df = drop_empty_rows(df)
                    processed_df = process_hq_item_cost_model_cpu_common(df, table_name)

                elif "acc_localdata_weekly" in filename.lower():
                    logging.info(f"-> Matched rule: 'acc_localdata_weekly'")
                    table_name = "acc_localrawdata_weekly"
                    df = pd.read_excel(local_filepath, header=0, engine='openpyxl', keep_default_na=False)
                    df = drop_empty_columns(df)
                    df = drop_empty_rows(df)
                    processed_df = process_acc_data(df, engine)

                elif "aci_localdata_weekly" in filename.lower():
                    logging.info(f"-> Matched rule: 'aci_localdata_weekly'")
                    table_name = "aci_localrawdata_weekly"
                    # Read and union NB, NR, NV sheets; add product_line_id from sheet name
                    sheets = ['NB', 'NR', 'NV']
                    frames = []
                    for sh in sheets:
                        try:
                            df_sh = pd.read_excel(local_filepath, sheet_name=sh, header=0, engine='openpyxl', keep_default_na=False)
                            df_sh['product_line_id'] = sh
                            # Normalize column names to lowercase before concatenating to avoid duplicate columns
                            df_sh.columns = df_sh.columns.str.strip().str.lower()
                            df_sh = drop_empty_columns(df_sh)
                            df_sh = drop_empty_rows(df_sh)
                            frames.append(df_sh)
                        except Exception as e:
                            logging.warning(f"Failed to read sheet '{sh}' from {filename}: {e}")

                    if not frames:
                        logging.warning(f"No ACI sheets (NB/NR/NV) could be read from {filename}; skipping.")
                        continue

                    df = pd.concat(frames, ignore_index=True)
                    processed_df = process_aci_data(df, engine)

                elif "acc_localdata_monthly" in filename.lower():
                    logging.info(f"-> Matched rule: 'acc_localdata_monthly'")
                    table_name = "acc_localrawdata_monthly"
                    df = pd.read_excel(local_filepath, header=0, engine='openpyxl', keep_default_na=False)
                    df = drop_empty_columns(df)
                    df = drop_empty_rows(df)
                    processed_df = process_acc_data_monthly(df, engine)

                elif "aci_localdata_monthly" in filename.lower():
                    logging.info(f"-> Matched rule: 'aci_localdata_monthly'")
                    table_name = "aci_localrawdata_monthly"
                    # Read single sheet (default)
                    df = pd.read_excel(local_filepath, header=0, engine='openpyxl', keep_default_na=False)
                    df = drop_empty_columns(df)
                    df = drop_empty_rows(df)
                    processed_df = process_aci_data_monthly(df, engine)

                else:
                    logging.info(f"-> Matched default rule")
                    df = pd.read_excel(local_filepath, header=0, engine='openpyxl', keep_default_na=False)
                    df = drop_empty_columns(df)
                    df = drop_empty_rows(df)
                    processed_df = default_process(df)


                if processed_df is not None:
                    # --- Run Validations and pass the report object ---
                    is_valid = run_validations(processed_df, table_name, engine, config, report)
                    
                    if is_valid:
                        force_string_columns = config.get('DATA_TYPES', 'FORCE_STRING_COLUMNS', fallback=None)
                        force_unicode_columns = config.get('DATA_TYPES', 'FORCE_UNICODE_COLUMNS', fallback=None)
                        write_to_db(processed_df, table_name, engine, force_string_columns, force_unicode_columns)
                        # Move processed file from PROCESSING_DIR to DONE_DIR on FTP
                        move_file_to_done(ftp_config, filename)
                        logging.info(f"Successfully processed and imported {filename}.")
                        # Record success for consolidated report
                        try:
                            imported_rows = len(processed_df)
                        except Exception:
                            imported_rows = 'N/A'
                        report.add_success(f"--- Table: {table_name} ---\n"
                                           f"Result: SUCCESS\n"
                                           f"Source File: {filename}\n"
                                           f"Rows Imported: {imported_rows}")

                        # After ACC or ACI import, rebuild and upsert combined table
                        try:
                            if table_name in {"acc_localrawdata_weekly", "aci_localrawdata_weekly"}:
                                logging.info("Rebuilding combined ACC+ACI data after source update...")
                                combined_df = build_acc_aci_combined(engine)
                                if combined_df is not None and len(combined_df) > 0:
                                    combined_table = "accaci_local_data_weekly"
                                    is_combined_valid = run_validations(combined_df, combined_table, engine, config, report)
                                    if is_combined_valid:
                                        write_to_db(combined_df, combined_table, engine, force_string_columns, force_unicode_columns)
                                        logging.info("Upserted combined ACC+ACI data to accaci_local_data_weekly.")
                                        try:
                                            combined_rows = len(combined_df)
                                        except Exception:
                                            combined_rows = 'N/A'
                                        report.add_success(f"--- Table: {combined_table} ---\n"
                                                           f"Result: SUCCESS\n"
                                                           f"Triggered By: {table_name}\n"
                                                           f"Rows Upserted: {combined_rows}")
                                    else:
                                        logging.warning("Combined ACC+ACI data failed validation; skipping upsert.")
                                else:
                                    logging.info("Combined ACC+ACI builder returned empty; skipping upsert.")

                            elif table_name in {"acc_localrawdata_monthly", "aci_localrawdata_monthly"}:
                                logging.info("Rebuilding combined ACC+ACI monthly data after source update...")
                                combined_df = build_acc_aci_combined_monthly(engine)
                                if combined_df is not None and len(combined_df) > 0:
                                    combined_table = "accaci_local_data_monthly"
                                    is_combined_valid = run_validations(combined_df, combined_table, engine, config, report)
                                    if is_combined_valid:
                                        write_to_db(combined_df, combined_table, engine, force_string_columns, force_unicode_columns)
                                        logging.info("Upserted combined ACC+ACI monthly data to accaci_local_data_monthly.")
                                        try:
                                            combined_rows = len(combined_df)
                                        except Exception:
                                            combined_rows = 'N/A'
                                        report.add_success(f"--- Table: {combined_table} ---\n"
                                                           f"Result: SUCCESS\n"
                                                           f"Triggered By: {table_name}\n"
                                                           f"Rows Upserted: {combined_rows}")
                                    else:
                                        logging.warning("Combined ACC+ACI monthly data failed validation; skipping upsert.")
                                else:
                                    logging.info("Combined ACC+ACI monthly builder returned empty; skipping upsert.")

                        except Exception as e:
                            logging.error(f"Failed to rebuild/upsert combined ACC+ACI data: {e}")
                    else:
                        logging.warning(f"Skipping file {filename} due to validation failure. Details added to report.")
                        # Optionally, move the failed file to an error directory on FTP
                        # move_file_to_error(ftp_config, filename)
                        continue

            except Exception as e:
                logging.error(f"FATAL: An unexpected error occurred while handling {filename}. Skipping file. Details: {e}")
                report.add_failure(f"--- File: {filename} ---\n"
                                   f"Result: SKIPPED\n"
                                   f"Reason: A fatal error occurred during processing: {e}")
                continue
    
    finally:
        # --- At the end of the process, send the consolidated report ---
        report.send_report_if_needed()

        # Clean up the local temporary files
        logging.info("Cleaning up local temporary files...")
        for local_filepath in local_files:
            try:
                if os.path.exists(local_filepath):
                    os.remove(local_filepath)
                    logging.info(f"Removed temporary file: {local_filepath}")
            except OSError as e:
                logging.error(f"Error removing temporary file {local_filepath}: {e}")
        # if os.path.exists(local_temp_dir) and not os.listdir(local_temp_dir):
        #     os.rmdir(local_temp_dir)
        #     logging.info(f"Removed temporary directory: {local_temp_dir}")


def move_file_to_done(ftp_config, filename):
    """Wrapper to move a file from PROCESSING to DONE directory."""
    try:
        move_file(ftp_config, filename, from_dir_key='PROCESSING_DIR', to_dir_key='DONE_DIR')
    except Exception as e:
        # Log the error but don't crash the main process, as the primary DB operation succeeded.
        logging.error(f"CRITICAL: Failed to move file {filename} to DONE_DIR after successful processing. Manual cleanup may be required. Error: {e}")

def move_file_to_error(ftp_config, filename):
    """Wrapper to move a file from PROCESSING to ERROR directory."""
    try:
        move_file(ftp_config, filename, from_dir_key='PROCESSING_DIR', to_dir_key='ERROR_DIR')
    except Exception as e:
        logging.error(f"CRITICAL: Failed to move file {filename} to ERROR_DIR after a failure. Manual cleanup may be required. Error: {e}")


def main():
    """Main function to run the ETL process."""
    logging.info("Starting file processing...")
    db_engine = None
    try:
        db_engine = get_db_engine(config['DATABASE'])
        process_files(db_engine)
    except Exception as e:
        logging.critical(f"Application failed with a critical error: {e}")
    finally:
        if db_engine:
            db_engine.dispose()
        logging.info("File processing finished.")

if __name__ == "__main__":
    main()
