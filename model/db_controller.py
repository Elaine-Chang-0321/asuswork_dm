import configparser
import datetime
import pandas as pd
import logging
from sqlalchemy import create_engine, text, table, column, String, delete, inspect
from sqlalchemy.types import DECIMAL, NVARCHAR
import os
from npspo_vault_client import VaultClient

role_id=os.getenv("VAULT_CSGP_ROLE_ID")
secret_id=os.getenv("VAULT_CSGP_SECRET_ID")

# 初始化客戶端
client = VaultClient()
client.get_approle_token(role_id=role_id, secret_id=secret_id)

# 取得 Secret (以 aocc API Key 為例)
secret_data = client.get_vault_secret("/v1/db_kv_secret/data/admin/WCPad")
username = secret_data.get("username")
password = secret_data.get("password")

def get_db_engine(db_config):
    """Creates and returns a new SQLAlchemy engine."""
    try:
        db_connection_str = (
            f"mssql+pyodbc://{username}:{password}@"
            f"{db_config['HOST']}/{db_config['NAME']}?"
            f"driver=ODBC+Driver+17+for+SQL+Server&charset=utf8"
        )
        engine = create_engine(db_connection_str, connect_args={'charset': 'utf8'})
        # Test connection
        with engine.connect() as connection:
            logging.info("Database connection successful.")
        return engine
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        raise

def get_table_columns(table_name, engine):
    """Gets a list of column names for a given table."""
    try:
        inspector = inspect(engine)
        if inspector.has_table(table_name):
            columns = [col['name'] for col in inspector.get_columns(table_name)]
            return columns
        return None # Table does not exist
    except Exception as e:
        logging.error(f"Failed to get columns for table {table_name}: {e}")
        raise

def backup_table(original_table, backup_table_name, engine):
    """Backs up an existing table by renaming it."""
    try:
        with engine.connect() as connection:
            trans = connection.begin()
            try:
                # If the backup table already exists, drop it
                connection.execute(text(f"IF OBJECT_ID('{backup_table_name}', 'U') IS NOT NULL DROP TABLE {backup_table_name};"))
                # Rename the original table to the backup name
                connection.execute(text(f"EXEC sp_rename '{original_table}', '{backup_table_name}';"))
                trans.commit()
                logging.info(f"Successfully backed up '{original_table}' to '{backup_table_name}'.")
            except Exception as e:
                trans.rollback()
                logging.error(f"Failed to backup table '{original_table}': {e}")
                raise
    except Exception as e:
        logging.error(f"Database connection failed during backup: {e}")
        raise

def write_to_db(df, table_name, engine, force_string_columns=None, force_unicode_columns=None):
    """
    Writes the DataFrame to the database.
    - For 'bsgp_finkpdata', it deletes existing data for the relevant
      year-month and product_line combinations before inserting new data.
    - For other tables, it uses 'replace' logic.
    - It also explicitly sets float columns to DECIMAL(38, 15) during insertion.
    - It forces specific columns to be treated as String based on the provided list.
    """
    try:
        df_to_write = df.copy()

        # --- Standardize column names before writing to DB ---
        df_to_write.columns = (
            df_to_write.columns
            .str.strip()
            .str.replace(r'\s+', '_', regex=True) # Replace one or more spaces with a single underscore
            .str.lower()
        )
        logging.info(f"Standardized columns to: {df_to_write.columns.tolist()}")

        df_to_write['dw_ins_time'] = datetime.datetime.now()

        dtype_mapping = {}

        force_string_list = []
        if force_string_columns:
            candidates = (
                force_string_columns.split(',')
                if isinstance(force_string_columns, str)
                else force_string_columns
            )
            force_string_list = [
                str(c).strip()
                for c in candidates
                if str(c).strip()
            ]
        force_unicode_list = []
        if force_unicode_columns:
            candidates = (
                force_unicode_columns.split(',')
                if isinstance(force_unicode_columns, str)
                else force_unicode_columns
            )
            force_unicode_list = [
                str(c).strip()
                for c in candidates
                if str(c).strip()
            ]
        # --- Data Type Mapping ---
        # 優先處理被強制指定的 string 類型
        force_string_list = []
        if force_string_columns:
            candidates = (
                force_string_columns.split(',')
                if isinstance(force_string_columns, str)
                else force_string_columns
            )
            force_string_list = [
                str(c).strip()
                for c in candidates
                if str(c).strip()
            ]
        
        force_string_set = set(force_string_list)
        dtype_mapping.update({
            col: String
            for col in force_string_set
            if col in df_to_write.columns
        })

        # Map explicitly Unicode columns to NVARCHAR to preserve CJK text
        force_unicode_set = set(force_unicode_list)
        dtype_mapping.update({
            col: NVARCHAR(length=255)
            for col in force_unicode_set
            if col in df_to_write.columns
        })

        # 接著處理 float 類型，但要避開已經被指定為 string 的欄位
        float_to_decimal_mapping = {
            col: DECIMAL(38, 15) 
            for col, dtype in df_to_write.dtypes.items() 
            if pd.api.types.is_float_dtype(dtype) and col not in force_string_set
        }
        dtype_mapping.update(float_to_decimal_mapping)

        if table_name == 'bsgp_finkpdata' and 'gl_yearmonth' in df_to_write.columns and 'product_line' in df_to_write.columns:
            try:
                with engine.begin() as connection:
                    # Get unique combinations of year-month and product_line to delete
                    keys_to_delete = df_to_write[['gl_yearmonth', 'product_line']].drop_duplicates()
                    
                    if not keys_to_delete.empty:
                        my_table = table(table_name, column('gl_yearmonth', String), column('product_line', String))
                        
                        # Iterate over each key combination and delete
                        for index, row in keys_to_delete.iterrows():
                            stmt = delete(my_table).where(
                                (my_table.c.gl_yearmonth == row['gl_yearmonth']) &
                                (my_table.c.product_line == row['product_line'])
                            )
                            connection.execute(stmt)
                        
                        logging.info(f"Deleted existing data from {table_name} for {len(keys_to_delete)} year-month/product-line combinations.")
                    
                    df_to_write.to_sql(table_name, connection, if_exists='append', index=False, dtype=dtype_mapping)
                    logging.info(f"Successfully inserted new data into {table_name}.")
            except Exception as e:
                logging.error(f"Could not upsert {table_name}. Error: {e}")
                # If delete-and-insert failed, try full replace as fallback
                try:
                    logging.info(f"Attempting full table replace for {table_name}...")
                    df_to_write.to_sql(table_name, engine, if_exists='replace', index=False, dtype=dtype_mapping)
                    logging.info(f"Successfully replaced table {table_name}.")
                except Exception as e2:
                    logging.error(f"Failed to replace {table_name}: {e2}")
                    raise
        
        elif table_name == 'pac' and 'period' in df_to_write.columns and 'product_line' in df_to_write.columns:
            try:
                with engine.begin() as connection:
                    # Get unique combinations of period and product_line to delete
                    keys_to_delete = df_to_write[['period', 'product_line']].drop_duplicates()
                    
                    if not keys_to_delete.empty:
                        my_table = table(table_name, column('period', String), column('product_line', String))
                        
                        # Iterate over each key combination and delete
                        for index, row in keys_to_delete.iterrows():
                            stmt = delete(my_table).where(
                                (my_table.c.period == str(row['period'])) &
                                (my_table.c.product_line == row['product_line'])
                            )
                            connection.execute(stmt)
                        
                        logging.info(f"Deleted existing data from {table_name} for {len(keys_to_delete)} period/product-line combinations.")
                    
                    df_to_write.to_sql(table_name, connection, if_exists='append', index=False, dtype=dtype_mapping)
                    logging.info(f"Successfully inserted new data into {table_name}.")
            except Exception as e:
                logging.error(f"Could not upsert {table_name}. Error: {e}")
                # If delete-and-insert failed, try full replace as fallback
                try:
                    logging.info(f"Attempting full table replace for {table_name}...")
                    df_to_write.to_sql(table_name, engine, if_exists='replace', index=False, dtype=dtype_mapping)
                    logging.info(f"Successfully replaced table {table_name}.")
                except Exception as e2:
                    logging.error(f"Failed to replace {table_name}: {e2}")
                    raise

        elif table_name in {'acc_localrawdata_weekly', 'acc_localrawdata_monthly'} and 'period' in df_to_write.columns and 'product_line_id' in df_to_write.columns:
            try:
                with engine.begin() as connection:
                    # Get unique combinations of period and product_line_id to delete
                    keys_to_delete = df_to_write[['period', 'product_line_id']].drop_duplicates()

                    if not keys_to_delete.empty:
                        my_table = table(table_name, column('period', String), column('product_line_id', String))

                        # Iterate over each key combination and delete
                        for index, row in keys_to_delete.iterrows():
                            stmt = delete(my_table).where(
                                (my_table.c.period == str(row['period'])) &
                                (my_table.c.product_line_id == str(row['product_line_id']))
                            )
                            connection.execute(stmt)

                        logging.info(f"Deleted existing data from {table_name} for {len(keys_to_delete)} period/product_line_id combinations.")

                    df_to_write.to_sql(table_name, connection, if_exists='append', index=False, dtype=dtype_mapping)
                    logging.info(f"Successfully inserted new data into {table_name}.")
            except Exception as e:
                logging.error(f"Could not upsert {table_name}. Error: {e}")
                # If delete-and-insert failed, try full replace as fallback
                try:
                    logging.info(f"Attempting full table replace for {table_name}...")
                    df_to_write.to_sql(table_name, engine, if_exists='replace', index=False, dtype=dtype_mapping)
                    logging.info(f"Successfully replaced table {table_name}.")
                except Exception as e2:
                    logging.error(f"Failed to replace {table_name}: {e2}")
                    raise

        elif table_name in {'aci_localrawdata_weekly', 'aci_localrawdata_monthly'} and 'period' in df_to_write.columns:
            try:
                with engine.begin() as connection:
                    # Delete existing rows for incoming periods (upsert by period)
                    periods = df_to_write['period'].dropna().astype(str).unique().tolist()
                    if periods:
                        my_table = table(table_name, column('period', String))
                        for p in periods:
                            stmt = delete(my_table).where(my_table.c.period == p)
                            connection.execute(stmt)
                        logging.info(f"Deleted existing data from {table_name} for {len(periods)} period(s).")

                    df_to_write.to_sql(table_name, connection, if_exists='append', index=False, dtype=dtype_mapping)
                    logging.info(f"Successfully inserted new data into {table_name}.")
            except Exception as e:
                logging.error(f"Could not upsert {table_name}. Error: {e}")
                # If delete-and-insert failed, try full replace as fallback
                try:
                    logging.info(f"Attempting full table replace for {table_name}...")
                    df_to_write.to_sql(table_name, engine, if_exists='replace', index=False, dtype=dtype_mapping)
                    logging.info(f"Successfully replaced table {table_name}.")
                except Exception as e2:
                    logging.error(f"Failed to replace {table_name}: {e2}")
                    raise


        elif table_name in {'accaci_local_data_weekly', 'accaci_local_data_monthly'} and {'period','product_line_id','source'}.issubset(set(df_to_write.columns)):
            try:
                with engine.begin() as connection:
                    keys_to_delete = df_to_write[['source', 'product_line_id', 'period']].drop_duplicates()
                    if not keys_to_delete.empty:
                        my_table = table(table_name, column('source', String), column('product_line_id', String), column('period', String))
                        for index, row in keys_to_delete.iterrows():
                            stmt = delete(my_table).where(
                                (my_table.c.source == str(row['source'])) &
                                (my_table.c.product_line_id == str(row['product_line_id'])) &
                                (my_table.c.period == str(row['period']))
                            )
                            connection.execute(stmt)
                        logging.info(f"Deleted existing data from {table_name} for {len(keys_to_delete)} source/product_line_id/period combinations.")

                    df_to_write.to_sql(table_name, connection, if_exists='append', index=False, dtype=dtype_mapping)
                    logging.info(f"Successfully inserted new data into {table_name}.")
            except Exception as e:
                logging.error(f"Could not upsert {table_name}. Error: {e}")
                # If delete-and-insert failed, try full replace as fallback
                try:
                    logging.info(f"Attempting full table replace for {table_name}...")
                    df_to_write.to_sql(table_name, engine, if_exists='replace', index=False, dtype=dtype_mapping)
                    logging.info(f"Successfully replaced table {table_name}.")
                except Exception as e2:
                    logging.error(f"Failed to replace {table_name}: {e2}")
                    raise

        else:
            df_to_write.to_sql(table_name, engine, if_exists='replace', index=False, dtype=dtype_mapping)
            logging.info(f"Successfully wrote data to {table_name} (table was replaced).")

    except Exception as e:
        logging.error(f"An error occurred in write_to_db for table {table_name}: {e}")
        raise

def read_table_to_df(table_name, engine):
    """Reads a full table from the database into a pandas DataFrame."""
    try:
        with engine.connect() as connection:
            logging.info(f"Reading table '{table_name}' from database.")
            df = pd.read_sql_table(table_name, connection)
            logging.info(f"Successfully read {len(df)} rows from '{table_name}'.")
            return df
    except Exception as e:
        logging.error(f"Error reading table '{table_name}' from database: {e}")
        raise

def get_category_mapping(engine):
    """Reads the category mapping from the database."""
    try:
        # This table contains the mapping from item_code to category
        df = read_table_to_df('bsgp_itemcode', engine)
        # The mapping logic uses 'category_1' as a standard name, so we rename it here.
        df.rename(columns={'category': 'category_1'}, inplace=True)
        return df[['item_code', 'category_1']]
    except Exception as e:
        logging.error(f"Could not read category mapping table 'bsgp_itemcode'. Error: {e}")
        raise
