"""
ACC and ACI Data Processing Module

This module contains all functions related to processing ACC (China local) 
and ACI (International) sales data files.

Functions:
    - Shared helpers for data cleaning and transformation
    - process_acc_data: Process ACC weekly data files
    - process_aci_data: Process ACI weekly data files
    - finalize_acc_columns: Standardize ACC output schema
"""

import pandas as pd
import logging
from db_controller import read_table_to_df
from decimal import Decimal

# --- Shared Helper Functions ---

def map_month_to_quarter(month_series: pd.Series) -> pd.Series:
    """Convert numeric month (1-12) to quarter string (Q1-Q4).
    
    Used by both ACC and ACI processing.
    
    Args:
        month_series: Series of numeric months (1-12)
    
    Returns:
        Series of quarter strings ('Q1', 'Q2', 'Q3', 'Q4')
    """
    def _to_quarter(m):
        if pd.isna(m):
            return None
        m = int(m)
        if 1 <= m <= 3:
            return 'Q1'
        if 4 <= m <= 6:
            return 'Q2'
        if 7 <= m <= 9:
            return 'Q3'
        return 'Q4'
    return month_series.apply(_to_quarter)


def read_and_clean_table(table_name: str, engine, lowercase: bool = True) -> pd.DataFrame:
    """Read table from DB and standardize column names.
    
    Args:
        table_name: Name of the table to read
        engine: Database engine
        lowercase: If True, convert column names to lowercase
    
    Returns:
        DataFrame with cleaned column names, or None if read fails
    """
    try:
        df = read_table_to_df(table_name, engine)
        if df is not None and len(df) > 0:
            if lowercase:
                df.columns = [c.strip().lower() for c in df.columns]
            else:
                df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        logging.error(f"Failed to read and clean table '{table_name}': {e}")
        return None


def clean_dataframe_columns(df: pd.DataFrame, lowercase: bool = False, deduplicate: bool = False) -> pd.DataFrame:
    """Standardize DataFrame column names.
    
    Args:
        df: Input DataFrame
        lowercase: If True, convert to lowercase
        deduplicate: If True, keep only first occurrence of duplicate columns
    
    Returns:
        DataFrame with cleaned columns
    """
    df = df.copy()
    
    # Strip whitespace
    df.columns = df.columns.str.strip()
    
    # Lowercase if requested
    if lowercase:
        df.columns = df.columns.str.lower()
    
    # Deduplicate if requested
    if deduplicate and df.columns.duplicated().any():
        dupes = df.columns[df.columns.duplicated()].tolist()
        logging.warning(f"Duplicate column names detected; keeping first: {dupes}")
        df = df.loc[:, ~df.columns.duplicated(keep='first')]
    
    return df


def safe_get_series(df: pd.DataFrame, column_name: str) -> pd.Series:
    """Safely get a column as a Series, handling duplicate column names.
    
    If column doesn't exist or is a DataFrame due to duplicates, returns empty Series.
    
    Args:
        df: Input DataFrame
        column_name: Name of column to retrieve
    
    Returns:
        Series containing the column data, or empty Series if not found
    """
    val = df.get(column_name)
    if isinstance(val, pd.DataFrame):
        return val.iloc[:, 0]
    if isinstance(val, pd.Series):
        return val
    return pd.Series(index=df.index, dtype=object)


def robust_date_converter(value):
    """Converts a value to datetime, handling Excel serial numbers and standard date strings.
    
    Args:
        value: Date value (can be Excel serial number or date string)
    
    Returns:
        Pandas datetime or NaT if conversion fails
    """
    if pd.api.types.is_number(value) and not pd.isna(value):
        return pd.to_datetime(value, unit='D', origin='1899-12-30')
    return pd.to_datetime(value, errors='coerce')


# --- Column Schema Constants ---

# Single source of truth for ACC output columns (order matters)
ACC_OUTPUT_COLUMNS = [
    'Source',
    'ou',
    'warranty',
    'product_type',
    'period',
    'year',
    'quarter',
    'month',
    'order_type',
    'business_type',
    'csid',
    'part_number',
    'description',
    'all_qty',
    'product_line_id',
    'distribution_type',
    'distribution',
    'revenue_rmb',
    'revenue_usd_hedge_rate',
    'revenue_fxdiff',
    'credit_note_rmb',
    'credit_note_hedge_rate',
    'credit_note_fxdiff',
    'net_revenue_rmb',
    'net_revenue_usd_agp',
    'net_revenue_fxdiff',
    'local_cogs_amount_rmb',
    'local_cogs_amount',
    'local_cogs_amount_fxdiff',
]

# Single source of truth for ACI output columns (order matters)
ACI_OUTPUT_COLUMNS = [
    'Source',
    'business_type',
    'territory',
    'territory2',
    'country_id',
    'country_chinese',
    'order_type',
    'branch',
    'product_line_id',
    'period',
    'year',
    'quarter',
    'month',
    'sold_to_customer',
    'part_number',
    'all_qty',
    'revenue_usd_hedge_rate',
    'net_revenue_usd_agp',
    'local_cogs_amount',
    'csc_ratio',
    'csc_amt',
    'credit_note_hedge_rate'
]

# Combined ACC+ACI output columns (for accaci_local_data_weekly table)
COMBINED_OUTPUT_COLUMNS = [
    'source',
    'product_line_id',
    'year',
    'quarter',
    'month',
    'period',
    'sold_to_customer',
    'customer',
    'order_type',
    'business_type',
    'product_type',
    'distribution',
    'distribution_type',
    'territory',
    'territory2',
    'branch',
    'branch2',
    'country_id',
    'country_chinese',
    'ou',
    'warranty',
    'csid',
    'part_number',
    'description',
    'all_qty',
    'revenue_usd_hedge_rate',
    'revenue_fxdiff',
    'net_revenue_usd_agp',
    'net_revenue_fxdiff',
    'local_cogs_amount',
    'local_cogs_amount_fxdiff',
    'credit_note_hedge_rate',
    'credit_note_fxdiff',
    'total_ship_fee_company',
    'total_ship_fee_company_fxdiff',
    'total_ship_fee_company_rmb',
    'cost_period',
    'funding',
    'csc_ratio',
    'csc_amt',
    'revenue_rmb',
    'net_revenue_rmb',
    'local_cogs_amount_rmb',
    'credit_note_rmb',
]


# --- Column Finalization Functions ---

def finalize_acc_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Apply standardized post-transform renames for ACC data and enforce schema.
    
    Performs:
    1. Column renames (OU -> ou, Period -> period, etc.)
    2. product_type derivation from Org
    3. Ensures all required columns exist
    4. Orders columns per ACC_OUTPUT_COLUMNS
    
    Args:
        df: Input DataFrame with ACC data
    
    Returns:
        DataFrame with finalized ACC schema
    """
    try:
        # Apply standard column renames
        renames = {
            'OU': 'ou',
            'Warranty': 'warranty',
            'Period': 'period',
            'Order Type': 'order_type',
            'Bill to Location': 'business_type',
            'CSID': 'csid',
            'Item': 'part_number',
            'Item Description': 'description',
            'Quantity': 'all_qty',
            'Product num': 'product_line_id',
        }
        apply_map = {k: v for k, v in renames.items() if k in df.columns}
        if apply_map:
            df.rename(columns=apply_map, inplace=True)

        # Derive product_type from Org
        if 'Org' in df.columns:
            if 'product_type' in df.columns:
                df['product_type'] = df['product_type'].fillna(df['Org'])
            else:
                df.rename(columns={'Org': 'product_type'}, inplace=True)
            if 'Org' in df.columns:
                df.drop(columns=['Org'], inplace=True)

        # Ensure all required columns exist (fill with NA if missing)
        required_cols = [c for c in ACC_OUTPUT_COLUMNS if c != 'Source']
        for c in required_cols:
            if c not in df.columns:
                df[c] = pd.NA

        # Enforce column order and restrict to schema
        df = df[[c for c in ACC_OUTPUT_COLUMNS if c in df.columns]]
        
        return df
    except Exception as e:
        logging.error(f"Failed during ACC column finalization: {e}")
        return df


def finalize_aci_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Apply standardized post-transform renames for ACI data and enforce schema.
    
    Performs:
    1. Column renames (cust full name -> sold_to_customer, etc.)
    2. Ensures all required columns exist
    3. Orders columns per ACI_OUTPUT_COLUMNS
    
    Args:
        df: Input DataFrame with ACI data
    
    Returns:
        DataFrame with finalized ACI schema
    """
    try:
        # Apply standard column renames
        rename_map = {
            'cust full name': 'sold_to_customer',
            'part no': 'part_number',
            'sales qty fg': 'all_qty',
            'cogs amt': 'local_cogs_amount',
            'csc unit cost': 'csc_ratio',
            'total csc cost': 'csc_amt',
            'total cn amt': 'credit_note_hedge_rate',
        }
        apply_map = {k: v for k, v in rename_map.items() if k in df.columns}
        if apply_map:
            df.rename(columns=apply_map, inplace=True)

        # Ensure all required columns exist (fill with NA if missing)
        required_cols = [c for c in ACI_OUTPUT_COLUMNS if c != 'Source']
        for c in required_cols:
            if c not in df.columns:
                df[c] = pd.NA

        # Enforce column order and restrict to schema
        df = df[[c for c in ACI_OUTPUT_COLUMNS if c in df.columns]]
        
        return df
    except Exception as e:
        logging.error(f"Failed during ACI column finalization: {e}")
        return df


# --- ACC Processing Function ---

def process_acc_data(df, engine):
    """Processes 'acc_localdata_weekly' files with column filtering and Org mapping.
    
    Applies filters, transformations, and mappings to standardize ACC data:
    - Filters: Order Type exclusions, Category=FG, series exclusions
    - Transforms: OU truncation, Org→product_type, Period normalization
    - Mappings: Order Type via acc_ordertype, Bill to Location classification,
      分类码→distribution, FX rates from exchange_rate_rmb
    - Computes: Revenue, credit note, net revenue, COGS in RMB and USD with FX rates
    
    Args:
        df: Input DataFrame from ACC Excel file
        engine: Database engine for table lookups
    
    Returns:
        DataFrame with standardized ACC schema and computed fields
    """
    logging.info("Processing acc_localdata_weekly...")
    
    # Clean column names
    df = clean_dataframe_columns(df, lowercase=False, deduplicate=False)

    # Expected columns from ACC Excel
    input_required_columns = [
        'OU', 'Warranty', 'Org', 'Period', 'Order Type', 'Bill to Location', 'CSID',
        'Item', 'Item Description', 'Quantity', 'Product num', '分类码', 
        '销售收入', '销售折扣', '销售净额', '销售成本'
    ]
    
    missing_cols = [c for c in input_required_columns if c not in df.columns]
    if missing_cols:
        logging.warning(f"ACC Data missing columns: {missing_cols}")

    # Helper: check if order type should be excluded
    def _should_exclude_order_type(val):
        if pd.isna(val):
            return False
        val_upper = str(val).upper()
        return any(kw in val_upper for kw in ['RTN', 'RETURN', 'DISCOUNT', 'REJECT'])

    # --- Add Source column ---
    df['Source'] = 'ACC'

    # --- Apply filters ---
    # Filter: Order Type (exclude returns, discounts, rejects)
    if 'Order Type' in df.columns:
        df = df[~df['Order Type'].apply(_should_exclude_order_type)]
    else:
        logging.warning("'Order Type' column not found. Skipping Order Type filter.")

    # Filter: Category (keep only FG)
    cat_col = 'CATEGORY' if 'CATEGORY' in df.columns else ('Category' if 'Category' in df.columns else None)
    if cat_col:
        df = df[df[cat_col].astype(str).str.strip() == 'FG']

    # Filter: series (exclude NB ACCY, NR-ACCESSORY)
    if 'series' in df.columns:
        exclude_series = ['NB ACCY', 'NR-ACCESSORY']
        df = df[~df['series'].astype(str).str.strip().isin(exclude_series)]

    # Filter: Item (keep only if contains '-M')
    if 'Item' in df.columns:
        df = df[df['Item'].astype(str).str.contains('-M', na=False)]
    else:
        logging.warning("'Item' column not found. Skipping Item filter.")

    # --- Transform 1: OU (First 4 chars) ---
    if 'OU' in df.columns:
        df['OU'] = df['OU'].astype(str).str[:4]

    # --- Transform 2: Org mapping ---
    if 'Org' in df.columns:
        try:
            acc_org_df = read_and_clean_table('acc_org', engine, lowercase=True)
            if acc_org_df is None:
                raise ValueError("Failed to load acc_org table")

            merged = pd.merge(df, acc_org_df[['org', 'product_type']], 
                              left_on='Org', right_on='org', how='left')
            
            merged['Org'] = merged['product_type'].fillna(merged['Org'])
            df = merged
        except Exception as e:
            logging.error(f"Failed to map Org column: {e}")

    # --- Transform 3: Derive year/quarter/month from Period ---
    if 'Period' in df.columns:
        # Normalize Period into 6-digit yyyymm string
        period_series = df['Period'].astype(str).str.replace(r'\D', '', regex=True)
        period_series = period_series.str.pad(6, side='right', fillchar='0').str[:6]

        df['year'] = period_series.str[:4]
        df['month'] = period_series.str[4:6]

        month_num = pd.to_numeric(df['month'], errors='coerce')
        df['quarter'] = map_month_to_quarter(month_num)

    # --- Transform 4: Map Order Type via acc_ordertype and replace ---
    if 'Order Type' in df.columns:
        try:
            acc_ordertype_df = read_and_clean_table('acc_ordertype', engine, lowercase=True)
            if acc_ordertype_df is None:
                raise ValueError("Failed to load acc_ordertype table")

            df = pd.merge(
                df,
                acc_ordertype_df[['order_type', 'order_type2']],
                left_on='Order Type',
                right_on='order_type',
                how='left'
            )

            # Replace original Order Type with mapped order_type2 when available
            df['Order Type'] = df['order_type2'].fillna(df['Order Type'])

            # Drop helper columns from the merge
            drop_cols = [c for c in ['order_type', 'order_type2'] if c in df.columns]
            if drop_cols:
                df.drop(columns=drop_cols, inplace=True)
        except Exception as e:
            logging.error(f"Failed to map Order Type via acc_ordertype: {e}")

    # --- Transform 5: Classify Bill to Location as Retail/Channel ---
    if 'Bill to Location' in df.columns:
        try:
            s = df['Bill to Location'].astype(str)
            retail_mask = s.str.upper().str.contains(r'(BTC|BTC1|NC|3C)$', regex=True, na=False)
            df.loc[retail_mask, 'Bill to Location'] = 'Retail'
            df.loc[~retail_mask, 'Bill to Location'] = 'Channel'
        except Exception as e:
            logging.error(f"Failed to classify Bill to Location: {e}")
    else:
        logging.warning("'Bill to Location' column not found. Skipping Retail/Channel classification.")

    # --- Transform 6: Split and map 分类码 -> distribution_type + distribution ---
    if '分类码' in df.columns:
        try:
            df['distribution_type'] = df['分类码'].astype(str).str.strip()
            dist_df = read_and_clean_table('acc_distribution', engine, lowercase=True)
            if dist_df is None:
                raise ValueError("Failed to load acc_distribution table")
            df = pd.merge(
                df,
                dist_df[['distribution_type', 'distribution']],
                left_on='distribution_type',
                right_on='distribution_type',
                how='left'
            )
        except Exception as e:
            logging.error(f"Failed to map 分类码 via acc_distribution: {e}")
    else:
        logging.warning("'分类码' column not found. Skipping distribution mapping.")

    # --- Transform 7: Revenue and COGS fields using exchange_rate_rmb ---
    def _apply_fx_conversion(amount_series, fx_rate_series, zero_to_na=True):
        """Apply FX conversion with proper null handling."""
        if fx_rate_series is None:
            return pd.Series([pd.NA] * len(amount_series), index=amount_series.index)
        result = amount_series / fx_rate_series
        if zero_to_na:
            result.loc[(fx_rate_series == 0) | (fx_rate_series.isna())] = pd.NA
        return result

    # 轉換函數：直接轉成 Decimal 物件，而不是 float
    def to_decimal_obj(x):
        if pd.isna(x) or str(x).strip() == '':
            return None
        return Decimal(str(x).replace(',', ''))

    try:
        # Normalize Period for FX joins
        if 'Period' in df.columns:
            period_series = df['Period'].astype(str).str.replace(r'\D', '', regex=True)
            df['Period'] = period_series.str.pad(6, side='right', fillchar='0').str[:6]

        # Join exchange rates (requires Period and Apply Prod Line)
        fx_rate, fx_rate_ex = None, None
        if ('Period' in df.columns) and ('Apply Prod Line' in df.columns):
            try:
                exch_df = read_and_clean_table('exchange_rate_rmb', engine, lowercase=True)
                if exch_df is None:
                    raise ValueError("Failed to load exchange_rate_rmb table")
                df = pd.merge(
                    df,
                    exch_df[['product_line', 'period', 'fx_rate', 'fx_rate_ex']],
                    left_on=['Apply Prod Line', 'Period'],
                    right_on=['product_line', 'period'],
                    how='left'
                )
                # Clean merge helper columns
                df.drop(columns=['product_line', 'period'], inplace=True, errors='ignore')
                
                # Extract FX rates as Series
                fx_rate = df['fx_rate'].apply(to_decimal_obj)
                fx_rate_ex = df['fx_rate_ex'].apply(to_decimal_obj)
            except Exception as e:
                logging.error(f"Failed joining exchange_rate_rmb: {e}")
        else:
            logging.warning("Missing 'Period' or 'Apply Prod Line' for FX join; USD/FX conversions will be skipped.")


        # Compute revenue fields: 销售收入 -> revenue
        if '销售收入' in df.columns:
            sales_rmb = df['销售收入'].apply(to_decimal_obj)
            df['revenue_rmb'] = sales_rmb
            df['revenue_usd_hedge_rate'] = _apply_fx_conversion(sales_rmb, fx_rate)
            df['revenue_fxdiff'] = _apply_fx_conversion(sales_rmb, fx_rate_ex)
        else:
            logging.warning("Missing '销售收入'; revenue fields will remain NULL.")

        # Compute credit note fields: 销售折扣 -> credit notes (sign inverted)
        if '销售折扣' in df.columns:
            sales_discount = df['销售折扣'].apply(to_decimal_obj)
            df['credit_note_rmb'] = -sales_discount
            df['credit_note_hedge_rate'] = _apply_fx_conversion(-sales_discount, fx_rate)
            df['credit_note_fxdiff'] = _apply_fx_conversion(-sales_discount, fx_rate_ex)
        else:
            logging.warning("Missing '销售折扣'; credit note fields will remain NULL.")

        # Compute net revenue fields: 销售净额 -> net revenue
        if '销售净额' in df.columns:
            net_amount = df['销售净额'].apply(to_decimal_obj)
            df['net_revenue_rmb'] = net_amount
            df['net_revenue_usd_agp'] = _apply_fx_conversion(net_amount, fx_rate)
            df['net_revenue_fxdiff'] = _apply_fx_conversion(net_amount, fx_rate_ex)
        else:
            logging.warning("Missing '销售净额'; net revenue fields will remain NULL.")

        # Compute COGS fields: 销售成本 -> local COGS
        if '销售成本' in df.columns:
            cogs_amount = df['销售成本'].apply(to_decimal_obj)
            df['local_cogs_amount_rmb'] = cogs_amount
            df['local_cogs_amount'] = _apply_fx_conversion(cogs_amount, fx_rate)
            df['local_cogs_amount_fxdiff'] = _apply_fx_conversion(cogs_amount, fx_rate_ex)
        else:
            logging.warning("Missing '销售成本'; COGS fields will remain NULL.")

        # Round all monetary fields to 15 decimals
        monetary_cols = [
            'revenue_usd_hedge_rate', 'revenue_fxdiff',
            'credit_note_hedge_rate', 'credit_note_fxdiff',
            'net_revenue_usd_agp', 'net_revenue_fxdiff',
            'local_cogs_amount', 'local_cogs_amount_fxdiff'
        ]
        for col in monetary_cols:
            if col in df.columns:
                df[col] = df[col].apply(to_decimal_obj).round(15)

        # Clean FX helper columns
        df.drop(columns=['fx_rate', 'fx_rate_ex'], inplace=True, errors='ignore')
        
    except Exception as e:
        logging.error(f"Failed to compute revenue/COGS fields: {e}")

    # --- Finalize column names (post-transform renames) ---
    df = finalize_acc_columns(df)

    # --- ACC-specific: Drop rows without period ---
    try:
        if 'period' in df.columns:
            before = len(df)
            df = df[df['period'].notna() & (df['period'].astype(str).str.strip() != '')]
            removed = before - len(df)
            if removed > 0:
                logging.info(f"Dropped {removed} rows without period in ACC data")
        else:
            logging.warning("ACC data missing 'period' column after rename; cannot enforce period filter.")
    except Exception as e:
        logging.warning(f"Failed to filter rows without period in ACC data: {e}")

    return df


# --- ACI Processing Function ---

def process_aci_data(df, engine):
    """Processes 'aci_localdata_weekly' files.
    
    Applies transformations and mappings to standardize ACI data:
    - Normalizes columns to lowercase and deduplicates
    - Filters: Sales Type != CSC
    - Derives: business_type from Sales Group, territory/territory2 from region,
      order_type and branch from Sales Type/Revenue Country/Sales Group
    - Computes: period/year/quarter/month from Date
    - Maps: country_id/country_chinese via aci_countryname
    - Computes: revenue_usd_hedge_rate and net_revenue_usd_agp
    
    Args:
        df: Input DataFrame from ACI Excel file
        engine: Database engine for table lookups
    
    Returns:
        DataFrame with standardized ACI schema
    """
    logging.info("Processing aci_localdata_weekly...")
    
    # 如果df['Product Line'] == 'NV'
    if 'Product Line' in df.columns:
        if (df['Product Line'].astype(str).str.strip() == 'NV').any():
            print('NV')

    # Clean column names (normalize to lowercase for case-insensitive handling across sheets)
    df = clean_dataframe_columns(df, lowercase=True, deduplicate=True)

    # Add Source column (kept as 'Source' to align with downstream selection)
    df['Source'] = 'ACI'

    # Filter Sales Type -> "排除文字是CSC的資料"
    if 'sales type' in df.columns:
        df = df[df['sales type'].astype(str).str.strip() != 'CSC']
    else:
        logging.warning("'sales type' column not found. Skipping Sales Type filter.")

    # Filter: part no (keep only if contains '-M')
    if 'part no' in df.columns:
        df = df[df['part no'].astype(str).str.contains('-M', na=False)]
    else:
        logging.warning("'part no' column not found. Skipping part no filter.")

    # Map Sales Group to business_type with fallback to Sales Type
    try:
        if 'sales group' in df.columns:
            sg = df['sales group'].astype(str)
            df['business_type'] = df.get('sales type', pd.Series(index=df.index, dtype=object)).astype(str)
            mask_channel = sg.str.contains('Channel', case=False, na=False)
            mask_refurb = sg.str.contains('Refurbish', case=False, na=False)
            mask_retail = sg.str.contains('Retail', case=False, na=False)
            df.loc[mask_channel, 'business_type'] = 'Channel'
            df.loc[mask_refurb, 'business_type'] = 'Refurbish'
            df.loc[mask_retail, 'business_type'] = 'Direct Retail'
        else:
            logging.warning("'sales group' column not found. Cannot derive business_type.")
    except Exception as e:
        logging.error(f"Failed to map Sales Group to business_type: {e}")

    # Derive territory and territory2 from region
    try:
        if 'region' in df.columns:
            region_raw = safe_get_series(df, 'region')
            region_norm = region_raw.astype(str).str.strip().str.upper()
            df['territory'] = region_norm
            df.loc[region_norm == 'NA', 'territory'] = 'ACI'
            df.loc[region_norm == 'SA', 'territory'] = 'LATAM'

            df['territory2'] = region_norm
            df.loc[region_norm == 'SA', 'territory2'] = 'LATAM'
        else:
            logging.warning("'region' column not found. Cannot derive territory fields.")
    except Exception as e:
        logging.error(f"Failed to derive territory fields from region: {e}")

    # Split Sales Type into order_type and branch
    try:
        st_raw = safe_get_series(df, 'sales type').astype(str).str.strip()
        st_norm = st_raw.str.upper()
        rc_norm = safe_get_series(df, 'revenue country').astype(str).str.strip().str.upper()
        sg_norm = safe_get_series(df, 'sales group').astype(str).str.strip().str.upper()

        # order_type mapping
        df['order_type'] = 'Standard'
        df.loc[st_norm == 'CSC', 'order_type'] = 'CSC'
        df.loc[st_norm == 'REFURBISH', 'order_type'] = 'Refurbish'

        # branch mapping (fallback to original Sales Type text)
        df['branch'] = st_raw
        df.loc[(rc_norm == 'US') & (sg_norm == 'RETAIL'), 'branch'] = 'US-Retail'
        df.loc[st_norm == 'CHANNEL-US', 'branch'] = 'US-CHANNEL'
        df.loc[rc_norm == 'CA', 'branch'] = 'CA'
    except Exception as e:
        logging.error(f"Failed to derive order_type/branch from Sales Type rules: {e}")

    if 'period' in df.columns:
        # Normalize Period into 6-digit yyyymm string
        period_series = df['period'].astype(str).str.replace(r'\D', '', regex=True)
        period_series = period_series.str.pad(6, side='right', fillchar='0').str[:6]

        df['year'] = period_series.str[:4]
        df['month'] = period_series.str[4:6]

        month_num = pd.to_numeric(df['month'], errors='coerce')
        df['quarter'] = map_month_to_quarter(month_num)


    # Derive period/year/quarter/month from Date
    # try:
    #     if 'date' in df.columns:
    #         date_series = df['date'].apply(robust_date_converter)
    #         df['period'] = date_series.dt.strftime('%Y%m')
    #         df['year'] = date_series.dt.strftime('%Y')
    #         df['month'] = date_series.dt.strftime('%m')
    #         mnum = pd.to_numeric(df['month'], errors='coerce')
    #         df['quarter'] = map_month_to_quarter(mnum)
            
    #         # Drop rows without Date
    #         before = len(df)
    #         df = df[date_series.notna()]
    #         removed = before - len(df)
    #         if removed > 0:
    #             logging.info(f"Dropped {removed} rows without Date in ACI data")
    #     else:
    #         logging.warning("'date' column not found; skipping period/year/quarter/month derivation.")
    # except Exception as e:
    #     logging.error(f"Failed to derive period fields from Date: {e}")

    # Derive country_id and country_chinese from revenue country
    try:
        if 'revenue country' in df.columns:
            df['country_id'] = safe_get_series(df, 'revenue country').astype(str).str.strip()
            try:
                cn_df = read_and_clean_table('aci_countryname', engine, lowercase=True)
                if cn_df is None:
                    raise ValueError("Failed to load aci_countryname table")
                
                # Build case-insensitive maps
                id_map = {
                    str(k).strip().upper(): v
                    for k, v in cn_df.set_index('country_id')['country_zh'].to_dict().items()
                    if k is not None
                }
                name_map = {
                    str(k).strip().upper(): v
                    for k, v in cn_df.set_index('country_name')['country_zh'].to_dict().items()
                    if k is not None
                }
                key_series = safe_get_series(df, 'revenue country').astype(str).str.strip().str.upper()
                zh_by_id = key_series.map(id_map)
                zh_by_name = key_series.map(name_map)
                df['country_chinese'] = zh_by_id.fillna(zh_by_name)
            except Exception as e:
                logging.error(f"Failed to load/merge aci_countryname mapping: {e}")
                df['country_chinese'] = pd.NA
        else:
            logging.warning("'revenue country' column not found. Skipping country mapping.")
    except Exception as e:
        logging.error(f"Failed during country_id/country_chinese derivation: {e}")

    # Compute revenue and net revenue fields
    try:
        if ('revenue amt' in df.columns) and ('total cn amt' in df.columns):
            rev = pd.to_numeric(df['revenue amt'], errors='coerce').fillna(0)
            cn = pd.to_numeric(df['total cn amt'], errors='coerce').fillna(0)
            df['revenue_usd_hedge_rate'] = rev
            df['net_revenue_usd_agp'] = rev - cn
        elif 'revenue amt' in df.columns:
            rev = pd.to_numeric(df['revenue amt'], errors='coerce').fillna(0)
            df['revenue_usd_hedge_rate'] = rev
            df['net_revenue_usd_agp'] = rev
            logging.warning("'total cn amt' not found; net_revenue_usd_agp set equal to revenue amt.")
        else:
            logging.warning("'revenue amt' not found; revenue fields cannot be computed.")
    except Exception as e:
        logging.error(f"Failed to compute revenue fields for ACI: {e}")

    # --- Finalize column names (post-transform renames and schema enforcement) ---
    df = finalize_aci_columns(df)

    return df


# --- ACC+ACI Combined Processing ---

def _get_column_or_null(df: pd.DataFrame, column_name: str) -> pd.Series:
    """Get column from DataFrame or return NULL series if not found.
    
    Helper for building combined DataFrames where some columns may not exist.
    
    Args:
        df: Input DataFrame
        column_name: Name of column to retrieve
    
    Returns:
        Series containing column data, or Series of None if column doesn't exist
    """
    if column_name in df.columns:
        return df[column_name]
    return pd.Series([None] * len(df), index=df.index)


def _map_customer_name(sold_to_customer_series: pd.Series) -> pd.Series:
    """Map sold_to_customer to standardized customer names based on keywords.
    
    Args:
        sold_to_customer_series: Series containing sold_to_customer values
    
    Returns:
        Series with mapped customer names
    """
    def _classify_customer(value):
        if pd.isna(value):
            return 'Others'
        
        value_upper = str(value).upper()
        
        if 'WALMART' in value_upper:
            return 'Walmart'
        elif 'BEST BUY' in value_upper:
            return 'Best Buy'
        elif 'AMAZON' in value_upper:
            return 'Amazon'
        elif 'TARGET' in value_upper:
            return 'Target'
        elif 'STAPLES' in value_upper:
            return 'Staples'
        elif 'COSTCO' in value_upper:
            return 'Costco'
        elif 'SAMS' in value_upper:
            return "Sam's Club"
        else:
            return 'Others'
    
    return sold_to_customer_series.apply(_classify_customer)


def _map_branch2(branch_series: pd.Series, is_acc: bool = True) -> pd.Series:
    """Map branch to branch2 based on keywords.
    
    Args:
        branch_series: Series containing branch values
        is_acc: True for ACC data (return 'ACC'), False for ACI (apply mapping)
    
    Returns:
        Series with mapped branch2 values
    """
    if is_acc:
        return pd.Series(['ACC'] * len(branch_series), index=branch_series.index)
    
    def _classify_branch(value):
        if pd.isna(value):
            return 'Channel-US'
        
        value_upper = str(value).upper()
        
        if 'RETAIL' in value_upper:
            return 'ACI-Retail'
        elif 'CA' in value_upper:
            return 'CA'
        elif 'MX' in value_upper:
            return 'ACMX'
        elif 'SA' in value_upper:
            return 'Channel-SA'
        else:
            return 'Channel-US'
    
    return branch_series.apply(_classify_branch)


def _compute_shipping_fee(mapped_df: pd.DataFrame, engine, is_acc: bool = True) -> pd.DataFrame:
    """Compute shipping fee metrics for ACC or ACI data.
    
    Joins with branch_freight table and computes:
    - total_ship_fee_company
    - total_ship_fee_company_fxdiff
    - total_ship_fee_company_rmb (ACC only, ACI is NULL)
    
    For ACC: also joins exchange_rate_rmb and applies FX conversion
    For ACI: direct calculation without FX conversion
    
    Args:
        mapped_df: Input DataFrame with product_line_id, branch, period, all_qty
        engine: Database engine
        is_acc: True for ACC data (apply FX), False for ACI
    
    Returns:
        DataFrame with shipping fee columns computed
    """
    try:
        bf_df = read_and_clean_table('branch_freight', engine, lowercase=True)
        if bf_df is None or len(bf_df) == 0:
            logging.warning("branch_freight table empty; shipping fee metrics remain NULL.")
            return mapped_df
        
        bf_df = bf_df[['product_line', 'sales_team', 'period', 'freight']]
        
        # bf_df['sales_team']要Upper
        bf_df['sales_team'] = bf_df['sales_team'].astype(str).str.strip().str.upper()
        # mapped_df['branch']也要Upper
        mapped_df['branch'] = mapped_df['branch'].astype(str).str.strip().str.upper()

        # Merge branch_freight
        mapped_df = pd.merge(
            mapped_df,
            bf_df,
            left_on=['product_line_id', 'branch', 'period'],
            right_on=['product_line', 'sales_team', 'period'],
            how='left'
        )
        
        qty = pd.to_numeric(mapped_df.get('all_qty'), errors='coerce')
        freight = pd.to_numeric(mapped_df.get('freight'), errors='coerce')
        
        if is_acc:
            # ACC: apply FX conversion
            ex_df = read_and_clean_table('exchange_rate_rmb', engine, lowercase=True)
            if ex_df is not None and len(ex_df) > 0:
                ex_df = ex_df[['product_line', 'period', 'fx_rate', 'fx_rate_ex']]
                mapped_df = pd.merge(
                    mapped_df,
                    ex_df,
                    left_on=['product_line_id', 'period'],
                    right_on=['product_line', 'period'],
                    how='left'
                )
            else:
                mapped_df['fx_rate'] = pd.NA
                mapped_df['fx_rate_ex'] = pd.NA
            
            fx_rate = pd.to_numeric(mapped_df.get('fx_rate'), errors='coerce')
            fx_rate_ex = pd.to_numeric(mapped_df.get('fx_rate_ex'), errors='coerce')
            
            # total_ship_fee_company: -(freight * qty) / fx_rate
            ship_val = -(freight * qty) / fx_rate
            ship_val[(fx_rate.isna()) | (fx_rate == 0)] = pd.NA
            mapped_df['total_ship_fee_company'] = ship_val.round(15)
            
            # total_ship_fee_company_fxdiff: -(freight * qty) / fx_rate_ex
            ship_val_ex = -(freight * qty) / fx_rate_ex
            ship_val_ex[(fx_rate_ex.isna()) | (fx_rate_ex == 0)] = pd.NA
            mapped_df['total_ship_fee_company_fxdiff'] = ship_val_ex.round(15)
            
            # total_ship_fee_company_rmb: -(freight * qty) without FX conversion
            mapped_df['total_ship_fee_company_rmb'] = (-(freight * qty)).round(15)
            
            # Clean helper columns
            for col in ['product_line', 'sales_team', 'freight', 'fx_rate', 'fx_rate_ex']:
                if col in mapped_df.columns:
                    mapped_df.drop(columns=[col], inplace=True)
        else:
            # ACI: direct calculation without FX
            mapped_df['total_ship_fee_company'] = (-(freight * qty)).round(15)
            mapped_df['total_ship_fee_company_fxdiff'] = (-(freight * qty)).round(15)
            mapped_df['total_ship_fee_company_rmb'] = pd.Series([None] * len(mapped_df), index=mapped_df.index)
            
            for col in ['product_line', 'sales_team', 'freight']:
                if col in mapped_df.columns:
                    mapped_df.drop(columns=[col], inplace=True)
                    
    except Exception as e:
        logging.error(f"Failed computing shipping fee metrics: {e}")
    
    return mapped_df


def _compute_cost_period(acc_mapped: pd.DataFrame, aci_mapped: pd.DataFrame, engine) -> tuple:
    """Compute cost_period mapping for ACC and ACI data.
    
    - ACI: NR/NV/NB -> map via acc_cost_range, others -> "由新到舊"
    - ACC: NR/NV -> "由新到舊", NB -> map via acc_cost_range table
    
    Args:
        acc_mapped: ACC DataFrame
        aci_mapped: ACI DataFrame
        engine: Database engine
    
    Returns:
        Tuple of (acc_mapped, aci_mapped) with cost_period computed
    """
    try:
        cost_df = read_and_clean_table('acc_cost_range', engine, lowercase=True)
        cost_map = None
        if cost_df is not None and len(cost_df) > 0:
            if 'version' in cost_df.columns:
                cost_df['version'] = cost_df['version'].astype(str).str.strip()
            
            if {'pn', 'version', 'cost_range'}.issubset(set(cost_df.columns)):
                cost_map = {
                    (str(row['pn']).strip(), str(row['version']).strip()): row['cost_range']
                    for _, row in cost_df[['pn', 'version', 'cost_range']].iterrows()
                }
            else:
                logging.warning("acc_cost_range missing expected columns; cost_period mapping skipped.")
        else:
            logging.warning("acc_cost_range table empty; cost_period defaults will be applied.")

        # ACI: map NR, NV, NB from acc_cost_range, others default to "由新到舊"
        if len(aci_mapped) > 0:
            aci_mapped['cost_period'] = '由新到舊'
            if cost_map:
                is_target_product = aci_mapped.get('product_line_id', pd.Series(index=aci_mapped.index)).astype(str).str.upper().isin(['NR', 'NV', 'NB'])
                if is_target_product.any():
                    keys = list(zip(
                        aci_mapped.loc[is_target_product, 'part_number'].astype(str).str.strip(),
                        aci_mapped.loc[is_target_product, 'period'].astype(str).str.strip()
                    ))
                    mapped_vals = pd.Series([cost_map.get(k) for k in keys], index=aci_mapped.loc[is_target_product].index)
                    aci_mapped.loc[is_target_product, 'cost_period'] = mapped_vals.combine_first(aci_mapped.loc[is_target_product, 'cost_period'])

        # ACC: default "由新到舊", override for NB via mapping
        if len(acc_mapped) > 0:
            acc_mapped['cost_period'] = '由新到舊'
            if cost_map:
                is_nb = acc_mapped.get('product_line_id', pd.Series(index=acc_mapped.index)).astype(str).str.upper() == 'NB'
                if is_nb.any():
                    keys = list(zip(
                        acc_mapped.loc[is_nb, 'part_number'].astype(str).str.strip(),
                        acc_mapped.loc[is_nb, 'period'].astype(str).str.strip()
                    ))
                    mapped_vals = pd.Series([cost_map.get(k) for k in keys], index=acc_mapped.loc[is_nb].index)
                    acc_mapped.loc[is_nb, 'cost_period'] = mapped_vals.combine_first(acc_mapped.loc[is_nb, 'cost_period'])
    except Exception as e:
        logging.error(f"Failed computing cost_period mapping: {e}")
    
    return acc_mapped, aci_mapped


def _compute_funding(acc_mapped: pd.DataFrame, aci_mapped: pd.DataFrame, engine) -> tuple:
    """Compute funding for ACC and ACI data.
    
    Reads funding table and computes: funding_per_unit * all_qty
    
    Args:
        acc_mapped: ACC DataFrame
        aci_mapped: ACI DataFrame
        engine: Database engine
    
    Returns:
        Tuple of (acc_mapped, aci_mapped) with funding computed
    """
    try:
        fund_df = read_and_clean_table('funding', engine, lowercase=True)
        if fund_df is None or len(fund_df) == 0:
            logging.warning("funding table empty; funding will remain NULL.")
            return acc_mapped, aci_mapped
        
        if not {'pn', 'period', 'funding'}.issubset(set(fund_df.columns)):
            logging.warning("funding table missing expected columns; funding computation skipped.")
            return acc_mapped, aci_mapped
        
        fund_df = fund_df[['pn', 'period', 'funding']].copy()
        fund_df['pn'] = fund_df['pn'].astype(str).str.strip()
        fund_df['period'] = fund_df['period'].astype(str).str.strip()
        fund_map = {(row['pn'], row['period']): row['funding'] for _, row in fund_df.iterrows()}
        
        # ACC funding
        if len(acc_mapped) > 0:
            acc_keys = list(zip(
                acc_mapped.get('part_number', pd.Series(index=acc_mapped.index)).astype(str).str.strip(),
                acc_mapped.get('period', pd.Series(index=acc_mapped.index)).astype(str).str.strip()
            ))
            per_unit = pd.Series([fund_map.get(k) for k in acc_keys], index=acc_mapped.index)
            qty = pd.to_numeric(acc_mapped.get('all_qty'), errors='coerce')
            acc_mapped['funding'] = (pd.to_numeric(per_unit, errors='coerce') * qty).round(15)
        
        # ACI funding
        if len(aci_mapped) > 0:
            aci_keys = list(zip(
                aci_mapped.get('part_number', pd.Series(index=aci_mapped.index)).astype(str).str.strip(),
                aci_mapped.get('period', pd.Series(index=aci_mapped.index)).astype(str).str.strip()
            ))
            per_unit = pd.Series([fund_map.get(k) for k in aci_keys], index=aci_mapped.index)
            qty = pd.to_numeric(aci_mapped.get('all_qty'), errors='coerce')
            aci_mapped['funding'] = (pd.to_numeric(per_unit, errors='coerce') * qty).round(15)
    
    except Exception as e:
        logging.error(f"Failed computing funding: {e}")
    
    return acc_mapped, aci_mapped


def build_acc_aci_combined(engine):
    """Build combined ACC+ACI DataFrame for accaci_local_data_weekly table.
    
    Reads acc_localrawdata_weekly and aci_localrawdata_weekly tables,
    applies field mapping, computes derived fields, and returns combined DataFrame.
    
    Computed fields:
    - total_ship_fee_company and total_ship_fee_company_fxdiff (via branch_freight)
    - cost_period (via acc_cost_range for ACC NB only)
    - funding (via funding table)
    
    Args:
        engine: Database engine
    
    Returns:
        Combined DataFrame with COMBINED_OUTPUT_COLUMNS schema
    """
    logging.info("Building ACC+ACI combined DataFrame...")
    
    # Read source tables
    acc_df = read_table_to_df('acc_localrawdata_weekly', engine)
    aci_df = read_table_to_df('aci_localrawdata_weekly', engine)
    
    # Build ACC mapped frame
    if acc_df is None or len(acc_df) == 0:
        acc_mapped = pd.DataFrame(columns=COMBINED_OUTPUT_COLUMNS)
    else:
        acc_mapped = pd.DataFrame({
            'source': pd.Series(['ACCRAW'] * len(acc_df), index=acc_df.index),
            'product_line_id': _get_column_or_null(acc_df, 'product_line_id'),
            'year': _get_column_or_null(acc_df, 'year'),
            'quarter': _get_column_or_null(acc_df, 'quarter'),
            'month': _get_column_or_null(acc_df, 'month'),
            'period': _get_column_or_null(acc_df, 'period'),
            'sold_to_customer': pd.Series([None] * len(acc_df), index=acc_df.index),
            'customer': pd.Series([None] * len(acc_df), index=acc_df.index),
            'order_type': _get_column_or_null(acc_df, 'order_type'),
            'business_type': _get_column_or_null(acc_df, 'business_type'),
            'product_type': _get_column_or_null(acc_df, 'product_type'),
            'distribution': _get_column_or_null(acc_df, 'distribution'),
            'distribution_type': _get_column_or_null(acc_df, 'distribution_type'),
            'territory': pd.Series(['ACC'] * len(acc_df), index=acc_df.index),
            'territory2': pd.Series(['ACC'] * len(acc_df), index=acc_df.index),
            'branch': pd.Series(['ACC'] * len(acc_df), index=acc_df.index),
            'branch2': pd.Series(['ACC'] * len(acc_df), index=acc_df.index),
            'country_id': pd.Series(['CN'] * len(acc_df), index=acc_df.index),
            'country_chinese': pd.Series(['中國大陸'] * len(acc_df), index=acc_df.index),
            'ou': _get_column_or_null(acc_df, 'ou'),
            'warranty': _get_column_or_null(acc_df, 'warranty'),
            'csid': _get_column_or_null(acc_df, 'csid'),
            'part_number': _get_column_or_null(acc_df, 'part_number'),
            'description': _get_column_or_null(acc_df, 'description'),
            'all_qty': _get_column_or_null(acc_df, 'all_qty'),
            'revenue_usd_hedge_rate': _get_column_or_null(acc_df, 'revenue_usd_hedge_rate'),
            'revenue_fxdiff': _get_column_or_null(acc_df, 'revenue_fxdiff'),
            'net_revenue_usd_agp': _get_column_or_null(acc_df, 'net_revenue_usd_agp'),
            'net_revenue_fxdiff': _get_column_or_null(acc_df, 'net_revenue_fxdiff'),
            'local_cogs_amount': _get_column_or_null(acc_df, 'local_cogs_amount'),
            'local_cogs_amount_fxdiff': _get_column_or_null(acc_df, 'local_cogs_amount_fxdiff'),
            'credit_note_hedge_rate': _get_column_or_null(acc_df, 'credit_note_hedge_rate'),
            'credit_note_fxdiff': _get_column_or_null(acc_df, 'credit_note_fxdiff'),
            'total_ship_fee_company': pd.Series([None] * len(acc_df), index=acc_df.index),
            'total_ship_fee_company_fxdiff': pd.Series([None] * len(acc_df), index=acc_df.index),
            'total_ship_fee_company_rmb': pd.Series([None] * len(acc_df), index=acc_df.index),
            'cost_period': pd.Series([None] * len(acc_df), index=acc_df.index),
            'funding': pd.Series([None] * len(acc_df), index=acc_df.index),
            'csc_ratio': pd.Series([None] * len(acc_df), index=acc_df.index),
            'csc_amt': pd.Series([None] * len(acc_df), index=acc_df.index),
            'revenue_rmb': _get_column_or_null(acc_df, 'revenue_rmb'),
            'net_revenue_rmb': _get_column_or_null(acc_df, 'net_revenue_rmb'),
            'local_cogs_amount_rmb': _get_column_or_null(acc_df, 'local_cogs_amount_rmb'),
            'credit_note_rmb': _get_column_or_null(acc_df, 'credit_note_rmb'),
        })
    
    # Build ACI mapped frame
    if aci_df is None or len(aci_df) == 0:
        aci_mapped = pd.DataFrame(columns=COMBINED_OUTPUT_COLUMNS)
    else:
        aci_mapped = pd.DataFrame({
            'source': pd.Series(['ACIRAW'] * len(aci_df), index=aci_df.index),
            'product_line_id': _get_column_or_null(aci_df, 'product_line_id'),
            'year': _get_column_or_null(aci_df, 'year'),
            'quarter': _get_column_or_null(aci_df, 'quarter'),
            'month': _get_column_or_null(aci_df, 'month'),
            'period': _get_column_or_null(aci_df, 'period'),
            'sold_to_customer': _get_column_or_null(aci_df, 'sold_to_customer'),
            'customer': _map_customer_name(_get_column_or_null(aci_df, 'sold_to_customer')),
            'order_type': _get_column_or_null(aci_df, 'order_type'),
            'business_type': _get_column_or_null(aci_df, 'business_type'),
            'product_type': pd.Series([None] * len(aci_df), index=aci_df.index),
            'distribution': pd.Series([None] * len(aci_df), index=aci_df.index),
            'distribution_type': pd.Series([None] * len(aci_df), index=aci_df.index),
            'territory': _get_column_or_null(aci_df, 'territory'),
            'territory2': _get_column_or_null(aci_df, 'territory2'),
            'branch': _get_column_or_null(aci_df, 'branch'),
            'branch2': _map_branch2(_get_column_or_null(aci_df, 'branch'), is_acc=False),
            'country_id': _get_column_or_null(aci_df, 'country_id'),
            'country_chinese': _get_column_or_null(aci_df, 'country_chinese'),
            'ou': pd.Series([None] * len(aci_df), index=aci_df.index),
            'warranty': pd.Series([None] * len(aci_df), index=aci_df.index),
            'csid': pd.Series([None] * len(aci_df), index=aci_df.index),
            'part_number': _get_column_or_null(aci_df, 'part_number'),
            'description': pd.Series([None] * len(aci_df), index=aci_df.index),
            'all_qty': _get_column_or_null(aci_df, 'all_qty'),
            'revenue_usd_hedge_rate': _get_column_or_null(aci_df, 'revenue_usd_hedge_rate'),
            'revenue_fxdiff': _get_column_or_null(aci_df, 'revenue_usd_hedge_rate'),
            #'revenue_fxdiff': pd.Series([None] * len(aci_df), index=aci_df.index),
            'net_revenue_usd_agp': _get_column_or_null(aci_df, 'net_revenue_usd_agp'),
            'net_revenue_fxdiff': _get_column_or_null(aci_df, 'net_revenue_usd_agp'),
            #'net_revenue_fxdiff': pd.Series([None] * len(aci_df), index=aci_df.index),
            'local_cogs_amount': _get_column_or_null(aci_df, 'local_cogs_amount'),
            'local_cogs_amount_fxdiff': _get_column_or_null(aci_df, 'local_cogs_amount'),
            #'local_cogs_amount_fxdiff': pd.Series([None] * len(aci_df), index=aci_df.index),
            'credit_note_hedge_rate': _get_column_or_null(aci_df, 'credit_note_hedge_rate'),
            'credit_note_fxdiff': _get_column_or_null(aci_df, 'credit_note_hedge_rate'),
            #'credit_note_fxdiff': pd.Series([None] * len(aci_df), index=aci_df.index),
            'total_ship_fee_company': pd.Series([None] * len(aci_df), index=aci_df.index),
            'total_ship_fee_company_fxdiff': pd.Series([None] * len(aci_df), index=aci_df.index),
            'total_ship_fee_company_rmb': pd.Series([None] * len(aci_df), index=aci_df.index),
            'cost_period': pd.Series([None] * len(aci_df), index=aci_df.index),
            'funding': pd.Series([None] * len(aci_df), index=aci_df.index),
            'csc_ratio': _get_column_or_null(aci_df, 'csc_ratio'),
            'csc_amt': _get_column_or_null(aci_df, 'csc_amt'),
            'revenue_rmb': pd.Series([None] * len(aci_df), index=aci_df.index),
            'net_revenue_rmb': pd.Series([None] * len(aci_df), index=aci_df.index),
            'local_cogs_amount_rmb': pd.Series([None] * len(aci_df), index=aci_df.index),
            'credit_note_rmb': pd.Series([None] * len(aci_df), index=aci_df.index),
        })
    
    # Compute shipping fee metrics
    if len(acc_mapped) > 0:
        acc_mapped = _compute_shipping_fee(acc_mapped, engine, is_acc=True)
    if len(aci_mapped) > 0:
        aci_mapped = _compute_shipping_fee(aci_mapped, engine, is_acc=False)
    
    # Compute cost_period
    acc_mapped, aci_mapped = _compute_cost_period(acc_mapped, aci_mapped, engine)
    
    # Compute funding
    acc_mapped, aci_mapped = _compute_funding(acc_mapped, aci_mapped, engine)
    
    # Ensure all monetary columns maintain decimal(38,15) precision
    monetary_columns = [
        'revenue_usd_hedge_rate', 'revenue_fxdiff',
        'net_revenue_usd_agp', 'net_revenue_fxdiff',
        'local_cogs_amount', 'local_cogs_amount_fxdiff',
        'credit_note_hedge_rate', 'credit_note_fxdiff',
        'total_ship_fee_company', 'total_ship_fee_company_fxdiff', 'total_ship_fee_company_rmb',
        'funding',
        'csc_ratio', 'csc_amt',
        'revenue_rmb', 'net_revenue_rmb',
        'local_cogs_amount_rmb', 'credit_note_rmb'
    ]
    
    for col in monetary_columns:
        if col in acc_mapped.columns:
            acc_mapped[col] = pd.to_numeric(acc_mapped[col], errors='coerce').round(15)
        if col in aci_mapped.columns:
            aci_mapped[col] = pd.to_numeric(aci_mapped[col], errors='coerce').round(15)
    
    # Concatenate and enforce final column order
    combined_df = pd.concat(
        [acc_mapped[COMBINED_OUTPUT_COLUMNS], aci_mapped[COMBINED_OUTPUT_COLUMNS]], 
        ignore_index=True
    )
    
    logging.info(f"Combined ACC+ACI shape: {combined_df.shape}")
    return combined_df
