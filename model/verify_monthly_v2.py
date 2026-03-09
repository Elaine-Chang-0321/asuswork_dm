
import pandas as pd
import logging
import sys
import os

# Add parent directory to path to import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from acc_aci_processor_monthly import process_aci_data_monthly, ACI_OUTPUT_COLUMNS

# Configure logging
logging.basicConfig(level=logging.INFO)

def test_aci_monthly_process():
    print("Testing process_aci_data_monthly...")

    # Mock Data matching User's monthly format
    data = {
        'Region': ['NA', 'SA', 'EU', 'NA'],
        'Revenue Country': ['USA', 'Canada', 'Germany', 'USA'],
        'type': ['Sales', 'Return', 'Sales', 'Other'],
        'Business Type': ['Direct Retail', 'Retail', 'Channel', 'Channel'],
        'Product Line': ['NB', 'NR', 'NV', 'NB'],
        'Period': ['202301', '202302', '202303', '202304'],
        'Sold to Customer': ['Walmart', 'Best Buy', 'Unknown', 'Amazon'],
        'Item': ['A-M', 'B-M', 'C', 'D-M'], # C missing -M, should be filtered? User said "Keep only if contains -M"
        'Item Description': ['Desc A', 'Desc B', 'Desc C', 'Desc D'],
        'Quantity': [10, -1, 5, 20],
        'Sales Amount': [100.0, -10.0, 50.0, 200.0],
        'Material Cost Amt': [80.0, -8.0, 40.0, 160.0],
        'AR Trx Type': ['Invoice', 'Credit Memo', 'Invoice', 'Invoice'] # Optional for biz logic
    }
    
    df = pd.DataFrame(data)
    
    # Mock engine (sqlite for testing or just None if not used heavily)
    # process_aci_data_monthly uses engine for:
    # - aci_countryname (for country_chinese) -> mocking read_and_clean_table?
    # - It calls read_and_clean_table. We need to mock that or the function will fail/warn.
    # For this syntax check, we can let it fail DB calls (it has try-except blocks) and check if logical transformations work.
    
    # Run processor
    try:
        processed_df = process_aci_data_monthly(df, None)
        
        print("\nPrcoessed DataFrame Shape:", processed_df.shape)
        print("\nColumns:", processed_df.columns.tolist())
        print("\nHead:")
        print(processed_df.head().to_string())
        
        # Check specific logic
        # 1. Filter Item: C should be gone (no -M)
        if 'C' in processed_df['part_number'].values:
            print("ERROR: Item filtering failed (C with no -M remained)")
        else:
            print("SUCCESS: Item filtering passed")
            
        # 2. Check Renames
        if 'part_number' in processed_df.columns and 'revenue_usd_hedge_rate' in processed_df.columns:
             print("SUCCESS: Column renames passed")
        else:
             print("ERROR: Column renames failed")
             
        # 3. Check Calculated Fields
        # COGS should be negative of input 80 -> -80
        # Wait, user input mock 80. Logic says: -df['Material Cost Amt'].
        # Input 80 -> Result -80.
        row0 = processed_df.iloc[0]
        if row0['local_cogs_amount'] == -80.0:
            print("SUCCESS: COGS sign inversion passed")
        else:
            print(f"ERROR: COGS Logic failed. Expected -80.0, got {row0['local_cogs_amount']}")
            
        # 4. Check Output Columns match exactly ACI_OUTPUT_COLUMNS
        if list(processed_df.columns) == ACI_OUTPUT_COLUMNS:
            print("SUCCESS: Output columns match ACI_OUTPUT_COLUMNS")
        else:
            print("WARNING: Output columns mismatch")
            print("Expected:", ACI_OUTPUT_COLUMNS)
            print("Actual:  ", list(processed_df.columns))

    except Exception as e:
        print(f"FAILED: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_aci_monthly_process()
