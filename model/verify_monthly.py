
import sys
import os

# Add the project root to sys.path
sys.path.append(r'd:\Project\aws_dbt\gp\local_excel_import')

try:
    import acc_aci_processor_monthly as monthly
    print("Module import successful.")
    
    expected_funcs = [
        'process_acc_data_monthly',
        'process_aci_data_monthly',
        'build_acc_aci_combined_monthly'
    ]
    
    for func in expected_funcs:
        if hasattr(monthly, func):
            print(f"Function '{func}' found.")
        else:
            print(f"ERROR: Function '{func}' NOT found.")
            
except Exception as e:
    print(f"Import failed: {e}")
