#!/usr/bin/env python3
"""
Example script demonstrating how to use the transaction threshold checker independently.
This shows how you can use the threshold checking functionality without running the full main.py
"""

import pandas as pd
from etl.threshold_checker import analyze_threshold_violations

def main():
    """
    Example of how to use the threshold checker with sample data or your own data.
    """
    print("Transaction Threshold Checker Example")
    print("=" * 50)
    
    # Example 1: Check with your existing combined data
    # (You would need to load your data here)
    print("Example 1: Check with your existing transaction data")
    print("(Uncomment the code below to use with your actual data)")
    
    # Uncomment these lines to use with your actual data:
    # from etl.amex_etl import process_amex_statement
    # from etl.rbc_etl import process_rbc_statement
    # from etl.scotia_etl import process_scotia_statement
    # from etl.cibc_etl import process_cibc_statement
    # 
    # # Load your data
    # costco_df = process_cibc_statement('data/cibcCostcoStmt.csv')
    # chq_df = process_cibc_statement('data/cibcChqStmt.csv')
    # # ... load other data sources
    # 
    # combined_df = pd.concat([costco_df, chq_df, ...], ignore_index=True)
    # combined_df["date"] = pd.to_datetime(combined_df["date"])
    # 
    # # Run threshold analysis
    # exceeded_transactions = analyze_threshold_violations(combined_df)
    
    # Example 2: Check with sample data
    print("\nExample 2: Check with sample data")
    
    # Create sample transaction data
    sample_data = {
        'date': ['2025-01-15', '2025-01-20', '2025-01-25', '2025-01-30'],
        'amount': [150.00, 200.00, 25.00, 500.00],
        'category': ['Dining', 'Shopping', 'Coffee', 'Travel'],
        'account': ['AMEX Gold', 'CIBC Costco CC', 'AMEX Cobalt', 'RBC'],
        'description': ['Sample Restaurant', 'Sample Store', 'Sample Coffee', 'Sample Travel']
    }
    
    sample_df = pd.DataFrame(sample_data)
    sample_df['date'] = pd.to_datetime(sample_df['date'])
    
    print("Sample transactions:")
    print(sample_df)
    
    print("\nRunning threshold analysis on sample data...")
    exceeded_transactions = analyze_threshold_violations(sample_df)
    
    if not exceeded_transactions.empty:
        print("\nYou can also access the exceeded transactions as a DataFrame:")
        print(exceeded_transactions[['date', 'amount', 'category', 'threshold', 'excess_amount']])

if __name__ == "__main__":
    main()
