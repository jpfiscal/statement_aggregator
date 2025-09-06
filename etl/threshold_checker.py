import pandas as pd
import json
from typing import List, Dict, Tuple

def load_threshold_config(config_path: str = "config/txnThreshold.json") -> Dict[str, float]:
    """
    Load transaction threshold configuration from JSON file.
    
    Args:
        config_path: Path to the threshold configuration JSON file
        
    Returns:
        Dictionary mapping category names to their threshold limits
    """
    try:
        with open(config_path, 'r') as file:
            threshold_data = json.load(file)
        
        # Convert list of dicts to category -> limit mapping
        threshold_dict = {item['category']: item['limit'] for item in threshold_data}
        return threshold_dict
    except FileNotFoundError:
        print(f"Warning: Threshold config file not found at {config_path}")
        return {}
    except json.JSONDecodeError:
        print(f"Warning: Invalid JSON in threshold config file {config_path}")
        return {}

def check_transaction_thresholds(transactions_df: pd.DataFrame, 
                                threshold_config: Dict[str, float]) -> pd.DataFrame:
    """
    Check transactions against their category thresholds.
    
    Args:
        transactions_df: DataFrame containing transactions with 'category' and 'amount' columns
        threshold_config: Dictionary mapping categories to their threshold limits
        
    Returns:
        DataFrame containing only transactions that exceed their category thresholds
    """
    # Create a copy to avoid modifying the original dataframe
    df = transactions_df.copy()
    
    # Add threshold column by mapping categories to their limits
    df['threshold'] = df['category'].map(threshold_config)
    
    # Filter transactions that exceed their threshold
    # Note: We use abs() since amounts might be negative (credits)
    exceeded_thresholds = df[
        (df['threshold'] > 0) &  # Only check categories with defined thresholds
        (df['amount'].abs() > df['threshold'])  # Transaction exceeds threshold
    ].copy()
    
    # Add a column showing how much the transaction exceeded the threshold
    exceeded_thresholds['excess_amount'] = exceeded_thresholds['amount'].abs() - exceeded_thresholds['threshold']
    
    return exceeded_thresholds

def print_threshold_violations(exceeded_transactions: pd.DataFrame) -> None:
    """
    Print a formatted report of transactions that exceeded their thresholds.
    
    Args:
        exceeded_transactions: DataFrame of transactions that exceeded thresholds
    """
    if exceeded_transactions.empty:
        print("\n✅ No transactions exceeded their category thresholds!")
        return
    
    print(f"\n⚠️  Found {len(exceeded_transactions)} transactions exceeding category thresholds:")
    print("=" * 80)
    
    # Sort by excess amount (most exceeded first)
    sorted_violations = exceeded_transactions.sort_values('excess_amount', ascending=False)
    
    for idx, row in sorted_violations.iterrows():
        print(f"📅 {row['date'].strftime('%Y-%m-%d')} | "
              f"💰 ${row['amount']:.2f} | "
              f"📂 {row['category']} | "
              f"🎯 Limit: ${row['threshold']:.2f} | "
              f"📈 Exceeded by: ${row['excess_amount']:.2f} | "
              f"🏦 {row['account']} | "
              f"📝 {row['description'][:50]}{'...' if len(row['description']) > 50 else ''}")
    
    print("=" * 80)
    
    # Summary statistics
    total_excess = exceeded_transactions['excess_amount'].sum()
    print(f"Total amount exceeding thresholds: ${total_excess:.2f}")
    
    # Breakdown by category
    print("\nBreakdown by category:")
    category_summary = exceeded_transactions.groupby('category').agg({
        'amount': 'count',
        'excess_amount': 'sum'
    }).rename(columns={'amount': 'transaction_count'})
    
    for category, data in category_summary.iterrows():
        print(f"  {category}: {data['transaction_count']} transactions, "
              f"${data['excess_amount']:.2f} total excess")

def analyze_threshold_violations(transactions_df: pd.DataFrame, 
                               config_path: str = "config/txnThreshold.json") -> pd.DataFrame:
    """
    Main function to analyze transaction threshold violations.
    
    Args:
        transactions_df: DataFrame containing all transactions
        config_path: Path to the threshold configuration file
        
    Returns:
        DataFrame of transactions that exceeded their thresholds
    """
    # Load threshold configuration
    threshold_config = load_threshold_config(config_path)
    
    if not threshold_config:
        print("No threshold configuration loaded. Cannot perform analysis.")
        return pd.DataFrame()
    
    # Check for violations
    exceeded_transactions = check_transaction_thresholds(transactions_df, threshold_config)
    
    # Print report
    print_threshold_violations(exceeded_transactions)
    
    return exceeded_transactions
