import pandas as pd
from .categorization import categorize_transaction

# Extract:
  # date
  # amount
  # description
  # account
  # category (if API provides it, otherwise infer)

def process_scotia_statement(file_path):
    # Read CSV with proper headers
    df = pd.read_csv(file_path)
    
    # Rename columns to match expected format
    df = df.rename(columns={
        "Date": "date",
        "Description": "description", 
        "Amount": "amount"
    })
    
    # Select only the columns we need
    df = df[["date", "amount", "description"]]
    
    df["account"] = "Scotiabank"
    df = df[["date", "amount", "description", "account"]]

    # Create and populate a categorization field in the df
    df["category"] = df["description"].apply(categorize_transaction)

    # Transform date format from YYYY-MM-DD to YYYY-MM-DD (already in correct format)
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

    # Flip signs on the amounts
    df["amount"] = -1*df["amount"]

    return df
