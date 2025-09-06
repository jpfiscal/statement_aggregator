import pandas as pd
from .categorization import categorize_transaction

# Extract:
  # date
  # amount
  # description
  # account
  # category (if API provides it, otherwise infer)

import pandas as pd

def process_rbc_statement(file_path):
    # Read CSV with proper headers
    df = pd.read_csv(file_path)
    
    # Clean up currency formatting and combine debit and credit columns into a single amount column
    df['Debit'] = df['Debit'].replace('[\$,]', '', regex=True).astype(float).fillna(0)
    df['Credit'] = df['Credit'].replace('[\$,]', '', regex=True).astype(float).fillna(0)
    df['amount'] = df['Debit'] - df['Credit']
    
    # Rename columns to match expected format
    df = df.rename(columns={
        "Date": "date",
        "Description": "description"
    })

    df["account"] = "RBC"
    df = df[["date", "amount", "description", "account"]]

    # Create and populate a categorization field in the df
    df["category"] = df["description"].apply(categorize_transaction)

    # Transform date format from "Month DD, YYYY" to YYYY-MM-DD
    df["date"] = pd.to_datetime(df["date"], format="%B %d, %Y").dt.strftime("%Y-%m-%d")

    # Flip signs on the amounts
    df["amount"] = -1*df["amount"]

    return df

