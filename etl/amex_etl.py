import pandas as pd
from .categorization import categorize_transaction
from decimal import Decimal

# Extract:
  # date
  # amount
  # description
  # account
  # category (if API provides it, otherwise infer)

def process_amex_statement(file_path):
    df = pd.read_csv(file_path, skiprows=11)
    # with open(file_path) as f:
    #   for i in range(15):
    #     print(f"{i}: {f.readline().strip()}")
   
    # df.columns = df.columns.str.strip()
    # print(df.columns.tolist())

    # Adjust column names to match your actual AMEX CSV
    df = df.rename(columns={
        "Date": "date",
        "Amount": "amount",
        "Description": "description"
    })

    df = df[["date", "amount", "description"]]
    if "Gold" in file_path:
        df["account"] = "AMEX Gold"
    else:
        df["account"] = "AMEX Cobalt"

    df = df[["date", "amount", "description", "account"]]

    # Create and populate a categorization field in the df
    df["category"] = df["description"].apply(categorize_transaction)

    # Remove $ sign from value and cast as number
    df["amount"] = df["amount"].replace('[\$,]', '', regex=True).astype(float)

    # Transform date format from DD MMM YYYY to YYYY-MM-DD
    df["date"] = pd.to_datetime(df["date"], format="%d %b %Y").dt.strftime("%Y-%m-%d")

    return df
