import pandas as pd
from .categorization import categorize_transaction

# Extract:
  # date
  # amount
  # description
  # account
  # category (if API provides it, otherwise infer)

def process_cibc_statement(file_path):
    # Define custom headers
    column_names = ["date", "description", "amount", "CR", "accct_no"]
    
    df = pd.read_csv(file_path, header=None, names=column_names)
    
    # Designate account based on file name
    if "Chq" in file_path:
      df["account"] = "CIBC Chequing"
    elif "67781" in file_path:
      df["account"] = "CIBC LOC 67781"
    elif "Indvl" in file_path:
      df["account"] = "CIBC Individual"
    else:
      df["account"] = "CIBC Costco CC"

    df = df[["date", "amount", "description", "account"]]

    # Create and populate a categorization field in the df
    df["category"] = df["description"].apply(categorize_transaction)

    return df
