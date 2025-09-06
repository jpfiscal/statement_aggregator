import json
import os

#load category keywords from categorizations JSON in config folder
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "categories.json")
with open(CONFIG_PATH, "r") as f:
    CATEGORY_KEYWORDS = json.load(f)

def categorize_transaction(description):
    desc_upper = description.upper()
    for category, keywords in CATEGORY_KEYWORDS.items():
        for keyword in keywords:
            if keyword.upper() in desc_upper:
                return category
    return "Uncategorized"

def get_uncategorized_descriptions(df, description_col="description"):
    # Returns a sorted list of unique transaction descriptions that were not categorized.
    if "category" not in df.columns:
        raise ValueError("DataFrame must have a 'category' column")

    uncategorized_df = df[df["category"] == "Uncategorized"]
    return sorted(uncategorized_df[description_col].unique())