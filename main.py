import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, date
import calendar
from sqlalchemy import text

from etl.amex_etl import process_amex_statement
from etl.rbc_etl import process_rbc_statement
from etl.scotia_etl import process_scotia_statement
from etl.cibc_etl import process_cibc_statement
from etl.categorization import get_uncategorized_descriptions
from etl.threshold_checker import analyze_threshold_violations
from etl.database import ExpenseDatabase, save_transactions_to_db
from etl.filter_negs import filter_cr

# Set pandas display to show all rows and columns
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

def load_and_process_data():
    """Load and process all transaction data from CSV files."""
    print("Loading transaction data from CSV files...")
    
    ## read in all data sets from each txn
    costco_df = process_cibc_statement('data/cibcCostcoStmt.csv')
    chq_df = process_cibc_statement('data/cibcChqStmt.csv')
    indv_df = process_cibc_statement('data/cibcIndvlStmt.csv')
    loc_df = process_cibc_statement('data/cibc67781Stmt.csv')
    scotia_df = process_scotia_statement('data/scotiaStmt.csv')
    amex_gold_df = process_amex_statement('data/AMEXGoldStmt.csv')
    amex_cobalt_df = process_amex_statement('data/AMEXCobaltStmt.csv')
    rbc_df = process_rbc_statement('data/rbcStmt.csv')

    ##Combine all account specific dfs into one df
    combined_df = pd.concat([costco_df, chq_df, indv_df, loc_df, scotia_df, amex_gold_df, amex_cobalt_df, rbc_df], ignore_index=True)
    combined_df["date"] = pd.to_datetime(combined_df["date"])
    combined_df = combined_df.sort_values(by="date", ascending=True).reset_index(drop=True)
    
    # Filter out negative values and NaN amounts (credits, refunds, etc.)
    combined_df = filter_cr(combined_df)
    
    return combined_df

def check_month_year_exists(db, month, year):
    """Check if data exists for the given month/year combination."""
    start_date = date(year, month, 1)
    if month == 12:
        end_date = date(year + 1, 1, 1)
    else:
        end_date = date(year, month + 1, 1)
    
    existing_data = db.get_transactions(
        start_date=start_date.strftime('%Y-%m-%d'),
        end_date=end_date.strftime('%Y-%m-%d')
    )
    
    return not existing_data.empty, len(existing_data)

def clear_month_year_data(db, month, year):
    """Clear data for the given month/year combination."""
    start_date = date(year, month, 1)
    if month == 12:
        end_date = date(year + 1, 1, 1)
    else:
        end_date = date(year, month + 1, 1)
    
    try:
        with db.engine.connect() as conn:
            conn.execute(text(f"""
                DELETE FROM txn 
                WHERE dt >= '{start_date}' AND dt < '{end_date}'
            """))
            conn.commit()
        print(f"Cleared existing data for {calendar.month_name[month]} {year}")
        return True
    except Exception as e:
        print(f"Error clearing data: {e}")
        return False

def generate_reports_and_graphs(combined_df, month=None, year=None):
    """Generate reports and graphs for the given data."""
    # Filter data by month/year if specified
    if month is not None and year is not None:
        start_date = date(year, month, 1)
        if month == 12:
            end_date = date(year + 1, 1, 1)
        else:
            end_date = date(year, month + 1, 1)
        
        combined_df = combined_df[
            (combined_df['date'] >= pd.Timestamp(start_date)) & 
            (combined_df['date'] < pd.Timestamp(end_date))
        ]
        
        if combined_df.empty:
            print(f"No data found for {calendar.month_name[month]} {year}")
            return
        
        print(f"\nGenerating reports for {calendar.month_name[month]} {year}")
        print(f"Found {len(combined_df)} transactions")
    
    #Read budget data from budget.json config file
    budget_df = pd.read_json("config/budget.json")

    ##Summarize all transaction amounts by category
    monthly_summary = combined_df.groupby(["category"])["amount"].sum().reset_index()

    filtered_df = monthly_summary[monthly_summary["category"] != "Uncategorized"]

    # Calculate total spending
    total_amount = filtered_df["amount"].sum()

    total_row = pd.DataFrame({"category": ["Total"], "amount": [total_amount]})

    monthly_summary_with_total = pd.concat([filtered_df, total_row], ignore_index=True)

    ##Plot the data
    plt.figure(figsize=(10, 8))
    plt.pie(filtered_df["amount"], labels=filtered_df["category"], autopct='%1.1f%%', startangle=90)
    title = "Spending by Category"
    if month and year:
        title += f" - {calendar.month_name[month]} {year}"
    plt.title(title)
    plt.axis('equal')
    plt.show()

    #merge budget data with filtered data
    bar_data = filtered_df.merge(budget_df, on="category", how="left").fillna(0)

    # Sort by actual amount descending
    bar_data = bar_data.sort_values(by="amount", ascending=False)

    #Plot bar chart
    plt.figure(figsize=(12,6))
    x= np.arange(len(bar_data["category"]))
    width = 0.35

    plt.bar(x-width/2, bar_data["amount"], width, label="Actual Spend ")
    plt.bar(x + width/2, bar_data["budget"], width/2, label='Budget', color="red")

    plt.xticks(x, bar_data["category"], rotation=45, ha='right')
    plt.ylabel("Amount ($)")
    title = "Budget vs. Actual Spending by Category (in $)"
    if month and year:
        title += f" - {calendar.month_name[month]} {year}"
    plt.title(title)
    plt.legend()
    plt.tight_layout()
    plt.show()

    ## Print Area
    print(monthly_summary_with_total)
    print("Total Spent: " + str(total_amount))
    print("Uncategorized Transactions:")
    print(combined_df[combined_df["category"] == "Uncategorized"][["account","description"]])

    # Check for transactions exceeding category thresholds
    print("\n" + "="*80)
    print("TRANSACTION THRESHOLD ANALYSIS")
    print("="*80)
    exceeded_transactions = analyze_threshold_violations(combined_df)

def main():
    """Main function with interactive menu."""
    print("="*60)
    print("EXPENSE TRACKER - DATABASE MANAGEMENT SYSTEM")
    print("="*60)
    print("1. View reports and graphs for existing data")
    print("2. Upload new transactions and generate reports")
    print("="*60)
    
    while True:
        choice = input("Please select an option (1 or 2): ").strip()
        
        if choice == "1":
            # Option 1: View existing data
            print("\n" + "="*60)
            print("VIEWING EXISTING DATA")
            print("="*60)
            
            # Initialize database connection
            db = ExpenseDatabase()
            if not db.connect():
                print("Failed to connect to database. Please check your PostgreSQL connection.")
                return
            
            # Get available months/years from database
            stats = db.get_summary_stats()
            if not stats or stats['total_transactions'] == 0:
                print("No data found in database. Please upload transactions first.")
                db.close()
                return
            
            print(f"Database contains {stats['total_transactions']} transactions")
            print(f"Date range: {stats['date_range']['start']} to {stats['date_range']['end']}")
            
            # Prompt for month and year
            while True:
                try:
                    month = int(input("Enter month number (1-12): "))
                    if month < 1 or month > 12:
                        print("Invalid month. Please enter a number between 1 and 12.")
                        continue
                    
                    year = int(input("Enter year (e.g., 2025): "))
                    if year < 1900 or year > 2100:
                        print("Invalid year. Please enter a reasonable year.")
                        continue
                    
                    # Check if data exists for this month/year
                    exists, count = check_month_year_exists(db, month, year)
                    
                    if not exists:
                        print(f"No data found for {calendar.month_name[month]} {year}")
                        print("Available data in database:")
                        # Show available months/years
                        all_data = db.get_transactions()
                        if not all_data.empty:
                            all_data['month'] = pd.to_datetime(all_data['date']).dt.month
                            all_data['year'] = pd.to_datetime(all_data['date']).dt.year
                            available = all_data.groupby(['year', 'month']).size().reset_index(name='count')
                            for _, row in available.iterrows():
                                print(f"  {calendar.month_name[row['month']]} {row['year']}: {row['count']} transactions")
                        
                        retry = input("Would you like to try another month/year? (y/n): ").lower()
                        if retry != 'y':
                            db.close()
                            return
                        continue
                    
                    print(f"Found {count} transactions for {calendar.month_name[month]} {year}")
                    break
                    
                except ValueError:
                    print("Invalid input. Please enter valid numbers.")
                    continue
            
            # Get data for the specified month/year
            start_date = date(year, month, 1)
            if month == 12:
                end_date = date(year + 1, 1, 1)
            else:
                end_date = date(year, month + 1, 1)
            
            combined_df = db.get_transactions(
                start_date=start_date.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d')
            )
            
            # Rename columns to match expected format and convert date column back to datetime for processing
            combined_df = combined_df.rename(columns={
                'dt': 'date',
                'amt': 'amount',
                'desc': 'description'
            })
            combined_df['date'] = pd.to_datetime(combined_df['date'])
            
            db.close()
            
            # Generate reports
            generate_reports_and_graphs(combined_df, month, year)
            break
            
        elif choice == "2":
            # Option 2: Upload new data
            print("\n" + "="*60)
            print("UPLOADING NEW TRANSACTIONS")
            print("="*60)
            
            # Load and process data from CSV files
            combined_df = load_and_process_data()
            
            if combined_df.empty:
                print("No data found in CSV files. Please check your data folder.")
                return
            
            # Analyze the date range in the data
            date_range = combined_df['date'].dt.to_period('M').value_counts().sort_index()
            unique_months = len(date_range)
            
            if unique_months > 1:
                print(f"⚠️  WARNING: Upload data contains transactions from {unique_months} different months:")
                for period, count in date_range.items():
                    year = period.year
                    month = period.month
                    print(f"   - {calendar.month_name[month]} {year}: {count} transactions")
                
                # Get the earliest month/year
                earliest_period = date_range.index[0]
                data_year = earliest_period.year
                data_month = earliest_period.month
                
                print(f"\n📅 Only transactions from {calendar.month_name[data_month]} {data_year} will be uploaded.")
                print("📋 Transactions from other months will be ignored.")
                
                # Filter data to only include the earliest month
                start_date = date(data_year, data_month, 1)
                if data_month == 12:
                    end_date = date(data_year + 1, 1, 1)
                else:
                    end_date = date(data_year, data_month + 1, 1)
                
                original_count = len(combined_df)
                combined_df = combined_df[
                    (combined_df['date'] >= pd.Timestamp(start_date)) & 
                    (combined_df['date'] < pd.Timestamp(end_date))
                ]
                
                filtered_count = len(combined_df)
                ignored_count = original_count - filtered_count
                
                print(f"📊 Upload summary:")
                print(f"   - Total transactions in files: {original_count}")
                print(f"   - Transactions to upload: {filtered_count}")
                print(f"   - Transactions ignored: {ignored_count}")
                
                # Ask for confirmation
                proceed = input(f"\nDo you want to proceed with uploading {filtered_count} transactions from {calendar.month_name[data_month]} {data_year}? (y/n): ").lower()
                if proceed != 'y':
                    print("Upload cancelled.")
                    return
                
            else:
                # Single month data
                data_month = combined_df['date'].dt.month.iloc[0]
                data_year = combined_df['date'].dt.year.iloc[0]
                print(f"Data contains transactions from {calendar.month_name[data_month]} {data_year}")
            
            # Initialize database connection
            db = ExpenseDatabase()
            if not db.connect():
                print("Failed to connect to database. Please check your PostgreSQL connection.")
                return
            
            # Create table if it doesn't exist
            if not db.create_txn_table():
                print("Failed to create database table.")
                db.close()
                return
            
            # Check if data already exists for this month/year
            exists, count = check_month_year_exists(db, data_month, data_year)
            
            if exists:
                print(f"Data already exists for {calendar.month_name[data_month]} {data_year} ({count} transactions)")
                overwrite = input("Would you like to overwrite the existing data? (y/n): ").lower()
                
                if overwrite == 'y':
                    if not clear_month_year_data(db, data_month, data_year):
                        print("Failed to clear existing data.")
                        db.close()
                        return
                else:
                    print("Upload cancelled.")
                    db.close()
                    return
            
            # Upload data to database
            print("Uploading transactions to database...")
            if db.insert_transactions(combined_df):
                print(f"Successfully uploaded {len(combined_df)} transactions to database")
            else:
                print("Failed to upload transactions to database.")
                db.close()
                return
            
            db.close()
            
            # Generate reports and graphs
            generate_reports_and_graphs(combined_df, data_month, data_year)
            break
            
        else:
            print("Invalid choice. Please enter 1 or 2.")

if __name__ == "__main__":
    main()