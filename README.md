# Expense Tracker with Database Integration

A comprehensive expense tracking system that processes transaction data from multiple bank statements and stores it in a PostgreSQL database for analysis and reporting.

## Features

- **Multi-bank Support**: Process transactions from CIBC, RBC, Scotiabank, and American Express
- **Database Storage**: Store all transactions in PostgreSQL for persistent data management
- **Interactive Menu**: Choose between viewing existing data or uploading new transactions
- **Smart Data Management**: Automatically detect and handle duplicate month/year data
- **Comprehensive Reporting**: Generate pie charts, bar charts, and threshold violation reports
- **Budget Tracking**: Compare actual spending against budgeted amounts
- **Transaction Thresholds**: Identify transactions that exceed category-specific limits

## Prerequisites

- Python 3.7+
- PostgreSQL database server
- Required Python packages (see requirements.txt)

## Installation

1. **Install Python dependencies:**
   ```bash
   pip3 install -r requirements.txt
   ```

2. **Set up PostgreSQL database:**
   - Create a database named `expense_express`
   - Note your database credentials (host, port, username, password)
   - On macOS, the default username is typically your system username (e.g., `joshuafiscalini`)

3. **Configure database connection:**
   ```bash
   python3 setup_database.py
   ```
   This will guide you through setting up the database connection and creating the necessary tables.

## Usage

### Main Application

Run the main application:
```bash
python3 main.py
```

You'll be presented with two options:

#### Option 1: View Reports for Existing Data
- Select a month and year to view reports
- System will show available data in the database
- Generate charts and reports for the selected period

#### Option 2: Upload New Transactions
- Load transaction data from CSV files in the `data/` folder
- Automatically detect month/year of the data
- Option to overwrite existing data for the same period
- Generate reports after upload

### Data Structure

The system expects CSV files in the `data/` folder with the following naming convention:
- `cibcCostcoStmt.csv` - CIBC Costco credit card
- `cibcChqStmt.csv` - CIBC chequing account
- `cibcIndvlStmt.csv` - CIBC individual account
- `cibc67781Stmt.csv` - CIBC line of credit
- `scotiaStmt.csv` - Scotiabank account
- `AMEXGoldStmt.csv` - American Express Gold card
- `AMEXCobaltStmt.csv` - American Express Cobalt card
- `rbcStmt.csv` - RBC account

### Configuration Files

- `config/budget.json` - Budget limits for each category
- `config/txnThreshold.json` - Maximum transaction amounts per category
- `config/categories.json` - Transaction categorization rules

## Database Schema

The `txn` table contains the following columns:
- `id` - Primary key (auto-increment)
- `date` - Transaction date
- `amount` - Transaction amount (decimal)
- `description` - Transaction description
- `account` - Source account
- `category` - Categorized transaction type
- `created_at` - Record creation timestamp
- `updated_at` - Record update timestamp

## Reports Generated

1. **Pie Chart**: Spending breakdown by category
2. **Bar Chart**: Budget vs. actual spending comparison
3. **Summary Table**: Category-wise spending totals
4. **Threshold Analysis**: Transactions exceeding category limits
5. **Uncategorized Transactions**: Transactions that couldn't be automatically categorized

## File Structure

```
spendTracker/
├── main.py                 # Main application
├── setup_database.py       # Database setup script
├── requirements.txt        # Python dependencies
├── README.md              # This file
├── config/
│   ├── budget.json        # Budget configuration
│   ├── categories.json    # Categorization rules
│   └── txnThreshold.json  # Transaction thresholds
├── data/                  # CSV transaction files
│   ├── cibcCostcoStmt.csv
│   ├── cibcChqStmt.csv
│   └── ...
└── etl/                   # Data processing modules
    ├── database.py        # Database operations
    ├── threshold_checker.py # Threshold analysis
    ├── categorization.py  # Transaction categorization
    ├── amex_etl.py        # AMEX data processing
    ├── cibc_etl.py        # CIBC data processing
    ├── rbc_etl.py         # RBC data processing
    └── scotia_etl.py      # Scotiabank data processing
```

## Troubleshooting

### Database Connection Issues
- Ensure PostgreSQL is running
- Verify database credentials
- Check if the `expense_express` database exists
- Run `setup_database.py` to test connection

### Missing Dependencies
- Install required packages: `pip3 install -r requirements.txt`
- Ensure you have PostgreSQL client libraries installed

### Data Processing Issues
- Verify CSV files are in the correct format
- Check that all required files are present in the `data/` folder
- Ensure date formats are consistent across files

## Environment Variables

You can set the following environment variable to avoid entering the database password:
```bash
export POSTGRES_PASSWORD="your_password_here"
```

## Contributing

To add support for new banks or modify categorization rules:
1. Create a new ETL module in the `etl/` folder
2. Update the categorization logic in `etl/categorization.py`
3. Modify `main.py` to include the new data source
4. Update this README with new file requirements
