#!/usr/bin/env python3
"""
Database setup script for the expense tracker.
This script helps you test your PostgreSQL connection and set up the database.
"""

import os
from etl.database import ExpenseDatabase

def setup_database():
    """Set up and test the database connection."""
    print("="*60)
    print("EXPENSE TRACKER - DATABASE SETUP")
    print("="*60)
    
    # Get database configuration
    print("Please provide your PostgreSQL database configuration:")
    
    host = input("Host (default: localhost): ").strip() or "localhost"
    port = input("Port (default: 5432): ").strip() or "5432"
    database = input("Database name (default: expense_express): ").strip() or "expense_express"
    user = input("Username (default: joshuafiscalini): ").strip() or "joshuafiscalini"
    
    # Get password
    password = input("Password: ").strip()
    if not password:
        password = os.getenv('POSTGRES_PASSWORD')
        if not password:
            print("No password provided and POSTGRES_PASSWORD environment variable not set.")
            return False
    
    try:
        port = int(port)
    except ValueError:
        print("Invalid port number. Using default port 5432.")
        port = 5432
    
    # Initialize database connection
    print(f"\nAttempting to connect to PostgreSQL database '{database}' on {host}:{port}...")
    
    db = ExpenseDatabase(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )
    
    # Test connection
    if not db.connect():
        print("Failed to connect to database. Please check your configuration.")
        return False
    
    print("✅ Successfully connected to database!")
    
    # Create table
    print("Creating transaction table...")
    if not db.create_txn_table():
        print("Failed to create transaction table.")
        db.close()
        return False
    
    print("✅ Transaction table created successfully!")
    
    # Test basic operations
    print("Testing database operations...")
    
    # Get summary stats (should be empty initially)
    stats = db.get_summary_stats()
    print(f"Current database status:")
    print(f"  - Total transactions: {stats.get('total_transactions', 0)}")
    print(f"  - Total amount: ${stats.get('total_amount', 0):.2f}")
    
    db.close()
    
    print("\n✅ Database setup completed successfully!")
    print("You can now run 'python3 main.py' to use the expense tracker.")
    
    return True

if __name__ == "__main__":
    setup_database()
