import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import os
from typing import Optional, Dict, Any
import logging
from datetime import date

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ExpenseDatabase:
    """
    Database handler for the expense_express PostgreSQL database.
    Handles connections, table creation, and data insertion for transaction data.
    """
    
    def __init__(self, 
                 host: str = "localhost",
                 port: int = 5432,
                 database: str = "expense_express",
                 user: str = "joshuafiscalini",
                 password: str = None):
        """
        Initialize database connection parameters.
        
        Args:
            host: PostgreSQL host
            port: PostgreSQL port
            database: Database name
            user: Database username
            password: Database password (will try to get from environment if None)
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password or os.getenv('POSTGRES_PASSWORD')
        
        # Connection objects
        self.engine = None
        self.connection = None
        
    def connect(self) -> bool:
        """
        Establish connection to PostgreSQL database.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Create SQLAlchemy engine for pandas operations
            connection_string = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
            self.engine = create_engine(connection_string)
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            logger.info(f"Successfully connected to PostgreSQL database: {self.database}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return False
    
    def create_txn_table(self) -> bool:
        """
        Create the txn table if it doesn't exist.
        
        Returns:
            True if table created successfully, False otherwise
        """
        # First check if table already exists
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = 'txn'
                    );
                """))
                table_exists = result.scalar()
                
                if table_exists:
                    logger.info("Transaction table already exists, skipping creation")
                    return True
                
                # If table doesn't exist, create it with proper syntax
                create_table_sql = """
                CREATE TABLE txn (
                    txn_id SERIAL PRIMARY KEY,
                    dt DATE NOT NULL,
                    amt DECIMAL(8,2) NOT NULL,
                    "desc" TEXT NOT NULL,
                    account TEXT NOT NULL,
                    category TEXT DEFAULT 'uncategorized',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                -- Create indexes for better query performance
                CREATE INDEX idx_txn_dt ON txn(dt);
                CREATE INDEX idx_txn_category ON txn(category);
                CREATE INDEX idx_txn_account ON txn(account);
                CREATE INDEX idx_txn_amt ON txn(amt);
                """
                
                conn.execute(text(create_table_sql))
                conn.commit()
            
            logger.info("Transaction table created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create transaction table: {e}")
            return False
    
    def insert_transactions(self, df: pd.DataFrame, 
                          if_exists: str = 'append') -> bool:
        """
        Insert transaction data into the txn table.
        
        Args:
            df: DataFrame containing transaction data
            if_exists: How to behave if table exists ('fail', 'replace', 'append')
            
        Returns:
            True if insertion successful, False otherwise
        """
        try:
            # Ensure required columns exist
            required_columns = ['date', 'amount', 'description', 'account', 'category']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                logger.error(f"Missing required columns: {missing_columns}")
                return False
            
            # Clean and prepare data
            df_clean = df[required_columns].copy()
            
            # Rename columns to match database schema
            df_clean = df_clean.rename(columns={
                'date': 'dt',
                'amount': 'amt',
                'description': 'desc'
            })
            
            # Convert date to proper format
            df_clean['dt'] = pd.to_datetime(df_clean['dt']).dt.date
            
            # Ensure amount is numeric
            df_clean['amt'] = pd.to_numeric(df_clean['amt'], errors='coerce')
            
            # Remove any rows with null values
            df_clean = df_clean.dropna()
            
            if df_clean.empty:
                logger.warning("No valid transaction data to insert")
                return False
            
            # Insert data using pandas to_sql, excluding the primary key column
            df_clean.to_sql('txn', self.engine, if_exists=if_exists, index=False, method='multi')
            
            logger.info(f"Successfully inserted {len(df_clean)} transactions into database")
            return True
            
        except Exception as e:
            logger.error(f"Failed to insert transactions: {e}")
            return False
    
    def get_transactions(self, 
                        start_date: Optional[str] = None,
                        end_date: Optional[str] = None,
                        category: Optional[str] = None,
                        account: Optional[str] = None,
                        limit: Optional[int] = None) -> pd.DataFrame:
        """
        Retrieve transactions from the database with optional filters.
        
        Args:
            start_date: Start date filter (YYYY-MM-DD)
            end_date: End date filter (YYYY-MM-DD)
            category: Category filter
            account: Account filter
            limit: Maximum number of records to return
            
        Returns:
            DataFrame containing filtered transactions
        """
        try:
            query = "SELECT * FROM txn WHERE 1=1"
            params = {}
            
            if start_date:
                query += " AND dt >= %(start_date)s"
                params['start_date'] = start_date
            
            if end_date:
                query += " AND dt <= %(end_date)s"
                params['end_date'] = end_date
            
            if category:
                query += " AND category = %(category)s"
                params['category'] = category
            
            if account:
                query += " AND account = %(account)s"
                params['account'] = account
            
            query += " ORDER BY dt DESC"
            
            if limit:
                query += f" LIMIT {limit}"
            
            df = pd.read_sql_query(query, self.engine, params=params)
            return df
            
        except Exception as e:
            logger.error(f"Failed to retrieve transactions: {e}")
            return pd.DataFrame()
    
    def get_summary_stats(self) -> Dict[str, Any]:
        """
        Get summary statistics from the transaction data.
        
        Returns:
            Dictionary containing summary statistics
        """
        try:
            with self.engine.connect() as conn:
                # Total transactions
                total_txns = conn.execute(text("SELECT COUNT(*) FROM txn")).scalar()
                
                # Total amount
                total_amount = conn.execute(text("SELECT SUM(amt) FROM txn")).scalar()
                
                # Date range
                date_range = conn.execute(text("""
                    SELECT MIN(dt), MAX(dt) FROM txn
                """)).fetchone()
                
                # Category breakdown
                category_summary = pd.read_sql_query("""
                    SELECT category, COUNT(*) as count, SUM(amt) as total_amount
                    FROM txn 
                    GROUP BY category 
                    ORDER BY total_amount DESC
                """, self.engine)
                
                # Account breakdown
                account_summary = pd.read_sql_query("""
                    SELECT account, COUNT(*) as count, SUM(amt) as total_amount
                    FROM txn 
                    GROUP BY account 
                    ORDER BY total_amount DESC
                """, self.engine)
                
                return {
                    'total_transactions': total_txns,
                    'total_amount': float(total_amount) if total_amount else 0,
                    'date_range': {
                        'start': str(date_range[0]) if date_range[0] else None,
                        'end': str(date_range[1]) if date_range[1] else None
                    },
                    'category_summary': category_summary.to_dict('records'),
                    'account_summary': account_summary.to_dict('records')
                }
                
        except Exception as e:
            logger.error(f"Failed to get summary stats: {e}")
            return {}
    
    def clear_table(self) -> bool:
        """
        Clear all data from the txn table.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text("DELETE FROM txn"))
                conn.commit()
            
            logger.info("Transaction table cleared successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to clear table: {e}")
            return False
    
    def close(self):
        """Close database connections."""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connections closed")

def save_transactions_to_db(combined_df: pd.DataFrame, 
                          db_config: Optional[Dict[str, Any]] = None) -> bool:
    """
    Convenience function to save combined transaction data to database.
    
    Args:
        combined_df: Combined transaction DataFrame
        db_config: Database configuration dictionary
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Initialize database connection
        db = ExpenseDatabase(**(db_config or {}))
        
        if not db.connect():
            return False
        
        # Create table if it doesn't exist
        if not db.create_txn_table():
            return False
        
        # Insert transactions
        success = db.insert_transactions(combined_df)
        
        # Close connection
        db.close()
        
        return success
        
    except Exception as e:
        logger.error(f"Failed to save transactions to database: {e}")
        return False
