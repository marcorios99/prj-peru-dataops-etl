"""
Test SQL loading functionality with SQLite and SQL Server.
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
from src.loading.sql_loader import SQLLoader, DatabaseConfig


def test_sqlite():
    """Test with SQLite database."""
    print("\n" + "=" * 70)
    print("TEST 1: SQLite Database")
    print("=" * 70)
    
    config = DatabaseConfig(
        db_type='sqlite',
        database=str(project_root / 'data' / 'etl_conciliacion.db')
    )
    
    return run_test(config, "SQLite")


def test_sqlserver():
    """Test with SQL Server (Windows Authentication)."""
    print("\n" + "=" * 70)
    print("TEST 2: SQL Server (Windows Auth)")
    print("=" * 70)
    
    config = DatabaseConfig(
        db_type='sqlserver',
        host='PC-MARCO\SQLEXPRESS',
        port=1433,
        database='ETL_Conciliacion',
        trusted_connection=True,
        driver='ODBC Driver 17 for SQL Server'
    )
    
    return run_test(config, "SQL Server")


def run_test(config: DatabaseConfig, db_name: str):
    """Run loading test with given configuration."""
    deduped_path = project_root / 'data' / 'processed' / 'deduplicated_operations.csv'
    
    if not deduped_path.exists():
        print(f"Error: File not found: {deduped_path}")
        return False
    
    try:
        print(f"\nLoading data file...")
        df = pd.read_csv(deduped_path, parse_dates=['fecha_operacion'])
        print(f"Loaded {len(df):,} rows")
        
        print(f"\nInitializing {db_name} loader...")
        loader = SQLLoader(config)
        
        print("Connecting to database...")
        loader.connect()
        
        print("Creating table...")
        loader.create_table('operaciones')
        
        print("\nLoading data to database...")
        stats = loader.upsert_data(df, 'operaciones')
        
        print("\n" + "-" * 70)
        print("LOAD STATISTICS")
        print("-" * 70)
        print(f"Total processed: {stats['total_processed']:,}")
        print(f"Rows inserted: {stats['rows_inserted']:,}")
        print(f"Rows updated: {stats['rows_updated']:,}")
        print(f"Rows failed: {stats['rows_failed']:,}")
        
        print("\n" + "-" * 70)
        print("DATABASE VERIFICATION")
        print("-" * 70)
        
        table_stats = loader.get_table_stats('operaciones')
        print(f"\nTotal rows: {table_stats['total_rows']:,}")
        print(f"Date range: {table_stats['date_range']['min']} to {table_stats['date_range']['max']}")
        print(f"Total amount: S/ {table_stats['amounts']['total']:,.2f}")
        print(f"Average amount: S/ {table_stats['amounts']['average']:,.2f}")
        
        print("\nQuerying sample data...")
        sample = loader.query_data('operaciones', limit=5)
        print("\nFirst 5 records:")
        print(sample[['numero_operacion', 'monto', 'estado']])
        
        print("\n" + "-" * 70)
        print("TESTING UPSERT")
        print("-" * 70)
        
        print("Re-loading 100 rows (should update, not insert)...")
        stats2 = loader.upsert_data(df.head(100), 'operaciones')
        
        final_stats = loader.get_table_stats('operaciones')
        print(f"Total rows after upsert: {final_stats['total_rows']:,}")
        
        loader.close()
        
        print(f"\n{db_name} test PASSED")
        return True
        
    except Exception as e:
        print(f"\n{db_name} test FAILED: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def main():
    print("=" * 70)
    print("SQL LOADING TESTS")
    print("=" * 70)
    
    print("\nStarting SQLite test...")
    sqlite_ok = test_sqlite()
    
    if sqlite_ok:
        print("\n" + "=" * 70)
        print("SQLite test completed successfully!")
        print("=" * 70)
    else:
        print("\n" + "=" * 70)
        print("SQLite test failed. Check errors above.")
        print("=" * 70)
        return
    
    print("\n" + "=" * 70)
    print("SQL Server Test (Optional)")
    print("=" * 70)
    print("Requirements:")
    print("  - SQL Server installed locally")
    print("  - Database 'ETL_Conciliacion' exists")
    print("  - Windows Authentication enabled")
    print("  - ODBC Driver 17 for SQL Server installed")
    print("=" * 70)
    
    choice = input("\nTest SQL Server? (y/n): ").strip().lower()
    
    if choice == 'y':
        sqlserver_ok = test_sqlserver()
        print("\n" + "=" * 70)
        print("TEST SUMMARY")
        print("=" * 70)
        print(f"SQLite: {'PASSED' if sqlite_ok else 'FAILED'}")
        print(f"SQL Server: {'PASSED' if sqlserver_ok else 'FAILED'}")
        print("=" * 70)
    else:
        print("\nSkipping SQL Server test")
        print("\n" + "=" * 70)
        print("TESTS COMPLETE")
        print("=" * 70)
        print(f"SQLite: {'PASSED' if sqlite_ok else 'FAILED'}")
        print("=" * 70)


if __name__ == "__main__":
    main()