"""Reset database tables."""
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.loading.sql_loader import SQLLoader, DatabaseConfig
from sqlalchemy import text

config = DatabaseConfig(
    db_type='sqlserver',
    host=r'PC-MARCO\SQLEXPRESS',
    database='ETL_Conciliacion',
    trusted_connection=True
)

loader = SQLLoader(config)
loader.connect()

with loader.engine.connect() as conn:
    conn.execute(text("DROP TABLE IF EXISTS dbo.operaciones"))
    conn.commit()
    print("Table dropped successfully")

loader.close()