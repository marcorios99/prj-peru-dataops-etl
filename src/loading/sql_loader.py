"""
SQL database loader with UPSERT capabilities.
Supports SQL Server (Windows Auth) and SQLite.
"""

import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime, Index, text
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional, Dict, List
from dataclasses import dataclass
import os
from urllib.parse import quote_plus
from pathlib import Path  
from sqlalchemy import VARCHAR, FLOAT, DATETIME


@dataclass
class DatabaseConfig:
    """Database connection configuration."""
    db_type: str
    host: str = 'localhost'
    port: int = 1433
    database: str = ''
    trusted_connection: bool = True
    driver: str = 'ODBC Driver 17 for SQL Server'
    schema: str = 'dbo'
    
    @classmethod
    def from_env(cls):
        """Create config from environment variables."""
        trusted = os.getenv('DB_TRUSTED_CONNECTION', 'true').lower() == 'true'
        
        return cls(
            db_type=os.getenv('DB_TYPE', 'sqlite'),
            host=os.getenv('DB_HOST', 'PC-MARCO\SQLEXPRESS'),
            port=int(os.getenv('DB_PORT', '1433')),
            database=os.getenv('DB_NAME', 'ETL_Conciliacion'),
            trusted_connection=trusted,
            driver=os.getenv('DB_DRIVER', 'ODBC Driver 17 for SQL Server'),
            schema=os.getenv('DB_SCHEMA', 'dbo')
        )
    
    def get_connection_string(self) -> str:
        """Generate SQLAlchemy connection string."""
        if self.db_type == 'sqlserver':
            is_named_instance = ('\\' in self.host)
            if self.trusted_connection:
                server_segment = self.host if is_named_instance else f"{self.host},{self.port}"

                extra = "TrustServerCertificate=yes;"
                if "ODBC Driver 18" in self.driver:
                    extra += "Encrypt=no;"  # opcional si te da lata el cifrado

                conn_str = (
                    f"DRIVER={{{self.driver}}};"
                    f"SERVER={server_segment};"
                    f"DATABASE={self.database};"
                    f"Trusted_Connection=yes;"
                    f"{extra}"
                )
                return f"mssql+pyodbc:///?odbc_connect={quote_plus(conn_str)}"

        
        # dentro de DatabaseConfig.get_connection_string()
        elif self.db_type == 'sqlite':
            db_path = Path(self.database)

            # Si es relativa y no arranca con "data", anteponer "data/"
            if not db_path.is_absolute():
                if not (db_path.parts and db_path.parts[0] == 'data'):
                    db_path = Path('data') / db_path

            # Asegurar que exista la carpeta
            db_path.parent.mkdir(parents=True, exist_ok=True)

            # Resolver a ruta absoluta y normalizar a slashes para URI SQLite
            db_path_resolved = db_path.resolve()
            return f"sqlite:///{db_path_resolved.as_posix()}"
        
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")


class SQLLoader:
    """Handles loading data into SQL databases with UPSERT support."""
    
    def __init__(self, config: Optional[DatabaseConfig] = None):
        self.config = config or DatabaseConfig.from_env()
        self.engine = None
        self.metadata = MetaData()
        self.load_stats = {
            'rows_inserted': 0,
            'rows_updated': 0,
            'rows_failed': 0,
            'total_processed': 0
        }
    
    def connect(self):
        """Establish database connection."""
        try:
            connection_string = self.config.get_connection_string()
            
            engine_kwargs = {'echo': False}
            
            if self.config.db_type == 'sqlserver':
                engine_kwargs['fast_executemany'] = True
            
            self.engine = create_engine(connection_string, **engine_kwargs)
            
            with self.engine.connect() as conn:
                if self.config.db_type == 'sqlserver':
                    result = conn.execute(text("SELECT @@VERSION"))
                    version = result.fetchone()[0]
                    print(f"Connected to SQL Server")
                    print(f"Database: {self.config.database}")
                elif self.config.db_type == 'sqlite':
                    result = conn.execute(text("SELECT sqlite_version()"))
                    version = result.fetchone()[0]
                    print(f"Connected to SQLite v{version}")
                    print(f"Database: {self.config.database}")
                    
        except Exception as e:
            raise ConnectionError(f"Failed to connect to database: {str(e)}")
    
    def create_table(self, table_name: str = 'operaciones'):
        if self.engine is None:
            self.connect()

        schema_kwargs = {'schema': self.config.schema} if self.config.db_type == 'sqlserver' else {}

        operations_table = Table(
            table_name,
            self.metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('fecha_operacion', DateTime, nullable=False),
            Column('numero_operacion', String(20), nullable=False),
            Column('tipo_operacion', String(20), nullable=False),
            Column('monto', Float, nullable=False),
            Column('moneda', String(3), nullable=False),
            Column('cuenta_origen', String(50), nullable=False),
            Column('cuenta_destino', String(50)),
            Column('banco_origen', String(50), nullable=False),
            Column('banco_destino', String(50)),
            Column('descripcion', String(500)),
            Column('estado', String(20), nullable=False),
            Column('canal', String(20), nullable=False),
            Column('content_hash', String(32)),
            Column('fecha_carga', DateTime),
            Column('is_duplicate', Integer, nullable=True),
            Index('idx_numero_operacion', 'numero_operacion'),
            Index('idx_fecha_operacion', 'fecha_operacion'),
            Index('idx_estado', 'estado'),
            **schema_kwargs,  
        )

        self.metadata.create_all(self.engine)
        shown_name = f"{self.config.schema}.{table_name}" if self.config.db_type == 'sqlserver' else table_name
        print(f"Table '{shown_name}' created/verified")

        if self.config.db_type == 'sqlserver':
            self._create_unique_constraint(table_name)

    
    def _create_unique_constraint(self, table_name: str):
        try:
            full_name = f"{self.config.schema}.{table_name}"
            with self.engine.connect() as conn:
                check_constraint = text(f"""
                    IF NOT EXISTS (
                        SELECT 1
                        FROM sys.indexes
                        WHERE name = 'UQ_{table_name}_numero_operacion'
                        AND object_id = OBJECT_ID('{full_name}')
                    )
                    BEGIN
                        CREATE UNIQUE INDEX UQ_{table_name}_numero_operacion
                        ON {full_name}(numero_operacion);
                    END
                """)
                conn.execute(check_constraint)
                conn.commit()
        except Exception as e:
            print(f"Note: Unique constraint may already exist: {e}")

    
    def upsert_data(
        self,
        df: pd.DataFrame,
        table_name: str = 'operaciones',
        conflict_columns: List[str] = None
    ) -> Dict:
        """Insert or update data in database."""
        if self.engine is None:
            self.connect()
        
        if conflict_columns is None:
            conflict_columns = ['numero_operacion']
        
        self.load_stats = {
            'rows_inserted': 0,
            'rows_updated': 0,
            'rows_failed': 0,
            'total_processed': len(df)
        }
        
        try:
            if self.config.db_type == 'sqlite':
                self._upsert_sqlite(df, table_name, conflict_columns)
            elif self.config.db_type == 'sqlserver':
                self._upsert_sqlserver(df, table_name, conflict_columns)
            else:
                raise ValueError(f"Unsupported database type: {self.config.db_type}")
            
        except Exception as e:
            self.load_stats['rows_failed'] = len(df)
            raise RuntimeError(f"Failed to load data: {str(e)}")
        
        return self.load_stats.copy()
    
    def _upsert_sqlite(
        self,
        df: pd.DataFrame,
        table_name: str,
        conflict_columns: List[str]
    ):
        """SQLite UPSERT using replace method."""
        df_copy = df.copy()
        df_copy['fecha_carga'] = pd.Timestamp.now()
        
        try:
            existing_df = pd.read_sql(f"SELECT numero_operacion FROM {table_name}", self.engine)
            existing_ids = set(existing_df['numero_operacion'].tolist())
        except:
            existing_ids = set()
        
        new_rows = df_copy[~df_copy['numero_operacion'].isin(existing_ids)]
        update_rows = df_copy[df_copy['numero_operacion'].isin(existing_ids)]
        
        if len(new_rows) > 0:
            new_rows.to_sql(table_name, self.engine, if_exists='append', index=False)
            self.load_stats['rows_inserted'] = len(new_rows)
        
        if len(update_rows) > 0:
            for _, row in update_rows.iterrows():
                fecha_op = pd.to_datetime(row['fecha_operacion']).to_pydatetime() if pd.notna(row['fecha_operacion']) else None
                fecha_carga = pd.to_datetime(row['fecha_carga']).to_pydatetime() if pd.notna(row['fecha_carga']) else None

                update_sql = text("""
                UPDATE operaciones
                SET fecha_operacion = :fecha_op,
                    tipo_operacion = :tipo_op,
                    monto = :monto,
                    moneda = :moneda,
                    cuenta_origen = :cta_orig,
                    cuenta_destino = :cta_dest,
                    banco_origen = :banco_orig,
                    banco_destino = :banco_dest,
                    descripcion = :desc,
                    estado = :estado,
                    canal = :canal,
                    content_hash = :hash,
                    fecha_carga = :fecha_carga
                WHERE numero_operacion = :num_op
                """)

                with self.engine.connect() as conn:
                    conn.execute(update_sql, {
                        'fecha_op': fecha_op,
                        'tipo_op': row['tipo_operacion'],
                        'monto': float(row['monto']) if pd.notna(row['monto']) else None,
                        'moneda': row['moneda'],
                        'cta_orig': row['cuenta_origen'],
                        'cta_dest': row['cuenta_destino'],
                        'banco_orig': row['banco_origen'],
                        'banco_dest': row['banco_destino'],
                        'desc': row['descripcion'],
                        'estado': row['estado'],
                        'canal': row['canal'],
                        'hash': row['content_hash'],
                        'fecha_carga': fecha_carga,
                        'num_op': row['numero_operacion']
                    })
                    conn.commit()
                        
            self.load_stats['rows_updated'] = len(update_rows)
    
    def _upsert_sqlserver(self, df: pd.DataFrame, table_name: str, conflict_columns: List[str]):
        from sqlalchemy import VARCHAR, FLOAT, DATETIME
        
        df_copy = df.copy()
        
        # Asegurar tipos correctos
        df_copy['fecha_operacion'] = pd.to_datetime(df_copy['fecha_operacion'])
        df_copy['fecha_carga'] = pd.Timestamp.now()
        
        # Truncar campos de texto
        df_copy['descripcion'] = df_copy['descripcion'].astype(str).str[:500]
        df_copy['banco_origen'] = df_copy['banco_origen'].astype(str).str[:50]
        df_copy['banco_destino'] = df_copy['banco_destino'].fillna('').astype(str).str[:50]
        df_copy['cuenta_origen'] = df_copy['cuenta_origen'].astype(str).str[:50]
        df_copy['cuenta_destino'] = df_copy['cuenta_destino'].fillna('').astype(str).str[:50]
        
        # Reemplazar vacíos con None
        df_copy['cuenta_destino'] = df_copy['cuenta_destino'].replace('', None)
        df_copy['banco_destino'] = df_copy['banco_destino'].replace('', None)
        
        full_target = f"{self.config.schema}.{table_name}"
        temp_table = f"temp_operaciones_{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}"
        
        with self.engine.begin() as conn:
            # Usar to_sql directamente con tipos específicos
            df_copy.to_sql(
                temp_table,
                con=conn,
                if_exists='replace',
                index=False,
                schema=self.config.schema,
                dtype={
                    'fecha_operacion': DATETIME,
                    'numero_operacion': VARCHAR(20),
                    'tipo_operacion': VARCHAR(20),
                    'monto': FLOAT,
                    'moneda': VARCHAR(3),
                    'cuenta_origen': VARCHAR(50),
                    'cuenta_destino': VARCHAR(50),
                    'banco_origen': VARCHAR(50),
                    'banco_destino': VARCHAR(50),
                    'descripcion': VARCHAR(500),
                    'estado': VARCHAR(20),
                    'canal': VARCHAR(20),
                    'content_hash': VARCHAR(32),
                    'fecha_carga': DATETIME
                }
            )
            
            source_table = f"{self.config.schema}.{temp_table}"
            
            merge_sql = text(f"""
            MERGE {full_target} AS target
            USING {source_table} AS source
            ON target.numero_operacion = source.numero_operacion
            WHEN MATCHED THEN
                UPDATE SET 
                    target.fecha_operacion = source.fecha_operacion,
                    target.tipo_operacion = source.tipo_operacion,
                    target.monto = source.monto,
                    target.moneda = source.moneda,
                    target.cuenta_origen = source.cuenta_origen,
                    target.cuenta_destino = source.cuenta_destino,
                    target.banco_origen = source.banco_origen,
                    target.banco_destino = source.banco_destino,
                    target.descripcion = source.descripcion,
                    target.estado = source.estado,
                    target.canal = source.canal,
                    target.content_hash = source.content_hash,
                    target.fecha_carga = source.fecha_carga
            WHEN NOT MATCHED THEN
                INSERT (fecha_operacion, numero_operacion, tipo_operacion, monto, moneda,
                        cuenta_origen, cuenta_destino, banco_origen, banco_destino,
                        descripcion, estado, canal, content_hash, fecha_carga)
                VALUES (source.fecha_operacion, source.numero_operacion, source.tipo_operacion,
                        source.monto, source.moneda, source.cuenta_origen, source.cuenta_destino,
                        source.banco_origen, source.banco_destino, source.descripcion,
                        source.estado, source.canal, source.content_hash, source.fecha_carga);
            """)
            
            conn.execute(merge_sql)
            
            drop_temp = text(f"DROP TABLE IF EXISTS {source_table};")
            conn.execute(drop_temp)
        
        self.load_stats['rows_inserted'] = len(df)

    
    def query_data(
        self,
        table_name: str = 'operaciones',
        filters: Optional[Dict] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """Query data from database."""
        if self.engine is None:
            self.connect()
        
        query = f"SELECT * FROM {table_name}"
        
        if filters:
            conditions = []
            for col, val in filters.items():
                if isinstance(val, str):
                    conditions.append(f"{col} = '{val}'")
                else:
                    conditions.append(f"{col} = {val}")
            query += " WHERE " + " AND ".join(conditions)
        
        if limit:
            if self.config.db_type == 'sqlserver':
                query = f"SELECT TOP {limit} * FROM {table_name}"
                if filters:
                    conditions = [f"{col} = '{val}'" if isinstance(val, str) else f"{col} = {val}" 
                                  for col, val in filters.items()]
                    query += " WHERE " + " AND ".join(conditions)
            else:
                query += f" LIMIT {limit}"
        
        return pd.read_sql(query, self.engine)
    
    def get_table_stats(self, table_name: str = 'operaciones') -> Dict:
        """Get statistics about table contents."""
        if self.engine is None:
            self.connect()
        
        stats = {}
        
        count_query = f"SELECT COUNT(*) as total FROM {table_name}"
        stats['total_rows'] = pd.read_sql(count_query, self.engine)['total'].iloc[0]
        
        date_query = f"""
        SELECT 
            MIN(fecha_operacion) as min_date, 
            MAX(fecha_operacion) as max_date 
        FROM {table_name}
        """
        date_stats = pd.read_sql(date_query, self.engine)
        stats['date_range'] = {
            'min': date_stats['min_date'].iloc[0],
            'max': date_stats['max_date'].iloc[0]
        }
        
        amount_query = f"""
        SELECT 
            SUM(monto) as total_amount, 
            AVG(monto) as avg_amount 
        FROM {table_name}
        """
        amount_stats = pd.read_sql(amount_query, self.engine)
        stats['amounts'] = {
            'total': float(amount_stats['total_amount'].iloc[0]),
            'average': float(amount_stats['avg_amount'].iloc[0])
        }
        
        return stats
    
    def close(self):
        """Close database connection."""
        if self.engine:
            self.engine.dispose()
            print("Database connection closed")