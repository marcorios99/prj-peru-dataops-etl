"""
Generador de datos sintéticos para pipeline ETL de conciliación.
Simula archivos CSV operativos y contables con casos edge realistas.
"""

import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
from typing import Tuple
import hashlib


class SyntheticDataGenerator:
    """Genera CSVs operativos/contables realistas para testing."""
    
    def __init__(self, seed: int = 42, locale: str = 'es_ES'):
        """
        Args:
            seed: Semilla para reproducibilidad
            locale: Localización para Faker
                   Opciones: 'es_ES' (España), 'es_MX' (México), 'pt_BR' (Brasil)
        """
        self.seed = seed
        np.random.seed(seed)
        Faker.seed(seed)
        
        # Verificar que el locale existe
        try:
            self.fake = Faker(locale)
            print(f"✅ Usando locale: {locale}")
        except AttributeError:
            print(f"⚠️  Locale '{locale}' no disponible, usando 'es_ES'")
            self.fake = Faker('es_ES')
    
    def generate_operational_file(
        self, 
        n_rows: int = 1000,
        duplicate_rate: float = 0.05,
        error_rate: float = 0.02
    ) -> pd.DataFrame:
        """
        Genera archivo de operaciones bancarias sintéticas.
        
        Args:
            n_rows: Número de registros a generar
            duplicate_rate: Porcentaje de duplicados (0.05 = 5%)
            error_rate: Porcentaje de registros con errores (0.02 = 2%)
        
        Returns:
            DataFrame con operaciones sintéticas
        """
        print(f"🔄 Generando {n_rows:,} operaciones...")
        
        # Base de datos limpia
        data = {
            'fecha_operacion': self._generate_dates(n_rows),
            'numero_operacion': self._generate_operation_ids(n_rows),
            'tipo_operacion': np.random.choice(
                ['DEPOSITO', 'RETIRO', 'TRANSFERENCIA'], 
                n_rows,
                p=[0.4, 0.3, 0.3]
            ),
            'monto': np.random.uniform(10, 50000, n_rows).round(2),
            'moneda': np.random.choice(['PEN', 'USD'], n_rows, p=[0.85, 0.15]),
            'cuenta_origen': self._generate_account_numbers(n_rows),
            'cuenta_destino': self._generate_account_numbers(n_rows, nullable=True),
            'banco_origen': [self._get_random_bank() for _ in range(n_rows)],
            'banco_destino': [self._get_random_bank() if np.random.random() > 0.3 else None for _ in range(n_rows)],
            'descripcion': [self.fake.sentence(nb_words=6) for _ in range(n_rows)],
            'estado': np.random.choice(
                ['COMPLETADA', 'PENDIENTE', 'FALLIDA'],
                n_rows,
                p=[0.85, 0.10, 0.05]
            ),
            'canal': np.random.choice(
                ['WEB', 'MOBILE', 'ATM', 'SUCURSAL'],
                n_rows,
                p=[0.40, 0.35, 0.15, 0.10]
            )
        }
        
        df = pd.DataFrame(data)
        
        # Inyectar duplicados
        df = self._inject_duplicates(df, duplicate_rate)
        
        # Inyectar errores
        df = self._inject_errors(df, error_rate)
        
        # Agregar hash para deduplicación
        df['content_hash'] = df.apply(self._generate_hash, axis=1)
        
        print(f" Generadas {len(df):,} operaciones")
        print(f"   - Duplicados esperados: ~{int(len(df) * duplicate_rate):,} ({duplicate_rate*100:.1f}%)")
        print(f"   - Errores esperados: ~{int(len(df) * error_rate):,} ({error_rate*100:.1f}%)")
        
        return df
    
    def _generate_dates(self, n: int) -> list:
        """Genera fechas en los últimos 30 días."""
        start_date = datetime.now() - timedelta(days=30)
        return [
            start_date + timedelta(
                days=np.random.randint(0, 31),
                hours=np.random.randint(0, 24),
                minutes=np.random.randint(0, 60)
            )
            for _ in range(n)
        ]
    
    def _generate_operation_ids(self, n: int) -> list:
        """Genera IDs únicos de operación."""
        return [f"OP-{i:08d}" for i in range(1, n + 1)]
    
    def _generate_account_numbers(self, n: int, nullable: bool = False) -> list:
        """
        Genera números de cuenta bancaria peruanos realistas.
        Formato típico: 191-1234567-0-89
        """
        accounts = []
        for _ in range(n):
            if nullable and np.random.random() > 0.7:
                accounts.append(None)
            else:
                # Formato: XXX-XXXXXXX-X-XX
                account = f"{np.random.randint(100, 999)}-{np.random.randint(1000000, 9999999)}-{np.random.randint(0, 9)}-{np.random.randint(10, 99)}"
                accounts.append(account)
        return accounts
    
    def _get_random_bank(self) -> str:
        """Retorna nombre de banco peruano aleatorio."""
        banks = [
            'BCP', 'BBVA', 'INTERBANK', 'SCOTIABANK', 
            'BANBIF', 'PICHINCHA', 'BN', 'FALABELLA', 
            'RIPLEY', 'CITIBANK'
        ]
        return np.random.choice(banks)
    
    def _inject_duplicates(self, df: pd.DataFrame, rate: float) -> pd.DataFrame:
        """Inyecta duplicados exactos aleatoriamente."""
        n_duplicates = int(len(df) * rate)
        if n_duplicates == 0:
            return df
        
        # Seleccionar registros aleatorios para duplicar
        indices_to_duplicate = np.random.choice(df.index, n_duplicates, replace=True)
        duplicates = df.loc[indices_to_duplicate].copy()
        
        print(f"   💫 Inyectando {n_duplicates:,} duplicados...")
        
        # Agregar duplicados
        df_with_dupes = pd.concat([df, duplicates], ignore_index=True)
        
        # Shuffle para mezclar
        return df_with_dupes.sample(frac=1, random_state=self.seed).reset_index(drop=True)
    
    def _inject_errors(self, df: pd.DataFrame, rate: float) -> pd.DataFrame:
        """Inyecta errores de formato en registros aleatorios."""
        n_errors = int(len(df) * rate)
        if n_errors == 0:
            return df
        
        print(f"  Inyectando {n_errors:,} errores...")
        
        error_indices = np.random.choice(df.index, n_errors, replace=False)
        
        for idx in error_indices:
            error_type = np.random.choice([
                'invalid_date', 
                'negative_amount', 
                'null_operation_id',
                'invalid_state',
                'missing_account'
            ])
            
            if error_type == 'invalid_date':
                df.at[idx, 'fecha_operacion'] = 'FECHA_INVALIDA'
            elif error_type == 'negative_amount':
                df.at[idx, 'monto'] = -abs(df.at[idx, 'monto'])
            elif error_type == 'null_operation_id':
                df.at[idx, 'numero_operacion'] = None
            elif error_type == 'invalid_state':
                df.at[idx, 'estado'] = 'ESTADO_DESCONOCIDO'
            elif error_type == 'missing_account':
                df.at[idx, 'cuenta_origen'] = None
        
        return df
    
    def _generate_hash(self, row: pd.Series) -> str:
        """Genera hash único del contenido del registro."""
        try:
            content = f"{row['numero_operacion']}{row['monto']}{row['fecha_operacion']}{row['cuenta_origen']}"
            return hashlib.sha256(content.encode()).hexdigest()[:16]
        except:
            # Si hay valores None, generar hash aleatorio
            return hashlib.sha256(str(np.random.random()).encode()).hexdigest()[:16]
    
    def save_to_csv(self, df: pd.DataFrame, filename: str, output_dir: str = 'data/input'):
        """Guarda DataFrame a CSV."""
        import os
        os.makedirs(output_dir, exist_ok=True)
        
        filepath = os.path.join(output_dir, filename)
        df.to_csv(filepath, index=False, encoding='utf-8')
        
        file_size_kb = os.path.getsize(filepath) / 1024
        
        print(f"\n Archivo guardado exitosamente:")
        print(f"    Ruta: {filepath}")
        print(f"   Tamaño: {file_size_kb:.2f} KB")
        print(f"    Filas: {len(df):,}")
        print(f"    Columnas: {len(df.columns)}")
        
        return filepath
    
    def generate_summary_stats(self, df: pd.DataFrame) -> dict:
        """Genera estadísticas resumen del dataset."""
        stats = {
            'total_registros': len(df),
            'duplicados_exactos': len(df) - df.drop_duplicates().shape[0],
            'valores_nulos': df.isnull().sum().to_dict(),
            'monto_total': df['monto'].sum(),
            'monto_promedio': df['monto'].mean(),
            'operaciones_por_tipo': df['tipo_operacion'].value_counts().to_dict(),
            'operaciones_por_estado': df['estado'].value_counts().to_dict(),
            'monedas': df['moneda'].value_counts().to_dict()
        }
        return stats


# Demo de uso
if __name__ == "__main__":
    print("=" * 60)
    print("🏦 GENERADOR DE DATOS SINTÉTICOS - ETL CONCILIACIÓN")
    print("=" * 60)
    
    # Inicializar generador
    generator = SyntheticDataGenerator(seed=42, locale='es_ES')
    
    # Generar 10,000 operaciones
    df = generator.generate_operational_file(
        n_rows=10000,
        duplicate_rate=0.05,  # 5% duplicados
        error_rate=0.02       # 2% errores
    )
    
    # Guardar
    filepath = generator.save_to_csv(df, 'operaciones_demo_2025.csv')
    
    # Mostrar estadísticas
    print("\n" + "=" * 60)
    print(" ESTADÍSTICAS DEL DATASET GENERADO")
    print("=" * 60)
    
    stats = generator.generate_summary_stats(df)
    print(f"\n Total de registros: {stats['total_registros']:,}")
    print(f" Duplicados exactos: {stats['duplicados_exactos']:,}")
    print(f" Monto total: S/ {stats['monto_total']:,.2f}")
    print(f" Monto promedio: S/ {stats['monto_promedio']:,.2f}")
    
    print("\n Operaciones por tipo:")
    for tipo, count in stats['operaciones_por_tipo'].items():
        print(f"   - {tipo}: {count:,} ({count/stats['total_registros']*100:.1f}%)")
    
    print("\n Operaciones por estado:")
    for estado, count in stats['operaciones_por_estado'].items():
        print(f"   - {estado}: {count:,} ({count/stats['total_registros']*100:.1f}%)")
    
    print("\n Distribución por moneda:")
    for moneda, count in stats['monedas'].items():
        print(f"   - {moneda}: {count:,} ({count/stats['total_registros']*100:.1f}%)")
    
    print("\n Primeras 5 filas del dataset:")
    print(df.head().to_string())
    
    print("\n" + "=" * 60)
    print(" GENERACIÓN COMPLETADA EXITOSAMENTE")
    print("=" * 60)