"""
Data validation engine.
Orchestrates schema validation and business rule checks.
"""

import pandas as pd
import pandera as pa
from typing import Tuple, Optional
import hashlib

from .schemas import OperationalSchema, ValidationReport


class DataValidator:
    """Main validation engine for ETL pipeline."""
    
    def __init__(self, schema: Optional[pa.DataFrameSchema] = None):
        """
        Initialize validator with schema.
        
        Args:
            schema: Pandera schema (defaults to OperationalSchema)
        """
        self.schema = schema or OperationalSchema.get_schema()
        self.report = ValidationReport()
    
    def validate(
        self, 
        df: pd.DataFrame,
        expected_checksum: Optional[float] = None
    ) -> Tuple[pd.DataFrame, ValidationReport]:
        """
        Validate DataFrame against schema and business rules.
        
        Args:
            df: Input DataFrame
            expected_checksum: Expected total amount for validation
        
        Returns:
            Tuple of (validated_df, validation_report)
        """
        self.report = ValidationReport()
        self.report.total_rows = len(df)
        
        try:
            validated_df = self.schema.validate(df, lazy=True)
            self.report.valid_rows = len(validated_df)
            
        except pa.errors.SchemaErrors as e:
            validated_df = self._handle_schema_errors(df, e)
        
        if expected_checksum is not None:
            checksum_valid = OperationalSchema.validate_checksum(
                validated_df, 
                expected_checksum
            )
            if not checksum_valid:
                self.report.add_warning(
                    f"Checksum mismatch: expected {expected_checksum}, "
                    f"got {validated_df['monto'].sum()}"
                )
        
        self.report.invalid_rows = self.report.total_rows - self.report.valid_rows
        
        return validated_df, self.report
    
    def _handle_schema_errors(
        self, 
        df: pd.DataFrame, 
        error: pa.errors.SchemaErrors
    ) -> pd.DataFrame:
        """
        Process schema validation errors and filter invalid rows.
        
        Args:
            df: Original DataFrame
            error: Pandera schema errors
        
        Returns:
            DataFrame with only valid rows
        """
        failure_cases = error.failure_cases
        
        for _, case in failure_cases.iterrows():
            self.report.add_error(
                row_index=case.get('index', -1),
                column=case.get('column', 'unknown'),
                error_msg=str(case.get('check', 'validation failed'))
            )
        
        invalid_indices = set()
        
        if 'index' in failure_cases.columns:
            for idx in failure_cases['index'].dropna():
                if idx is not None and pd.notna(idx):
                    try:
                        invalid_indices.add(int(idx))
                    except (ValueError, TypeError):
                        continue
        
        if invalid_indices:
            valid_mask = ~df.index.isin(invalid_indices)
            valid_df = df[valid_mask].copy().reset_index(drop=True)
        else:
            valid_df = df.copy()
        
        self.report.valid_rows = len(valid_df)
        
        return valid_df
    
    def validate_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Check for duplicate operation IDs.
        
        Args:
            df: DataFrame to check
        
        Returns:
            DataFrame with duplicates flagged
        """
        df['is_duplicate'] = df.duplicated(subset=['numero_operacion'], keep='first')
        
        duplicate_count = df['is_duplicate'].sum()
        if duplicate_count > 0:
            self.report.add_warning(
                f"Found {duplicate_count} duplicate operation IDs"
            )
        
        return df
    
    def generate_content_hash(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate content hash for each row for deduplication.
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with content_hash column
        """
        hash_columns = ['numero_operacion', 'monto', 'fecha_operacion', 'cuenta_origen']
        
        df['content_hash'] = df[hash_columns].apply(
            lambda row: hashlib.sha256(
                ''.join(str(row[col]) for col in hash_columns).encode()
            ).hexdigest()[:16],
            axis=1
        )
        
        return df


def quick_validate(csv_path: str) -> Tuple[pd.DataFrame, ValidationReport]:
    """
    Convenience function for quick validation of CSV file.
    
    Args:
        csv_path: Path to CSV file
    
    Returns:
        Tuple of (validated_df, report)
    """
    df = pd.read_csv(csv_path, parse_dates=['fecha_operacion'])
    validator = DataValidator()
    return validator.validate(df)