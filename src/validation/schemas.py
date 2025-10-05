"""
Data validation schemas using Pandera.
Defines structure, types, and business rules for operational data.
"""

import pandera as pa
from pandera import Column, Check, DataFrameSchema
from datetime import datetime
import re


class OperationalSchema:
    """Schema definitions for operational transaction files."""
    
    @staticmethod
    def get_schema() -> DataFrameSchema:
        """
        Returns Pandera schema for operational transactions.
        
        Business rules:
        - Operation IDs must be unique and follow format OP-XXXXXXXX
        - Amounts must be positive and below 1M
        - Dates cannot be in the future
        - Account numbers follow format XXX-XXXXXXX-X-XX
        """
        return DataFrameSchema(
            columns={
                "fecha_operacion": Column(
                    pa.DateTime,
                    nullable=False,
                    checks=[
                        Check.less_than_or_equal_to(
                            datetime.now(),
                            error="Future dates not allowed"
                        )
                    ]
                ),
                "numero_operacion": Column(
                    pa.String,
                    nullable=False,
                    unique=True,
                    checks=[
                        Check.str_matches(
                            r"^OP-\d{8}$",
                            error="Invalid operation ID format"
                        )
                    ]
                ),
                "tipo_operacion": Column(
                    pa.String,
                    nullable=False,
                    checks=[
                        Check.isin(["DEPOSITO", "RETIRO", "TRANSFERENCIA"])
                    ]
                ),
                "monto": Column(
                    pa.Float,
                    nullable=False,
                    checks=[
                        Check.greater_than(0, error="Amount must be positive"),
                        Check.less_than_or_equal_to(
                            1_000_000,
                            error="Amount exceeds maximum limit"
                        )
                    ]
                ),
                "moneda": Column(
                    pa.String,
                    nullable=False,
                    checks=[Check.isin(["PEN", "USD"])]
                ),
                "cuenta_origen": Column(
                    pa.String,
                    nullable=False,
                    checks=[
                        Check.str_matches(
                            r"^\d{3}-\d{7}-\d-\d{2}$",
                            error="Invalid account format"
                        )
                    ]
                ),
                "banco_origen": Column(pa.String, nullable=False),
                "descripcion": Column(pa.String, nullable=False),
                "estado": Column(
                    pa.String,
                    nullable=False,
                    checks=[Check.isin(["COMPLETADA", "PENDIENTE", "FALLIDA"])]
                ),
                "canal": Column(
                    pa.String,
                    nullable=False,
                    checks=[Check.isin(["WEB", "MOBILE", "ATM", "SUCURSAL"])]
                )
            },
            coerce=True,
            strict=False
        )
    
    @staticmethod
    def validate_checksum(df, expected_total: float = None) -> bool:
        """
        Validates total amount checksum.
        
        Args:
            df: DataFrame to validate
            expected_total: Expected sum of amounts (optional)
        
        Returns:
            True if checksum passes
        """
        if expected_total is None:
            return True
        
        actual_total = df['monto'].sum()
        tolerance = 0.01
        
        return abs(actual_total - expected_total) < tolerance


class ValidationReport:
    """Container for validation results."""
    
    def __init__(self):
        self.total_rows = 0
        self.valid_rows = 0
        self.invalid_rows = 0
        self.errors = []
        self.warnings = []
    
    def add_error(self, row_index: int, column: str, error_msg: str):
        """Add validation error."""
        self.errors.append({
            'row': row_index,
            'column': column,
            'error': error_msg
        })
    
    def add_warning(self, message: str):
        """Add validation warning."""
        self.warnings.append(message)
    
    def to_dict(self):
        """Convert report to dictionary."""
        return {
            'total_rows': self.total_rows,
            'valid_rows': self.valid_rows,
            'invalid_rows': self.invalid_rows,
            'success_rate': self.valid_rows / self.total_rows if self.total_rows > 0 else 0,
            'errors': self.errors,
            'warnings': self.warnings
        }