"""
Deduplication engine for ETL pipeline.
Implements hash-based and business-rule deduplication strategies.
"""

import pandas as pd
import hashlib
from typing import Tuple, List, Dict
from datetime import datetime


class DeduplicationEngine:
    """Engine for identifying and removing duplicate records."""
    
    def __init__(self):
        self.dedup_stats = {
            'total_input': 0,
            'duplicates_found': 0,
            'duplicates_removed': 0,
            'final_count': 0
        }
    
    def deduplicate(
        self,
        df: pd.DataFrame,
        strategy: str = 'hash',
        key_columns: List[str] = None
    ) -> Tuple[pd.DataFrame, Dict]:
        """
        Remove duplicates from DataFrame.
        
        Args:
            df: Input DataFrame
            strategy: 'hash' (content-based) or 'key' (business key)
            key_columns: Columns to use for key-based deduplication
        
        Returns:
            Tuple of (deduplicated_df, statistics)
        """
        self.dedup_stats['total_input'] = len(df)
        
        if strategy == 'hash':
            result_df = self._deduplicate_by_hash(df)
        elif strategy == 'key':
            if key_columns is None:
                key_columns = ['numero_operacion']
            result_df = self._deduplicate_by_key(df, key_columns)
        else:
            raise ValueError(f"Unknown strategy: {strategy}")
        
        self.dedup_stats['final_count'] = len(result_df)
        self.dedup_stats['duplicates_removed'] = (
            self.dedup_stats['total_input'] - self.dedup_stats['final_count']
        )
        
        return result_df, self.dedup_stats.copy()
    
    def _deduplicate_by_hash(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Deduplicate using content hash.
        Removes exact duplicates based on hash of critical fields.
        """
        if 'content_hash' not in df.columns:
            df = self._generate_content_hash(df)
        
        duplicates_mask = df.duplicated(subset=['content_hash'], keep='first')
        self.dedup_stats['duplicates_found'] = duplicates_mask.sum()
        
        return df[~duplicates_mask].reset_index(drop=True)
    
    def _deduplicate_by_key(
        self,
        df: pd.DataFrame,
        key_columns: List[str]
    ) -> pd.DataFrame:
        """
        Deduplicate using business key columns.
        Keeps first occurrence of each key.
        """
        duplicates_mask = df.duplicated(subset=key_columns, keep='first')
        self.dedup_stats['duplicates_found'] = duplicates_mask.sum()
        
        return df[~duplicates_mask].reset_index(drop=True)
    
    def _generate_content_hash(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generate content hash if not already present."""
        hash_columns = ['numero_operacion', 'monto', 'fecha_operacion', 'cuenta_origen']
        
        def create_hash(row):
            try:
                content = ''.join(str(row[col]) for col in hash_columns if col in df.columns)
                return hashlib.sha256(content.encode()).hexdigest()[:16]
            except Exception:
                return None
        
        df['content_hash'] = df.apply(create_hash, axis=1)
        return df
    
    def find_duplicates(
        self,
        df: pd.DataFrame,
        columns: List[str] = None
    ) -> pd.DataFrame:
        """
        Find and return duplicate records.
        
        Args:
            df: Input DataFrame
            columns: Columns to check for duplicates
        
        Returns:
            DataFrame containing only duplicate records
        """
        if columns is None:
            columns = ['numero_operacion']
        
        duplicates_mask = df.duplicated(subset=columns, keep=False)
        duplicates_df = df[duplicates_mask].sort_values(by=columns)
        
        return duplicates_df
    
    def get_dedup_report(self) -> Dict:
        """
        Generate detailed deduplication report.
        
        Returns:
            Dictionary with deduplication statistics
        """
        if self.dedup_stats['total_input'] > 0:
            duplicate_rate = (
                self.dedup_stats['duplicates_removed'] / 
                self.dedup_stats['total_input']
            )
        else:
            duplicate_rate = 0
        
        return {
            **self.dedup_stats,
            'duplicate_rate': duplicate_rate,
            'retention_rate': 1 - duplicate_rate
        }


class AdvancedDeduplicator:
    """Advanced deduplication with conflict resolution."""
    
    def deduplicate_with_priority(
        self,
        df: pd.DataFrame,
        key_columns: List[str],
        priority_column: str = 'fecha_operacion',
        priority_order: str = 'desc'
    ) -> pd.DataFrame:
        """
        Deduplicate keeping record with highest priority.
        
        Args:
            df: Input DataFrame
            key_columns: Columns defining uniqueness
            priority_column: Column to determine which record to keep
            priority_order: 'asc' or 'desc'
        
        Returns:
            Deduplicated DataFrame
        """
        ascending = priority_order == 'asc'
        
        df_sorted = df.sort_values(
            by=key_columns + [priority_column],
            ascending=[True] * len(key_columns) + [ascending]
        )
        
        return df_sorted.drop_duplicates(
            subset=key_columns,
            keep='first'
        ).reset_index(drop=True)
    
    def merge_duplicates(
        self,
        df: pd.DataFrame,
        key_columns: List[str],
        merge_strategy: Dict[str, str]
    ) -> pd.DataFrame:
        """
        Merge duplicate records using specified strategy per column.
        
        Args:
            df: Input DataFrame
            key_columns: Columns defining duplicate groups
            merge_strategy: Dict mapping column -> strategy ('first', 'last', 'max', 'min', 'sum')
        
        Returns:
            DataFrame with merged duplicates
        """
        agg_dict = {}
        
        for col in df.columns:
            if col in key_columns:
                agg_dict[col] = 'first'
            elif col in merge_strategy:
                agg_dict[col] = merge_strategy[col]
            else:
                agg_dict[col] = 'first'
        
        return df.groupby(key_columns, as_index=False).agg(agg_dict)