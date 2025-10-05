"""
Test deduplication engine.
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
from src.deduplication.dedup_engine import DeduplicationEngine, AdvancedDeduplicator


def main():
    validated_path = project_root / 'data' / 'processed' / 'validated_operations.csv'
    
    if not validated_path.exists():
        print(f"Error: File not found: {validated_path}")
        print("Run 'python tests/test_validation.py' first")
        return
    
    print("=" * 70)
    print("DEDUPLICATION TEST")
    print("=" * 70)
    
    print(f"\nLoading: {validated_path}")
    df = pd.read_csv(validated_path, parse_dates=['fecha_operacion'])
    print(f"Loaded {len(df):,} rows")
    
    print("\n" + "-" * 70)
    print("TEST 1: Hash-based Deduplication")
    print("-" * 70)
    
    engine = DeduplicationEngine()
    deduped_df, stats = engine.deduplicate(df, strategy='hash')
    
    print(f"\nInput rows: {stats['total_input']:,}")
    print(f"Duplicates found: {stats['duplicates_found']:,}")
    print(f"Duplicates removed: {stats['duplicates_removed']:,}")
    print(f"Final rows: {stats['final_count']:,}")
    print(f"Duplicate rate: {stats['duplicates_found']/stats['total_input']:.2%}")
    
    print("\n" + "-" * 70)
    print("TEST 2: Key-based Deduplication")
    print("-" * 70)
    
    engine2 = DeduplicationEngine()
    deduped_df2, stats2 = engine2.deduplicate(
        df,
        strategy='key',
        key_columns=['numero_operacion']
    )
    
    print(f"\nInput rows: {stats2['total_input']:,}")
    print(f"Duplicates found: {stats2['duplicates_found']:,}")
    print(f"Duplicates removed: {stats2['duplicates_removed']:,}")
    print(f"Final rows: {stats2['final_count']:,}")
    
    print("\n" + "-" * 70)
    print("TEST 3: Find Duplicate Groups")
    print("-" * 70)
    
    duplicates = engine.find_duplicates(df, columns=['numero_operacion'])
    print(f"\nFound {len(duplicates):,} duplicate records")
    
    if len(duplicates) > 0:
        print("\nSample duplicate group:")
        sample_op = duplicates['numero_operacion'].iloc[0]
        sample_group = duplicates[duplicates['numero_operacion'] == sample_op]
        print(sample_group[['numero_operacion', 'monto', 'fecha_operacion', 'estado']])
    
    print("\n" + "-" * 70)
    print("TEST 4: Advanced Deduplication with Priority")
    print("-" * 70)
    
    adv_dedup = AdvancedDeduplicator()
    priority_deduped = adv_dedup.deduplicate_with_priority(
        df,
        key_columns=['numero_operacion'],
        priority_column='fecha_operacion',
        priority_order='desc'
    )
    
    print(f"\nInput rows: {len(df):,}")
    print(f"After priority dedup: {len(priority_deduped):,}")
    print(f"Removed: {len(df) - len(priority_deduped):,}")
    
    output_path = project_root / 'data' / 'processed' / 'deduplicated_operations.csv'
    deduped_df.to_csv(output_path, index=False)
    print(f"\nDeduplicated data saved to: {output_path}")
    
    print("\nSample of deduplicated data:")
    print(deduped_df[['numero_operacion', 'monto', 'fecha_operacion', 'estado']].head(10))
    
    print("\n" + "=" * 70)
    print("DEDUPLICATION COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()