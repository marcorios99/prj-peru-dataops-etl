"""
Test validation module with synthetic data.
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
from src.validation.validators import DataValidator


def main():
    csv_path = project_root / 'data' / 'input' / 'operaciones_demo_2025.csv'
    
    if not csv_path.exists():
        print(f"Error: File not found: {csv_path}")
        print("Run 'python data/synthetic/generator.py' first to generate data")
        return
    
    print("=" * 70)
    print("DATA VALIDATION TEST")
    print("=" * 70)
    
    print(f"\nLoading: {csv_path}")
    df = pd.read_csv(csv_path)
    print(f"Loaded {len(df):,} rows")
    
    print("\nColumn dtypes before parsing:")
    print(df.dtypes)
    
    print("\nParsing dates...")
    try:
        df['fecha_operacion'] = pd.to_datetime(df['fecha_operacion'], errors='coerce')
    except Exception as e:
        print(f"Warning: Could not parse all dates: {e}")
    
    print("\nInitializing validator...")
    validator = DataValidator()
    
    print("Running validation...")
    try:
        validated_df, report = validator.validate(df)
    except Exception as e:
        print(f"Validation failed with error: {e}")
        import traceback
        traceback.print_exc()
        return
    
    print("\n" + "=" * 70)
    print("VALIDATION RESULTS")
    print("=" * 70)
    
    report_dict = report.to_dict()
    
    print(f"\nTotal rows processed: {report_dict['total_rows']:,}")
    print(f"Valid rows: {report_dict['valid_rows']:,}")
    print(f"Invalid rows: {report_dict['invalid_rows']:,}")
    print(f"Success rate: {report_dict['success_rate']:.2%}")
    
    if report_dict['errors']:
        print(f"\nErrors found: {len(report_dict['errors'])}")
        print("\nFirst 10 errors:")
        for i, error in enumerate(report_dict['errors'][:10], 1):
            print(f"  {i}. Row {error['row']}, Column '{error['column']}': {error['error']}")
        
        if len(report_dict['errors']) > 10:
            print(f"  ... and {len(report_dict['errors']) - 10} more errors")
    
    if report_dict['warnings']:
        print(f"\nWarnings: {len(report_dict['warnings'])}")
        for warning in report_dict['warnings']:
            print(f"  - {warning}")
    
    print("\nChecking duplicates...")
    validated_df = validator.validate_duplicates(validated_df)
    
    print("\nGenerating content hashes...")
    validated_df = validator.generate_content_hash(validated_df)
    
    output_dir = project_root / 'data' / 'processed'
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / 'validated_operations.csv'
    
    validated_df.to_csv(output_path, index=False)
    print(f"\nValidated data saved to: {output_path}")
    
    print("\nSample of validated data:")
    print(validated_df.head())
    
    print("\n" + "=" * 70)
    print("VALIDATION COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()