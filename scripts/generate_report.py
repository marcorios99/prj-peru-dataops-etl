"""
Generate Excel report from pipeline metrics.
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import json
import argparse

from src.reporting.excel_report import ExcelReportGenerator, SummaryReportGenerator
from src.loading.sql_loader import SQLLoader, DatabaseConfig


def main():
    parser = argparse.ArgumentParser(description="Generate pipeline report")
    parser.add_argument(
        "--pipeline-id",
        required=True,
        help="Pipeline ID to generate report for"
    )
    parser.add_argument(
        "--db-type",
        choices=['sqlite', 'sqlserver'],
        default='sqlserver',
        help="Database type"
    )
    
    args = parser.parse_args()
    
    metrics_file = project_root / 'data' / 'output' / 'metrics' / f'metrics_{args.pipeline_id}.json'
    
    if not metrics_file.exists():
        print(f"Error: Metrics file not found: {metrics_file}")
        sys.exit(1)
    
    with open(metrics_file, 'r') as f:
        metrics = json.load(f)
    
    if args.db_type == 'sqlserver':
        db_config = DatabaseConfig(
            db_type='sqlserver',
            host=r'PC-MARCO\SQLEXPRESS',
            database='ETL_Conciliacion',
            trusted_connection=True
        )
    else:
        db_config = DatabaseConfig(
            db_type='sqlite',
            database='data/etl_conciliacion.db'
        )
    
    loader = SQLLoader(db_config)
    loader.connect()
    data_sample = loader.query_data('operaciones', limit=1000)
    loader.close()
    
    print("=" * 70)
    print("GENERANDO REPORTE")
    print("=" * 70)
    
    text_summary = SummaryReportGenerator.generate_text_summary(metrics)
    print(text_summary)
    
    excel_gen = ExcelReportGenerator()
    excel_path = excel_gen.generate_pipeline_report(metrics, data_sample, args.pipeline_id)
    
    print(f"Reporte Excel generado: {excel_path}")
    print("=" * 70)


if __name__ == "__main__":
    main()