"""
Main ETL pipeline orchestrator.
Executes complete data flow: Extract -> Validate -> Deduplicate -> Load
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
from datetime import datetime
import argparse

from src.validation.validators import DataValidator
from src.deduplication.dedup_engine import DeduplicationEngine
from src.loading.sql_loader import SQLLoader, DatabaseConfig
from src.utils.logging_config import setup_logging, get_logger
from src.utils.metrics import PipelineMetrics


class ETLPipeline:
    """Main ETL pipeline orchestrator."""
    
    def __init__(self, db_config: DatabaseConfig):
        self.db_config = db_config
        self.logger = get_logger(__name__)
        self.metrics = None
    
    def run(self, input_file: str, output_dir: str = "data/processed") -> PipelineMetrics:
        """
        Execute complete ETL pipeline.
        
        Args:
            input_file: Path to input CSV file
            output_dir: Directory for intermediate outputs
        
        Returns:
            PipelineMetrics with execution statistics
        """
        pipeline_id = f"etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        self.metrics = PipelineMetrics(
            pipeline_id=pipeline_id,
            start_time=datetime.now(),
            input_file=input_file
        )
        
        self.logger.info(
            "pipeline_started",
            pipeline_id=pipeline_id,
            input_file=input_file
        )
        
        try:
            df = self._extract(input_file)
            
            df_validated, validation_report = self._validate(df)
            
            df_deduped, dedup_stats = self._deduplicate(df_validated, output_dir)
            
            load_stats = self._load(df_deduped)
            
            self.metrics.end_time = datetime.now()
            self.metrics.status = "success"
            self.metrics.finalize()
            
            self.logger.info(
                "pipeline_completed",
                pipeline_id=pipeline_id,
                status="success",
                processing_time=self.metrics.processing_time_seconds
            )
            
            self._save_metrics()
            
            return self.metrics
            
        except Exception as e:
            self.metrics.end_time = datetime.now()
            self.metrics.status = "failed"
            self.metrics.error_message = str(e)
            self.metrics.finalize()
            
            self.logger.error(
                "pipeline_failed",
                pipeline_id=pipeline_id,
                error=str(e)
            )
            
            self._save_metrics()
            raise
    
    def _extract(self, input_file: str) -> pd.DataFrame:
        """Extract data from CSV file."""
        self.logger.info("extraction_started", file=input_file)
        
        df = pd.read_csv(input_file, parse_dates=['fecha_operacion'])
        self.metrics.input_rows = len(df)
        
        self.logger.info("extraction_completed", rows=len(df))
        
        return df
    
    def _validate(self, df: pd.DataFrame):
        """Validate data with schema checks."""
        self.logger.info("validation_started", input_rows=len(df))
        
        validator = DataValidator()
        validated_df, report = validator.validate(df)
        
        report_dict = report.to_dict()
        self.metrics.validation_passed = report_dict['valid_rows']
        self.metrics.validation_failed = report_dict['invalid_rows']
        self.metrics.validation_errors = report_dict['errors'][:100]
        
        self.logger.info(
            "validation_completed",
            valid_rows=report_dict['valid_rows'],
            invalid_rows=report_dict['invalid_rows'],
            success_rate=report_dict['success_rate']
        )
        
        if report_dict['warnings']:
            for warning in report_dict['warnings']:
                self.logger.warning("validation_warning", message=warning)
        
        return validated_df, report
    
    def _deduplicate(self, df: pd.DataFrame, output_dir: str):
        """Remove duplicate records."""
        self.logger.info("deduplication_started", input_rows=len(df))
        
        engine = DeduplicationEngine()
        deduped_df, stats = engine.deduplicate(df, strategy='hash')
        
        self.metrics.duplicates_found = stats['duplicates_found']
        self.metrics.duplicates_removed = stats['duplicates_removed']
        
        self.logger.info(
            "deduplication_completed",
            duplicates_found=stats['duplicates_found'],
            duplicates_removed=stats['duplicates_removed'],
            final_rows=stats['final_count']
        )
        
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        deduped_file = output_path / f"deduped_{self.metrics.pipeline_id}.csv"
        deduped_df.to_csv(deduped_file, index=False)
        
        return deduped_df, stats
    
    def _load(self, df: pd.DataFrame):
        """Load data to database."""
        self.logger.info(
            "loading_started",
            rows=len(df),
            database=self.db_config.db_type
        )
        
        loader = SQLLoader(self.db_config)
        loader.connect()
        loader.create_table('operaciones')
        
        stats = loader.upsert_data(df, 'operaciones')
        
        self.metrics.rows_loaded = stats['rows_inserted']
        self.metrics.rows_updated = stats['rows_updated']
        self.metrics.load_failed = stats['rows_failed']
        
        self.logger.info(
            "loading_completed",
            rows_inserted=stats['rows_inserted'],
            rows_updated=stats['rows_updated'],
            rows_failed=stats['rows_failed']
        )
        
        loader.close()
        
        return stats
    
    def _save_metrics(self):
        """Save pipeline metrics."""
        filepath = self.metrics.save()
        self.logger.info("metrics_saved", filepath=str(filepath))


def main():
    parser = argparse.ArgumentParser(description="Execute ETL pipeline")
    parser.add_argument(
        "--input",
        required=True,
        help="Path to input CSV file"
    )
    parser.add_argument(
        "--db-type",
        choices=['sqlite', 'sqlserver'],
        default='sqlserver',
        help="Database type"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level"
    )
    
    args = parser.parse_args()
    
    setup_logging(
        log_level=args.log_level,
        log_file=f"logs/etl_pipeline_{datetime.now().strftime('%Y%m%d')}.log"
    )
    
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
    
    pipeline = ETLPipeline(db_config)
    
    print("=" * 70)
    print("ETL PIPELINE EXECUTION")
    print("=" * 70)
    print(f"Input file: {args.input}")
    print(f"Database: {args.db_type}")
    print("=" * 70)
    
    try:
        metrics = pipeline.run(args.input)
        
        print("\n" + "=" * 70)
        print("PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 70)
        
        summary = metrics.get_summary()
        for key, value in summary.items():
            print(f"{key}: {value}")
        
        print("=" * 70)
        
    except Exception as e:
        print("\n" + "=" * 70)
        print("PIPELINE FAILED")
        print("=" * 70)
        print(f"Error: {str(e)}")
        print("=" * 70)
        sys.exit(1)


if __name__ == "__main__":
    main()