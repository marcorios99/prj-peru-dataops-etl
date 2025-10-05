"""
Test logging and metrics functionality.
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logging_config import setup_logging, get_logger
from src.utils.metrics import PipelineMetrics, MetricsCollector
from datetime import datetime
import time


def test_logging():
    """Test structured logging."""
    print("=" * 70)
    print("LOGGING TEST")
    print("=" * 70)
    
    setup_logging(log_level="INFO", log_file="logs/etl_pipeline.log")
    logger = get_logger(__name__)
    
    logger.info("pipeline_started", pipeline_id="test_001", input_file="test.csv")
    logger.info("validation_completed", rows_validated=1000, errors=5)
    logger.warning("duplicates_found", count=50)
    logger.info("data_loaded", rows=950, database="sqlite")
    
    print("\nLogs written to: logs/etl_pipeline.log")
    print("PASSED")


def test_metrics():
    """Test metrics collection."""
    print("\n" + "=" * 70)
    print("METRICS TEST")
    print("=" * 70)
    
    pipeline_id = f"test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    metrics = PipelineMetrics(
        pipeline_id=pipeline_id,
        start_time=datetime.now(),
        input_file="test.csv",
        input_rows=1000
    )
    
    time.sleep(0.5)
    
    metrics.validation_passed = 950
    metrics.validation_failed = 50
    metrics.duplicates_found = 30
    metrics.duplicates_removed = 30
    metrics.rows_loaded = 920
    metrics.rows_updated = 0
    metrics.end_time = datetime.now()
    metrics.status = "success"
    
    metrics.finalize()
    
    print(f"\nPipeline ID: {metrics.pipeline_id}")
    print(f"Processing time: {metrics.processing_time_seconds:.3f}s")
    print(f"Success rate: {metrics.rows_loaded/metrics.input_rows:.2%}")
    
    filepath = metrics.save()
    print(f"\nMetrics saved to: {filepath}")
    
    print("\nMetrics summary:")
    summary = metrics.get_summary()
    for key, value in summary.items():
        print(f"  {key}: {value}")
    
    print("\nPASSED")


def test_metrics_collector():
    """Test metrics aggregation."""
    print("\n" + "=" * 70)
    print("METRICS COLLECTOR TEST")
    print("=" * 70)
    
    collector = MetricsCollector()
    
    all_metrics = collector.get_all_metrics()
    print(f"\nTotal pipeline runs recorded: {len(all_metrics)}")
    
    if all_metrics:
        print("\nLast 3 runs:")
        for m in all_metrics[:3]:
            print(f"  {m.pipeline_id}: {m.status} - {m.rows_loaded}/{m.input_rows} rows")
        
        print("\nAggregated statistics:")
        stats = collector.get_aggregated_stats()
        for key, value in stats.items():
            print(f"  {key}: {value}")
    
    print("\nPASSED")


def main():
    test_logging()
    test_metrics()
    test_metrics_collector()
    
    print("\n" + "=" * 70)
    print("ALL TESTS PASSED")
    print("=" * 70)


if __name__ == "__main__":
    main()