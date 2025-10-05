"""
Metrics tracking for ETL pipeline.
Records performance metrics and statistics.
"""

from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Dict, List, Optional
import json
from pathlib import Path
import pandas as pd

@dataclass
class PipelineMetrics:
    """Container for pipeline execution metrics."""
    
    pipeline_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    
    input_file: str = ""
    input_rows: int = 0
    
    validation_passed: int = 0
    validation_failed: int = 0
    validation_errors: List[Dict] = field(default_factory=list)
    
    duplicates_found: int = 0
    duplicates_removed: int = 0
    
    rows_loaded: int = 0
    rows_updated: int = 0
    load_failed: int = 0
    
    processing_time_seconds: float = 0.0
    status: str = "running"
    error_message: str = ""
    
    def finalize(self):
        """Calculate final metrics."""
        if self.end_time:
            self.processing_time_seconds = (
                self.end_time - self.start_time
            ).total_seconds()
    
    def to_dict(self) -> Dict:
        """Convert metrics to dictionary."""
        data = asdict(self)
        data['start_time'] = self.start_time.isoformat()
        if self.end_time:
            data['end_time'] = self.end_time.isoformat()
        return data
    
    def save(self, output_dir: str = "data/output/metrics"):
        """Save metrics to JSON file."""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        filename = f"metrics_{self.pipeline_id}.json"
        filepath = output_path / filename
        
        def convert_types(obj):
            """Convert numpy/pandas types to Python native types."""
            import numpy as np
            if isinstance(obj, (np.integer, np.int64)):
                return int(obj)
            elif isinstance(obj, (np.floating, np.float64)):
                return float(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            elif pd.isna(obj):
                return None
            return obj
        
        data = self.to_dict()
        
        # Convert all numeric types
        for key, value in data.items():
            if isinstance(value, (list, dict)):
                continue
            data[key] = convert_types(value)
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        
        return filepath
    
    def get_summary(self) -> Dict:
        """Get summary statistics."""
        success_rate = 0
        if self.input_rows > 0:
            success_rate = self.rows_loaded / self.input_rows
        
        return {
            'pipeline_id': self.pipeline_id,
            'status': self.status,
            'total_input': self.input_rows,
            'total_loaded': self.rows_loaded,
            'success_rate': f"{success_rate:.2%}",
            'duplicates_removed': self.duplicates_removed,
            'validation_errors': len(self.validation_errors),
            'processing_time': f"{self.processing_time_seconds:.2f}s"
        }


class MetricsCollector:
    """Collects and aggregates metrics from multiple pipeline runs."""
    
    def __init__(self, storage_dir: str = "data/output/metrics"):
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
    
    def load_metrics(self, pipeline_id: str) -> Optional[PipelineMetrics]:
        """Load metrics for a specific pipeline run."""
        filepath = self.storage_dir / f"metrics_{pipeline_id}.json"
        
        if not filepath.exists():
            return None
        
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        data['start_time'] = datetime.fromisoformat(data['start_time'])
        if data.get('end_time'):
            data['end_time'] = datetime.fromisoformat(data['end_time'])
        
        return PipelineMetrics(**data)
    
    def get_all_metrics(self) -> List[PipelineMetrics]:
        """Load all stored metrics."""
        metrics_list = []
        
        for filepath in self.storage_dir.glob("metrics_*.json"):
            with open(filepath, 'r') as f:
                data = json.load(f)
            
            data['start_time'] = datetime.fromisoformat(data['start_time'])
            if data.get('end_time'):
                data['end_time'] = datetime.fromisoformat(data['end_time'])
            
            metrics_list.append(PipelineMetrics(**data))
        
        return sorted(metrics_list, key=lambda m: m.start_time, reverse=True)
    
    def get_aggregated_stats(self) -> Dict:
        """Get aggregated statistics across all runs."""
        all_metrics = self.get_all_metrics()
        
        if not all_metrics:
            return {}
        
        total_runs = len(all_metrics)
        successful_runs = sum(1 for m in all_metrics if m.status == "success")
        total_rows_processed = sum(m.input_rows for m in all_metrics)
        total_rows_loaded = sum(m.rows_loaded for m in all_metrics)
        avg_processing_time = sum(m.processing_time_seconds for m in all_metrics) / total_runs
        
        return {
            'total_runs': total_runs,
            'successful_runs': successful_runs,
            'success_rate': f"{successful_runs/total_runs:.2%}",
            'total_rows_processed': total_rows_processed,
            'total_rows_loaded': total_rows_loaded,
            'avg_processing_time': f"{avg_processing_time:.2f}s",
            'last_run': all_metrics[0].start_time.isoformat() if all_metrics else None
        }