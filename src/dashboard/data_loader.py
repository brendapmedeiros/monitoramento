import json
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime
import pandas as pd

##Carrega relatórios do diretório
class ReportLoader:

    
    def __init__(self, reports_dir: str = "data/reports"):
        self.reports_dir = Path(reports_dir)
        
    def load_quality_metrics(self) -> List[Dict]:
        files = sorted(self.reports_dir.glob("quality_metrics_*.json"))
        metrics = []
        
        for file in files:
            try:
                with open(file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    metrics.append(data)
            except Exception as e:
                print(f"Erro ao ler {file}: {e}")
        
        return metrics
    
    def load_anomaly_reports(self) -> List[Dict]:
        files = sorted(self.reports_dir.glob("anomaly_report_*.json"))
        reports = []
        
        for file in files:
            try:
                with open(file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if 'details' in data and isinstance(data['details'], dict):
                                severity = data['details'].get('severity_distribution', {})
                                data['severity_high'] = severity.get('high', 0)
                                data['severity_medium'] = severity.get('medium', 0)
                                data['severity_low'] = severity.get('low', 0)
                    else:
                                data['severity_high'] = 0
                                data['severity_medium'] = 0
                                data['severity_low'] = 0

                    reports.append(data)
            except Exception as e:
                print(f"Erro ao ler {file}: {e}")
        
        return reports
    
    def load_final_reports(self) -> List[Dict]:
        files = sorted(self.reports_dir.glob("final_report_*.json"))
        reports = []
        
        for file in files:
            try:
                with open(file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    reports.append(data)
            except Exception as e:
                print(f"Erro ao ler {file}: {e}")
        
        return reports
    
    def get_latest_report(self) -> Optional[Dict]:
        reports = self.load_final_reports()
        return reports[-1] if reports else None
    
    def get_metrics_dataframe(self) -> pd.DataFrame:
        metrics = self.load_quality_metrics()
        
        if not metrics:
            return pd.DataFrame()
        
        df = pd.DataFrame([
            {
                'timestamp': m['timestamp'],
                'dataset': m['dataset_name'],
                'quality_score': m['quality_score'],
                'completeness': m['completeness'],
                'uniqueness': m['uniqueness'],
                'validity': m['validity'],
                'consistency': m['consistency'],
                'total_rows': m['total_rows']
            }
            for m in metrics
        ])
        
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')
        
        return df
    
    def get_anomalies_dataframe(self) -> pd.DataFrame:
        reports = self.load_anomaly_reports()

        if not reports:
            return pd.DataFrame()

        rows = []

        for r in reports:
            severity = (
                r.get('details', {}).get('severity_distribution')
                or r.get('details', {}).get('distribuicao_severity')
                or {}
            )

            rows.append({
                'timestamp': r['timestamp'],
                'dataset': r['dataset_name'],
                'total_anomalies': r['total_anomalies'],
                'anomaly_percentage': r['anomaly_percentage'],
                'zscore': r['anomalies_by_method'].get('zscore', 0),
                'iqr': r['anomalies_by_method'].get('iqr', 0),
                'isolation_forest': r['anomalies_by_method'].get('isolation_forest', 0),
                'severity_high': severity.get('high', 0),
                'severity_medium': severity.get('medium', 0),
                'severity_low': severity.get('low', 0),
            })

        df = pd.DataFrame(rows)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df.sort_values('timestamp')
    
            ## Retorna resultados resumidos
    def get_summary_stats(self) -> Dict:
        latest = self.get_latest_report()
        
        if not latest:
            return {
                'status': 'Sem dados',
                'last_execution': None,
                'quality_score': 0,
                'total_anomalies': 0,
                'total_executions': 0
            }
        
        all_reports = self.load_final_reports()
        
        return {
            'status': latest['summary']['status'],
            'last_execution': latest['execution_info']['timestamp'],
            'quality_score': latest['quality_metrics']['quality_score'],
            'total_anomalies': latest['anomaly_report']['total_anomalies'],
            'anomaly_percentage': latest['anomaly_report']['anomaly_percentage'],
            'total_executions': len(all_reports),
            'alert_sent': latest['alert_status']['status'] == 'success'
        }
    
    def get_time_range(self) -> tuple:
        df = self.get_metrics_dataframe()
        
        if df.empty:
            return None, None
        
        return df['timestamp'].min(), df['timestamp'].max()