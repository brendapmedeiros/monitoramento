import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, asdict
from datetime import datetime
import logging
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import json
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class RelatorioAnomalias:
    timestamp: str
    dataset_name: str
    total_rows: int
    total_anomalies: int
    anomaly_percentage: float
    methods_used: List[str]
    anomalies_by_method: Dict[str, int]
    anomalies_by_column: Dict[str, int]
    details: Dict


class DetectorAnomalias:
    """
    Classe principal para detecção de anomalias
    Implementa múltiplos algoritmos de detecção
    """
    
    def __init__(self, dataset_name: str = "default", contamination: float = 0.1):
        """
        Inicializa o detector de anomalias
        
        Args:
            dataset_name: Nome do dataset
            contamination: Proporção esperada de anomalias (0.1 = 10%)
        """
        self.dataset_name = dataset_name
        self.contamination = contamination
        self.anomaly_history = []
        logger.info(f"AnomalyDetector inicializado para: {dataset_name}")
    
    def detect_all(self, df: pd.DataFrame, 
                   methods: List[str] = None) -> RelatorioAnomalias:
        """
        Executa todos os métodos de detecção de anomalias
        
        Args:
            df: DataFrame para análise
            methods: Lista de métodos a usar. Se None, usa todos.
                    Opções: ['zscore', 'iqr', 'isolation_forest']
        
        Returns:
            AnomalyReport com resultados de todas as detecções
        """
        if methods is None:
            methods = ['zscore', 'iqr', 'isolation_forest']
        
        logger.info(f"Iniciando detecção de anomalias com métodos: {methods}")
        
        anomalies_by_method = {}
        anomalies_by_column = {}
        all_anomaly_indices = set()
        
        # Z-Score
        if 'zscore' in methods:
            zscore_anomalies = self.detect_zscore(df)
            anomalies_by_method['zscore'] = len(zscore_anomalies)
            all_anomaly_indices.update(zscore_anomalies)
            self._count_by_column(df, zscore_anomalies, anomalies_by_column, 'zscore')
        
        # IQR 
        if 'iqr' in methods:
            iqr_anomalies = self.detect_iqr(df)
            anomalies_by_method['iqr'] = len(iqr_anomalies)
            all_anomaly_indices.update(iqr_anomalies)
            self._count_by_column(df, iqr_anomalies, anomalies_by_column, 'iqr')
        
        # Isolation Forest
        if 'isolation_forest' in methods:
            if_anomalies = self.detect_isolation_forest(df)
            anomalies_by_method['isolation_forest'] = len(if_anomalies)
            all_anomaly_indices.update(if_anomalies)
        
        total_anomalies = len(all_anomaly_indices)
        anomaly_percentage = (total_anomalies / len(df)) * 100
        
        # Cria o relatório
        report = RelatorioAnomalias(
            timestamp=datetime.now().isoformat(),
            dataset_name=self.dataset_name,
            total_rows=len(df),
            total_anomalies=total_anomalies,
            anomaly_percentage=round(anomaly_percentage, 2),
            methods_used=methods,
            anomalies_by_method=anomalies_by_method,
            anomalies_by_column=anomalies_by_column,
            details={
                'indices_anomalias': list(all_anomaly_indices),
                'distribuicao_severity': self._calculate_severity(df, all_anomaly_indices)
            }
        )
        
        self.anomaly_history.append(report)
        logger.info(f"Detecção concluída: {total_anomalies} anomalias ({anomaly_percentage:.2f}%)")
        
        return report
    
    def detect_zscore(self, df: pd.DataFrame, threshold: float = 3.0) -> set:
        """
        Detecta anomalias usando Z-Score
        Valores com |z-score| > threshold são considerados anomalias
        
        Args:
            df: DataFrame para análise
            threshold: limiar do z-score (padrão: 3.0)
        
        Returns:
            Set com índices das linhas divergentes
        """
        anomaly_indices = set()
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            if df[col].std() == 0:  
                continue
            
            z_scores = np.abs((df[col] - df[col].mean()) / df[col].std())
            col_anomalies = df[z_scores > threshold].index.tolist()
            anomaly_indices.update(col_anomalies)
            
            if col_anomalies:
                logger.debug(f"Z-Score: {len(col_anomalies)} anomalias em '{col}'")
        
        return anomaly_indices
    
    def detect_iqr(self, df: pd.DataFrame, multiplier: float = 1.5) -> set:
        """
        Detecta anomalias usando IQR 
        Valores fora de [Q1 - multiplier*IQR, Q3 + multiplier*IQR] são anomalias
        
        Args:
            df: DataFrame para análise
            multiplier: Multiplicador do IQR (padrão: 1.5)
        
        Returns:
            Set com índices das linhas com divergência
        """
        anomaly_indices = set()
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            
            lower_bound = Q1 - multiplier * IQR
            upper_bound = Q3 + multiplier * IQR
            
            col_anomalies = df[(df[col] < lower_bound) | (df[col] > upper_bound)].index.tolist()
            anomaly_indices.update(col_anomalies)
            
            if col_anomalies:
                logger.debug(f"IQR: {len(col_anomalies)} anomalias em '{col}'")
        
        return anomaly_indices
    
    def detect_isolation_forest(self, df: pd.DataFrame) -> set:
        """
        Detecta anomalias usando IsoForest
        
        Args:
            df: DataFrame para análise
        
        Returns:
            Set com índices das linhas anômalas
        """
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        if len(numeric_cols) == 0:
            logger.warning("Nenhuma coluna numérica para Isolation Forest")
            return set()

        X = df[numeric_cols].fillna(df[numeric_cols].mean())
        
        # Normalizar
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        # Treinar modelo
        iso_forest = IsolationForest(
            contamination=self.contamination,
            random_state=42,
            n_estimators=100
        )
        
        predictions = iso_forest.fit_predict(X_scaled)
        
        # -1 = anomalia, 1 = normal
        anomaly_indices = set(df[predictions == -1].index.tolist())
        
        logger.debug(f"Isolation Forest: {len(anomaly_indices)} anomalias detectadas")
        
        return anomaly_indices
    
    def detect_drift(self, df_current: pd.DataFrame, 
                    df_reference: pd.DataFrame,
                    threshold: float = 0.1) -> Dict:
        """
        Detecta drift (mudança na distribuição dos dados)
        Compara dataset atual com dataset de referência
        
        Args:
            df_current: DataFrame atual
            df_reference: DataFrame de referência
            threshold: Limiar aceitável (0.1 = 10%)
        
        Returns:
            Dict com infos sobre drift detectado
        """
        drift_report = {
            'drift_detected': False,
            'columns_with_drift': [],
            'drift_scores': {}
        }
        
        numeric_cols = df_current.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            if col not in df_reference.columns:
                continue
            
            # mudança na média e desvio padrão
            mean_current = df_current[col].mean()
            mean_reference = df_reference[col].mean()
            
            std_current = df_current[col].std()
            std_reference = df_reference[col].std()
            
            # drift score 
            mean_change = abs(mean_current - mean_reference) / (abs(mean_reference) + 1e-10)
            std_change = abs(std_current - std_reference) / (abs(std_reference) + 1e-10)
            
            drift_score = (mean_change + std_change) / 2
            
            drift_report['drift_scores'][col] = round(drift_score, 4)
            
            if drift_score > threshold:
                drift_report['drift_detected'] = True
                drift_report['columns_with_drift'].append({
                    'column': col,
                    'drift_score': round(drift_score, 4),
                    'mean_change': round(mean_change * 100, 2),
                    'std_change': round(std_change * 100, 2)
                })
                
                logger.warning(f"Drift detectado em '{col}': score={drift_score:.4f}")
        
        return drift_report
    
    def get_anomaly_details(self, df: pd.DataFrame, 
                           anomaly_indices: set) -> pd.DataFrame:
        """
        Retorna DataFrame com detalhes das anomalias detectadas
        
        Args:
            df: DataFrame original
            anomaly_indices: Índices das anomalias
        
        Returns:
            DataFrame contendo apenas as linhas anômalas
        """
        return df.loc[list(anomaly_indices)]
    
    def _count_by_column(self, df: pd.DataFrame, anomaly_indices: set, 
                        counter: Dict, method: str):
        """Conta anomalias por coluna"""
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            key = f"{col}_{method}"
            counter[key] = counter.get(key, 0) + len(anomaly_indices)
    
    def _calculate_severity(self, df: pd.DataFrame, 
                           anomaly_indices: set) -> Dict:
        """Calcula distribuição dA severidade das anomalias"""
        if not anomaly_indices:
            return {'low': 0, 'medium': 0, 'high': 0}
        
        # Severity baseada na quantidade de colunas afetadas por linha
        severity = {'low': 0, 'medium': 0, 'high': 0}
        
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        for idx in anomaly_indices:
            affected_cols = 0
            for col in numeric_cols:
                z_score = abs((df.loc[idx, col] - df[col].mean()) / (df[col].std() + 1e-10))
                if z_score > 3:
                    affected_cols += 1
            
            # Classificar severidade
            if affected_cols >= len(numeric_cols) * 0.5:
                severity['high'] += 1
            elif affected_cols >= len(numeric_cols) * 0.2:
                severity['medium'] += 1
            else:
                severity['low'] += 1
        
        return severity
    
    def save_report(self, report: RelatorioAnomalias, filepath: str):
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(asdict(report), f, indent=2, ensure_ascii=False)
            logger.info(f"Relatório salvo: {filepath}")
        except Exception as e:
            logger.error(f"Erro ao salvar relatório: {e}")
    
    def print_report(self, report: RelatorioAnomalias):

        print(f"""

        RELATÓRIO DE DETECÇÃO DE ANOMALIAS             

 Dataset: {report.dataset_name:<43}
 Timestamp: {report.timestamp:<41} 
 Total de linhas: {report.total_rows:<37} 

 ANOMALIAS DETECTADAS:                                  
   Total: {report.total_anomalies:<45} 
   Percentual: {report.anomaly_percentage}%{' '*(39-len(str(report.anomaly_percentage)))} 

 POR MÉTODO:                                            """)
        
        for method, count in report.anomalies_by_method.items():
            print(f"   {method}: {count:<44} ")
        
        print(f"""
 SEVERIDADE:                                            
   Alta: {report.details['severity_distribution']['high']:<46} 
   Média: {report.details['severity_distribution']['medium']:<45} 
   Baixa: {report.details['severity_distribution']['low']:<45} 

""")



if __name__ == "__main__":
    np.random.seed(42)
    
    # Dados normais
    normal_data = {
        'age': np.random.normal(35, 10, 95).astype(int),
        'salary': np.random.normal(50000, 15000, 95),
        'score': np.random.normal(75, 10, 95)
    }
    
    # Injetar divergências 
    anomaly_data = {
        'age': [150, -5, 200, 0, 250],  
        'salary': [500000, -10000, 1000000, 0, 800000],  
        'score': [150, -20, 200, -50, 180]  
    }
    
    # Agregar informaações
    df = pd.DataFrame(normal_data)
    df_anomalies = pd.DataFrame(anomaly_data)
    df = pd.concat([df, df_anomalies], ignore_index=True)
    
    print("Dataset criado com 100 linhas \n")
    
   # Gera o relatório
    detector = DetectorAnomalias(dataset_name="customers", contamination=0.1)
    report = detector.detect_all(df)
    detector.print_report(report)
    output_dir = Path("data/anomalies")
    output_dir.mkdir(parents=True, exist_ok=True)
    detector.save_report(report, "data/anomalies/anomaly_report.json")
    
    # Mostra anomalias detectadas
    anomaly_indices = set(report.details['anomaly_indices'])
    anomalies_df = detector.get_anomaly_details(df, anomaly_indices)
    
    print("\n Anomalias identificadas:")
    print(anomalies_df)
    