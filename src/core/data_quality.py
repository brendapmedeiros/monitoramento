"""
Sistema de Métricas de Data Quality
Calcula e monitora métricas de qualidade de dados
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any
from datetime import datetime
import logging
from dataclasses import dataclass, asdict
import json

# Config log
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

## Classe para armazenar métricas de qualidade
@dataclass
class QualityMetrics:
    timestamp: str
    dataset_name: str
    total_rows: int
    total_columns: int
    completeness: float
    uniqueness: float
    validity: float
    consistency: float
    quality_score: float
    details: Dict[str, Any]


 
## Cálculo de métricas de qualidade
    
class DataQualityMetrics:
    
    def __init__(self, dataset_name: str = "default"):
        self.dataset_name = dataset_name
        self.metrics_history: List[QualityMetrics] = []
        logger.info(f"Inicializado DataQualityMetrics para dataset: {dataset_name}")
    
    def calculate_completeness(self, df: pd.DataFrame) -> Dict[str, float]:
        """
        Calcula porcentagem de valores não nulos
        
        Args:
            df: DataFrame para análise
            
        Returns:
            Dict com completude geral e por coluna
        """
        total_cells = df.shape[0] * df.shape[1]
        non_null_cells = df.count().sum()
        overall_completeness = (non_null_cells / total_cells) * 100
        
        # Completude por coluna
        column_completeness = {}
        for col in df.columns:
            completeness = (df[col].count() / len(df)) * 100
            column_completeness[col] = round(completeness, 2)
        
        logger.info(f"Completude calculada: {overall_completeness:.2f}%")
        
        return {
            'overall': round(overall_completeness, 2),
            'by_column': column_completeness
        }
    
    def calculate_uniqueness(self, df: pd.DataFrame, 
                           key_columns: Optional[List[str]] = None) -> Dict[str, float]:
        """
        Calcula porcentagem de registros únicos
        
        Args:
            df: DataFrame para análise
            key_columns: Colunas chave para verificar duplicatas
            
        Returns:
            Dict com unicidade geral e por coluna
        """
        # Unicidade geral
        if key_columns:
            duplicates = df.duplicated(subset=key_columns).sum()
        else:
            duplicates = df.duplicated().sum()
        
        uniqueness = ((len(df) - duplicates) / len(df)) * 100
        
        # Unicidade por coluna
        column_uniqueness = {}
        for col in df.columns:
            unique_pct = (df[col].nunique() / len(df)) * 100
            column_uniqueness[col] = round(unique_pct, 2)
        
        logger.info(f"Unicidade calculada: {uniqueness:.2f}%")
        
        return {
            'overall': round(uniqueness, 2),
            'duplicates_count': int(duplicates),
            'by_column': column_uniqueness
        }
    
    def calculate_validity(self, df: pd.DataFrame, 
                          validation_rules: Optional[Dict[str, Any]] = None) -> Dict[str, float]:
        """
        Calcula porcentagem de dados que seguem regras de negócio
        
        Args:
            df: DataFrame para análise
            validation_rules: Dicionário com regras de validação
            
        Returns:
            Dict com validade geral e por regra
        """
        if not validation_rules:
            # Validações básicas 
            validation_rules = self._get_default_validations(df)
        
        valid_counts = []
        rule_results = {}
        
        for rule_name, rule_func in validation_rules.items():
            try:
                valid = rule_func(df)
                valid_count = valid.sum() if isinstance(valid, pd.Series) else valid
                valid_pct = (valid_count / len(df)) * 100
                rule_results[rule_name] = round(valid_pct, 2)
                valid_counts.append(valid_pct)
                logger.debug(f"Regra '{rule_name}': {valid_pct:.2f}% válido")
            except Exception as e:
                logger.error(f"Erro ao aplicar regra '{rule_name}': {str(e)}")
                rule_results[rule_name] = 0.0
        
        overall_validity = np.mean(valid_counts) if valid_counts else 0.0
        
        logger.info(f"Validade calculada: {overall_validity:.2f}%")
        
        return {
            'overall': round(overall_validity, 2),
            'by_rule': rule_results
        }
    
    def calculate_consistency(self, df: pd.DataFrame) -> Dict[str, float]:
        """
        Calcula pocentagem de dados seguem formatos e padrões esperados
        
        Args:
            df: DataFrame para análise
            
        Returns:
            Dict com consistência geral e por tipo
        """
        consistency_checks = []
        details = {}
        
        # Verifica consistência de tipos
        for col in df.columns:
            dtype_consistency = self._check_dtype_consistency(df[col])
            consistency_checks.append(dtype_consistency)
            details[f'{col}_dtype'] = round(dtype_consistency, 2)
        
        # Verifica range de valores numéricos
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            range_consistency = self._check_range_consistency(df[col])
            consistency_checks.append(range_consistency)
            details[f'{col}_range'] = round(range_consistency, 2)
        
        overall_consistency = np.mean(consistency_checks) if consistency_checks else 100.0
        
        logger.info(f"Consistência calculada: {overall_consistency:.2f}%")
        
        return {
            'overall': round(overall_consistency, 2),
            'details': details
        }
    
    def calculate_quality_score(self, metrics: Dict[str, Dict]) -> float:
        """
        Calcula score geral de qualidade (0-100)
        Usa média ponderada das métricas principais
        
        Args:
            metrics: Dict com todas as métricas calculadas
            
        Returns:
            Score de qualidade (0-100)
        """
        weights = {
            'completeness': 0.30,
            'uniqueness': 0.25,
            'validity': 0.25,
            'consistency': 0.20
        }
        
        score = (
            metrics['completeness']['overall'] * weights['completeness'] +
            metrics['uniqueness']['overall'] * weights['uniqueness'] +
            metrics['validity']['overall'] * weights['validity'] +
            metrics['consistency']['overall'] * weights['consistency']
        )
        
        logger.info(f"Quality Score calculado: {score:.2f}")
        
        return round(score, 2)
    
    def analyze_dataset(self, df: pd.DataFrame, 
                       key_columns: Optional[List[str]] = None,
                       validation_rules: Optional[Dict] = None) -> QualityMetrics:
        """
        Executa análise completa de qualidade do dataset
        
        Args:
            df: DataFrame para análise
            key_columns: Colunas chave para verificar duplicatas
            validation_rules: Regras customizadas de validação
            
        Returns:
            QualityMetrics com todas as métricas calculadas
        """
        logger.info(f"Iniciando análise de qualidade do dataset '{self.dataset_name}'")
        logger.info(f"Dimensões: {df.shape[0]} linhas x {df.shape[1]} colunas")
        
        # Calcula todas as métricas
        completeness = self.calculate_completeness(df)
        uniqueness = self.calculate_uniqueness(df, key_columns)
        validity = self.calculate_validity(df, validation_rules)
        consistency = self.calculate_consistency(df)
        
        # Monta dict de métricas
        metrics_dict = {
            'completeness': completeness,
            'uniqueness': uniqueness,
            'validity': validity,
            'consistency': consistency
        }
        
        # Calcula score geral
        quality_score = self.calculate_quality_score(metrics_dict)
        
        # Cria objeto de métricas
        quality_metrics = QualityMetrics(
            timestamp=datetime.now().isoformat(),
            dataset_name=self.dataset_name,
            total_rows=len(df),
            total_columns=len(df.columns),
            completeness=completeness['overall'],
            uniqueness=uniqueness['overall'],
            validity=validity['overall'],
            consistency=consistency['overall'],
            quality_score=quality_score,
            details=metrics_dict
        )
        
        # Adiciona ao histórico
        self.metrics_history.append(quality_metrics)
        
        logger.info(f"Análise concluída. Quality Score: {quality_score:.2f}")
        
        return quality_metrics
    
    def get_quality_summary(self, metrics: QualityMetrics) -> str:
        """
        Gera resumo das métricas de qualidade
        
        Args:
            metrics: Objeto QualityMetrics
            
        Returns:
            String com resumo formatado
        """
        summary = f"""
╔═══════════════════════════════════════════════════════╗
║                RELATÓRIO DE QUALIDADE                 ║
╠═══════════════════════════════════════════════════════╣
║ Dataset: {metrics.dataset_name:<43} ║
║ Timestamp: {metrics.timestamp:<41} ║
║ Dimensões: {metrics.total_rows} linhas x {metrics.total_columns} colunas{' '*(27-len(str(metrics.total_rows))-len(str(metrics.total_columns)))} ║
╠═══════════════════════════════════════════════════════╣
║ MÉTRICAS DE QUALIDADE:                               ║
║                                                       ║
║ 📊 Quality Score:      {metrics.quality_score:>6.2f}%{' '*22} ║
║ ✓  Completude:         {metrics.completeness:>6.2f}%{' '*22} ║
║ ✓  Unicidade:          {metrics.uniqueness:>6.2f}%{' '*22} ║
║ ✓  Validade:           {metrics.validity:>6.2f}%{' '*22} ║
║ ✓  Consistência:       {metrics.consistency:>6.2f}%{' '*22} ║
╚═══════════════════════════════════════════════════════╝
"""
        return summary
    
    def save_metrics(self, metrics: QualityMetrics, filepath: str):
        """
        Salva métricas em JSON
        
        Args:
            metrics: Objeto QualityMetrics
            filepath: Caminho do arquivo
        """
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(asdict(metrics), f, indent=2, ensure_ascii=False)
            logger.info(f"Métricas salvas em: {filepath}")
        except Exception as e:
            logger.error(f"Erro ao salvar métricas: {str(e)}")
    
    def load_metrics_history(self, filepath: str):
        """
        Carrega histórico de métricas de arquivo JSON
        
        Args:
            filepath: Caminho do arquivo
        """
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    self.metrics_history = [QualityMetrics(**m) for m in data]
                else:
                    self.metrics_history = [QualityMetrics(**data)]
            logger.info(f"Histórico carregado de: {filepath}")
        except Exception as e:
            logger.error(f"Erro ao carregar histórico: {str(e)}")
    
    # Métodos auxiliares privados
    
    def _get_default_validations(self, df: pd.DataFrame) -> Dict:
        """Retorna validações padrão baseadas no schema do dataframe"""
        validations = {}
        
        # Validações para colunas numéricas
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            validations[f'{col}_no_negatives'] = lambda x, c=col: x[c] >= 0
        
        # Validações para colunas de texto
        text_cols = df.select_dtypes(include=['object']).columns
        for col in text_cols:
            validations[f'{col}_not_empty'] = lambda x, c=col: x[c].str.len() > 0
        
        return validations
    
    def _check_dtype_consistency(self, series: pd.Series) -> float:
        """Verifica consistência de tipos de dados"""
        try:
            if series.dtype == 'object':
                # Para strings, verificar se não há valores misturados
                type_counts = series.apply(type).value_counts()
                consistency = (type_counts.iloc[0] / len(series)) * 100
            else:
                # Para tipos numéricos, considerar 100% consistente
                consistency = 100.0
            return consistency
        except:
            return 100.0
    
    def _check_range_consistency(self, series: pd.Series) -> float:
        """Verifica se valores estão em range razoável (sem outliers extremos)"""
        try:
            Q1 = series.quantile(0.25)
            Q3 = series.quantile(0.75)
            IQR = Q3 - Q1
            lower = Q1 - 3 * IQR
            upper = Q3 + 3 * IQR
            in_range = series.between(lower, upper).sum()
            consistency = (in_range / len(series)) * 100
            return consistency
        except:
            return 100.0


# Exemplo de uso
if __name__ == "__main__":
    # Criar dataset de exemplo
    sample_data = {
        'id': [1, 2, 3, 4, 5, 5, 7, 8],
        'name': ['Alice', 'Bob', 'Charlie', None, 'Eve', 'Frank', 'Grace', 'Henry'],
        'age': [25, 30, 35, 28, -5, 45, 50, 55],
        'email': ['alice@test.com', 'bob@test', 'charlie@test.com', 
                  'dave@test.com', 'eve@test.com', 'frank@test.com',
                  'grace@test.com', 'henry@test.com'],
        'salary': [50000, 60000, 70000, 55000, 65000, 75000, 80000, 90000]
    }
    
    df = pd.DataFrame(sample_data)
    
    # Inicializar sistema de métricas
    dq = DataQualityMetrics(dataset_name="employees")
    
    # Executar análise
    metrics = dq.analyze_dataset(df, key_columns=['id'])
    
    # Imprimir resumo
    print(dq.get_quality_summary(metrics))
    
    # Salvar métricas
    dq.save_metrics(metrics, 'quality_metrics.json')