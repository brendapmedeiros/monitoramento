import great_expectations as gx
import pandas as pd
import logging
import json
from pathlib import Path
import sys
from datetime import datetime

# Adiciona src ao path
sys.path.append(str(Path(__file__).parent))
from src.core.data_quality import DataQualityMetrics

logger = logging.getLogger(__name__)


class DataQualityPipeline:

    
    def __init__(self, context_root_dir: str = "."):
        self.context = None
        self.ge_available = False
        
        if Path(context_root_dir).exists():
            try:
                self.context = gx.get_context()
                self.ge_available = True
                logger.info("✓ Great Expectations carregado")
            except Exception as e:
                logger.warning(f"Great Expectations não disponível: {e}")
        else:
            logger.info("Pasta gx/ não existe. Pipeline funcionará apenas com métricas customizadas.")
        
        self.metrics_system = None
    
    def run_quality_check(self, 
                         df: pd.DataFrame, 
                         dataset_name: str,
                         checkpoint_name: str = None,
                         key_columns: list = None,
                         validation_rules: dict = None):
        """
        Executa validação completa
        
        Args:
            df: DataFrame para validar
            dataset_name: Nome do dataset
            checkpoint_name: Nome do checkpoint GE
            key_columns: Colunas chave para duplicatas
            validation_rules: Regras customizadas
            
        Returns:
            Dict com resultados combinados
        """
        logger.info(f"=== Validando: {dataset_name} ===")
        
        # Great Expectations
        ge_results = None
        if self.ge_available and checkpoint_name:
            logger.info(f"Executando checkpoint: {checkpoint_name}")
            try:
                ge_results = self._run_checkpoint(checkpoint_name)
            except Exception as e:
                logger.error(f"Erro no checkpoint: {e}")
        
        # Métricas Customizadas 
        logger.info("Calculando métricas de qualidade...")
        self.metrics_system = DataQualityMetrics(dataset_name=dataset_name)
        custom_metrics = self.metrics_system.analyze_dataset(
            df,
            key_columns=key_columns,
            validation_rules=validation_rules
        )
        
        # Combinar e salvar
        report = self._build_report(ge_results, custom_metrics)
        self._save_report(report, dataset_name)
        
        logger.info("=== Validação concluída ===\n")
        
        return report
    
    def _run_checkpoint(self, checkpoint_name: str):
        """Executa checkpoint já existente"""
        try:
            checkpoint = self.context.get_checkpoint(checkpoint_name)
            results = checkpoint.run()
            
            status = "✓ APROVADO" if results.success else "✗ REPROVADO"
            logger.info(f"Checkpoint '{checkpoint_name}': {status}")
            
            return results
        except Exception as e:
            logger.error(f"Erro ao executar checkpoint '{checkpoint_name}': {e}")
            raise
    
    def _build_report(self, ge_results, custom_metrics):
        """Monta relatório combinado"""
        
        # Extrai dados do GE
        ge_data = {'enabled': False}
        
        if ge_results:
            ge_data['enabled'] = True
            ge_data['success'] = ge_results.success
            
            # Estatísticas de validação
            try:
                for result in ge_results.run_results.values():
                    if 'validation_result' in result:
                        stats = result['validation_result'].get('statistics', {})
                        ge_data['stats'] = {
                            'evaluated': stats.get('evaluated_expectations', 0),
                            'successful': stats.get('successful_expectations', 0),
                            'percent': stats.get('success_percent', 0)
                        }
                        break
            except:
                pass
        
        # Status geral
        status = self._get_status(ge_data, custom_metrics.quality_score)
        
        return {
            'dataset_name': custom_metrics.dataset_name,
            'timestamp': custom_metrics.timestamp,
            'dimensions': {
                'rows': custom_metrics.total_rows,
                'columns': custom_metrics.total_columns
            },
            'status': status,
            'great_expectations': ge_data,
            'metrics': {
                'quality_score': custom_metrics.quality_score,
                'completeness': custom_metrics.completeness,
                'uniqueness': custom_metrics.uniqueness,
                'validity': custom_metrics.validity,
                'consistency': custom_metrics.consistency,
                'details': custom_metrics.details
            }
        }
    
    def _get_status(self, ge_data, quality_score):
        """Determina status geral"""
        
        # Se GE falhou, crítico
        if ge_data.get('enabled') and not ge_data.get('success'):
            return {
                'level': 'CRÍTICO',
                'symbol': '❌',
                'message': 'Great Expectations falhou'
            }
        
        # Baseado no quality score
        if quality_score >= 95:
            return {'level': 'EXCELENTE', 'symbol': '✅', 'message': 'Qualidade excelente'}
        elif quality_score >= 80:
            return {'level': 'BOM', 'symbol': '✓', 'message': 'Qualidade boa'}
        elif quality_score >= 60:
            return {'level': 'ABAIXO', 'symbol': '⚠️', 'message': 'Requer atenção'}
        else:
            return {'level': 'CRÍTICO', 'symbol': '❌', 'message': 'Qualidade crítica'}
    
    def _save_report(self, report, dataset_name):
        """Salva relatório"""
        try:
            output_dir = Path("data/reports")
            output_dir.mkdir(parents=True, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filepath = output_dir / f"{dataset_name}_{timestamp}.json"
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            
            logger.info(f" Relatório salvo: {filepath}")
        except Exception as e:
            logger.error(f"Erro ao salvar: {e}")
    
    def print_report(self, report):
        """Imprime relatório formatado"""
        
        st = report['status']
        m = report['metrics']
        ge = report['great_expectations']
        
        print(f"""
        RELATÓRIO DE QUALIDADE DE DADOS                

 Dataset: {report['dataset_name']:<45} 
 Timestamp: {report['timestamp']:<43} 
 Dimensões: {report['dimensions']['rows']} linhas x {report['dimensions']['columns']} colunas{' '*(30-len(str(report['dimensions']['rows']))-len(str(report['dimensions']['columns'])))} 

 STATUS: {st['symbol']} {st['level']:<44} 
 {st['message']:<54} 
 GREAT EXPECTATIONS: {'Ativo ✓' if ge['enabled'] else 'Inativo ✗':<34} """)
        
        if ge['enabled'] and ge.get('success') is not None:
            print(f"   Validação: {'PASSOU ✓' if ge['success'] else 'FALHOU ✗':<43} ")
            if 'stats' in ge:
                s = ge['stats']
                print(f"   Expectations: {s['successful']}/{s['evaluated']} ({s['percent']:.1f}%){' '*(28-len(str(s['successful']))-len(str(s['evaluated'])))} ")
        
        print(f"""
 MÉTRICAS DE QUALIDADE:                                 
   Quality Score:    {m['quality_score']:>6.2f}%{' '*30} 
  Completude:       {m['completeness']:>6.2f}%{' '*30} 
   Unicidade:        {m['uniqueness']:>6.2f}%{' '*30} 
   Validade:         {m['validity']:>6.2f}%{' '*30} 
   Consistência:     {m['consistency']:>6.2f}%{' '*30} 

""")


# Exemplo de uso
if __name__ == "__main__":
    import numpy as np
    
    # Dataset de exemplo
    df = pd.DataFrame({
        'customer_id': range(1, 101),
        'name': [f'Customer_{i}' for i in range(1, 101)],
        'email': [f'customer{i}@email.com' for i in range(1, 101)],
        'age': np.random.randint(18, 80, 100),
        'purchase_amount': np.random.uniform(10, 1000, 100).round(2)
    })
    

    validation_rules = {
        'valid_email': lambda df: df['email'].str.contains('@', na=False),
        'positive_age': lambda df: df['age'] > 0,
        'positive_amount': lambda df: df['purchase_amount'] > 0,
        'reasonable_age': lambda df: df['age'].between(0, 120)
    }
    

    pipeline = DataQualityPipeline()
    
    report = pipeline.run_quality_check(
        df=df,
        dataset_name="customers",
        checkpoint_name="sales_checkpoint",
        key_columns=['customer_id'],
        validation_rules=validation_rules
    )
    
    # Imprime relatório
    pipeline.print_report(report)
    
  