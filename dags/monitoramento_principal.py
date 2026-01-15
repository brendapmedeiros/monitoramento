from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import pandas as pd
import json
from pathlib import Path


sys.path.insert(0, '/opt/airflow/src')

from core.data_quality import DataQualityMetrics
from core.detector_anomalias import DetectorAnomalias
from core.slack_notifier import SlackNotifier
from core.alert_manager import Alert, AlertSeverity, create_quality_alert, create_anomaly_alert

default_args = {
    'owner': 'time-dados',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

with DAG(
    'monitoramento_qualidade_dados',
    default_args=default_args,
    description='Pipeline completo de monitoramento',
    schedule_interval='0 */6 * * *',  # Executa a cada 6 horas
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['monitoramento', 'qualidade', 'anomalias', 'slack'],
) as dag:
    
##TASK 1
    def carregar_dados(**context):
        print(" Carregando dados...")
        
        try:
            data_path = '/opt/airflow/data/sales_transactions.csv'
            
            if not os.path.exists(data_path):
                raise FileNotFoundError(f"Arquivo não encontrado: {data_path}")
            df = pd.read_csv(data_path)
            
            print(f"Dados carregados: {len(df)} linhas x {len(df.columns)} colunas")
            print(f"Colunas: {', '.join(df.columns[:5])}...")
            
            context['task_instance'].xcom_push(
                key='data_info',
                value={
                    'rows': len(df),
                    'columns': len(df.columns),
                    'file': 'sales_transactions.csv'
                }
            )
            
            # Salva dados em JSON
            data_sample = df.head(100).to_json()
            context['task_instance'].xcom_push(key='data_sample', value=data_sample)
            
            return {'status': 'success', 'rows': len(df)}
            
        except Exception as e:
            print(f" Erro ao carregar dados: {str(e)}")
            raise
            
        ## TASK 2
    def validar_qualidade(**context):
        print("Validando qualidade...")
        
        try:
            df = pd.read_csv('/opt/airflow/data/sales_transactions.csv')
            dq = DataQualityMetrics(dataset_name="sales_transactions")
            
            metrics = dq.analyze_dataset(
                df,
                key_columns=['transaction_id']
            )
            
            # Log resumo
            print(dq.get_quality_summary(metrics))
            output_dir = Path('/opt/airflow/data/reports')
            output_dir.mkdir(parents=True, exist_ok=True)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            metrics_file = output_dir / f'quality_metrics_{timestamp}.json'
            dq.save_metrics(metrics, str(metrics_file))
            
            context['task_instance'].xcom_push(
                key='quality_metrics',
                value={
                    'quality_score': metrics.quality_score,
                    'completeness': metrics.completeness,
                    'uniqueness': metrics.uniqueness,
                    'validity': metrics.validity,
                    'consistency': metrics.consistency,
                    'total_rows': metrics.total_rows,
                    'total_columns': metrics.total_columns,
                }
            )
            
            print(f" Validação concluída. Score: {metrics.quality_score:.2f}")
            
            return {
                'status': 'success',
                'quality_score': metrics.quality_score,
                'metrics_file': str(metrics_file)
            }
            
        except Exception as e:
            print(f" Erro na validação: {str(e)}")
            raise

    def detectar_anomalias(**context):
##Task 3
        print(" Detectando anomalias...")
        
        try:
            df = pd.read_csv('/opt/airflow/data/sales_transactions.csv')
            detector = DetectorAnomalias(
                dataset_name="sales_transactions",
                contamination=0.05  
            )
            
            report = detector.detect_all(
                df,
                methods=['zscore', 'iqr', 'isolation_forest']
            )
            
            # Imprime e salva o relatório
            detector.print_report(report)
            output_dir = Path('/opt/airflow/data/reports')
            output_dir.mkdir(parents=True, exist_ok=True)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            report_file = output_dir / f'anomaly_report_{timestamp}.json'
            detector.save_report(report, str(report_file))
            
            # Passa dados pelo xcom
            context['task_instance'].xcom_push(
                key='anomaly_report',
                value={
                    'total_anomalies': report.total_anomalies,
                    'anomaly_percentage': report.anomaly_percentage,
                    'methods_used': report.methods_used,
                    'anomalies_by_method': report.anomalies_by_method,
                    'severity_distribution': report.details['severity_distribution']
                }
            )
            
            print(f" Detecção concluída: {report.total_anomalies} anomalias ({report.anomaly_percentage:.2f}%)")
            
            return {
                'status': 'success',
                'total_anomalies': report.total_anomalies,
                'report_file': str(report_file)
            }
            
        except Exception as e:
            print(f" Erro na detecção: {str(e)}")
            raise
##TASK 4
    def enviar_alerta_slack(**context):
        print(" Enviando alertas...")
        
        try:
            ti = context['task_instance']
            quality_metrics = ti.xcom_pull(task_ids='validar_qualidade', key='quality_metrics')
            anomaly_report = ti.xcom_pull(task_ids='detectar_anomalias', key='anomaly_report')
            
            # Chama iniciador
            notifier = SlackNotifier()

            quality_score = quality_metrics['quality_score']
            anomaly_pct = anomaly_report['anomaly_percentage']
            
            if quality_score < 70 or anomaly_pct > 10:
                severity = AlertSeverity.CRITICAL
                channel = os.getenv('SLACK_ALERT_CHANNEL_CRITICAL')
                mention_user = os.getenv('SLACK_ONCALL_USER_ID')
            elif quality_score < 85 or anomaly_pct > 5:
                severity = AlertSeverity.WARNING
                channel = os.getenv('SLACK_ALERT_CHANNEL_WARNING')
                mention_user = None
            else:
                severity = AlertSeverity.INFO
                channel = None  
                mention_user = None
            
            # Cria alerta 
            alert = Alert(
                title=f"📊 Relatório de Monitoramento - {context['ds']}",
                message=f"""
*Análise Completa do Dataset Sales Transactions*

 *Qualidade dos Dados:*
• Quality Score: *{quality_score:.2f}%*
• Completude: {quality_metrics['completeness']:.2f}%
• Unicidade: {quality_metrics['uniqueness']:.2f}%
• Validade: {quality_metrics['validity']:.2f}%
• Consistência: {quality_metrics['consistency']:.2f}%

*Anomalias:*
• Total: *{anomaly_report['total_anomalies']}* registros
• Percentual: *{anomaly_pct:.2f}%*
• Métodos usados: {', '.join(anomaly_report['methods_used'])}

 *Detalhamento por métricas:*
{chr(10).join([f"• {m}: {c} anomalias" for m, c in anomaly_report['anomalies_by_method'].items()])}

 *Severidade das Anomalias:*
• Alta: {anomaly_report['severity_distribution']['high']}
• Média: {anomaly_report['severity_distribution']['medium']}
• Baixa: {anomaly_report['severity_distribution']['low']}

*Dataset:*
• Total de registros: {quality_metrics['total_rows']:,}
• Total de colunas: {quality_metrics['total_columns']}
                """,
                severity=severity,
                source="airflow_data_monitoring",
                metric_name="quality_score",
                metric_value=quality_score,
                threshold=85.0,
                metadata={
                    'dag_id': context['dag'].dag_id,
                    'execution_date': str(context['execution_date']),
                    'anomalies': anomaly_report['total_anomalies']
                }
            )
            
            # Envia alerta
            mentions = [mention_user] if mention_user else None
            success = notifier.send_alert(alert, channel=channel, mention_users=mentions)
            
            if success:
                print(f" Alerta enviado com sucesso (Severidade: {severity.value})")
            else:
                print(" Falha ao enviar alerta")
            
            return {
                'status': 'success' if success else 'failed',
                'severity': severity.value,
                'channel': channel or 'default'
            }
            
        except Exception as e:
            print(f" Erro ao enviar alerta: {str(e)}")
            return {'status': 'error', 'error': str(e)}

#TASK 5
    def gerar_relatorio_final(**context):
        print(" Gerando relatório final...")
        
        try:
            ti = context['task_instance']
            
            data_info = ti.xcom_pull(task_ids='carregar_dados', key='data_info')
            quality_metrics = ti.xcom_pull(task_ids='validar_qualidade', key='quality_metrics')
            anomaly_report = ti.xcom_pull(task_ids='detectar_anomalias', key='anomaly_report')
            alert_status = ti.xcom_pull(task_ids='enviar_alerta_slack')
            

            final_report = {
                'execution_info': {
                    'dag_id': context['dag'].dag_id,
                    'run_id': context['dag_run'].run_id,
                    'execution_date': str(context['execution_date']),
                    'timestamp': datetime.now().isoformat()
                },
                'data_info': data_info,
                'quality_metrics': quality_metrics,
                'anomaly_report': anomaly_report,
                'alert_status': alert_status,
                'summary': {
                    'status': 'completed',
                    'quality_score': quality_metrics['quality_score'],
                    'anomalies_found': anomaly_report['total_anomalies'],
                    'alert_sent': alert_status['status'] == 'success'
                }
            }
            
            output_dir = Path('/opt/airflow/data/reports')
            output_dir.mkdir(parents=True, exist_ok=True)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            final_report_file = output_dir / f'final_report_{timestamp}.json'
            
            with open(final_report_file, 'w', encoding='utf-8') as f:
                json.dump(final_report, f, indent=2, ensure_ascii=False)
            
            print(f"Relatório final salvo: {final_report_file}")
            print("\n" + "="*50)
            print(" RESUMO DA EXECUÇÃO")
            print("="*50)
            print(f"Quality Score: {quality_metrics['quality_score']:.2f}%")
            print(f"Anomalias: {anomaly_report['total_anomalies']} ({anomaly_report['anomaly_percentage']:.2f}%)")
            print(f"Alerta Enviado: {'' if alert_status['status'] == 'success' else ''}")
            print(f"Relatório: {final_report_file}")
            print("="*50)
            
            return final_report
            
        except Exception as e:
            print(f"Erro ao gerar: {str(e)}")
            raise

  # Definição das tarefas
    task_carregar = PythonOperator(
        task_id='carregar_dados',
        python_callable=carregar_dados,
    )

    task_qualidade = PythonOperator(
        task_id='validar_qualidade',
        python_callable=validar_qualidade,
    )

    task_anomalias = PythonOperator(
        task_id='detectar_anomalias',
        python_callable=detectar_anomalias,
    )

    task_slack = PythonOperator(
        task_id='enviar_alerta_slack',
        python_callable=enviar_alerta_slack,
    )

    task_relatorio = PythonOperator(
        task_id='gerar_relatorio_final',
        python_callable=gerar_relatorio_final,
    )
# Ordem de execução
    task_carregar >> task_qualidade >> task_anomalias >> task_slack >> task_relatorio