import os
import logging
from typing import Dict, List, Optional
from datetime import datetime
from dotenv import load_dotenv

from alert_manager import (
    AlertManager, 
    Alert, 
    AlertSeverity,
    create_quality_alert,
    create_anomaly_alert,
    create_pipeline_alert
)
from slack_notifier import SlackNotifier

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AlertingSystem:
    
    def __init__(self, 
                 enable_slack: bool = True,
                 enable_rate_limiting: bool = True,
                 config: Optional[Dict] = None):
        """
        Inicializa sistema de alertas
        
        Args:
            enable_slack: Ativa integração com Slack
            enable_rate_limiting: Ativa rate limiting de alertas
            config: Configurações customizadas
        """
        load_dotenv()
        
        self.config = config or self._load_default_config()
        self.alert_manager = AlertManager(enable_rate_limiting=enable_rate_limiting)
        
        self.slack_notifier = None
        if enable_slack:
            try:
                self.slack_notifier = SlackNotifier()
                logger.info(" Sistema de alertas Slack inicializado")
            except Exception as e:
                logger.warning(f" Slack não configurado: {e}")
    
    def _load_default_config(self) -> Dict:
        return {
            "thresholds": {
                "completeness": {
                    "warning": 0.95,
                    "error": 0.90,
                    "critical": 0.80
                },
                "uniqueness": {
                    "warning": 0.98,
                    "error": 0.95,
                    "critical": 0.90
                },
                "validity": {
                    "warning": 0.95,
                    "error": 0.90,
                    "critical": 0.85
                }
            },
            "channels": {
                "info": os.getenv("SLACK_CHANNEL_ID"),
                "warning": os.getenv("SLACK_ALERT_CHANNEL_WARNING"),
                "error": os.getenv("SLACK_ALERT_CHANNEL_WARNING"),
                "critical": os.getenv("SLACK_ALERT_CHANNEL_CRITICAL")
            },
            "mentions": {
                "critical": os.getenv("SLACK_ONCALL_USER_ID", "").split(",")
            }
        }
    
    def check_data_quality(self, metrics: Dict) -> List[Alert]:
        """
        Verifica métricas de qualidade e gera alertas
        
        Args:
            metrics: Dict com métricas {metric_name: score}
            
        Returns:
            Lista de alertas gerados
        """
        alerts = []
        
        for metric_name, score in metrics.items():
            thresholds = self.config["thresholds"].get(
                metric_name, 
                self.config["thresholds"]["completeness"]  
            )
            
            severity = None
            threshold_value = None
            
            if score < thresholds["critical"]:
                severity = AlertSeverity.CRITICAL
                threshold_value = thresholds["critical"]
            elif score < thresholds["error"]:
                severity = AlertSeverity.ERROR
                threshold_value = thresholds["error"]
            elif score < thresholds["warning"]:
                severity = AlertSeverity.WARNING
                threshold_value = thresholds["warning"]
            
            # Cria alerta 
            if severity:
                alert = create_quality_alert(
                    metric_name=metric_name,
                    score=score,
                    threshold=threshold_value,
                    severity=severity
                )
                alerts.append(alert)
                
                # Envia para o Slack
                self._send_alert_to_slack(alert)
        
        return alerts
    
    def check_anomalies(self, 
                       metric_name: str,
                       current_value: float,
                       expected_range: tuple,
                       severity: AlertSeverity = AlertSeverity.ERROR) -> Optional[Alert]:
        """
        Verifica anomalias e gera alerta
        
        Args:
            metric_name: Nome da métrica
            current_value: Valor atual
            expected_range: (min, max) esperado
            severity: Severidade do alerta
            
        Returns:
            "Alert" se anomalia detectada, "None" caso contrário
        """
        min_val, max_val = expected_range
        
        if not (min_val <= current_value <= max_val):
            alert = create_anomaly_alert(
                metric_name=metric_name,
                current_value=current_value,
                expected_range=expected_range,
                severity=severity
            )
            
            self._send_alert_to_slack(alert)
            return alert
        
        return None
    
    def report_pipeline_error(self,
                            pipeline_name: str,
                            error_message: str,
                            severity: AlertSeverity = AlertSeverity.CRITICAL) -> Alert:
        alert = create_pipeline_alert(
            pipeline_name=pipeline_name,
            error_message=error_message,
            severity=severity
        )
        
        self._send_alert_to_slack(alert)
        return alert
    
    def _send_alert_to_slack(self, alert: Alert) -> bool:
        if not self.slack_notifier:
            logger.info(f"Slack desabilitado. Alerta: {alert.title}")
            return False
        
        can_send, reason = self.alert_manager.should_send_alert(alert)
        
        if not can_send:
            logger.warning(f"Alerta não enviado: {reason}")
            return False
        
        channel = self.config["channels"].get(alert.severity.value)
        
        mentions = None
        if alert.severity == AlertSeverity.CRITICAL:
            mentions = self.config["mentions"].get("critical")
        
        # Envia
        try:
            success = self.slack_notifier.send_alert(
                alert=alert,
                channel=channel,
                mention_users=mentions
            )
            return success
        except Exception as e:
            logger.error(f"Erro ao enviar alerta: {e}")
            return False
    
    def send_daily_summary(self, channel: Optional[str] = None):
        """Envia resumo diário para o Slack"""
        if not self.slack_notifier:
            logger.info("Slack desabilitado. Summary não enviado.")
            return
        
        summary = self.alert_manager.get_summary()
        self.slack_notifier.send_summary(summary, channel)
    
    def get_alert_stats(self) -> Dict:
        """Retorna estatísticas dos alertas"""
        return self.alert_manager.get_summary()
    
    def clear_old_alerts(self, days: int = 7):
        """Limpa alertas antigos"""
        self.alert_manager.clear_old_alerts(days)
        logger.info(f"Alertas mais antigos que {days} dias removidos")


# Funções de conveniência
def quick_quality_check(metrics: Dict, 
                       send_slack: bool = True) -> List[Alert]:
    """
    Checagem ráída de qualidade de dados
    
    Uso:
        metrics = {"completeness": 0.89, "uniqueness": 0.97}
        alerts = quick_quality_check(metrics)
    """
    system = AlertingSystem(enable_slack=send_slack)
    return system.check_data_quality(metrics)


def quick_anomaly_check(metric_name: str,
                       value: float,
                       expected_range: tuple,
                       send_slack: bool = True) -> Optional[Alert]:
    """
    
    Uso:
        alert = quick_anomaly_check("row_count", 5000, (10000, 15000))
    """
    system = AlertingSystem(enable_slack=send_slack)
    return system.check_anomalies(metric_name, value, expected_range)


# Exemplo de uso
if __name__ == "__main__":
    alerting = AlertingSystem(
        enable_slack=True,
        enable_rate_limiting=True
    )
    
    print("="*60)
    print("TESTANDO SISTEMA DE ALERTAS")
    print("="*60)
   
   
    print("\n1️ Testando alertas de qualidade...")
    quality_metrics = {
        "completeness": 0.89,  
        "uniqueness": 0.97,    
        "validity": 0.94       
    }
    
    alerts = alerting.check_data_quality(quality_metrics)
    print(f"   → {len(alerts)} alertas gerados")
   
    print("\n2️ Testando detecção de anomalias...")
    alert = alerting.check_anomalies(
        metric_name="row_count",
        current_value=5000,
        expected_range=(10000, 15000),
        severity=AlertSeverity.ERROR
    )
    if alert:
        print(f"   → Anomalia detectada: {alert.title}")
    

    print("\n3️ Testando erro de pipeline...")
    alerting.report_pipeline_error(
        pipeline_name="daily_ingestion",
        error_message="Database connection timeout after 30s",
        severity=AlertSeverity.CRITICAL
    )
    

    print("\n4️ Testando rate limit...")
    for i in range(3):
        alert = create_quality_alert("test_metric", 0.85, 0.95)
        can_send, reason = alerting.alert_manager.should_send_alert(alert)
        print(f"   Tentativa {i+1}: {can_send} - {reason}")

    print("\n" + "="*60)
    print("ESTATÍSTICAS")
    print("="*60)
    stats = alerting.get_alert_stats()
    print(f"Total de alertas: {stats['total_alerts']}")
    print(f"Últimas 24h: {stats['last_24h']}")
    print(f"Por severidade: {stats['by_severity']}")
    
    # Envia resumo
    print("\n Enviando resumo diário...")
    alerting.send_daily_summary()
    
    print("\n Testes concluídos!")