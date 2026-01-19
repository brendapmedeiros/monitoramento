from enum import Enum
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import json
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

class AlertSeverity(Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class Alert:
    severity: AlertSeverity
    title: str
    message: str
    source: str  
    metric_name: Optional[str] = None
    metric_value: Optional[float] = None
    threshold: Optional[float] = None
    timestamp: datetime = None
    metadata: Dict = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
        if self.metadata is None:
            self.metadata = {}
    
    def to_dict(self) -> Dict:
        return {
            "severity": self.severity.value,
            "title": self.title,
            "message": self.message,
            "source": self.source,
            "metric_name": self.metric_name,
            "metric_value": self.metric_value,
            "threshold": self.threshold,
            "timestamp": self.timestamp.isoformat(),
            "metadata": self.metadata
        }


class AlertRateLimiter:
    
    def __init__(self, 
                 max_alerts_per_hour: int = 10,
                 cooldown_minutes: int = 30):
        self.max_alerts_per_hour = max_alerts_per_hour
        self.cooldown_minutes = cooldown_minutes
        self.alert_history: Dict[str, List[datetime]] = defaultdict(list)
        self.cooldown_until: Dict[str, datetime] = {}
    
    def can_send_alert(self, alert_key: str) -> Tuple[bool, str]:
        """
        Verifica se pode enviar alerta
        
        Args:
            alert_key: ID do alerta 
            
        Return:
            (pode_enviar, motivo)
        """
        now = datetime.now()
        

        if alert_key in self.cooldown_until:
            if now < self.cooldown_until[alert_key]:
                remaining = (self.cooldown_until[alert_key] - now).seconds // 60
                return False, f"Em cooldown. Aguarde {remaining} minutos"
            else:
                del self.cooldown_until[alert_key]
        

        cutoff_time = now - timedelta(hours=1)
        self.alert_history[alert_key] = [
            ts for ts in self.alert_history[alert_key] 
            if ts > cutoff_time
        ]

        if len(self.alert_history[alert_key]) >= self.max_alerts_per_hour:
            self.cooldown_until[alert_key] = now + timedelta(minutes=self.cooldown_minutes)
            return False, f"Limite de {self.max_alerts_per_hour} alertas por hora atingido"
        

        self.alert_history[alert_key].append(now)
        return True, "OK"
    
    def get_stats(self, alert_key: str) -> Dict:
        now = datetime.now()
        cutoff = now - timedelta(hours=1)
        recent_alerts = [ts for ts in self.alert_history[alert_key] if ts > cutoff]
        
        return {
            "alert_key": alert_key,
            "alerts_last_hour": len(recent_alerts),
            "max_per_hour": self.max_alerts_per_hour,
            "in_cooldown": alert_key in self.cooldown_until,
            "cooldown_until": self.cooldown_until.get(alert_key)
        }


class AlertManager:
    
    def __init__(self, enable_rate_limiting: bool = True):
        self.alerts: List[Alert] = []
        self.rate_limiter = AlertRateLimiter() if enable_rate_limiting else None
        
    def create_alert(self,
                    severity: AlertSeverity,
                    title: str,
                    message: str,
                    source: str,
                    **kwargs) -> Alert:
        alert = Alert(
            severity=severity,
            title=title,
            message=message,
            source=source,
            **kwargs
        )
        self.alerts.append(alert)
        return alert
    
    def should_send_alert(self, alert: Alert) -> Tuple[bool, str]:
        if self.rate_limiter is None:
            return True, "Rate limiting desabilitado"
        
        alert_key = f"{alert.source}:{alert.metric_name or alert.title}"
        
        if alert.severity == AlertSeverity.CRITICAL:
            return True, "Alerta crítico - bypass rate limiting"
        
        return self.rate_limiter.can_send_alert(alert_key)
    
    def get_alerts_by_severity(self, severity: AlertSeverity) -> List[Alert]:
        return [a for a in self.alerts if a.severity == severity]
    
    def get_recent_alerts(self, hours: int = 24) -> List[Alert]:
        cutoff = datetime.now() - timedelta(hours=hours)
        return [a for a in self.alerts if a.timestamp > cutoff]
    
    def clear_old_alerts(self, days: int = 7):
        cutoff = datetime.now() - timedelta(days=days)
        self.alerts = [a for a in self.alerts if a.timestamp > cutoff]
    
    def get_summary(self) -> Dict:
        recent = self.get_recent_alerts(hours=24)
        
        return {
            "total_alerts": len(self.alerts),
            "last_24h": len(recent),
            "by_severity": {
                severity.value: len([a for a in recent if a.severity == severity])
                for severity in AlertSeverity
            },
            "by_source": {
                source: len([a for a in recent if a.source == source])
                for source in set(a.source for a in recent)
            }
        }



def create_quality_alert(metric_name: str, 
                        score: float, 
                        threshold: float,
                        severity: AlertSeverity = AlertSeverity.WARNING) -> Alert:
    return Alert(
        severity=severity,
        title=f"Qualidade abaixo do esperado: {metric_name}",
        message=f"Métrica {metric_name} está em {score:.2%}, abaixo do limite de {threshold:.2%}",
        source="data_quality",
        metric_name=metric_name,
        metric_value=score,
        threshold=threshold
    )


def create_anomaly_alert(metric_name: str,
                        current_value: float,
                        expected_range: tuple,
                        severity: AlertSeverity = AlertSeverity.ERROR) -> Alert:
    return Alert(
        severity=severity,
        title=f"Anomalia detectada: {metric_name}",
        message=f"Valor {current_value:.2f} fora do range esperado {expected_range}",
        source="anomaly_detector",
        metric_name=metric_name,
        metric_value=current_value,
        metadata={"expected_range": expected_range}
    )


def create_pipeline_alert(pipeline_name: str,
                         error_message: str,
                         severity: AlertSeverity = AlertSeverity.CRITICAL) -> Alert:
    return Alert(
        severity=severity,
        title=f"Erro no pipeline: {pipeline_name}",
        message=error_message,
        source="pipeline",
        metadata={"pipeline": pipeline_name}
    )


if __name__ == "__main__":
    manager = AlertManager()
    
  
    alert1 = create_quality_alert("completeness", 0.85, 0.95, AlertSeverity.WARNING)
    alert2 = create_anomaly_alert("row_count", 5000, (10000, 15000), AlertSeverity.ERROR)
    alert3 = create_pipeline_alert("daily_ingestion", "Connection timeout", AlertSeverity.CRITICAL)
    

    for alert in [alert1, alert2, alert3]:
        can_send, reason = manager.should_send_alert(alert)
        print(f"\n{alert.severity.value.upper()}: {alert.title}")
        print(f"Pode enviar: {can_send} - {reason}")
    
    # Resumo
    print("\n" + "="*50)
    print("RESUMO DOS ALERTAS:")
    print(json.dumps(manager.get_summary(), indent=2))