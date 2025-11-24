import os
from typing import List, Dict, Optional
from datetime import datetime
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from tenacity import retry, stop_after_attempt, wait_exponential
import logging

from alert_manager import Alert, AlertSeverity

logger = logging.getLogger(__name__)


class SlackNotifier:
    """Gerenciador de notificações Slack"""
    SEVERITY_CONFIG = {
        AlertSeverity.INFO: {
            "color": "#36a64f",  
            "emoji": "ℹ️",
            "priority": "Informativo"
        },
        AlertSeverity.WARNING: {
            "color": "#ff9900",  
            "emoji": "⚠️",
            "priority": "Atenção"
        },
        AlertSeverity.ERROR: {
            "color": "#e01e5a",  
            "emoji": "🚨",
            "priority": "Erro"
        },
        AlertSeverity.CRITICAL: {
            "color": "#8b0000",  
            "emoji": "🔥",
            "priority": "CRÍTICO"
        }
    }
    
    def __init__(self, 
                 token: Optional[str] = None,
                 default_channel: Optional[str] = None):
        """
        Inicializa notificador Slack
        
        Args:
            token: Bot token do Slack (ou usa o SLACK_BOT_TOKEN do ambiente)
            default_channel: Canal padrão (ou usa o SLACK_CHANNEL_ID)
        """
        self.token = token or os.getenv("SLACK_BOT_TOKEN")
        self.default_channel = default_channel or os.getenv("SLACK_CHANNEL_ID")
        
        if not self.token:
            raise ValueError("Token não configurado!")
        
        self.client = WebClient(token=self.token)
        self._verify_connection()
    
    def _verify_connection(self):
        try:
            response = self.client.auth_test()
            logger.info(f" Conectado como: {response['user']}")
        except SlackApiError as e:
            logger.error(f" Erro ao conectar: {e.response['error']}")
            raise
    
    @retry(stop=stop_after_attempt(3), 
           wait=wait_exponential(multiplier=1, min=2, max=10))
    def send_alert(self, 
                   alert: Alert, 
                   channel: Optional[str] = None,
                   mention_users: Optional[List[str]] = None) -> bool:
        """
        Envia alerta com retry automático
        
        Args:
            alert: Objeto a ser enviado
            channel: Canal de destino (usa default se não especificado)
            mention_users: Lista de user IDs para mencionar (@user)
            
        Returns:
            True se enviado 
        """
        channel = channel or self.default_channel
        
        if not channel:
            logger.error("Canal não especificado.")
            return False
        
        try:
            blocks = self._build_alert_blocks(alert, mention_users)
            
            response = self.client.chat_postMessage(
                channel=channel,
                blocks=blocks,
                text=f"{alert.severity.value.upper()}: {alert.title}"  
            )
            
            logger.info(f"✅ Alerta enviado: {alert.title} (ts: {response['ts']})")
            return True
            
        except SlackApiError as e:
            logger.error(f" Erro ao enviar alerta: {e.response['error']}")
            raise
    
    def _build_alert_blocks(self, 
                           alert: Alert, 
                           mention_users: Optional[List[str]] = None) -> List[Dict]:
    
        config = self.SEVERITY_CONFIG[alert.severity]
        
        blocks = []
        
        blocks.append({
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{config['emoji']} {alert.title}",
                "emoji": True
            }
        })

        blocks.append({
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"*Prioridade:* {config['priority']}"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Fonte:* {alert.source}"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Horário:* {alert.timestamp.strftime('%d/%m/%Y %H:%M:%S')}"
                }
            ]
        })
        
        blocks.append({"type": "divider"})
        

        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": alert.message
            }
        })
        
        if alert.metric_name:
            fields = []
            
            if alert.metric_value is not None:
                fields.append({
                    "type": "mrkdwn",
                    "text": f"*Métrica:*\n{alert.metric_name}"
                })
                fields.append({
                    "type": "mrkdwn",
                    "text": f"*Valor:*\n{self._format_value(alert.metric_value)}"
                })
            
            if alert.threshold is not None:
                fields.append({
                    "type": "mrkdwn",
                    "text": f"*Limite:*\n{self._format_value(alert.threshold)}"
                })
            
            if fields:
                blocks.append({
                    "type": "section",
                    "fields": fields
                })
        
        # Metadata adicional para controle
        if alert.metadata:
            metadata_text = "\n".join([
                f"• *{k}:* {v}" 
                for k, v in alert.metadata.items()
            ])
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Detalhes:*\n{metadata_text}"
                }
            })
        
        # Menções
        if mention_users:
            mentions = " ".join([f"<@{user_id}>" for user_id in mention_users])
            blocks.append({
                "type": "context",
                "elements": [{
                    "type": "mrkdwn",
                    "text": f"cc: {mentions}"
                }]
            })
        
        # Cor lateral (contextual color)
        blocks.append({
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": f"━━━━━━━━━━━━━━━━━━━"
            }]
        })
        
        return blocks
    
    def _format_value(self, value: float) -> str:
        if isinstance(value, float):
            if 0 < value < 1:
                return f"{value:.2%}"
            return f"{value:.2f}"
        return str(value)
    
    def send_summary(self, 
                    summary: Dict,
                    channel: Optional[str] = None) -> bool:
        """Envia resumo de alertas do dia"""
        channel = channel or self.default_channel
        
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "📊 Resumo Diário",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Total de Alertas:*\n{summary.get('total_alerts', 0)}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Últimas 24h:*\n{summary.get('last_24h', 0)}"
                    }
                ]
            },
            {"type": "divider"}
        ]
        
        # Por severidade
        by_severity = summary.get('by_severity', {})
        if by_severity:
            severity_text = "\n".join([
                f"• {k.upper()}: {v}"
                for k, v in by_severity.items()
            ])
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Por Severidade:*\n{severity_text}"
                }
            })
        
        # Por fonte
        by_source = summary.get('by_source', {})
        if by_source:
            source_text = "\n".join([
                f"• {k}: {v}"
                for k, v in by_source.items()
            ])
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Por Fonte:*\n{source_text}"
                }
            })
        
        try:
            self.client.chat_postMessage(
                channel=channel,
                blocks=blocks,
                text="Resumo Diário de Alertas"
            )
            return True
        except SlackApiError as e:
            logger.error(f"Erro ao enviar resumo: {e.response['error']}")
            return False
    
    def send_file(self,
                 file_path: str,
                 title: str,
                 channel: Optional[str] = None,
                 comment: Optional[str] = None) -> bool:
        """Envia arquivo para o Slack """
        channel = channel or self.default_channel
        
        try:
            response = self.client.files_upload_v2(
                channel=channel,
                file=file_path,
                title=title,
                initial_comment=comment
            )
            logger.info(f"Arquivo enviado: {title}")
            return True
        except SlackApiError as e:
            logger.error(f" Erro ao enviar arquivo: {e.response['error']}")
            return False


# Exemplo de uso
if __name__ == "__main__":
    from dotenv import load_dotenv
    from alert_manager import create_quality_alert, create_anomaly_alert
    

    load_dotenv()
    
## Teste
    logging.basicConfig(level=logging.INFO)

    try:
        notifier = SlackNotifier()
        
        # Testa diferentes tipos de alertas
        alerts = [
            create_quality_alert("completeness", 0.85, 0.95, AlertSeverity.WARNING),
            create_anomaly_alert("row_count", 5000, (10000, 15000), AlertSeverity.ERROR),
        ]
        
        for alert in alerts:
            success = notifier.send_alert(alert)
            print(f"Alert enviado: {success}")
        
        # Envia resumo
        summary = {
            "total_alerts": 25,
            "last_24h": 5,
            "by_severity": {
                "info": 10,
                "warning": 8,
                "error": 5,
                "critical": 2
            },
            "by_source": {
                "data_quality": 15,
                "anomaly_detector": 10
            }
        }
        
        notifier.send_summary(summary)
        
    except Exception as e:
        logger.error(f"Erro: {e}")