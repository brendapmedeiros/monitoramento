from dotenv import load_dotenv
from slack_sdk import WebClient
import os

load_dotenv()

client = WebClient(token=os.getenv("SLACK_BOT_TOKEN"))

print(" Testando conexão...")
response = client.auth_test()
print(f" Conectado como: {response['user']}")

print("\n Enviando mensagem de teste...")
result = client.chat_postMessage(
    channel=os.getenv("SLACK_CHANNEL_ID"),
    text="Tudo certo, o sistema de notificações está configurado! "
)
print(f"Mensagem enviada! (ts: {result['ts']})")