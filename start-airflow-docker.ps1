Write-Host " Iniciando Airflow com Docker..." -ForegroundColor Green
Write-Host ""


try {
    docker ps | Out-Null
    Write-Host " Docker rodando" -ForegroundColor Green
} catch {
    Write-Host " Docker não está rodando!" -ForegroundColor Red
    Write-Host "Execute o Docker Desktop primeiro" -ForegroundColor Yellow
    exit 1
}


$folders = @("dags", "logs", "plugins", "config", "data")
foreach ($folder in $folders) {
    if (-not (Test-Path $folder)) {
        New-Item -ItemType Directory -Path $folder -Force | Out-Null
        Write-Host " Criada pasta: $folder" -ForegroundColor Cyan
    }
}

Write-Host ""
Write-Host " Iniciando containers..." -ForegroundColor Green
Write-Host ""

docker compose up -d

Write-Host ""
Write-Host " Aguardando Airflow  (1-2 minutos)..." -ForegroundColor Yellow
Start-Sleep -Seconds 30

Write-Host ""
Write-Host " Airflow iniciado!" -ForegroundColor Green
Write-Host ""
Write-Host " Acesse: http://localhost:8080" -ForegroundColor Magenta
Write-Host " Login: admin / admin123" -ForegroundColor Magenta
Write-Host ""