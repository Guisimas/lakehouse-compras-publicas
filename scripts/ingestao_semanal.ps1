<#
Alternativa de emergência: ingestão semanal fora do Databricks, para quando o workspace não
alcança a API do governo (o Free Edition limita a saída para a internet). O caminho padrão é a
task de ingestão do próprio job. Para usar este script, faça o deploy com
`--var ingestao_no_job=false --var agendamento=PAUSED`.

Baixa os últimos 7 dias até ontem, as janelas de 14 e 30 dias atrás e a varredura de
contratos (quando vencida) direto no UC Volume e, se tudo deu certo, dispara o job
`compras-semanal`, que processa Bronze → Gold.

Uso manual:
    .\scripts\ingestao_semanal.ps1 -JobId 123456789 -Catalogo dev_compras

Agendar no Windows (toda segunda às 7h; o computador precisa estar ligado):
    $acao = New-ScheduledTaskAction -Execute "powershell.exe" `
        -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$PWD\scripts\ingestao_semanal.ps1`" -JobId 123456789"
    $gatilho = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday -At 7am
    Register-ScheduledTask -TaskName "compras-ingestao-semanal" -Action $acao -Trigger $gatilho `
        -Settings (New-ScheduledTaskSettingsSet -StartWhenAvailable)

O ID do job aparece em `databricks bundle summary -t dev`. A autenticação usa o perfil da
Databricks CLI (`databricks auth login`).
#>
param(
    [Parameter(Mandatory = $true)][long]$JobId,
    [string]$Catalogo = "dev_compras",
    [string]$Perfil = "DEFAULT"
)

$ErrorActionPreference = "Stop"
$raiz = Split-Path -Parent $PSScriptRoot
Set-Location $raiz

$logs = Join-Path $raiz "logs"
New-Item -ItemType Directory -Force $logs | Out-Null
$log = Join-Path $logs ("ingestao_{0:yyyy-MM-dd}.log" -f (Get-Date))

$env:UV_SYSTEM_CERTS = "1"          # proxy/certificado corporativo
$env:DATABRICKS_CONFIG_PROFILE = $Perfil

uv run compras-ingest --destino volume --catalog $Catalogo api --disparar-job $JobId *>&1 |
    Tee-Object -FilePath $log -Append
exit $LASTEXITCODE
