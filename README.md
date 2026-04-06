# Jelastic Billing Sync

Automação em Python para ler dados de consumo da API SaveinCloud (Jelastic/Virtuozzo) 
via Google Sheets e popular um Dashboard no Grafana.

## Como configurar
1. Renomeie o arquivo `.env.example` para `.env` e preencha as variáveis.
2. Coloque suas credenciais do Google em `google_credentials.json`.
3. Certifique-se de que a planilha está compartilhada com o e-mail da Service Account.

## Rodando com Docker
```bash
docker-compose up -d --build
```

---

## Env Exemplo

```bash
SESSION_TOKEN=
APPID=cluster
DB_HOST=
DB_NAME=
DB_USER=
DB_PASSWORD=
GOOGLE_SHEET_ID=
```