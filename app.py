import pandas as pd
import requests
import psycopg2
import os
import datetime
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv()

SESSION_TOKEN = os.getenv("SESSION_TOKEN")
APPID = os.getenv("APPID", "cluster")

URL_BILLING = "https://jca.paas.saveincloud.net.br/JBilling/billing/account/rest/getaccountbillinghistorybyperiodinner"
URL_FUNDING = "https://jca.paas.saveincloud.net.br/JBilling/billing/account/rest/getfundaccounthistory"

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "db"),
        database=os.getenv("DB_NAME", "billing"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD")
    )

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    # Tabela 1: Cadastro do Cliente
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS clientes_planilha (
            uid INTEGER PRIMARY KEY,
            nome_cliente VARCHAR(255),
            data_conversao TIMESTAMP
        )
    ''')
    # Tabela 2: Faturamento Diário (Time Series)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS consumo_diario_grafana (
            uid INTEGER,
            data_consumo DATE,
            custo NUMERIC(10, 4),
            PRIMARY KEY (uid, data_consumo)
        )
    ''')
    conn.commit()
    cursor.close()
    conn.close()

def descobrir_primeira_recarga(uid):
    data_inicio = "2026-01-01 00:00:00"
    data_hoje = datetime.datetime.now().strftime("%Y-%m-%d 23:59:59")
    
    params = {
        'appid': APPID,
        'session': SESSION_TOKEN,
        'uid': uid,
        'starttime': data_inicio,
        'endtime': data_hoje,
        'startRow': 0,
        'resultCount': 1000,
        'charset': 'UTF-8'
    }
    
    try:
        response = requests.get(URL_FUNDING, params=params, timeout=30)
        data = response.json()
        
        if data.get('result') == 0 and 'responses' in data:
            fundings = [r for r in data['responses'] if r.get('chargeType') == 'FUND']
            if fundings:
                fundings.sort(key=lambda x: x['operationDate'])
                primeira_recarga_ms = fundings[0]['operationDate']
                data_exata = datetime.datetime.fromtimestamp(primeira_recarga_ms / 1000.0)
                return data_exata
        return None
    except Exception as e:
        print(f"Erro ao buscar histórico de recarga para UID {uid}: {e}")
        return None

def consultar_consumo_api(uid, data_inicio):
    data_hoje = datetime.datetime.now().strftime("%Y-%m-%d 23:59:59")
    
    if isinstance(data_inicio, datetime.datetime) or type(data_inicio).__name__ == 'Timestamp':
        data_inicio = data_inicio.strftime("%Y-%m-%d 00:00:00")

    params = {
        'appid': APPID,
        'session': SESSION_TOKEN,
        'period': 'day',
        'groupNodes': 'false',
        'uid': uid,
        'node': 'root',
        'starttime': data_inicio,
        'endtime': data_hoje
    }

    try:
        response = requests.get(URL_BILLING, params=params, timeout=30)
        data = response.json()
        
        # Dicionário para somar o custo de todos os servidores do mesmo dia
        consumo_agrupado = {}
        
        if data.get('result') == 0 and 'array' in data:
            # === DEBUG MÁGICO ===
            # Pega só o primeiro cliente para a gente ver o nome exato das chaves que a API retorna
            if len(data['array']) > 0 and uid == 36997:
                print(f"   [DEBUG API] Chaves recebidas do Jelastic: {list(data['array'][0].keys())}")
            # ====================

            for item in data['array']:
                # 👇 O MISTÉRIO REVELADO AQUI! Trocamos para buscar 'dateTime'
                data_bruta = item.get('dateTime') 
                custo = item.get('cost', 0.0)
                
                if data_bruta and custo > 0:
                    # Converte pra string e pega só a parte YYYY-MM-DD
                    data_dia = str(data_bruta).split(' ')[0]
                    
                    # SOMA o custo no dicionário
                    consumo_agrupado[data_dia] = consumo_agrupado.get(data_dia, 0.0) + custo

        # Retorna uma lista organizada: [('2026-01-20', 10.50), ('2026-01-21', 5.20)]
        return list(consumo_agrupado.items())
    
    except Exception as e:
        print(f"   ❌ Erro de conexão ao consultar consumo para UID {uid}: {e}")
        return []

def processar_google_sheets(planilha_key):
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly"
    ]
    creds = Credentials.from_service_account_file("google_credentials.json", scopes=scopes)
    client = gspread.authorize(creds)

    planilha = client.open_by_key(planilha_key)

    init_db()
    conn = get_db_connection()
    cursor = conn.cursor()

    for aba in planilha.worksheets():
        titulo_aba = aba.title
        print(f"\n📂 Lendo aba: {titulo_aba}...")
        
        dados = aba.get_all_values()
        
        if not dados or len(dados) < 2:
            print(f"   ⚠️ Aba '{titulo_aba}' vazia ou sem dados suficientes. Pulando...")
            continue

        df = pd.DataFrame(dados[1:], columns=dados[0])

        df.columns = df.columns.str.strip()
        if 'DATA DE CONVERSÃO' in df.columns:
            df.rename(columns={'DATA DE CONVERSÃO': 'DATA_CONVERSAO'}, inplace=True)

        if 'ID' not in df.columns:
            print(f"   ⚠️ Coluna 'ID' não encontrada na aba '{titulo_aba}'. Pulando...")
            continue

        df['ID'] = pd.to_numeric(df['ID'], errors='coerce')
        df = df.dropna(subset=['ID'])

        print(f"🚀 Processando {len(df)} clientes válidos da aba {titulo_aba}...")

        for _, row in df.iterrows():
            uid = int(row['ID'])
            nome = row.get('CLIENTE', f'UID_{uid}')
            
            data_planilha = row.get('DATA_CONVERSAO')
            
            if pd.isna(data_planilha) or str(data_planilha).strip() == '':
                data_conv = descobrir_primeira_recarga(uid)
                if not data_conv:
                    continue
            else:
                data_conv = pd.to_datetime(data_planilha, dayfirst=True)
            
            print(f"💸 Agrupando consumo de {nome} (ID: {uid})...")
            
            consumos_diarios = consultar_consumo_api(uid, data_conv)
            
            cursor.execute('''
                INSERT INTO clientes_planilha (uid, nome_cliente, data_conversao)
                VALUES (%s, %s, %s)
                ON CONFLICT (uid) DO UPDATE SET
                    nome_cliente = EXCLUDED.nome_cliente,
                    data_conversao = EXCLUDED.data_conversao;
            ''', (uid, nome, data_conv))

            for data_dia, custo in consumos_diarios:
                cursor.execute('''
                    INSERT INTO consumo_diario_grafana (uid, data_consumo, custo)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (uid, data_consumo) DO UPDATE SET
                        custo = EXCLUDED.custo;
                ''', (uid, data_dia, custo))

    conn.commit()
    cursor.close()
    conn.close()
    print("\n✅ Todas as abas foram processadas com sucesso!")

if __name__ == "__main__":
    ID_DA_PLANILHA = os.getenv("GOOGLE_SHEET_ID")
    processar_google_sheets(ID_DA_PLANILHA)