import os
import io
import json
import time
import logging
import requests
import pandas as pd
import boto3
from datetime import datetime
from typing import List, Dict, Optional, Any
from botocore.config import Config
from botocore.exceptions import ClientError

# --- CONFIGURAÇÃO DE LOGGING DIDÁTICO ---
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def log_header(msg: str):
    logger.info(f"\n{'='*60}\n🚀 {msg.upper()}\n{'='*60}")

def log_step(emoji: str, msg: str):
    logger.info(f"{emoji} {msg}")

# --- CONFIGURAÇÕES DE AMBIENTE ---
SANKHYA_URL = os.getenv("SANKHYA_API_URL", "https://api-sankhya.pluralmed.com.br/api/query")
SANKHYA_TOKEN = os.getenv("SANKHYA_TOKEN")
BUCKET_NAME = os.getenv("AWS_BUCKET_NAME", "pluralmed-data-platform")
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")

# Configurações de Execução
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "2000"))
MAX_PAGES = int(os.getenv("MAX_PAGES", "5000"))
SLEEP_BETWEEN = int(os.getenv("SLEEP_BETWEEN", "30"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "20"))

class SankhyaToParquetPipeline:
    def __init__(self):
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=AWS_REGION,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"})
        )
        self.headers = {
            "Authorization": f"Bearer {SANKHYA_TOKEN}",
            "Content-Type": "application/json"
        }
        self.results = []

    def _paged_sql(self, base_sql: str, offset: int, limit: int) -> str:
        """Gera o SQL paginado para o Sankhya/SQL Server."""
        return f"SELECT * FROM ({base_sql}) OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"

    def _fetch_page(self, sql_paginado: str) -> Optional[pd.DataFrame]:
        """Busca uma página de dados da API do Sankhya com lógica de retentativa."""
        payload = {
            "body": {
                "requestBody": {"sql": sql_paginado},
                "serviceName": "DbExplorerSP.executeQuery"
            },
            "module": "mge",
            "service_name": "string"
        }

        for tentativa in range(MAX_RETRIES):
            try:
                response = requests.post(SANKHYA_URL, headers=self.headers, data=json.dumps(payload), timeout=180)
                if response.status_code == 200:
                    data = response.json()
                    rb = data.get("data", {}).get("responseBody", {})
                    fields = rb.get("fieldsMetadata")
                    
                    if not fields:
                        return pd.DataFrame()
                    
                    colunas = [c["name"] for c in fields]
                    linhas = next((rb[k] for k in ["records", "rows", "result", "data"] if k in rb), None)
                    
                    if linhas is None:
                        return pd.DataFrame(columns=colunas)
                    
                    df = pd.DataFrame(linhas, columns=colunas)
                    self._standardize_dates(df)
                    return df
                
                # Mensagens amigáveis para erros comuns (como 502)
                error_msg = f"Status {response.status_code}"
                if response.status_code == 502:
                    error_msg = "Instabilidade no Servidor (Erro 502 - Bad Gateway)"
                elif response.status_code == 401:
                    error_msg = "Token Expirado ou Inválido (Erro 401)"
                
                log_step("⚠️", f"{error_msg}. Tentativa {tentativa+1}/{MAX_RETRIES} - Aguardando 30s...")
                time.sleep(30)
            except Exception as e:
                log_step("⚠️", f"Falha na conexão: {str(e)}. Tentativa {tentativa+1}/{MAX_RETRIES} - Aguardando 30s...")
                time.sleep(30)
        return None

    def _standardize_dates(self, df: pd.DataFrame):
        """Padroniza colunas de data conhecidas para o formato datetime do Pandas."""
        for col in df.columns:
            c_upper = col.upper()
            # Detecta se é uma coluna de data (DT..., DATA... ou contém DATA)
            if c_upper.startswith("DT") or c_upper.startswith("DATA") or "DATA" in c_upper:
                # Tenta converter do formato DDMMYYYY HH:MM:SS
                temp_date = pd.to_datetime(df[col], format='%d%m%Y %H:%M:%S', errors='coerce')
                
                # Se falhou e for numérico, tenta unit='ms' (comum na API Sankhya)
                if temp_date.isna().all() and pd.api.types.is_numeric_dtype(df[col]):
                    temp_date = pd.to_datetime(df[col], unit='ms', errors='coerce')
                
                # Fallback genérico
                if temp_date.isna().all():
                    temp_date = pd.to_datetime(df[col], errors='coerce')
                
                df[col] = temp_date

    def _format_dates_to_string(self, df: pd.DataFrame):
        """Converte todas as colunas datetime para String no formato DDMMYYYY HH:MM:SS antes de salvar."""
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                # Se for NaT, fica vazio, senão formata. fillna("") garante que não salve NaT como texto
                df[col] = df[col].dt.strftime('%d%m%Y %H:%M:%S').replace("NaT", "")

    def get_max_value_s3(self, s3_key: str, col_controle: str) -> Any:
        """Obtém o valor máximo de uma coluna de controle em um arquivo Parquet no S3."""
        try:
            response = self.s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_key)
            df_existente = pd.read_parquet(io.BytesIO(response['Body'].read()), columns=[col_controle])
            if not df_existente.empty:
                # Tentamos converter para data no formato do nosso sistema
                val_date = pd.to_datetime(df_existente[col_controle], format='%d%m%Y %H:%M:%S', errors='coerce')
                validos_date = val_date.dropna()
                if not validos_date.empty:
                    return validos_date.max()
                
                # Se não for data (tudo NaT), tentamos pegar o max numérico
                val_num = pd.to_numeric(df_existente[col_controle], errors='coerce')
                validos_num = val_num.dropna()
                if not validos_num.empty:
                    return validos_num.max()
                
                # Fallback genérico
                validos = df_existente[col_controle].dropna()
                return validos.max() if not validos.empty else None
        except ClientError:
            return None
        except Exception as e:
            return None
        return None

    def process_task(self, tarefa: Dict[str, Any]):
        """Processa uma única tarefa de extração (Full ou Incremental)."""
        sql_base = tarefa["sql"]
        s3_key = tarefa["path"]
        col_controle = tarefa.get("col_controle")
        pk = tarefa.get("pk")
        
        logger.info(f"\n🔹 Processando Tabela: {s3_key}")
        
        max_val = self.get_max_value_s3(s3_key, col_controle) if col_controle else None
        sql_query = sql_base
        
        # Lógica Incremental
        tipo_carga = "FULL"
        if col_controle and max_val is not None and not pd.isna(max_val):
            tipo_carga = "INCREMENTAL"
            if isinstance(max_val, pd.Timestamp) or isinstance(max_val, datetime):
                # O Sankhya/Oracle espera TO_DATE para filtros precisos
                ts_str = max_val.strftime('%d/%m/%Y %H:%M:%S')
                filtro = f"{col_controle} > TO_DATE('{ts_str}', 'DD/MM/YYYY HH24:MI:SS')"
                log_step("⚖️", f"Modo Incremental detectado (Data). Buscando desde: {ts_str}")
            else:
                filtro = f"{col_controle} > {max_val}"
                log_step("⚖️", f"Modo Incremental detectado (Numérico). Buscando desde: {max_val}")
                
            conector = "AND" if "WHERE" in sql_base.upper() else "WHERE"
            sql_query = f"{sql_base} {conector} {filtro}"
        else:
            log_step("📤", "Modo Carga Total (Full Load).")

        frames = []
        offset = 0
        while True:
            sql_paginado = self._paged_sql(sql_query, offset=offset, limit=PAGE_SIZE)
            df_pagina = self._fetch_page(sql_paginado)
            
            if df_pagina is None:
                self.results.append({"tabela": s3_key, "status": "Erro", "linhas": 0, "tipo": tipo_carga})
                raise Exception(f"Falha crítica na API após {MAX_RETRIES} tentativas.")
            
            if df_pagina.empty:
                break
            
            frames.append(df_pagina)
            log_step("🔄", f"Página {len(frames)} processada... (+{len(df_pagina)} linhas)")
            
            if len(df_pagina) < PAGE_SIZE or len(frames) >= MAX_PAGES:
                if len(frames) >= MAX_PAGES:
                    log_step("⚠️", f"Limite de {MAX_PAGES} páginas atingido.")
                break
            
            offset += PAGE_SIZE
            time.sleep(3)

        if not frames:
            log_step("😴", "Nenhum dado novo encontrado.")
            self.results.append({"tabela": s3_key, "status": "Sem Dados", "linhas": 0, "tipo": tipo_carga})
            return

        df_novo = pd.concat(frames, ignore_index=True)
        linhas_novas = len(df_novo)

        # Merge de dados (se incremental)
        if col_controle:
            try:
                resp = self.s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_key)
                df_antigo = pd.read_parquet(io.BytesIO(resp['Body'].read()))
                # Importante converter o antigo de volta para data antes do merge para evitar NaTs
                self._standardize_dates(df_antigo)
                
                df_total = pd.concat([df_antigo, df_novo], ignore_index=True)
                if pk:
                    df_total = df_total.drop_duplicates(subset=[pk], keep='last')
            except ClientError:
                df_total = df_novo
        else:
            df_total = df_novo

        # --- FORMATAÇÃO FINAL PARA EXPORTAÇÃO (PADRÃO PRINT) ---
        self._format_dates_to_string(df_total)

        # Escrita no S3
        buffer = io.BytesIO()
        df_total.to_parquet(buffer, engine="pyarrow", index=False)
        buffer.seek(0)
        self.s3_client.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=buffer.getvalue())
        
        log_step("✅", f"Sucesso! {linhas_novas} novos registros adicionados. ({len(df_total)} linhas totais).")
        self.results.append({"tabela": s3_key, "status": "Sucesso", "linhas": len(df_total), "tipo": tipo_carga})

    def print_summary(self, duration):
        log_header("Resumo Final da Execução")
        logger.info(f"{'TABELA':<40} | {'TIPO':<11} | {'STATUS':<10} | {'LINHAS':<10}")
        logger.info("-" * 80)
        for r in self.results:
            logger.info(f"{r['tabela']:<40} | {r['tipo']:<11} | {r['status']:<10} | {r['linhas']:<10}")
        logger.info("-" * 80)
        logger.info(f"Duração Total: {duration}")

# --- CONFIGURAÇÃO DAS TAREFAS ---
TAREFAS = [
    {"sql": "SELECT * FROM VW_FATO_CONSUMO where id_chave is not null", "path": "bronze/consumo_contratos_financeiro.parquet"}, #Consumo dos contratos
    {"sql": "SELECT * FROM VW_LIBERACAO_PEDIDOS where numcontrato is not null and usuario <> 'CAMILA.RIPARDO'", "path": "bronze/liberacao_pedidos.parquet"}, #Consumo dos contratos
    {"sql": "SELECT * FROM VW_FATO_PLANEJAMENTO", "path": "bronze/consumo_contratos_planejamento.parquet"}, #Consumo dos contratos
    {"sql": "SELECT * FROM VW_DIM_CONTRATO", "path": "bronze/consumo_contratos_base.parquet"}, #Consumo dos contratos
    {"sql": "SELECT * FROM VW_AUDITORIA_LANCAMENTOS", "path": "bronze/auditoria_lancamentos.parquet"}, #Consumo dos contratos
    {"sql": "SELECT * FROM VW_CONSUMO_NIVEL_V2", "path": "bronze/consumo_contratos_resumo.parquet"}, #Consumo dos contratos

    {"sql": "SELECT * FROM VW_CONTAS_A_PAGAR", "path": "bronze/contas_a_pagar.parquet"}, #Financeiro
    {"sql": "SELECT * FROM VW_CONTAS_A_RECEBER", "path": "bronze/contas-a-receber.parquet"}, #Financeiro
    {"sql": "SELECT * FROM VW_DFC_CONSOLIDADA_REDUZIDA", "path": "bronze/dfc_consolidada_reduzida.parquet"}, #Financeiro
    {"sql": "SELECT * FROM VW_DFC_CONSOLIDADA_FINAL", "path": "bronze/dfc_consolidada_final.parquet"}, #Financeiro

    {"sql": "SELECT * FROM VW_FORNECEDOR_RUBRICA", "path": "bronze/rubricas_fornecedor.parquet"}, #Compras
    {"sql": "SELECT * FROM VW_COMPRAS_NOTAS_FISCAIS", "path": "bronze/solicitacoes_compras_nf.parquet"}, #Compras
    {"sql": "SELECT * FROM VW_DIM_COMPRAS_SOLICITACOES", "path": "bronze/dim_solicitacoes_compras.parquet"}, #Compras
    {"sql": "SELECT * FROM VW_FATO_COMPRAS_PEDIDOS where cod_pedido is not null", "path": "bronze/fato_pedidos_compras.parquet"}, #Compras
    {"sql": "SELECT * FROM VW_SOLICITACOES_ITENS_FULL", "path": "bronze/itens_solicitacoes_compras.parquet"}, #Compras
    {"sql": "SELECT * FROM VW_COMPRAS_POR_ITEM", "path": "bronze/compras_por_item.parquet"}, #Compras
    {"sql": "SELECT * FROM VW_ITENS_COTACAO", "path": "bronze/itens_cotacao.parquet"} #Compras

    # INCREMENTAIS
    # {
    #     "sql": "SELECT * FROM VW_DFC_CONSOLIDADA_FINAL", 
    #     "path": "bronze/dfc_consolidada_final.parquet",
    #     "col_controle": "DATA", "pk": None
    # },
    # {
    #     "sql": "SELECT * FROM VW_COMPRAS_POR_ITEM", 
    #     "path": "bronze/compras_por_item.parquet",
    #     "col_controle": "DATA_NEG", "pk": None
    # },
    # {
    #     "sql": "SELECT * FROM VW_SOLICITACOES_ITENS_FULL", 
    #     "path": "bronze/itens_solicitacoes_compras.parquet",
    #     "col_controle": "COD_DOCUMENTO", "pk": "COD_DOCUMENTO"
    # },
    # {
    #     "sql": "SELECT * FROM VW_FATO_CONSUMO where id_chave is not null ", 
    #     "path": "bronze/consumo_contratos_financeiro.parquet",
    #     "col_controle": "NUFIN", "pk": "NUFIN"
    # },
    # {
    #     "sql": "SELECT * FROM VW_ITENS_COTACAO", 
    #     "path": "bronze/itens_cotacao.parquet",
    #     "col_controle": "NUMCOTACAO", "pk": "NUMCOTACAO"
    # }
]

if __name__ == "__main__":
    start_time = datetime.now()
    log_header(f"Iniciando Pipeline Sankhya -> S3 Parquet")
    
    if not SANKHYA_TOKEN:
        logger.error("❌ ERRO: SANKHYA_TOKEN não configurado nas variáveis de ambiente.")
        exit(1)
        
    pipeline = SankhyaToParquetPipeline()
    
    for t in TAREFAS:
        try:
            pipeline.process_task(t)
        except Exception as e:
            logger.error(f"❌ FALHA CRÍTICA na tarefa {t['path']}: {str(e)}")
            
    end_time = datetime.now()
    pipeline.print_summary(end_time - start_time)
