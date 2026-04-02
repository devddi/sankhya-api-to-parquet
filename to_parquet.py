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

# --- CONFIGURAÇÃO DE LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

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
                    # Tenta encontrar a chave correta que contém os registros
                    linhas = next((rb[k] for k in ["records", "rows", "result", "data"] if k in rb), None)
                    
                    if linhas is None:
                        return pd.DataFrame(columns=colunas)
                    
                    df = pd.DataFrame(linhas, columns=colunas)
                    self._standardize_dates(df)
                    return df
                
                logger.warning(f"  ⚠️ Tentativa {tentativa+1} falhou (Sankhya Status {response.status_code}).")
                time.sleep(30)
            except Exception as e:
                logger.error(f"  ⚠️ Erro na tentativa {tentativa+1}: {str(e)}")
                time.sleep(30)
        return None

    def _standardize_dates(self, df: pd.DataFrame):
        """Padroniza colunas de data conhecidas para o formato datetime do Pandas."""
        colunas_data = ["DATA", "DHALTER", "DTALTER", "DTNEG", "DATA_NEG"]
        for col in df.columns:
            if col.upper() in colunas_data or "DATA" in col.upper() or "DT" in col.upper():
                try:
                    df[col] = pd.to_datetime(df[col], format='%d%m%Y %H:%M:%S', errors='coerce')
                    mask = df[col].isna()
                    if mask.any():
                        df.loc[mask, col] = pd.to_datetime(df.loc[mask, col], dayfirst=True, errors='coerce')
                except Exception as e:
                    logger.debug(f"Falha ao converter coluna {col}: {e}")

    def get_max_value_s3(self, s3_key: str, col_controle: str) -> Any:
        """Obtém o valor máximo de uma coluna de controle em um arquivo Parquet no S3."""
        try:
            response = self.s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_key)
            df_existente = pd.read_parquet(io.BytesIO(response['Body'].read()), columns=[col_controle])
            if not df_existente.empty:
                validos = df_existente[col_controle].dropna()
                return validos.max() if not validos.empty else None
        except ClientError:
            return None
        except Exception as e:
            logger.warning(f"Erro ao ler arquivo existente {s3_key}: {e}")
            return None
        return None

    def process_task(self, tarefa: Dict[str, Any]) -> str:
        """Processa uma única tarefa de extração (Full ou Incremental)."""
        sql_base = tarefa["sql"]
        s3_key = tarefa["path"]
        col_controle = tarefa.get("col_controle")
        pk = tarefa.get("pk")
        
        logger.info(f"Iniciando processamento: {s3_key}")
        
        max_val = self.get_max_value_s3(s3_key, col_controle) if col_controle else None
        sql_query = sql_base
        
        # Lógica Incremental
        if col_controle and max_val is not None and not pd.isna(max_val):
            if hasattr(max_val, 'strftime') or "DATA" in col_controle.upper() or "DT" in col_controle.upper():
                ts_str = max_val.strftime('%d/%m/%Y %H:%M:%S') if hasattr(max_val, 'strftime') else str(max_val)
                filtro = f"{col_controle} > TO_DATE('{ts_str}', 'DD/MM/YYYY HH24:MI:SS')"
            else:
                filtro = f"{col_controle} > {max_val}"
                
            conector = "AND" if "WHERE" in sql_base.upper() else "WHERE"
            sql_query = f"SELECT * FROM ({sql_base}) {conector} {filtro}"
            logger.info(f"  -> [INCREMENTAL] Desde: {max_val}")
        else:
            logger.info(f"  -> [FULL LOAD]")

        frames = []
        offset = 0
        while True:
            sql_paginado = self._paged_sql(sql_query, offset=offset, limit=PAGE_SIZE)
            df_pagina = self._fetch_page(sql_paginado)
            
            if df_pagina is None:
                raise Exception(f"Falha ao buscar dados na página {offset//PAGE_SIZE}")
            
            if df_pagina.empty:
                break
            
            frames.append(df_pagina)
            logger.info(f"     Página {len(frames)}: {len(df_pagina)} linhas baixadas.")
            
            if len(df_pagina) < PAGE_SIZE or len(frames) >= MAX_PAGES:
                if len(frames) >= MAX_PAGES:
                    logger.warning(f"  ⚠️ Limite de {MAX_PAGES} páginas atingido. Interrompendo para segurança.")
                break
            
            offset += PAGE_SIZE
            time.sleep(SLEEP_BETWEEN)

        if not frames:
            return f"Finalizado: {s3_key} sem novos dados."

        df_novo = pd.concat(frames, ignore_index=True)

        # Merge de dados (se incremental)
        if col_controle:
            try:
                resp = self.s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_key)
                df_antigo = pd.read_parquet(io.BytesIO(resp['Body'].read()))
                self._standardize_dates(df_antigo)
                
                df_total = pd.concat([df_antigo, df_novo], ignore_index=True)
                if pk:
                    df_total = df_total.drop_duplicates(subset=[pk], keep='last')
            except ClientError:
                df_total = df_novo
        else:
            df_total = df_novo

        # Escrita no S3
        buffer = io.BytesIO()
        df_total.to_parquet(buffer, engine="pyarrow", index=False)
        buffer.seek(0)
        self.s3_client.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=buffer.getvalue())
        
        return f"Sucesso: {s3_key} ({len(df_total)} linhas totais no arquivo final)."

# --- CONFIGURAÇÃO DAS TAREFAS ---
TAREFAS = [
    {"sql": "SELECT * FROM VW_LIBERACAO_PEDIDOS where numcontrato is not null and usuario <> 'CAMILA.RIPARDO'", "path": "bronze/liberacao_pedidos.parquet"},
    {"sql": "SELECT * FROM VW_CONTAS_A_PAGAR", "path": "bronze/contas_a_pagar.parquet"},
    {"sql": "SELECT * FROM VW_CONSUMO_NIVEL_V2", "path": "bronze/consumo_contratos_resumo.parquet"},
    {"sql": "SELECT * FROM VW_CONTAS_A_RECEBER", "path": "bronze/contas-a-receber.parquet"},
    {"sql": "SELECT * FROM VW_COMPRAS_NOTAS_FISCAIS", "path": "bronze/solicitacoes_compras_nf.parquet"},
    {"sql": "SELECT * FROM VW_FATO_PLANEJAMENTO", "path": "bronze/consumo_contratos_planejamento.parquet"},
    {"sql": "SELECT * FROM VW_DIM_CONTRATO", "path": "bronze/consumo_contratos_base.parquet"},
    {"sql": "SELECT * FROM VW_AUDITORIA_LANCAMENTOS", "path": "bronze/auditoria_lancamentos.parquet"},
    {"sql": "SELECT * FROM VW_DFC_CONSOLIDADA_REDUZIDA", "path": "bronze/dfc_consolidada_reduzida.parquet"},
    {"sql": "SELECT * FROM VW_FATO_COMPRAS_PEDIDOS where cod_pedido is not null", "path": "bronze/fato_pedidos_compras.parquet"},
    {"sql": "SELECT * FROM VW_DIM_COMPRAS_SOLICITACOES", "path": "bronze/dim_solicitacoes_compras.parquet"},

    # INCREMENTAIS
    {
        "sql": "SELECT * FROM VW_DFC_CONSOLIDADA_FINAL", 
        "path": "bronze/dfc_consolidada_final.parquet",
        "col_controle": "DATA", "pk": None
    },
    {
        "sql": "SELECT * FROM VW_COMPRAS_POR_ITEM", 
        "path": "bronze/compras_por_item.parquet",
        "col_controle": "DATA_NEG", "pk": None
    },
    {
        "sql": "SELECT * FROM VW_SOLICITACOES_ITENS_FULL", 
        "path": "bronze/itens_solicitacoes_compras.parquet",
        "col_controle": "COD_DOCUMENTO", "pk": "COD_DOCUMENTO"
    },
    {
        "sql": "SELECT * FROM VW_FATO_CONSUMO where id_chave is not null ", 
        "path": "bronze/consumo_contratos_financeiro.parquet",
        "col_controle": "NUFIN", "pk": "NUFIN"
    },
    {
        "sql": "SELECT * FROM VW_ITENS_COTACAO", 
        "path": "bronze/itens_cotacao.parquet",
        "col_controle": "NUMCOTACAO", "pk": "NUMCOTACAO"
    }
]

if __name__ == "__main__":
    start_time = datetime.now()
    logger.info(f"Pipeline Iniciado: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    if not SANKHYA_TOKEN:
        logger.error("ERRO: SANKHYA_TOKEN não configurado nas variáveis de ambiente.")
        exit(1)
        
    pipeline = SankhyaToParquetPipeline()
    
    for t in TAREFAS:
        try:
            resultado = pipeline.process_task(t)
            logger.info(resultado)
        except Exception as e:
            logger.error(f"FALHA CRÍTICA na tarefa {t['path']}: {str(e)}")
            
    end_time = datetime.now()
    logger.info(f"Pipeline Finalizado. Duração: {end_time - start_time}")