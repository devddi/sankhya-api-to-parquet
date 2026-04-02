# Sankhya to Parquet Pipeline

Este repositório contém um pipeline de extração de dados da API do Sankhya, conversão para o formato Parquet e carregamento para um bucket AWS S3. O pipeline é projetado para ser executado de forma orquestrada pelo **Kestra**.

## Estrutura do Projeto

- `to_parquet.py`: Script principal em Python que realiza a extração (Full e Incremental) e o upload.
- `to_parquet.yaml`: Definição do fluxo (Flow) do Kestra.
- `requirements.txt`: Dependências do Python.
- `.gitignore`: Arquivos ignorados pelo Git.

## Requisitos

- Python 3.9+
- Acesso à API do Sankhya (Token e URL).
- Credenciais AWS (Access Key, Secret Key) com permissão de escrita no bucket S3.

## Configuração no Kestra

Para rodar este pipeline no Kestra, você deve configurar as seguintes variáveis no **KV Store** ou como **Secrets** do namespace:

### Secrets (Recomendado)
- `SANKHYA_TOKEN`: Token de autenticação da API Sankhya.
- `AWS_ACCESS_KEY_ID`: Chave de acesso AWS.
- `AWS_SECRET_ACCESS_KEY`: Chave secreta AWS.

### KV Store (Variáveis)
- `SANKHYA_API_URL`: URL base da API (Ex: `https://api-sankhya.pluralmed.com.br/api/query`).
- `AWS_BUCKET_NAME`: Nome do bucket S3 onde os arquivos serão salvos.

## Como usar

1. Crie um novo repositório no GitHub.
2. Suba todos os arquivos desta pasta.
3. No arquivo `to_parquet.yaml`, atualize a variável `GITHUB_REPO_URL` para apontar para a URL 'raw' do seu repositório (ex: `https://raw.githubusercontent.com/seu-usuario/seu-repo/main`).
4. Importe o conteúdo de `to_parquet.yaml` no editor de fluxos do seu Kestra.
5. Execute manualmente ou aguarde o agendamento (Trigger).

## Funcionalidades

- **Carga Incremental**: O script verifica automaticamente o maior valor de uma coluna de controle (ex: NUI ou Data) no arquivo existente no S3 e busca apenas os novos registros.
- **Deduplicação**: Utiliza chaves primárias (PK) para garantir que não haja registros duplicados durante o merge de dados incrementais.
- **Logging**: Logs detalhados para monitoramento através do console do Kestra.
- **Resiliência**: Lógica de retentativa (retries) para falhas temporárias na API.
