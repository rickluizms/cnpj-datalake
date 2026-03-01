# CNPJ Datalake

Pipeline de dados abertos de CNPJ da Receita Federal do Brasil, com arquitetura **Medallion** (Bronze → Silver → Gold).

Faz a ingestão dos arquivos ZIP via WebDAV, transforma os CSVs em Parquet com tipagem e limpeza progressiva, e gera datasets refinados para SQL, BI e Analytics/ML.

## Arquitetura

```
data/raw/{YYYY-MM}/          ← ZIPs originais da Receita Federal
data/bronze/{YYYY-MM}/       ← CSV → Parquet bruto (sem transformação)
data/silver/{YYYY-MM}/       ← Limpeza, tipagem, deduplicação
data/gold/{YYYY-MM}/
    ├── relacional/          ← Tabelas normalizadas para banco SQL
    ├── dimensional/         ← Modelo estrela (fatos + dimensões) para BI
    └── analytics/           ← Wide table desnormalizada para EDA e ML
```

## Estrutura do Projeto

```
cnpj-datalake/
├── main.py                        # CLI principal
├── pyproject.toml                 # Dependências e configuração
├── core/
│   ├── db.py                      # Utilitários de banco de dados
│   └── progress.py                # Barra de progresso Rich
├── src/
│   ├── ingest/
│   │   └── main.py                # Download async via WebDAV (httpx)
│   └── transform/
│       ├── main.py                # Orquestrador do pipeline
│       ├── bronze.py              # Raw ZIP/CSV → Parquet
│       ├── silver.py              # Limpeza, tipagem, normalização
│       ├── gold.py                # Relacional + Dimensional + Analytics
│       ├── schemas.py             # Definição de colunas e tipos
│       └── metrics.py             # Relatório de volumetria
└── tests/
    ├── test_ingestion.py          # Testes de download com cronometragem
    └── test_data_quality.py       # Testes de qualidade + transformação
```

## Pré-requisitos

- **Python** ≥ 3.10
- **[uv](https://docs.astral.sh/uv/)** (gerenciador de pacotes)

## Instalação

```bash
# Clonar o repositório
git clone <url-do-repo>
cd cnpj-datalake

# Instalar dependências
uv sync
```

## Configuração

Crie um arquivo `.env` na raiz do projeto com as seguintes variáveis:

```env
SHARE_ID=get_your_share_id_at_receita_federal
BASE_HOST=https://arquivos.receitafederal.gov.br
MAX_WORKERS=6
MAX_RETRIES=3
RETRY_DELAY=5
```

| Variável | Descrição |
|---|---|
| `BASE_HOST` | URL base do servidor WebDAV da Receita Federal |
| `SHARE_ID` | ID de compartilhamento público para autenticação WebDAV |
| `MAX_WORKERS` | Número máximo de downloads simultâneos (padrão: `5`) |
| `MAX_RETRIES` | Número máximo de tentativas por arquivo (padrão: `3`) |
| `RETRY_DELAY` | Delay base em segundos entre retries, com backoff exponencial (padrão: `5`) |

## Uso

### Pipeline Completo (Ingestão + Transformação)

```bash
uv run main.py --all
```

### Apenas Ingestão (Download)

```bash
uv run main.py --ingest
```

### Apenas Transformação (Bronze → Silver → Gold)

```bash
# Todas as camadas
uv run main.py --transform

# Camadas individuais via módulo de transformação
uv run -m src.transform.main --bronze
uv run -m src.transform.main --silver
uv run -m src.transform.main --gold

# Especificar período
uv run -m src.transform.main --period 2026-02
```

### Relatório de Volumetria

```bash
uv run -m src.transform.metrics
```

## Testes

### Todos os Testes

```bash
uv run pytest -s
```

> **Nota:** O flag `-s` é necessário para exibir as tabelas Rich com os relatórios de tempo e métricas.

### Teste de Ingestão (Download)

Executa o download de todos os arquivos e mede o tempo de cada um:

```bash
uv run pytest tests/test_ingestion.py -s
```

**O que testa:**
- Diretório de destino foi criado
- Arquivos baixados existem e não estão vazios
- Arquivos essenciais (Empresas, Socios, Estabelecimentos) foram baixados
- Nenhum erro de download ocorreu
- Ingestão completa em < 60 minutos

**Relatório gerado:** tabela com arquivo, status, tamanho, duração e velocidade de cada download.

### Teste de Qualidade de Dados (Transformação)

Executa a transformação completa (Bronze → Silver → Gold) e valida os resultados:

```bash
uv run pytest tests/test_data_quality.py -s
```

**O que testa:**
- Diretórios das três camadas foram criados
- Tabelas essenciais na Silver possuem registros
- Chaves primárias (CNPJ) sem valores nulos
- Modelo dimensional (fatos + dimensões) existe na Gold
- Pipeline completa em < 30 minutos

**Relatórios gerados:**
- ⏱ Tempo de processamento por camada (Bronze, Silver, Gold)
- 📊 Contagem de registros por tabela em cada camada

## Tabelas Processadas

| Tabela | Particionada | Descrição |
|---|---|---|
| Empresas | ✅ (10 partes) | Dados cadastrais das empresas |
| Socios | ✅ (10 partes) | Quadro societário |
| Estabelecimentos | ✅ (10 partes) | Filiais e matrizes |
| Simples | ❌ | Opção pelo Simples Nacional / MEI |
| Cnaes | ❌ | Classificação Nacional de Atividades Econômicas |
| Motivos | ❌ | Motivos de situação cadastral |
| Naturezas | ❌ | Naturezas jurídicas |
| Paises | ❌ | Tabela de países |
| Qualificacoes | ❌ | Qualificações de sócios |
| Municipios | ❌ | Tabela de municípios |

## Tecnologias

| Tecnologia | Uso |
|---|---|
| [Polars](https://pola.rs/) | Processamento de dados (DataFrames) |
| [httpx](https://www.python-httpx.org/) | Downloads assíncronos via WebDAV |
| [Rich](https://rich.readthedocs.io/) | Interface CLI (progresso, tabelas, cores) |
| [pytest](https://docs.pytest.org/) | Testes automatizados |
| [uv](https://docs.astral.sh/uv/) | Gerenciamento de pacotes e ambiente |
