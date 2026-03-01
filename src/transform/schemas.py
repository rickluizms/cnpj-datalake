"""
Schemas de dados do CNPJ - Receita Federal.

Define colunas, tipos e metadados para cada tabela nas camadas
Bronze, Silver e Gold da arquitetura Medallion.
"""

import polars as pl

# =============================================================================
# CONFIGURAÇÃO DOS ARQUIVOS
# =============================================================================

CSV_SEPARATOR = ";"
CSV_ENCODING = "latin1"

# Mapeamento: nome base → sufixo identificador do CSV dentro do ZIP
# Os arquivos dentro dos ZIPs da RF têm nomes como:
#   F.K03200$Z.D60214.CNAECSV  ou  K3241.K03200Y0.D60214.EMPRECSV
# O sufixo é a parte final que identifica o tipo do arquivo.
FILE_META = {
    "Empresas": {"suffix": "EMPRECSV", "partitioned": True},
    "Socios": {"suffix": "SOCIOCSV", "partitioned": True},
    "Estabelecimentos": {"suffix": "ESTABELE", "partitioned": True},
    "Simples": {"suffix": "SIMPLES.CSV", "partitioned": False},
    "Cnaes": {"suffix": "CNAECSV", "partitioned": False},
    "Motivos": {"suffix": "MOTICSV", "partitioned": False},
    "Naturezas": {"suffix": "NATJUCSV", "partitioned": False},
    "Paises": {"suffix": "PAISCSV", "partitioned": False},
    "Qualificacoes": {"suffix": "QUALSCSV", "partitioned": False},
    "Municipios": {"suffix": "MUNICCSV", "partitioned": False},
}

# =============================================================================
# SCHEMAS BRONZE (nomes de colunas apenas, tudo como Utf8)
# =============================================================================

EMPRESAS_COLS = [
    "cnpj_basico",
    "razao_social",
    "natureza_juridica",
    "qualificacao_responsavel",
    "capital_social",
    "porte_empresa",
    "ente_federativo_responsavel",
]

ESTABELECIMENTOS_COLS = [
    "cnpj_basico",
    "cnpj_ordem",
    "cnpj_dv",
    "identificador_matriz_filial",
    "nome_fantasia",
    "situacao_cadastral",
    "data_situacao_cadastral",
    "motivo_situacao_cadastral",
    "nome_cidade_exterior",
    "pais",
    "data_inicio_atividade",
    "cnae_fiscal_principal",
    "cnae_fiscal_secundaria",
    "tipo_logradouro",
    "logradouro",
    "numero",
    "complemento",
    "bairro",
    "cep",
    "uf",
    "municipio",
    "ddd_1",
    "telefone_1",
    "ddd_2",
    "telefone_2",
    "ddd_fax",
    "fax",
    "correio_eletronico",
    "situacao_especial",
    "data_situacao_especial",
]

SOCIOS_COLS = [
    "cnpj_basico",
    "identificador_socio",
    "nome_socio_razao_social",
    "cpf_cnpj_socio",
    "qualificacao_socio",
    "data_entrada_sociedade",
    "pais",
    "representante_legal",
    "nome_do_representante",
    "qualificacao_representante_legal",
    "faixa_etaria",
]

SIMPLES_COLS = [
    "cnpj_basico",
    "opcao_pelo_simples",
    "data_opcao_simples",
    "data_exclusao_simples",
    "opcao_mei",
    "data_opcao_mei",
    "data_exclusao_mei",
]

GENERIC_COLS = [
    "codigo",
    "descricao",
]

# Mapeamento nome base → colunas
SCHEMA_COLUMNS = {
    "Empresas": EMPRESAS_COLS,
    "Estabelecimentos": ESTABELECIMENTOS_COLS,
    "Socios": SOCIOS_COLS,
    "Simples": SIMPLES_COLS,
    "Cnaes": GENERIC_COLS,
    "Motivos": GENERIC_COLS,
    "Naturezas": GENERIC_COLS,
    "Paises": GENERIC_COLS,
    "Qualificacoes": GENERIC_COLS,
    "Municipios": GENERIC_COLS,
}

# =============================================================================
# SCHEMAS SILVER (tipos específicos para cada coluna)
# =============================================================================

# Colunas de data no formato YYYYMMDD da Receita Federal
DATE_COLUMNS = {
    "data_situacao_cadastral",
    "data_inicio_atividade",
    "data_situacao_especial",
    "data_entrada_sociedade",
    "data_opcao_simples",
    "data_exclusao_simples",
    "data_opcao_mei",
    "data_exclusao_mei",
}

# Colunas que devem ser convertidas para Float64
FLOAT_COLUMNS = {
    "capital_social",
}

# Colunas que devem ser convertidas para Int32
INT_COLUMNS = {
    "identificador_matriz_filial",
    "situacao_cadastral",
    "motivo_situacao_cadastral",
    "identificador_socio",
    "faixa_etaria",
    "porte_empresa",
}

# Colunas de texto que devem ter trim + uppercase
TEXT_NORMALIZE_COLUMNS = {
    "razao_social",
    "nome_fantasia",
    "nome_socio_razao_social",
    "nome_do_representante",
    "nome_cidade_exterior",
    "logradouro",
    "complemento",
    "bairro",
    "tipo_logradouro",
    "correio_eletronico",
    "descricao",
    "ente_federativo_responsavel",
}

# =============================================================================
# LABELS DE DOMÍNIO (para enriquecimento na Silver/Gold)
# =============================================================================

PORTE_EMPRESA_MAP = {
    0: "NÃO INFORMADO",
    1: "MICRO EMPRESA",
    3: "EMPRESA DE PEQUENO PORTE",
    5: "DEMAIS",
}

IDENTIFICADOR_MATRIZ_FILIAL_MAP = {
    1: "MATRIZ",
    2: "FILIAL",
}

SITUACAO_CADASTRAL_MAP = {
    1: "NULA",
    2: "ATIVA",
    3: "SUSPENSA",
    4: "INAPTA",
    8: "BAIXADA",
}

IDENTIFICADOR_SOCIO_MAP = {
    1: "PESSOA JURÍDICA",
    2: "PESSOA FÍSICA",
    3: "ESTRANGEIRO",
}
