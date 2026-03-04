-- models/bronze/brz_ibge_estados.sql
-- Camada Bronze: view sobre a tabela bruta de estados.
-- Apenas expõe o dado original com colunas de auditoria padronizadas.
-- NÃO aplica transformações de negócio — isso é responsabilidade da Silver.

{{
  config(
    materialized = 'view',
    tags = ['bronze', 'ibge']
  )
}}

SELECT
    sigla_uf,
    nome_uf,
    regiao,
    populacao,
    pib_per_capita,
    idh,
    CAST(ano AS INTEGER)          AS ano,
    fonte,
    CAST(data_carga AS TIMESTAMP) AS data_carga,
    _loaded_at,
    _source_file
FROM {{ source('bronze', 'ibge_estados') }}
