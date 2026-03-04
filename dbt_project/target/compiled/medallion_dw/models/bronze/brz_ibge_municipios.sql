-- models/bronze/brz_ibge_municipios.sql


SELECT
    CAST(id_municipio AS BIGINT)   AS id_municipio,
    cod_ibge,
    nome_municipio,
    sigla_uf,
    regiao,
    CAST(populacao AS BIGINT)       AS populacao,
    CAST(pib_per_capita AS DOUBLE)  AS pib_per_capita,
    CAST(idh_municipal AS DOUBLE)   AS idh_municipal,
    CAST(area_km2 AS DOUBLE)        AS area_km2,
    CAST(ano AS INTEGER)            AS ano,
    fonte,
    CAST(data_carga AS TIMESTAMP)   AS data_carga,
    _loaded_at
FROM "medallion"."bronze"."ibge_municipios"