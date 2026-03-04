-- models/bronze/brz_datasus_internacoes.sql


SELECT
    id_aih,
    sigla_uf,
    tipo_hospital,
    natureza_juridica,
    carater_internacao,
    cid_principal,
    descricao_cid,
    capitulo_cid,
    CAST(idade AS INTEGER)           AS idade,
    faixa_etaria,
    sexo,
    CAST(dias_internacao AS INTEGER) AS dias_internacao,
    CAST(valor_aih AS DOUBLE)        AS valor_aih,
    CAST(obito AS INTEGER)           AS obito,
    CAST(ano AS INTEGER)             AS ano,
    CAST(mes AS INTEGER)             AS mes,
    CAST(data_internacao AS DATE)    AS data_internacao,
    fonte,
    CAST(data_carga AS TIMESTAMP)    AS data_carga,
    _loaded_at
FROM "medallion"."bronze"."datasus_internacoes"