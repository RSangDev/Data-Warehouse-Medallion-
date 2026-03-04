{# macros/utils.sql — macros reutilizáveis do projeto medallion_dw #}


{# ─── Formata valor monetário em BRL ───────────────────────────── #}
{% macro format_brl(column) %}
    'R$ ' || FORMAT('{:,.2f}', {{ column }})
{% endmacro %}


{# ─── Gera hash surrogate key a partir de colunas ──────────────── #}
{% macro surrogate_key(columns) %}
    MD5(CONCAT_WS('|', {% for col in columns %}COALESCE(CAST({{ col }} AS VARCHAR), '_null_'){% if not loop.last %}, {% endif %}{% endfor %}))
{% endmacro %}


{# ─── Trunca texto com reticências ─────────────────────────────── #}
{% macro truncate_text(column, max_len=50) %}
    CASE
        WHEN LENGTH({{ column }}) > {{ max_len }}
        THEN LEFT({{ column }}, {{ max_len }}) || '...'
        ELSE {{ column }}
    END
{% endmacro %}


{# ─── Macro de teste customizado: verifica se % de nulos < threshold #}
{% macro test_null_proportion(model, column_name, threshold=0.05) %}
    SELECT
        COUNT(*) AS failing_rows
    FROM {{ model }}
    HAVING
        SUM(CASE WHEN {{ column_name }} IS NULL THEN 1 ELSE 0 END)::DOUBLE
        / NULLIF(COUNT(*), 0) > {{ threshold }}
{% endmacro %}


{# ─── Adiciona colunas de auditoria padrão ─────────────────────── #}
{% macro audit_columns() %}
    CURRENT_TIMESTAMP AS _dbt_updated_at,
    '{{ invocation_id }}' AS _dbt_run_id
{% endmacro %}


{# ─── Classifica IDH em português ──────────────────────────────── #}
{% macro classify_idh(column) %}
    CASE
        WHEN {{ column }} >= 0.800 THEN 'Muito Alto'
        WHEN {{ column }} >= 0.700 THEN 'Alto'
        WHEN {{ column }} >= 0.555 THEN 'Médio'
        ELSE 'Baixo'
    END
{% endmacro %}


{# ─── Classifica porte de município ────────────────────────────── #}
{% macro classify_porte(pop_column) %}
    CASE
        WHEN {{ pop_column }} >= 1000000 THEN 'Metrópole'
        WHEN {{ pop_column }} >= 500000  THEN 'Grande'
        WHEN {{ pop_column }} >= 100000  THEN 'Médio'
        WHEN {{ pop_column }} >= 20000   THEN 'Pequeno'
        ELSE 'Micro'
    END
{% endmacro %}
