
    
    

with all_values as (

    select
        sexo as value_field,
        count(*) as n_records

    from "medallion"."bronze"."datasus_internacoes"
    group by sexo

)

select *
from all_values
where value_field not in (
    'M','F'
)


