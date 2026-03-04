
    
    

with all_values as (

    select
        nivel_idh as value_field,
        count(*) as n_records

    from "medallion"."main_silver"."slv_estados"
    group by nivel_idh

)

select *
from all_values
where value_field not in (
    'Muito Alto','Alto','Medio','Baixo'
)


