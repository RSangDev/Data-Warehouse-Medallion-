
    
    

with all_values as (

    select
        obito as value_field,
        count(*) as n_records

    from "medallion"."main_silver"."slv_internacoes"
    group by obito

)

select *
from all_values
where value_field not in (
    '0','1'
)


