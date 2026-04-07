
    
    

with all_values as (

    select
        quartil_pib as value_field,
        count(*) as n_records

    from "medallion"."main_silver"."slv_estados"
    group by quartil_pib

)

select *
from all_values
where value_field not in (
    '1','2','3','4'
)


