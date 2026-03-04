select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        sigla_uf as value_field,
        count(*) as n_records

    from "medallion"."bronze"."ibge_estados"
    group by sigla_uf

)

select *
from all_values
where value_field not in (
    'AM','PA','AC','RO','RR','AP','TO','MA','PI','CE','RN','PB','PE','AL','SE','BA','MG','ES','RJ','SP','PR','SC','RS','MT','MS','GO','DF'
)



      
    ) dbt_internal_test