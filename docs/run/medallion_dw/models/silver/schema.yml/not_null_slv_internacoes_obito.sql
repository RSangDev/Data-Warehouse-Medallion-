select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select obito
from "medallion"."main_silver"."slv_internacoes"
where obito is null



      
    ) dbt_internal_test