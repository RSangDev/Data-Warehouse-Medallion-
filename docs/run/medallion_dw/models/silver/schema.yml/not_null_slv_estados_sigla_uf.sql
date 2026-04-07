select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select sigla_uf
from "medallion"."main_silver"."slv_estados"
where sigla_uf is null



      
    ) dbt_internal_test