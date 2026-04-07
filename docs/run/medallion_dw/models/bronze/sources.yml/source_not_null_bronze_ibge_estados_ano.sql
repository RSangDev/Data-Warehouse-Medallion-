select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select ano
from "medallion"."bronze"."ibge_estados"
where ano is null



      
    ) dbt_internal_test