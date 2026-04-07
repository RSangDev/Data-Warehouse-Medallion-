select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select populacao
from "medallion"."bronze"."ibge_estados"
where populacao is null



      
    ) dbt_internal_test