select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select pib_per_capita
from "medallion"."bronze"."ibge_estados"
where pib_per_capita is null



      
    ) dbt_internal_test