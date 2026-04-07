select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select idh
from "medallion"."bronze"."ibge_estados"
where idh is null



      
    ) dbt_internal_test