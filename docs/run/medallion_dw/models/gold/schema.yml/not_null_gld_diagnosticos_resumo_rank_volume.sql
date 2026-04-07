select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select rank_volume
from "medallion"."main_gold"."gld_diagnosticos_resumo"
where rank_volume is null



      
    ) dbt_internal_test