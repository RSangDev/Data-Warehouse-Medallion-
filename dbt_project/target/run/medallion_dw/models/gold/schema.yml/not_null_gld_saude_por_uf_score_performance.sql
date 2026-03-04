select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select score_performance
from "medallion"."main_gold"."gld_saude_por_uf"
where score_performance is null



      
    ) dbt_internal_test