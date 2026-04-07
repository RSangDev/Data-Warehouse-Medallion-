select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    id_aih as unique_field,
    count(*) as n_records

from "medallion"."bronze"."datasus_internacoes"
where id_aih is not null
group by id_aih
having count(*) > 1



      
    ) dbt_internal_test