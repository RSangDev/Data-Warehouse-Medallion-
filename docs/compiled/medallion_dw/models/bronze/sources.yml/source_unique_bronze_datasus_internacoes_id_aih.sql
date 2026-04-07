
    
    

select
    id_aih as unique_field,
    count(*) as n_records

from "medallion"."bronze"."datasus_internacoes"
where id_aih is not null
group by id_aih
having count(*) > 1


