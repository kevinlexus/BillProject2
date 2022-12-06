alter table USL add USL_VOL_LIST varchar2(1000);
comment on column USL.USL_VOL_LIST is 'Список услуг, откуда брать объем (44 fk_calс_tp услуга)';

create or replace trigger usl_adu_l2c
    after update or delete
    on usl
begin
-- очистить кэш Hibernate L2C
if lower(user) <> 'gen' then
     p_java.evictL2Cache;
end if;
end usl_adu_l2c;

/

update usl t set t.usl_vol_list='015, 062' where t.fk_calc_tp=44;

