create or replace trigger c_vvod_adu_e_l2c
    after update or delete
    on c_vvod
    for each row
begin
if lower(user) <> 'gen' and scott.p_java.c_vvod_updated_cnt = 0
    then
    scott.p_java.evictl2centity(p_entity => 'com.dic.bill.model.scott.Vvod',
                        p_id => :old.id);
end if;
scott.p_java.c_vvod_updated_cnt:=scott.p_java.c_vvod_updated_cnt+1;

end;
/

create or replace trigger c_vvod_adu_l2c
    after update or delete
    on c_vvod
begin
if lower(user) <> 'gen' and scott.p_java.c_vvod_updated_cnt > 1
      then
    scott.p_java.evictl2cregion(p_region => 'BillDirectEntitiesCacheVvod');
end if;

end;
/

create or replace trigger c_vvod_bdu_l2c
    before update or delete
    on c_vvod
begin
scott.p_java.c_vvod_updated_cnt := 0;
end;
/

