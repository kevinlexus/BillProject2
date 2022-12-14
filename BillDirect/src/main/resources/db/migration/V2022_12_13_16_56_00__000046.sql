CREATE OR REPLACE TRIGGER t_objxpar_BIU_E
    before insert or update on T_OBJXPAR
    for each row
declare
    l_user number;
begin
select t.id, t.id, sysdate
into l_user, :new.fk_user_upd, :new.dt_upd from t_user t where t.cd=user;

if :new.fk_user is null then
    :new.fk_user:=l_user;
end if;

if :new.id is null then
select scott.T_OBJXPAR_id.nextval into :new.id from dual;
end if;

end t_objxpar_BIU_E;

/

