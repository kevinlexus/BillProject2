CREATE OR REPLACE TRIGGER xitog3_lsk_bi_e
  before insert on xitog3_lsk
    for each row
begin
if inserting then
    if :new.id is null then
select scott.xitog3_lsk_id.nextval into :new.id from dual;
end if;
end if;

end xitog3_lsk_bi_e;

