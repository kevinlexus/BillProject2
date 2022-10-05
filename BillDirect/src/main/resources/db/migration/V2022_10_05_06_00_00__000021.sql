
alter table load_bank add id number;
create unique index load_bank_id_uindex on load_bank (id);
alter table load_bank add constraint load_bank_pk primary key (id);

-- Create sequence
create sequence LOAD_BANK_ID
    minvalue 1
    maxvalue 999999999999999999999999999
    start with 1
    increment by 1
    cache 20
    order;

create trigger load_bank_bi_e
    before insert
    on load_bank
    for each row
declare
    begin
    if :new.id is null then
select scott.load_bank_id.nextval into :new.id from dual;
end if;
end load_bank_bi_e;