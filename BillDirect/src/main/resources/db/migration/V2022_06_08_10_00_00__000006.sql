-- Create sequence
create sequence XITOG3_LSK_ID
    minvalue 1
    maxvalue 999999999999999999999999999
    start with 1
    increment by 1
    cache 10
    order;

-- Add/modify columns
alter table XITOG3_LSK add id number;
-- Add comments to the columns
comment on column XITOG3_LSK.id
    is 'ID';

update XITOG3_LSK set id=XITOG3_LSK_ID.nextval;

alter table XITOG3_LSK
    add constraint XITOG3_LSK_P primary key (ID);



