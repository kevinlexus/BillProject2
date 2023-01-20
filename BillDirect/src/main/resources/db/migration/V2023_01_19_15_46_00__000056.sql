alter table C_CHANGE_DOCS add DESCRIPTION varchar2(2000);
comment on column C_CHANGE_DOCS.DESCRIPTION is 'Описание расчета';

alter table A_CHANGE_DOCS add DESCRIPTION varchar2(2000);
comment on column A_CHANGE_DOCS.DESCRIPTION is 'Описание расчета';


