-- Create/Recreate indexes
drop index DEBT_SUB_REQUEST_I;
create unique index DEBT_SUB_REQUEST_I on EXS.DEBT_SUB_REQUEST (REQUEST_GUID)
    tablespace INDX_FAST
    pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
  )
  nologging;
