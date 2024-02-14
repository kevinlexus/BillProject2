-- Create/Recreate indexes
create index DEBT_SUB_REQUEST_I2 on EXS.DEBT_SUB_REQUEST (response_date)
    tablespace INDX_FAST
    storage
(
    initial 64K
    minextents 1
    maxextents unlimited
);

create index DEBT_SUB_REQUEST_I3 on EXS.DEBT_SUB_REQUEST (response_status)
    tablespace INDX_FAST
    storage
(
    initial 64K
    minextents 1
    maxextents unlimited
);
