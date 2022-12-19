create index DEBT_SUB_REQUEST_I on EXS.DEBT_SUB_REQUEST (request_guid)
    tablespace INDX_FAST
    storage
(
    initial 64K
    minextents 1
    maxextents unlimited
);