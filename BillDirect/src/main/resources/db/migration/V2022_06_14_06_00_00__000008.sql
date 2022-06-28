-- Create/Recreate indexes
create index C_PEN_CORR_I2 on C_PEN_CORR (lsk)
    tablespace INDX_FAST
    storage
(
    initial 64K
    minextents 1
    maxextents unlimited
);
