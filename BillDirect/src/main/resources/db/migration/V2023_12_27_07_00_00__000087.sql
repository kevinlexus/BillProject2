-- alter table exs.EOLINK add constraint "EOLINK_ADDR_TP_ID_fk" foreign key (FK_OBJTP) references BS.ADDR_TP (ID);
insert into bs.addr_tp(id, cd, name, npp, v, parent_id, nm, comm)
values (73, 'Блок', 'Блок', 1, 1, 12, 'Блок', 'Блок (для жилого дома блокированной застройки) - для ГИС ЖКХ');

