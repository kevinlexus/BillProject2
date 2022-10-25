insert into u_list (cd, name, nm, fk_listtp, npp, val_tp, fk_unit, sqltext, fk_exs_u_list)
select 'TelegramId',
       'Telegram Id',
       null,
       2021,
       0,
       'ST',
       null,
       null,
       null
from dual
where not exists(select * from u_list t where t.cd = 'TelegramId');

update u_list t
set t.val_tp='ST'
where t.cd = 'TelegramId'
  and t.val_tp <> 'ST';

