insert into u_list (cd, name, nm, fk_listtp, npp, val_tp, fk_unit, sqltext, fk_exs_u_list)
select 'TelegramId',
       'Telegram Id',
       null,
       tp.id,
       0,
       'ST',
       null,
       null,
       null
from u_listtp tp
where not exists(select * from u_list t where t.cd = 'TelegramId')
and tp.cd='Параметры лиц.счета';

update u_list t
set t.val_tp='ST'
where t.cd = 'TelegramId'
  and t.val_tp <> 'ST';

