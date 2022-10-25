insert into t_user (id, cd, name, npp, parent_id, v, visa, lic, licp, hotora_sql, cnt_enters, guid)
select 100, 'TLG', 'Телеграм бот', 0, null, 1, 1, null, null, null, null, null from dual
 where not exists (select * from t_user t where t.cd='TLG');

