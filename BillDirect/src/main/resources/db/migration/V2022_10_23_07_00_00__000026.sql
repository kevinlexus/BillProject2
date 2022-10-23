alter table usl add nm_for_bot varchar2(50);

comment on column usl.nm_for_bot is 'Наименование для Telegram bot';