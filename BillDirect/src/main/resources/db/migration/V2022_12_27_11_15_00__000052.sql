-- добавление дополнительного кода GUID (из ГИС), согласно сервиса: https://dom.gosuslugi.ru/#!/eServices
comment on column exs.EOLINK.GUID is 'GUID объекта во внешней системе (Код из ФИАС)';

alter table exs.EOLINK add GUID_GIS varchar2(36);

comment on column exs.EOLINK.GUID_GIS is 'GUID объекта во внешней системе (Код из ГИС)';
