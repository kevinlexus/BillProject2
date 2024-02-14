-- добавление дополнительного кода HM_GUID (из ГИС), согласно сервиса: https://dom.gosuslugi.ru/#!/eServices

alter table exs.EOLINK add HM_GUID varchar2(36);

comment on column exs.EOLINK.HM_GUID is 'Глобальный уникальный идентификатор объекта жилищного фонда';
