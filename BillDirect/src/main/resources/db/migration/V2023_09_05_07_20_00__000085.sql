-- Add/modify columns
alter table T_ORG add is_auto_send_deb_req number default 0;
-- Add comments to the columns
comment on column T_ORG.is_auto_send_deb_req
    is 'Отправлять ответы на запросы о задолженности в УСЗН автоматически? (0-нет, 1-да)';
