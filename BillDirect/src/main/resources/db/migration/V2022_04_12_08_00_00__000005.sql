-- 13.04.22
-- Хотел убрать переплату по пене, полученную в результате смены ставки в меньшую сторону. в итоге - ничего не получилось,
-- так как две разные версии программы, в каждой надо было учесть в расчете долга

-- Add/modify columns
alter table T_CORRECTS_PAYMENTS add penya number;
-- Add comments to the columns
comment on column T_CORRECTS_PAYMENTS.penya
    is 'Пеня';
