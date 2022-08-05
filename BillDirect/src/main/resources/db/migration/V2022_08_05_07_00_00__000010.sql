create or replace procedure drn66_Служебные_отчеты_списки is
begin
Null;
end drn66_Служебные_отчеты_списки;

create or replace procedure drn102_Список_задолжников is
begin
Null;
end drn102_Список_задолжников;

create or replace procedure drn124_Реестр_для_УСЗН is
begin
Null;
end drn124_Реестр_для_УСЗН;

create or replace procedure drn130_Реестр_для_Фонда_капремонта_МКД is
begin
Null;
end drn130_Реестр_для_Фонда_капремонта_МКД;

create or replace procedure drn133_Реестр_для_УСЗН_для_обмена is
begin
Null;
end drn133_Реестр_для_УСЗН_для_обмена;

create or replace procedure drn138_Обмен_с_ГИС_ЖКХ is
begin
Null;
end drn138_Обмен_с_ГИС_ЖКХ;

-- Grant/Revoke object privileges
grant execute on drn66_Служебные_отчеты_списки to БУХГ_КВАРТПЛАТЫ;
grant execute on drn102_Список_задолжников to БУХГ_КВАРТПЛАТЫ;
grant execute on drn102_Список_задолжников to БУХГ_СКЭК;

grant execute on drn124_Реестр_для_УСЗН to АДМИНИСТРАТОР;
grant execute on drn130_Реестр_для_Фонда_капремонта_МКД to АДМИНИСТРАТОР;
grant execute on drn133_Реестр_для_УСЗН_для_обмена to АДМИНИСТРАТОР;
grant execute on drn138_Обмен_с_ГИС_ЖКХ to АДМИНИСТРАТОР;

