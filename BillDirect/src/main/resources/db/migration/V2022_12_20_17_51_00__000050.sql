create or replace package body       p_meter is

  --добавить счетчик
  function ins_meter(p_npp         number, --№ п.п.
                     p_usl         in usl.usl%type, -- услуга
                     p_dt1         in date, -- период работы С..
                     p_dt2         in date, -- период работы по..
                     p_n1          in number, --начальное показание
                     p_fk_klsk_obj in number, --объект к которому прикреплён
                     p_tp          in u_list.cd%type --тип (например ИПУ)
                     ) return number is
    l_num_id  number;
l_vol_id  number;
l_user    number;
l_counter usl.counter%type;
l_lsk     kart.lsk%type;
l_mg      params.period%type;
  begin

for c in (select k_lsk_id.nextval as klsk, u.id as fk_tp
                from u_list u, u_listtp tp
               where u.cd = p_tp
                 and tp.cd = 'object_type') loop

insert into k_lsk (id, fk_addrtp) values (c.klsk, c.fk_tp);

for c2 in (select 1 as exs, t.id as fk_tp
                   from u_list t
                  where t.cd = p_tp) loop
insert into meter
(npp, k_lsk_id, fk_usl, dt1, dt2, fk_klsk_obj)
values
    (p_npp, c.klsk, p_usl, p_dt1, p_dt2, p_fk_klsk_obj);

select max(decode(u.cd, 'ins_sch', u.id, 0)), max(decode(u.cd,
                                                         'ins_vol_sch',
                                                         u.id,
                                                         0)), max(s.id), trim(max(u2.counter)), trim(max(k.lsk)), max(p.period)
into l_num_id, l_vol_id, l_user, l_counter, l_lsk, l_mg
from u_list u, t_user s, meter m, usl u2, kart k, params p
where u.cd in ('ins_sch', 'ins_vol_sch')
  and s.cd = user
  and m.k_lsk_id = c.klsk
  and m.fk_klsk_obj = k.k_lsk_id
  and m.fk_usl = u2.usl;

--отразить начальные показания (без объёма!!!)
if nvl(p_n1, 0) <> 0 then
insert into t_objxpar
(fk_k_lsk, fk_list, n1, fk_user, mg)
values
    (c.klsk, l_num_id, p_n1, l_user, l_mg);
--сохранить последние показания в счетчике
update meter m set m.n1 = p_n1 where m.k_lsk_id = c.klsk;
--обновить показания в kart
--Raise_application_error(-20000, 'update kart k set k.' || l_counter || '=nvl(' || p_n1 ||
--                  ',0) where k.lsk=' || l_lsk);
execute immediate 'update kart k set k.' || l_counter || '=nvl(' || p_n1 ||
                            ',0) where k.lsk=''' || l_lsk||'''';
end if;
--вернуть klsk нового счетчика
return c.klsk;
end loop;

exit;
end loop;
end;

-- внести расход по счетчику
-- вызывается из Директа, в форме ввода показаний
function ins_vol_meter(p_met_klsk in number, -- klsk счетчика
                         p_lsk      in kart.lsk%type, --лиц.счет     --либо лиц.счет + услуга!
                         p_usl      in usl.usl%type, --услуга
                         p_vol      in number, -- объем
                         p_n1       in number, -- НЕ используется!
                         p_tp       in number default 0 -- тип (0-ручной ввод, 1-автоначисление, 2-отмена начисления
                         ) return number is
    l_num_id   number;
l_vol_id   number;
l_user     number;
l_counter  usl.counter%type;
l_lsk      kart.lsk%type;
l_dt2      date; --дата закрытия счетчика
l_dt       date; --дата начала месяца
l_period   params.period%type; --текущий период
l_met_klsk number;
l_cnt      number;
l_flag     number;
  begin
    -- флаг, что не надо добавлять информацию в триггере
g_flag := 1;
if p_met_klsk is null and (p_lsk is null or p_usl is null) then
      Raise_application_error(-20000,
                              'Некорректное использование функции p_meter.ins_vol_meter, p_lsk и p_usl или k_lsk одновременно пустые!');
end if;
if p_vol is null then
      -- ред.25.01.18 был случай в Полыс. два раза внесли пустое показание счетчика (как?)
      return 3; -- ошибка! не был записан объем
end if;

if p_met_klsk is not null then
      -- по klsk счетчика
      l_met_klsk := p_met_klsk;
else
      -- по lsk лиц.счета, выбрать первый активный по порядку id счетчик, провести по нему объем
      for c in (select m.k_lsk_id
                  into l_met_klsk
                  from kart k
                  join params p
                    on 1 = 1
                  join v_lsk_tp tp
                    on k.fk_tp = tp.id
                   and tp.cd in ('LSK_TP_MAIN')
                  join meter m
                    on k.k_lsk_id = m.fk_klsk_obj
                   and m.fk_usl = p_usl
                   and k.lsk = p_lsk
                   and case
                         when m.dt1 <=
                              last_day(to_date(p.period || '01', 'YYYYMMDD')) and
                              m.dt2 >
                              last_day(to_date(p.period || '01', 'YYYYMMDD')) then
                          1
                         else
                          0
                       end = 1
                 order by m.id) loop

        l_met_klsk := c.k_lsk_id;
exit;
end loop;

if l_met_klsk is null then
        g_flag := 0;
return 2; -- ошибка, нет активных счетчиков, провести объем невозможно!
end if;
end if;

-- если открыты два лиц.счета с одним k_lsk то здесь может выйти максимальный из них и не тот обновится kart.mgw или kart.mhw в итоге...
select max(decode(u.cd, 'ins_sch', u.id, 0)), max(decode(u.cd,
                                                         'ins_vol_sch',
                                                         u.id,
                                                         0)), max(s.id), trim(max(u2.counter)), trim(max(k.lsk)), max(m.dt2), max(to_date(p.period || '01',
                                                                                                                                          'YYYYMMDD')), max(p.period)
into l_num_id, l_vol_id, l_user, l_counter, l_lsk, l_dt2, l_dt, l_period
from u_list u, t_user s, meter m, usl u2, kart k, params p, v_lsk_tp tp
where u.cd in ('ins_sch', 'ins_vol_sch')
  and s.cd = user
  and m.k_lsk_id = l_met_klsk
  and m.fk_klsk_obj = k.k_lsk_id
  and k.fk_tp = tp.id
  and tp.cd = 'LSK_TP_MAIN'
  and m.fk_usl = u2.usl
  and k.psch not in (8, 9);

if p_lsk is not null then
      --если указан лиц.счет, то использовать его
      l_lsk := p_lsk;
end if;

l_flag := 0;
if l_dt2 > l_dt then
      if (nvl(p_vol, 0)) <> 0 then
        -- всегда брать только расход
        --отразить объем
insert into t_objxpar
(fk_k_lsk, fk_list, n1, fk_user, mg, tp)
values
    (l_met_klsk, l_vol_id, p_vol, l_user, l_period, p_tp);

select nvl(m.n1, 0) + nvl(p_vol, 0)
into l_cnt
from meter m
where m.k_lsk_id = l_met_klsk;

--изменить последние показания в счетчике + расход
update meter m set m.n1 = l_cnt where m.k_lsk_id = l_met_klsk;

--отразить показания
insert into t_objxpar
(fk_k_lsk, fk_list, n1, fk_user, mg, tp)
values
    (l_met_klsk, l_num_id, l_cnt, l_user, l_period, p_tp);

--обновить показания и общий расход за месяц в kart
execute immediate 'update kart k set k.' || l_counter || '=nvl(k.' ||
                          l_counter || ',0)+nvl(' || p_vol ||
                          ',0) where k.lsk=:lsk' using l_lsk;
--меняем первую букву на "m" - получаем поле для расхода (изврат)
execute immediate 'update kart k set k.' || 'm' ||
                          substr(l_counter, 2, 3) || '=nvl(k.' || 'm' ||
                          substr(l_counter, 2, 3) || ',0) + nvl(' || p_vol || +
                          ',0) where k.lsk=:lsk' using l_lsk;

l_flag := 1;

g_flag := 0; -- снять флаг от добавления в триггере

end if;

if l_flag = 1 then
        return 0;
else
        return 3; -- ошибка! не был записан объем
end if;
else
      -- ошибка! попытка передать объем по закрытому счетчику!
      return 1;
end if;
end;

-- привязать или отменить связь  счетчика с гис жкх
procedure connect_meter_gis(p_klsk_meter in number, -- klsk счетчика
                          p_eolink in number, -- id Eolink счетчика
                          is_connect in number -- привязать - 0, снять связь - 1
                          ) is
  begin
if is_connect = 0 then
      -- привязать
update exs.eolink e set e.fk_klsk_obj=p_klsk_meter where e.id=p_eolink;
else
      -- снять связь
update exs.eolink e set e.fk_klsk_obj=null where e.id=p_eolink;
end if;
end;

-- отменить реестр показаний по счетчикам и все последующие показания, принятые
-- либо через реестр, либо вручную
function revert_data_meter(p_doc_par_id in number -- id реестра
    ) return number is
  l_period params.period%type;
l_is_changed number;
l_cnt number;
  begin
select period into l_period from params;
l_cnt:=0;
    -- флаг, что не надо добавлять информацию в триггере
g_flag := 1;
for c in (select min(x.id) as min_id, x.fk_k_lsk
        from t_doc d join t_objxpar x on d.id=x.fk_doc and d.id=p_doc_par_id
        where d.mg=l_period
        group by x.fk_k_lsk) loop
        -- найти предыдущее показание и установить его
        l_is_changed:=0;
for c2 in (select x.n1 from t_objxpar x
           join u_list u on x.fk_list=u.id and u.cd in ('ins_sch')
           where x.fk_k_lsk=c.fk_k_lsk and x.id < c.min_id
           order by x.id desc) loop
          --изменить последнее показание в счетчике
update meter m set m.n1 = c2.n1 where m.k_lsk_id = c.fk_k_lsk;
l_is_changed:=1;
exit;
end loop;
if l_is_changed=0 then
          -- не было изменено, занулить
update meter m set m.n1 = 0 where m.k_lsk_id = c.fk_k_lsk;
end if;
-- удалить показания и объемы, прошедшие начиная с реестра
delete from t_objxpar t where t.mg=l_period
                          and t.fk_list in (select u.id from u_list u where u.cd in ('ins_sch','ins_vol_sch'))
                          and t.id >=c.min_id
                          and t.fk_k_lsk=c.fk_k_lsk;
if sql%rowcount > 0 then
          l_cnt:=l_cnt+1;
end if;
end loop;
update t_doc t set t.v=0 where t.id=p_doc_par_id;
-- восстановить флаг, что не надо добавлять информацию в триггере
g_flag := 0;
return l_cnt;
end;

--внести показание по счетчику (обычно - Java вызов из BillDirect - загрузка показаний из файла)
procedure ins_data_meter(p_lsk in varchar2, --  лиц.счет
                           p_usl in varchar2, -- услуга
                           p_prev_val       in number, -- предыдущее показание
                           p_cur_val       in number, -- новое показание
                           p_is_set_prev   in number, -- установить предыдущее показание? (1-да, 0-нет) ВНИМАНИЕ! Текущие введёные показания будут сброшены назад
                           p_ts       in date, -- timestamp
                           p_period   in varchar2 default null, -- текущий период - НЕ ИСПОЛЬЗУЕТСЯ!
                           p_status   in number, -- статус показания (см. t_objxpar.status)
                           p_user in number, -- пользователь
                           p_doc_par_id in number, -- id реестра
                           p_ret      out number) is
  l_ret number;
l_diff number;
l_period params.period%type;
  begin
p_ret:=4; -- не найден активный счетчик
select p.period into l_period from params p;
-- найти активный счетчик по услуге
for c in (select m.k_lsk_id, nvl(m.n1,0) as prev_val from meter m join params p on 1=1
        join kart k
                      on k.lsk=p_lsk and m.fk_klsk_obj=k.k_lsk_id and m.fk_usl=p_usl
                      and last_day(to_date(trim(l_period)||'01','YYYYMMDD')) between m.dt1 and m.dt2 -- активный счетчик на конец месяца
                      ) loop
        if p_is_set_prev = 1 then
            -- установить предыдущие показания (отматать назад или вперед текущие), без контроля значений
            ins_data_meter(p_met_klsk => c.k_lsk_id,
                           p_n1       => p_prev_val,
                           p_ts       => p_ts,
                           p_period   => l_period,
                           p_status   => p_status,
                           p_user     => p_user,
                           p_control  => 0,
                           p_is_set_prev =>1,
                           p_doc_par_id => p_doc_par_id,
                           p_ret      => l_ret);
          -- установить текущие показания, с контролем значений, но не знака (+-) (ред. 27.02.20 Кис.просят чтобы можно было отматывать счетчик назад)
ins_data_meter(p_met_klsk => c.k_lsk_id,
                  p_n1       => p_cur_val,
                  p_ts       => p_ts,
                  p_period   => l_period,
                  p_status   => p_status,
                  p_user     => p_user,
                  p_control  => 2,
                  p_doc_par_id => p_doc_par_id,
                  p_ret      => l_ret);
else
          -- просто установить текущие показания, с контролем значений, но не знака (+-) (ред. 27.02.20 Кис.просят чтобы можно было отматывать счетчик назад)
          ins_data_meter(p_met_klsk => c.k_lsk_id,
                         p_n1       => p_cur_val,
                         p_ts       => p_ts,
                         p_period   => l_period,
                         p_status   => p_status,
                         p_user     => p_user,
                         p_control  => 2, -- контроль только объема, но не его знака
                         p_doc_par_id => p_doc_par_id,
                         p_ret      => l_ret);
end if;
p_ret:=l_ret;
exit;
end loop;
end;


--внести показание по счетчику (обычно - Java вызов из BillDirect, от Telegram-bot)
procedure ins_data_meter(p_met_klsk in number default null, -- klsk счетчика
                           p_meter_id in number default null, -- Id счетчика
                           p_n1       in number, -- новое показание
                           p_ts       in date, -- timestamp
                           p_period   in varchar2 default null, -- текущий период - НЕ ИСПОЛЬЗУЕТСЯ!
                           p_status   in number, -- статус (например 3 - загружено из ГИС)
                           p_user in number, -- пользователь
                           p_control in number default 1, -- контролировать перерасход? (1-да, 0-нет, 2 - да, но не знак)
                           p_is_set_prev   in number default 0, -- установить предыдущее показание? (1-да, 0-нет) ВНИМАНИЕ! Текущие введёные показания будут сброшены назад
                           p_lsk in kart.lsk%type default null, -- если передан лиц.сч., использовать его, если нет, - взять Основной лиц.сч.
                           p_doc_par_id in number default null, -- id реестра
                           p_ret      out number) is
    l_vol number;
l_n1  number;
l_met_klsk  number;
stt varchar2(1000);
l_period params.period%type;
l_day_restrict_meter_vol number;
  begin

p_ret := 0;
select p.period into l_period from params p;
if p_met_klsk is null and p_meter_id is null then
      Raise_application_error(-20000,
                              'Некорректное использование функции p_meter.ins_data_meter, p_met_klsk или p_meter_id - пустое!');
end if;
l_day_restrict_meter_vol := utils.get_int_param('DAY_RESTRICT_METER_VOL');
    -- если l_day_restrict_meter_vol=0, то без ограничений
if l_day_restrict_meter_vol != 0 and trunc(sysdate) >= to_date(l_period|| case when l_day_restrict_meter_vol >= 10
       then to_char(l_day_restrict_meter_vol) else '0'||to_char(l_day_restrict_meter_vol) end ,'YYYYMMDD') then
       -- прием показаний по счетчикам ограничен
       p_ret:=6;
return;
end if;

if p_met_klsk is null then
select m.k_lsk_id into l_met_klsk from meter m where m.id=p_meter_id;
else
      l_met_klsk:=p_met_klsk;
end if;

l_n1 := 0;
for c in (select k.lsk, m.id, m.fk_usl, trim(u3.counter) as counter, u.id as vol_id, u2.id as num_id
                from meter m
                join kart k
                  on m.fk_klsk_obj = k.k_lsk_id
                join v_lsk_tp tp
                  on k.fk_tp = tp.id
                 and coalesce(p_lsk, k.lsk)=k.lsk
                   and case when p_lsk is null then 'LSK_TP_MAIN' else tp.cd end = tp.cd -- или указан лиц.сч или основной лиц.сч. к которому привязан счетчик
                 and k.psch not in (8, 9)
                join usl u3
                  on m.fk_usl = u3.usl
                join u_list u
                  on u.cd = 'ins_vol_sch'
                join u_list u2
                  on u2.cd = 'ins_sch'
                join params p on 1=1
               where m.k_lsk_id = l_met_klsk
                 and last_day(to_date(p.period||'01','YYYYMMDD')) between m.dt1 and m.dt2 -- активный счетчик на конец месяца
              ) loop
      for c2 in (select x.n1
                   from t_objxpar x
                  where x.fk_k_lsk = l_met_klsk
                  and x.fk_list=c.num_id -- последние показания счетчика (любые, в т.ч. из ГИС)
                  order by x.id desc) loop
        l_n1 := c2.n1;
exit;
end loop;

if p_control in (0,2) or p_n1 > l_n1 then
          -- обработка новых показаний
          -- получить объем
          l_vol := p_n1 - l_n1;
if p_control=1 and (c.fk_usl in ('011','015') and l_vol > 200 or c.fk_usl in ('038') and l_vol > 1500) or
             p_control=2 and (c.fk_usl in ('011','015') and abs(l_vol) > 200 or c.fk_usl in ('038') and abs(l_vol) > 1500) then
            if l_vol>0 then
              -- Ошибка! переданы показания, вызывающие начисление слишком большого объема!
              p_ret:=5;
            else
              -- Ошибка! переданы показания, вне диапазона (слишком много сняли, например -2500 квт)!
              p_ret:=7;
            end if;
return;
end if;
-- флаг, что не надо добавлять информацию в триггере
g_flag := 1;
          -- отразить объем (только если передаётся текущее показание, иначе вызовет большой расход в периоде)
if p_is_set_prev=0 then
            if l_vol <>0 then
insert into t_objxpar
(fk_k_lsk, fk_list, n1, fk_user, mg, tp, ts, status, fk_doc)
values
    (l_met_klsk, c.vol_id, l_vol, p_user, l_period, 0, p_ts, p_status, p_doc_par_id);
end if;
else
            -- при установке предыдущего показания - отменить текущий объем
insert into t_objxpar
(fk_k_lsk, fk_list, n1, fk_user, mg, tp, ts, status, fk_doc)
select t.fk_k_lsk, c.vol_id, sum(t.n1*-1) as n1, p_user, t.mg, t.tp, p_ts, p_status, p_doc_par_id
from t_objxpar t where t.fk_k_lsk=l_met_klsk and t.mg=l_period
                   and t.fk_list=c.vol_id and nvl(t.n1,0)<>0
group by t.fk_k_lsk, c.vol_id, t.mg, t.tp;
end if;
-- отразить показание
insert into t_objxpar
(fk_k_lsk, fk_list, n1, fk_user, mg, tp, ts, status, fk_doc)
values
    (l_met_klsk, c.num_id, p_n1, p_user, l_period, 0, p_ts, p_status, p_doc_par_id);
--изменить последние показания в счетчике
update meter m set m.n1 = p_n1 where m.k_lsk_id = l_met_klsk;

--обновить показания и общий расход за месяц в kart -- TODO Спорно! Как быть когда 2 и больше счетчиков в системе?
begin
execute immediate 'update kart k set k.' || c.counter || '=nvl(' || p_n1 ||
                              ',0) where k.lsk=:lsk' using c.lsk;
if p_is_set_prev=0 then
              --меняем первую букву на "m" - получаем поле для расхода (изврат)
execute immediate 'update kart k set k.' || 'm' ||
                                substr(c.counter, 2, 3) || '=nvl(k.' || 'm' ||
                                substr(c.counter, 2, 3) || ',0) + nvl(' ||
                                l_vol || + ',0) where k.lsk=:lsk'
                                using c.lsk;
else
              -- при установке предыдущего показания - занулить расход
execute immediate 'update kart k set k.' || 'm' ||
                                substr(c.counter, 2, 3) || '=0 where k.lsk=:lsk'
                                using c.lsk;
end if;
exception
            when others then
              if SQLCODE = -1438 then
rollback;
p_ret:=5;
return;
else
                raise;
end if;
end;
g_flag := 0; -- снять флаг от добавления в триггере
p_ret  := 0; -- успешно сохранено
return;

else
        p_ret := 3; -- новые показания меньше или равны существующим в базе
return;
end if;
exit;
end loop;
p_ret := 4; -- не найден счетчик
return;
end;

-- автоначисление по счетчикам
function gen_auto_chrg_all(p_set in number, p_usl in usl.usl%type)
    return number is
    l_counter     usl.counter%type;
l_mg1         params.period%type;
l_mg2         params.period%type;
l_cnt         t_objxpar.n1%type;
l_tp          t_objxpar.tp%type;
l_ret         number;
l_ret2        number;
l_months      spr_params.parn1%type;
l_usl_nm      varchar2(100);
l_otop        number; --отопит.период
--    l_Java_Charge number;
l_Autocharge_Type number;
l_norm_vol    number;
  begin
logger.log_(null,
                'p_meter.gen_auto_chrg_all Начало по услуге usl=' || p_usl);
    --автоначисление по счетчикам, по услуге
l_ret := 1;

    --узнать отопительный ли сезон?
    --(по последней дате месяца) - пока так... ничего умнее не придумал
select case
           when last_day(to_date(p.period || '01', 'YYYYMMDD')) between
               utils.get_date_param('MONTH_HEAT1') --обраб.отопит.период
               and utils.get_date_param('MONTH_HEAT2') then
               1
           else
               0
           end
into l_otop
from params p;

--по среднему, за последний N месяцев, но не менее чем за последние 3 мес.
select trim(t.counter), trim(t.nm)
into l_counter, l_usl_nm
from usl t
where t.usl = p_usl;
l_months := utils.get_int_param('AUTOCHRGM');

if p_set = 1 then
      --АВТОНАЧИСЛЕНИЕ
      --период, от года назад до прошлого месяца
select to_char(add_months(to_date(p.period || '01', 'YYYYMMDD'),
                          -1 * l_months),
               'YYYYMM'), to_char(add_months(to_date(p.period || '01',
                                                     'YYYYMMDD'),
                                             -1),
                                  'YYYYMM')
into l_mg1, l_mg2
from params p;

--снять неисправные счетчики (превратить в нормативы)
if utils.get_int_param('DEL_BRK_SCH') = 1 then
        del_broken_meter(p_usl);
end if;
--      l_Java_Charge := utils.get_int_param('JAVA_CHARGE');
l_Autocharge_Type := utils.get_int_param('AUTOCHARGE_TYPE');


for c in (select  /*+ USE_HASH(k, a, tp, m )*/ k.lsk, k.k_lsk_id, nvl(sum(case
                                 when a.psch in (1, 2) then
                                  1
                                 else
                                  0
                               end),
                           0) as m_hw, --месяцев, когда счетчик был установлен
                       nvl(sum(case
                                 when a.psch in (1, 2) then
                                  a.mhw
                                 else
                                  0
                               end),
                           0) as cnt_hw, --объем, когда счетчик был установлен
                       nvl(sum(case
                                 when a.psch in (1, 3) then
                                  1
                                 else
                                  0
                               end),
                           0) as m_gw, --и так далее...
                       nvl(sum(case
                                 when a.psch in (1, 3) then
                                  a.mgw
                                 else
                                  0
                               end),
                           0) as cnt_gw,nvl(sum(case
                                 when a.sch_el in (1) then
                                  1
                                 else
                                  0
                               end),
                           0) as m_el,nvl(sum(case
                                 when a.sch_el in (1) then
                                  a.mel
                                 else
                                  0
                               end),
                           0) as cnt_el,
                          sum(case when m.max_mg is null then 1
                               when a.mg > m.max_mg then 1
                               else 0 end) as cnt_month_non_send_vol -- кол-во месяцев непередачи показаний
                  from kart k
                  join arch_kart a
                    on a.k_lsk_id = k.k_lsk_id
                   and a.mg between l_mg1 and l_mg2
                   and k.fk_tp = a.fk_tp
                   and k.psch not in (8, 9)
                   and (l_otop = 0 and l_counter = 'pgw' and
                       nvl(k.kran1, 0) <> 1 or --обрабатываем краны из сист.отопления (только для Г.В.!!!)
                       l_otop = 1 and l_counter = 'pgw' or
                       l_counter <> 'pgw')
                   and ((k.psch in (1, 2) and l_counter = 'phw' and
                       nvl(k.mhw, 0) = 0) or
                       (k.psch in (1, 3) and l_counter = 'pgw' and
                       nvl(k.mgw, 0) = 0) or
                       (k.sch_el = 1 and l_counter = 'pel') and
                       nvl(k.mel, 0) = 0)
                  join v_lsk_tp tp
                    on a.fk_tp = tp.id
                   and tp.cd = 'LSK_TP_MAIN'
                   and a.psch not in (8, 9)
                  and not exists (select * from meter m, params p where m.fk_klsk_obj=k.k_lsk_id and m.fk_usl=p_usl
                  and to_char(m.dt1,'YYYYMM') = p.period) -- кроме установленых счетчиков в тек.периоде. ред.27.05.2019
                  left join (select m.fk_klsk_obj, max(t.mg) as max_mg  -- кол-во месяцев непередачи показаний
                            from meter m, T_OBJXPAR t, u_list s, params p where
                           m.k_lsk_id=t.fk_k_lsk and m.fk_usl=p_usl
                            and t.fk_list=s.id and s.cd='ins_vol_sch'
                            and t.mg>=l_mg1 and t.tp not in (1,2)
                            and t.n1 <> 0
                            group by m.fk_klsk_obj) m on k.k_lsk_id=m.fk_klsk_obj
                 group by k.lsk, k.k_lsk_id) loop
        --ВНИМАНИЕ!, ПЕРЕПИСАТЬ ДЛЯ КИС, ЕСЛИ БУДУТ ИСПОЛЬЗОВАТЬ распределение по расходу!
          if c.cnt_month_non_send_vol >= 3 and l_Autocharge_Type = 0 then
          -- кол-во месяцев непередачи показаний превысило 3 (не включая текущий) - начислять нормативный объем
          -- получить нормативный объем по услуге (в начислении отключается счетчик, рассчитывается нормативный объем)
          l_norm_vol := p_java.gen(p_tp        => 4,
                                   p_house_id  => null,
                                   p_vvod_id   => null,
                                   p_usl_id    => p_usl,
                                   p_klsk_id   => c.k_lsk_id,
                                   p_debug_lvl => 0,
                                   p_gen_dt    => init.get_date,
                                   p_stop      => 0);
l_ret2:= ins_vol_meter(p_met_klsk => null,
                                      p_lsk      => c.lsk,
                                      p_usl      => p_usl,
                                      p_vol      => round(l_norm_vol,3),
                                      p_n1       => null,
                                      p_tp       => 1);
if l_ret2 = 0 then
            l_ret := 0;
end if;
else
          -- кол-во месяцев непередачи показаний < 3 - начислять средний объем
          if l_counter = 'phw' then
            --автоначислить по х.в.
            if c.m_hw >= 3 and c.cnt_hw > 0 then
              --не менее 3 месяцев счетчик
              l_ret2 := ins_vol_meter(p_met_klsk => null,
                                      p_lsk      => c.lsk,
                                      p_usl      => p_usl,
                                      p_vol      => round(c.cnt_hw / c.m_hw,
                                                          3),
                                      p_n1       => null,
                                      p_tp       => 1);
if l_ret2 = 0 then
                l_ret := 0;
end if;
end if;
elsif l_counter = 'pgw' then
            --автоначислить по г.в.
            if c.m_gw >= 3 and c.cnt_gw > 0 then
              --не менее 3 месяцев счетчик
              l_ret2 := ins_vol_meter(p_met_klsk => null,
                                      p_lsk      => c.lsk,
                                      p_usl      => p_usl,
                                      p_vol      => round(c.cnt_gw / c.m_gw,
                                                          3),
                                      p_n1       => null,
                                      p_tp       => 1);
if l_ret2 = 0 then
                l_ret := 0;
end if;
end if;
elsif l_counter = 'pel' then
            --автоначислить по эл.эн.
            if c.m_el >= 3 and c.cnt_el > 0 then
              --не менее 3 месяцев счетчик
              l_ret2 := ins_vol_meter(p_met_klsk => null,
                                      p_lsk      => c.lsk,
                                      p_usl      => p_usl,
                                      p_vol      => round(c.cnt_el / c.m_el,
                                                          3),
                                      p_n1       => null,
                                      p_tp       => 1);
if l_ret2 = 0 then
                l_ret := 0;
end if;
end if;
end if;
end if;

end loop;
elsif p_set = 0 then
      --СНЯТИЕ АВТОНАЧИСЛЕНИЯ (отмена)
      for c in (select m.k_lsk_id, t.n1
                  from meter m, t_objxpar t, params p, u_list s
                 where t.fk_k_lsk = m.k_lsk_id
                   and t.mg = p.period
                   and s.cd = 'ins_vol_sch'
                   and m.fk_usl = p_usl
                   and t.fk_list = s.id
                   and t.tp in (1,2) --тип - автоначислено или снято автоначисление ред.27.05.2019
                   /*and t.id = --последняя итерация - в корне не верно! ред.27.05.2019
                       (select max(t.id)
                          from meter m2, t_objxpar t, params p, u_list s
                         where t.fk_k_lsk = m2.k_lsk_id
                           and t.mg = p.period
                           and s.cd = 'ins_vol_sch'
                           and m2.fk_usl = p_usl
                           and t.fk_list = s.id
                           and t.tp in (1) --тип - автоначислено
                           and m2.id = m.id)*/) loop

        l_ret2 := ins_vol_meter(p_met_klsk => c.k_lsk_id,
                                p_lsk      => null,
                                p_usl      => null,
                                p_vol      => -1 * c.n1,
                                p_n1       => null,
                                p_tp       => 2);
if l_ret2 = 0 then
          l_ret := 0;
end if;
end loop;

end if;
commit;
if p_set = 1 then
      logger.log_(null,
                  'p_meter.gen_auto_chrg_all Окончание-автоначислено по среднему по услуге usl=' ||
                  p_usl);
elsif p_set = 0 then
      logger.log_(null,
                  'p_meter.gen_auto_chrg_all Окончание-Снятие:автоначисления по среднему по услуге usl=' ||
                  p_usl);
end if;

return l_ret;
end;

--получить код наличия счетчика по последней дате месяца для kart.psch
function getpsch(p_lsk in kart.lsk%type) return number is
  begin

for c in (with t as
                 (select distinct d.dat,case
                                   when m.fk_usl is not null and
                                        m2.fk_usl is not null then
                                    1 -- х.в. и г.в.
                                   when m.fk_usl is not null and
                                        m2.fk_usl is null then
                                    2 -- х.в.
                                   when m.fk_usl is null and
                                        m2.fk_usl is not null then
                                    3 -- г.в.
                                   else
                                    0
                                 end as psch
                   from v_cur_days d
                   join kart k
                     on k.lsk = p_lsk
                   join usl u
                     on u.cd = 'х.вода'
                   join usl u2
                     on u2.cd = 'г.вода'
                   left join meter m
                     on k.k_lsk_id = m.fk_klsk_obj
                    and d.dat between m.dt1 and m.dt2
                    and m.fk_usl = u.usl
                   left join meter m2
                     on k.k_lsk_id = m2.fk_klsk_obj
                    and d.dat between m2.dt1 and m2.dt2
                    and m2.fk_usl = u2.usl)
                select psch, min(dat) as dt1, max(dat) as dt2
                  from (select t.*, row_number() over(order by dat) - row_number() over(partition by psch order by dat) as grp
                           from t)
                 group by grp, psch
                 order by dt1 desc) loop

      return c.psch;

end loop;

-- нет возможности определить, вернуть норматив
return 0;

end;

--получить код наличия Электросчетчика для по последней дате месяца для kart.psch
function getElpsch(p_lsk in kart.lsk%type) return number is
  begin

for c in (with t as
                 (select distinct d.dat,case
                                   when m.fk_usl is not null then
                                    1 -- есть счетчик
                                   else
                                    0 -- нет счетчика
                                 end as psch
                   from v_cur_days d
                   join kart k
                     on k.lsk = p_lsk
                   join usl u
                     on u.cd = 'эл.энерг.2'
                   left join meter m
                     on k.k_lsk_id = m.fk_klsk_obj
                    and d.dat between m.dt1 and m.dt2
                    and m.fk_usl = u.usl)
                select psch, min(dat) as dt1, max(dat) as dt2
                  from (select t.*, row_number() over(order by dat) - row_number() over(partition by psch order by dat) as grp
                           from t)
                 group by grp, psch
                 order by dt1 desc) loop

      return c.psch;

end loop;

-- нет возможности определить, вернуть норматив
return 0;

end;

-- обработать неисправные счетчики
procedure del_broken_meter(p_usl in varchar2) is
    l_dt1          date;
l_dt2          date;
l_mg           params.period%type;
l_back_6_month params.period%type;
l_ret          number;
  begin
    -- первая дата месяца
l_dt1 := gdt(1, 0, 0);
    -- последняя дата месяца
l_dt2 := gdt(32, 0, 0);
select p.period into l_mg from params p;
-- период назад на 6 мес
l_back_6_month := utils.add_months_pr(mg_ => l_mg, cnt_ => -6);

logger.log_(time_     => null,
                comments_ => 'p_meter.del_broken_meters: начало обработки неисправных счетчиков');
    -- найти неисправные счетчики
for c in (select m.id, us.nm, m.fk_usl, k.lsk, m.k_lsk_id, t.fk_meter, months_between(l_dt1,
                                     t.dt1) as cnt_months, --кол-во мес. не работы
                     case
                       when not t.dt1 < l_dt1 --дата закрытия счетчика
                        then
                        l_dt1
                       else
                        t.dt1
                     end dt1
                from c_reg_sch t
                join u_list u
                  on t.fk_tp = u.id
                 and u.cd = 'Поверка ПУ'
                join u_list u2
                  on t.fk_state = u2.id
                 and u2.cd = 'Неисправен ПУ'
                 and t.dt1 < l_dt2 -- ограничить тек.периодом
                join meter m
                  on m.id = t.fk_meter
                 and m.dt2 > l_dt2 -- открытый счетчик
                 and m.fk_usl = p_usl
                left join (select k_lsk_id, trim(max(t.lsk)) as lsk
                            from kart t
                           where t.psch not in (8, 9)
                           group by k_lsk_id) k
                  on m.fk_klsk_obj = k.k_lsk_id
                join usl us
                  on m.fk_usl = us.usl
               where months_between(l_dt1, t.dt1) > 0
                 and not exists
               (select *
                        from c_reg_sch t2 -- где нет других статусов позднее
                        join u_list u3
                          on t2.fk_tp = u3.id
                         and u3.cd = 'Поверка ПУ'
                        join u_list u4
                          on t2.fk_state = u4.id
                         and u4.cd in ('Неисправен ПУ',
                                       'Исправен ПУ')
                       where t2.dt1 > t.dt1 -- позднее!
                         and t2.fk_meter = t.fk_meter)) loop

      if c.cnt_months > 2 then

        -- снять текущий расход по счетчику
        for c2 in (select t.n1 * -1 as vol
                     from t_objxpar t
                     join u_list u
                       on t.fk_list = u.id
                      and u.cd = 'ins_vol_sch'
                     join t_user s
                       on s.cd = user
                    where t.fk_k_lsk = c.k_lsk_id
                      and t.mg = l_mg
                      and not exists (select *
                             from t_objxpar x
                            where x.fk_k_lsk = t.fk_k_lsk
                              and x.mg = l_mg
                              and x.tp = 4) --где не было снятий по счетчику
                   ) loop

          l_ret := ins_vol_meter(p_met_klsk => c.k_lsk_id,
                                 p_lsk      => null,
                                 p_usl      => null,
                                 p_vol      => c2.vol,
                                 p_n1       => null,
                                 p_tp       => 4);
if l_ret <> 0 then
            Raise_application_error(-20000,
                                    'Ошибка снятия объемов счетчика с id=' || c.id);
end if;
end loop;

-- закрыть счетчик, который не действует больше 2 мес.
update meter t set t.dt2 = c.dt1 where t.id = c.fk_meter;

logger.log_act(c.lsk,
                       '### Неисправный счетчик id=' || c.id ||
                       ' по услуге: ' || trim(c.nm) ||
                       ', >= 3 месяца, установлен норматив',
                       2);
else
        -- снять текущий расход по счетчику, который не действует менее 2 мес.(в автоначислении начислится среднее за 6 мес)
        for c2 in (select t.n1 * -1 as vol
                     from t_objxpar t
                     join u_list u
                       on t.fk_list = u.id
                      and u.cd = 'ins_vol_sch'
                     join t_user s
                       on s.cd = user
                    where t.fk_k_lsk = c.k_lsk_id
                      and t.mg = l_mg
                      and not exists (select *
                             from t_objxpar x
                            where x.fk_k_lsk = t.fk_k_lsk
                              and x.mg = l_mg
                              and x.tp = 4) --где не было снятий по счетчику
                   ) loop

          l_ret := ins_vol_meter(p_met_klsk => c.k_lsk_id,
                                 p_lsk      => null,
                                 p_usl      => null,
                                 p_vol      => c2.vol,
                                 p_n1       => null,
                                 p_tp       => 4);
if l_ret <> 0 then
            Raise_application_error(-20000,
                                    'Ошибка снятия объемов счетчика с id=' || c.id);
end if;
end loop;
logger.log_act(c.lsk,
                       '### Неисправный счетчик id=' || c.id ||
                       ' по услуге: ' || trim(c.nm) ||
                       ', < 3 месяца, снят текущий расход',
                       2);
end if;

end loop;

logger.log_(time_     => null,
                comments_ => 'p_meter.del_broken_meters: окончание обработки неисправных счетчиков');
  /* закомментировал 08.07.2019 после разговора с Полыс, что не должны переходить счетчики по которым не передают показания
    logger.log_(time_     => null,
                comments_ => 'p_meter.del_broken_meters: начало обработки счетчиков по которым не передают показания');

    for c in (select m.id, k.lsk, us.nm,case
                       when not m.dt1 < l_dt1 --дата закрытия счетчика
                        then
                        l_dt1
                       else
                        m.dt1
                     end dt1
                from meter m
                join (select k_lsk_id, trim(max(t.lsk)) as lsk
                       from kart t, v_lsk_tp tp
                      where t.psch not in (8, 9)
                        and t.fk_tp = tp.id
                        and tp.cd = 'LSK_TP_MAIN'
                      group by k_lsk_id) k
                  on m.fk_klsk_obj = k.k_lsk_id
                 and m.dt2 > l_dt2 -- открытый счетчик
                 and m.dt1 <= to_date(l_back_6_month || '01', 'YYYYMMDD') -- дата начала существования счетчика ранее 6 мес назад
                join usl us
                  on m.fk_usl = us.usl
               where not exists (select *
                        from c_reg_sch t2 -- где нет статусов неисправности, позднее начальной даты счетчика
                        join u_list u3
                          on t2.fk_tp = u3.id
                         and u3.cd = 'Поверка ПУ'
                        join u_list u4
                          on t2.fk_state = u4.id
                         and u4.cd in ('Неисправен ПУ')
                      -- и позднее начальной даты не предоставления показаний (если некорректна начальная дата счетчиков)
                       where t2.dt1 >= m.dt1
                         and t2.dt1 > to_date(l_back_6_month || '01',
                                              'YYYYMMDD')
                         and t2.fk_meter = m.id)
                 and not exists -- где не передавались объемы по счетчикам, начиная с периода 6 мес назад
               (select t2.*
                        from T_OBJXPAR t2, u_list s
                       where t2.fk_list = s.id
                         and s.cd in ('ins_vol_sch', 'ins_sch') -- объем или показания
                         and t2.fk_k_lsk = m.k_lsk_id
                         and t2.mg > l_back_6_month
                         and t2.tp not in (1, 2) -- кроме автоначисления
                         and nvl(n1, 0) <> 0 -- ненулевой объем
                      )) loop

      -- закрыть счетчик, по которому не передают показаний больше 6 мес. (сделать норматив)
      update meter t set t.dt2 = c.dt1 where t.id = c.id;

      logger.log_act(c.lsk,
                     '### Закрыт счетчик по которому не передают показания id=' || c.id ||
                     ' по услуге: ' || trim(c.nm) ||
                     ', > 6 месяцов, установлен норматив',
                     2);

    end loop;*/

commit;
end;

-- импорт всех счетчиков
procedure imp_all_meter is
    l_usl_hw varchar2(3);
l_usl_gw varchar2(3);
l_usl_el varchar2(3);
l_usl_ot varchar2(3);
  begin

l_usl_hw := '011';
l_usl_gw := '015';
l_usl_el := '038';
l_usl_ot := '007';

    -- почистить k_lsk от незначащих id счетчиков
delete from k_lsk t
where not exists (select * from meter m where m.k_lsk_id = t.id)
  and exists (select *
              from u_list u
              where u.id = t.fk_addrtp
                and u.cd = 'ИПУ');

--  ПОЛНАЯ ОЧИСТКА СЧЕТЧИКОВ
delete from meter;

--  импорт
for c in (select * from kart k where k.psch not in (8, 9)) loop
      imp_lsk_meter(c.lsk, l_usl_hw, l_usl_gw, l_usl_el, l_usl_ot);
end loop;

-- НЕ НАДО УДАЛЯТЬ, так как буду загружать импорт много раз!
-- удалить старые статусы поверки счетчиков
/*     delete from c_reg_sch r where exists
(select * from c_reg_sch s join u_list u on s.fk_tp=u.id
         and u.cd='Поверка ПУ'
         join usl us on us.usl=s.fk_usl and us.usl in (l_usl_hw, l_usl_gw, l_usl_el)
         where r.rowid=s.rowid
         );  */
-- заполнить дубль kart - в конце, так как счетчики замещаются показаниями во время иммпорта
/*  delete from kart2 t;
    insert into kart2
      (lsk, kul, nd, kw, fio, kpr, kpr_wr, kpr_ot, kpr_cem, kpr_s, opl, ppl, pldop, ki, psch, psch_dt, status, kwt, lodpl, bekpl, balpl, komn, et, kfg, kfot, phw, mhw, pgw, mgw, pel, mel, sub_nach, subsidii, sub_data, polis, sch_el, reu, text, schel_dt, eksub1, eksub2, kran, kran1, el, el1, sgku, doppl, subs_cor, house_id, c_lsk_id, mg1, mg2, kan_sch, subs_inf, k_lsk_id, dog_num, schel_end, fk_deb_org, subs_cur, k_fam, k_im, k_ot, memo, fk_distr, law_doc, fk_pasp_org, flag, flag1, fk_err, law_doc_dt, prvt_doc, prvt_doc_dt, cpn, kpr_wrp, pn_dt, lsk_ext, fk_tp, sel1, vvod_ot, entr, pot, mot, elsk)
    select lsk, kul, nd, kw, fio, kpr, kpr_wr, kpr_ot, kpr_cem, kpr_s, opl, ppl, pldop, ki, psch, psch_dt, status, kwt, lodpl, bekpl, balpl, komn, et, kfg, kfot, phw, mhw, pgw, mgw, pel, mel, sub_nach, subsidii, sub_data, polis, sch_el, reu, text, schel_dt, eksub1, eksub2, kran, kran1, el, el1, sgku, doppl, subs_cor, house_id, c_lsk_id, mg1, mg2, kan_sch, subs_inf, k_lsk_id, dog_num, schel_end, fk_deb_org, subs_cur, k_fam, k_im, k_ot, memo, fk_distr, law_doc, fk_pasp_org, flag, flag1, fk_err, law_doc_dt, prvt_doc, prvt_doc_dt, cpn, kpr_wrp, pn_dt, lsk_ext, fk_tp, sel1, vvod_ot, entr, pot, mot, elsk from kart;
*/
commit;
end;

-- импортировать статусы поверки
procedure imp_states_meter(p_lsk      in varchar2,
                             p_klsk_met in number,
                             p_usl      in varchar2) is
  begin
insert into c_reg_sch
(dt1, fk_tp, fk_state, text, fk_usl, lsk, fk_meter)
select s.dt1, s.fk_tp, s.fk_state, s.text, null as fk_usl, s.lsk, m.id as fk_meter
from c_reg_sch s
         join u_list u
              on s.fk_tp = u.id
                  and u.cd = 'Поверка ПУ'
                  and s.fk_usl = p_usl
                  and s.lsk = p_lsk
         join meter m
              on m.k_lsk_id = p_klsk_met;

end;

-- импорт счетчиков из kart, c_states_sch
procedure imp_lsk_meter(p_lsk    in kart.lsk%type,
                          p_usl_hw in varchar2,
                          p_usl_gw in varchar2,
                          p_usl_el in varchar2,
                          p_usl_ot in varchar2) is
    l_mfd      date; --самая первая дата в системе
l_mld      date; --самая последняя дата в системе
l_usl_hw   varchar2(3);
l_usl_gw   varchar2(3);
l_usl_el   varchar2(3);
l_usl_ot   varchar2(3);
l_met_klsk number;
l_cur_dt1  date;
l_cur_dt2  date;
l_sch_el   number;
l_pel      number;
l_klsk_obj number;
l_arch_mg  params.period%type; -- месяц, с которого импортировать архивные счетчики
  begin

l_mfd     := gdt(1, 1, 1990);
l_mld     := gdt(1, 1, 2050);
l_arch_mg := '201701'; -- за год обычно

    --  текущая дата
select to_date(p.period || '01', 'YYYYMMDD'), last_day(to_date(p.period || '01',
                                                               'YYYYMMDD'))
into l_cur_dt1, l_cur_dt2
from params p;

l_usl_hw := p_usl_hw;
l_usl_gw := p_usl_gw;
l_usl_el := p_usl_el;
l_usl_ot := p_usl_ot;

for c in (select s.id, s.lsk, s.fk_status, nvl(s.dt1, l_mfd) as dt1, nvl(s.dt2,
                          l_mld) as dt2, k.k_lsk_id, k.phw, k.pgw, k.pel,case
                       when l_cur_dt2 between nvl(s.dt1, l_mfd) and
                            nvl(s.dt2, l_mld) then
                        1
                       else
                        0
                     end as active, k.sch_el
                from c_states_sch s
                join kart k
                  on s.lsk = k.lsk
               where k.lsk = p_lsk
               order by nvl(s.dt1, l_mfd), nvl(s.dt1, l_mld)) loop

      if c.fk_status = 1 then
        --сч.х.в. и г.в

        l_met_klsk := ins_meter(p_npp         => 1,
                                p_usl         => l_usl_hw,
                                p_dt1         => c.dt1,
                                p_dt2         => c.dt2,
                                p_n1          => case
                                                   when c.active = 1 then
                                                    c.phw
                                                   else
                                                    null
                                                 end,
                                p_fk_klsk_obj => c.k_lsk_id,
                                p_tp          => 'ИПУ');

if c.active = 1 then
          imp_arch_meter(p_lsk      => p_lsk,
                         p_met_klsk => l_met_klsk,
                         p_mg       => l_arch_mg,
                         p_counter  => 'mhw');
imp_states_meter(p_lsk      => p_lsk,
                    p_klsk_met => l_met_klsk,
                    p_usl      => l_usl_hw);
end if;

l_met_klsk := ins_meter(p_npp         => 1,
                                p_usl         => l_usl_gw,
                                p_dt1         => c.dt1,
                                p_dt2         => c.dt2,
                                p_n1          => case
                                                   when c.active = 1 then
                                                    c.pgw
                                                   else
                                                    null
                                                 end,
                                p_fk_klsk_obj => c.k_lsk_id,
                                p_tp          => 'ИПУ');
if c.active = 1 then
          imp_arch_meter(p_lsk      => p_lsk,
                         p_met_klsk => l_met_klsk,
                         p_mg       => l_arch_mg,
                         p_counter  => 'mgw');
imp_states_meter(p_lsk      => p_lsk,
                    p_klsk_met => l_met_klsk,
                    p_usl      => l_usl_gw);
end if;

elsif c.fk_status = 2 then
        --сч.х.в.

        l_met_klsk := ins_meter(p_npp         => 1,
                                p_usl         => l_usl_hw,
                                p_dt1         => c.dt1,
                                p_dt2         => c.dt2,
                                p_n1          => case
                                                   when c.active = 1 then
                                                    c.phw
                                                   else
                                                    null
                                                 end,
                                p_fk_klsk_obj => c.k_lsk_id,
                                p_tp          => 'ИПУ');
if c.active = 1 then
          imp_arch_meter(p_lsk      => p_lsk,
                         p_met_klsk => l_met_klsk,
                         p_mg       => l_arch_mg,
                         p_counter  => 'mhw');
imp_states_meter(p_lsk      => p_lsk,
                    p_klsk_met => l_met_klsk,
                    p_usl      => l_usl_hw);
end if;

elsif c.fk_status = 3 then
        --сч.г.в.

        l_met_klsk := ins_meter(p_npp         => 1,
                                p_usl         => l_usl_gw,
                                p_dt1         => c.dt1,
                                p_dt2         => c.dt2,
                                p_n1          => case
                                                   when c.active = 1 then
                                                    c.pgw
                                                   else
                                                    null
                                                 end,
                                p_fk_klsk_obj => c.k_lsk_id,
                                p_tp          => 'ИПУ');
if c.active = 1 then
          imp_arch_meter(p_lsk      => p_lsk,
                         p_met_klsk => l_met_klsk,
                         p_mg       => l_arch_mg,
                         p_counter  => 'mgw');
imp_states_meter(p_lsk      => p_lsk,
                    p_klsk_met => l_met_klsk,
                    p_usl      => l_usl_gw);
end if;

end if;

l_sch_el   := c.sch_el;
l_pel      := c.pel;
l_klsk_obj := c.k_lsk_id;
end loop;

-- счетчики отопления
for c in (select *
                from kart k
               where k.psch not in (8, 9)
                 and k.pot <> 0
                 and k.lsk = p_lsk) loop
      l_met_klsk := ins_meter(p_npp         => 1,
                              p_usl         => l_usl_ot,
                              p_dt1         => l_mfd,
                              p_dt2         => l_mld,
                              p_n1          => c.pot,
                              p_fk_klsk_obj => l_klsk_obj,
                              p_tp          => 'ИПУ');
imp_arch_meter(p_lsk      => p_lsk,
                  p_met_klsk => l_met_klsk,
                  p_mg       => l_arch_mg,
                  p_counter  => 'mot');
imp_states_meter(p_lsk      => p_lsk,
                    p_klsk_met => l_met_klsk,
                    p_usl      => l_usl_ot);
end loop;

-- счетчики Эл.Эн
for c in (select *
                from kart k
               where k.psch not in (8, 9)
                 and k.pel <> 0
                 and k.sch_el <> 0
                 and k.lsk = p_lsk) loop

      l_met_klsk := ins_meter(p_npp         => 1,
                              p_usl         => l_usl_el,
                              p_dt1         => l_mfd,
                              p_dt2         => l_mld,
                              p_n1          => c.pel,
                              p_fk_klsk_obj => c.k_lsk_id,
                              p_tp          => 'ИПУ');
imp_arch_meter(p_lsk      => p_lsk,
                  p_met_klsk => l_met_klsk,
                  p_mg       => l_arch_mg,
                  p_counter  => 'mel');
imp_states_meter(p_lsk      => p_lsk,
                    p_klsk_met => l_met_klsk,
                    p_usl      => l_usl_el);
end loop;

end;

-- импортировать архивные объемы счетчиков
procedure imp_arch_meter(p_lsk      in kart.lsk%type, -- лс
                           p_met_klsk in number, -- klsk счетчика
                           p_mg       in params.period%type, -- начать с периода
                           p_counter  in varchar2 -- код счетчика
                           ) is
    l_vol_id number;
l_user   number;
  begin

select max(decode(u.cd, 'ins_vol_sch', u.id, 0)), max(s.id)
into l_vol_id, l_user
from u_list u, t_user s
where u.cd in ('ins_sch', 'ins_vol_sch')
  and s.cd = user;

for c in (select t.sch_el, t.mg, t.psch, decode(p_counter,
                             'mhw',
                             t.mhw,
                             'mgw',
                             t.mgw,
                             'mel',
                             t.mel,
                             'mot',
                             t.mot) as vol
                from arch_kart t, params p
               where t.lsk = p_lsk
                 and t.mg >= p_mg -- архив
                 and t.mg <> p.period
              union all
              select t.sch_el, p.period, t.psch, decode(p_counter,
                             'mhw',
                             t.mhw,
                             'mgw',
                             t.mgw,
                             'mel',
                             t.mel,
                             'mot',
                             t.mot) as vol
                from kart t, params p
               where t.lsk = p_lsk -- присоединить текущий период
              ) loop

      if nvl(c.vol, 0) <> 0 then
        if p_counter = 'mhw' and (c.psch = 1 or c.psch = 2) then
          --сч.х.в.
          --отразить объем на счетчике
insert into t_objxpar
(fk_k_lsk, fk_list, n1, mg, fk_user)
values
    (p_met_klsk, l_vol_id, c.vol, c.mg, l_user);

elsif p_counter = 'mgw' and (c.psch = 1 or c.psch = 3) then
          --сч.х.в.
          --отразить объем на счетчике
insert into t_objxpar
(fk_k_lsk, fk_list, n1, mg, fk_user)
values
    (p_met_klsk, l_vol_id, c.vol, c.mg, l_user);

elsif p_counter = 'mel' and c.sch_el = 1 then
          --сч.эл.эн.
          --отразить объем на счетчике
insert into t_objxpar
(fk_k_lsk, fk_list, n1, mg, fk_user)
values
    (p_met_klsk, l_vol_id, c.vol, c.mg, l_user);

elsif p_counter = 'mot' and nvl(c.vol, 0) <> 0 then
          --сч.отоп.
          --отразить объем на счетчике
insert into t_objxpar
(fk_k_lsk, fk_list, n1, mg, fk_user)
values
    (p_met_klsk, l_vol_id, c.vol, c.mg, l_user);

end if;
end if;

end loop;
end;

procedure test1 is
    l_klsk   number;
met_klsk number;
dt1      date;
dt2      date;
  begin

dt1 := gdt(1, 1, 0);
dt2 := gdt(31, 12, 14);

l_klsk := 104880;
delete from meter t where t.fk_klsk_obj = l_klsk;

met_klsk := ins_meter(p_npp         => 1,
                          p_usl         => '011',
                          p_dt1         => dt1,
                          p_dt2         => dt2,
                          p_n1          => 0,
                          p_fk_klsk_obj => l_klsk,
                          p_tp          => 'ИПУ');

    --   insert into t_objxpar (fk_k_lsk, fk_list, n1)
    --     values(met_klsk, 2694, 5.56);
insert into t_objxpar
(fk_k_lsk, fk_list, n1)
values
    (met_klsk, 2693, 555);
update meter t set t.n1 = 555 where t.k_lsk_id = met_klsk;
update kart k set k.phw = 555, k.mhw = null where k.k_lsk_id = l_klsk;

met_klsk := ins_meter(p_npp         => 1,
                          p_usl         => '015',
                          p_dt1         => dt1,
                          p_dt2         => dt2,
                          p_n1          => 0,
                          p_fk_klsk_obj => l_klsk,
                          p_tp          => 'ИПУ');

    --   insert into t_objxpar (fk_k_lsk, fk_list, n1)
    --     values(met_klsk, 2694, 12);
insert into t_objxpar
(fk_k_lsk, fk_list, n1)
values
    (met_klsk, 2693, 777);
update meter t set t.n1 = 777 where t.k_lsk_id = met_klsk;
update kart k set k.pgw = 777, k.mgw = null where k.k_lsk_id = l_klsk;

commit;
end;

/*
  удалить! пытался реализовать по новому
procedure gen_auto_chrg_all(p_set in number, p_usl in usl.usl%type) is
    l_otop number;
    l_months number;
    l_dt1 date;
    l_dt2 date;
  begin


  if p_set = 1 then
  --АВТОНАЧИСЛЕНИЕ
  --узнать отопительный ли сезон?(по последней дате месяца)
    select case
             when last_day(to_date(p.period || '01', 'YYYYMMDD')) between
                  utils.get_date_param('MONTH_HEAT1') --обраб.отопит.период
                  and utils.get_date_param('MONTH_HEAT2') then
              1
             else
              0
           end
      into l_otop
      from params p;
    -- кол-во месяцев, для вычисления по-среднему
    l_months := utils.get_int_param('AUTOCHRGM');

    --период, от года назад до прошлого месяца
      select to_date(utils.add_months_pr(p.period, -1 * l_months)||'01'), --первый день начального месяца
             last_day(to_date(utils.add_months_pr(p.period, -1)||'01'))  --последний день конечного месяца
        into l_dt1, l_dt2
        from params p;

-- insert into t_objxpar (fk_k_lsk, fk_list, n1, fk_user, mg, tp)
--           values(p_met_klsk, l_vol_id, p_vol, l_user, l_period, p_tp)


      select m.fk_klsk_obj, sum(x.n1) from meter m join t_objxpar x on m.k_lsk_id=x.fk_k_lsk
        join u_list u on x.fk_list=u.id and u.cd='ins_vol_sch'
        where m.fk_usl=p_usl and (m.dt1 between l_dt1 and l_dt2 or m.dt2 between l_dt1 and l_dt2)
        and m.fk_usl=p_usl


  else
    --СНЯТЬ АВТОНАЧИСЛЕНИЕ
    null;
  end if;

  end;*/
end p_meter;
/

