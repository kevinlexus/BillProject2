create or replace package body gen is
  procedure gen_check (err_ out number, err_str_ out varchar2,
    var_ in number) is
  cnt_ number;
l_cd_org t_org.cd%type;
l_str varchar2(1000);
cursor cur_params is
select * from params;
l_mg1 params.period%type;
rec_params cur_params%rowtype;
  begin
  -- блок не работает, встречается в Delphi Direct (проверить, как работает в ТСЖ формирование) ред.02.03.2021
return;
end;

procedure smpl_chk (p_var in number,
                           prep_refcursor IN OUT rep_refcursor) is
  begin

 -- Raise_application_error(-20000, 'TST');
  --проверка перед формированием, с выводом л.с.
  --содержащих ошибки
if p_var=1 then
    --список лс. по которым не корректно распр.пеня
      OPEN prep_refcursor FOR
select 'Некорректные лицевые, содержащие услугу х.в.ОДН, но у прочих л.с. дома услуги ОДН нет' as lsk
from dual
union all
select k.lsk from kart k, nabor n, usl u where
        k.lsk=n.lsk and k.psch not in (8,9)
                                           and n.usl=u.usl
                                           and u.cd='х.вода'
                                           and exists
        (select * from nabor r, usl u2 where r.lsk=k.lsk
                                         and r.usl=u2.usl --есть услуга ОДН
                                         and u2.cd='х.вода.ОДН')
                                           and not exists --а у других л.с. нет этой услуги
        (select * from kart t, nabor r, usl u2 where r.lsk<>k.lsk
                                                 and r.usl=u2.usl
                                                 and u2.cd='х.вода.ОДН'
                                                 and t.lsk=r.lsk
                                                 and t.house_id=k.house_id)
                                           and exists
        (select a.house_id from kart a where a.house_id=k.house_id
         having count(*)>1
         group by a.house_id);
elsif p_var=2 then
    --список лс. по которым не корректно распр.пеня
      OPEN prep_refcursor FOR
select 'Некорректные лицевые, НЕ содержащие х.в.услугу ОДН, но у прочих л.с. дома услуга ОДН есть' as lsk
from dual
union all
select k.lsk from kart k, nabor n, usl u where
        k.lsk=n.lsk and k.psch not in (8,9)
                                           and n.usl=u.usl
                                           and u.cd='х.вода'
                                           and exists
        (select * from nabor r, usl u2 where r.lsk=k.lsk
                                         and r.usl=u2.usl --есть услуга ОДН
                                         and u2.cd='х.вода.ОДН')
                                           and not exists --а у других л.с. нет этой услуги
        (select * from kart t, nabor r, usl u2 where r.lsk<>k.lsk
                                                 and r.usl=u2.usl
                                                 and u2.cd='х.вода.ОДН'
                                                 and t.lsk=r.lsk
                                                 and t.house_id=k.house_id)
                                           and exists
        (select a.house_id from kart a where a.house_id=k.house_id
         having count(*)>1
         group by a.house_id);
elsif p_var=3 then
    --список лс. по которым не корректно распр.пеня
      OPEN prep_refcursor FOR
select 'Некорректные лицевые, содержащие услугу г.в.ОДН, но у прочих л.с. дома услуги ОДН нет' as lsk
from dual
union all
select k.lsk from kart k, nabor n, usl u where
        k.lsk=n.lsk and k.psch not in (8,9)
                                           and n.usl=u.usl
                                           and u.cd='г.вода'
                                           and exists
        (select * from nabor r, usl u2 where r.lsk=k.lsk
                                         and r.usl=u2.usl --есть услуга ОДН
                                         and u2.cd='г.вода.ОДН')
                                           and not exists --а у других л.с. нет этой услуги
        (select * from kart t, nabor r, usl u2 where r.lsk<>k.lsk
                                                 and r.usl=u2.usl
                                                 and u2.cd='г.вода.ОДН'
                                                 and t.lsk=r.lsk
                                                 and t.house_id=k.house_id)
                                           and exists
        (select a.house_id from kart a where a.house_id=k.house_id
         having count(*)>1
         group by a.house_id);
elsif p_var=4 then
    --список лс. по которым не корректно распр.пеня
      OPEN prep_refcursor FOR
select 'Некорректные лицевые, НЕ содержащие г.в.услугу ОДН, но у прочих л.с. дома услуга ОДН есть' as lsk
from dual
union all
select k.lsk from kart k, nabor n, usl u where
        k.lsk=n.lsk and k.psch not in (8,9)
                                           and n.usl=u.usl
                                           and u.cd='г.вода'
                                           and exists
        (select * from nabor r, usl u2 where r.lsk=k.lsk
                                         and r.usl=u2.usl --есть услуга ОДН
                                         and u2.cd='г.вода.ОДН')
                                           and not exists --а у других л.с. нет этой услуги
        (select * from kart t, nabor r, usl u2 where r.lsk<>k.lsk
                                                 and r.usl=u2.usl
                                                 and u2.cd='г.вода.ОДН'
                                                 and t.lsk=r.lsk
                                                 and t.house_id=k.house_id)
                                           and exists
        (select a.house_id from kart a where a.house_id=k.house_id
         having count(*)>1
         group by a.house_id);
end if;
end;

procedure prep_kart_pr is
  time_ date;
  --Подготовка льготников для статистики
  begin
    /*  time_ := SYSDATE;
    EXECUTE IMMEDIATE 'TRUNCATE TABLE lg_pr';
    UPDATE KART_PR SET id = kart_pr_id.NEXTVAL;
    INSERT INTO LG_PR
      (kart_pr_id, lg_id, main, TYPE)
      SELECT id,
             SUBSTR(lp, 1, 3) AS lg_id,
             DECODE(trim(k.osno_lp1), NULL, 0, 1) AS main,
             1 AS TYPE
        FROM KART_PR k
       WHERE SUBSTR(lp, 1, 3) NOT IN ('   ', '001')
      UNION ALL
      SELECT id,
             SUBSTR(lp, 5, 3) AS lg_id,
             DECODE(trim(k.osno_lp2), NULL, 0, 1) AS main,
             1 AS TYPE
        FROM KART_PR k
       WHERE SUBSTR(lp, 5, 3) NOT IN ('   ', '001')
      UNION ALL
      SELECT id,
             SUBSTR(lp, 9, 3) AS lg_id,
             DECODE(trim(k.osno_lp3), NULL, 0, 1) AS main,
             1 AS TYPE
        FROM KART_PR k
       WHERE SUBSTR(lp, 9, 3) NOT IN ('   ', '001')
      UNION ALL
      SELECT id,
             SUBSTR(lp, 13, 3) AS lg_id,
             DECODE(trim(k.osno_lp4), NULL, 0, 1) AS main,
             1 AS TYPE
        FROM KART_PR k
       WHERE SUBSTR(lp, 13, 3) NOT IN ('   ', '001')
      UNION ALL
      SELECT id,
             SUBSTR(lpk, 1, 3) AS lg_id,
             DECODE(trim(k.osno_lp1), NULL, 0, 1) AS main,
             0 AS TYPE
        FROM KART_PR k
       WHERE SUBSTR(lpk, 1, 3) NOT IN ('   ', '001')
      UNION ALL
      SELECT id,
             SUBSTR(lpk, 5, 3) AS lg_id,
             DECODE(trim(k.osno_lp2), NULL, 0, 1) AS main,
             0 AS TYPE
        FROM KART_PR k
       WHERE SUBSTR(lpk, 5, 3) NOT IN ('   ', '001')
      UNION ALL
      SELECT id,
             SUBSTR(lpk, 9, 3) AS lg_id,
             DECODE(trim(k.osno_lp3), NULL, 0, 1) AS main,
             0 AS TYPE
        FROM KART_PR k
       WHERE SUBSTR(lpk, 9, 3) NOT IN ('   ', '001')
      UNION ALL
      SELECT id,
             SUBSTR(lpk, 13, 3) AS lg_id,
             DECODE(trim(k.osno_lp4), NULL, 0, 1) AS main,
             0 AS TYPE
        FROM KART_PR k
       WHERE SUBSTR(lpk, 13, 3) NOT IN ('   ', '001');
    COMMIT;
    ADMIN.ANALYZE('lg_pr');*/
logger.log_(time_, 'gen.prep_kart_pr');
end prep_kart_pr;

procedure gen_opl_xito3 is
    stmt varchar2(2500);
type_otchet constant number := 3; --тип отчета - оплата по пункатм (XITO3)
time_ date;
mg_ params.period%type;
  begin
select period into mg_ from params p;
time_ := sysdate;
    --ЗА МЕСЯЦ
    --Оплата по пунктам начисления
delete from xxito3 where mg = mg_;
insert into xxito3
(trest, reu, usl, dopl, summa, mg)
select s.trest,
       s.reu,
       u.usl,
       t.dopl,
       sum(summa) summa,
       mg_ as mg
from kwtp_day t, usl u, kart k, s_reu_trest s, oper o
where t.usl = u.usl
  and t.lsk = k.lsk
  and k.reu = s.reu
  and t.oper = o.oper
  and substr(o.oigu, 1, 1) = '1'
  and to_char(t.dat_ink, 'YYYYMM') = mg_
  and t.priznak = 1
  and t.dat_ink between init.g_dt_start and init.g_dt_end
group by u.usl, s.trest, s.reu, t.dopl;
delete from period_reports p
where p.id = type_otchet
  and p.mg = mg_; --обновляем период для отчета
stmt := 'insert into period_reports (id, mg) values(:id, :mg)';
execute immediate stmt
      using type_otchet, mg_;
commit;
logger.log_(time_, 'gen.gen_opl_xito3 ' || mg_);
end;

procedure gen_opl_xito5_ is
    stmt varchar2(2500);
type_otchet constant number := 5; --тип отчета - оплата по операциям (XITO10)
time_ date;
mg_ params.period%type;
  begin
select period into mg_ from params p;
time_ := sysdate;
    --ЗА МЕСЯЦ
    --Оплата по операциям (для оборотной ведомости)
delete from xito5_ where mg = mg_;
insert into xito5_
(ska, pn, trest, reu, from_reu, other, nal, ink, oper, mg)
select sum(ska) ska,
       sum(pn) pn,
       trest,
       reu,
       from_reu,
       other,
       nal,
       ink,
       oper,
       mg_ as mg
from v_xito5_all_ v
group by trest,
         reu,
         from_reu,
         other,
         nal,
         ink,
         oper,
         mg_,
         to_char(sysdate, 'DD/MM/YYYY HH24:MI');
delete from period_reports p
where p.id = type_otchet
  and p.mg = mg_; --обновляем период для отчета
stmt := 'insert into period_reports (id, mg) values(:id, :mg)';
execute immediate stmt
      using type_otchet, mg_;
commit;
logger.log_(time_, 'gen.gen_opl_xito5_ ' || mg_);
end;

procedure gen_opl_xito5 is
    stmt varchar2(2500);
type_otchet  constant number := 4; --тип отчета - оплата по операциям (XITO5)
type_otchet2 constant number := 10; --тип отчета - пеня (Ф.8.1)
time_ date;
mg_ params.period%type;
  begin
select period into mg_ from params p;
time_ := sysdate;
    --ЗА МЕСЯЦ
    --Оплата по операциям (по инкассациям)
delete from xito5 where mg = mg_;
insert into xito5
(ska, pn, trest, reu, other, nal, ink, oper, mg)
select sum(ska) ska,
       sum(pn) pn,
       trest,
       reu,
       other,
       nal,
       ink,
       oper,
       mg_ as mg
from v_xito5_all v
group by trest,
         reu,
         other,
         nal,
         ink,
         oper,
         mg_,
         to_char(sysdate, 'DD/MM/YYYY HH24:MI');
delete from period_reports p
where p.id = type_otchet
  and p.mg = mg_; --обновляем период для отчета
stmt := 'insert into period_reports (id, mg) values(:id, :mg)';
execute immediate stmt
      using type_otchet, mg_;
delete from period_reports p
where p.id = type_otchet2
  and p.mg = mg_; --обновляем период для отчета
stmt := 'insert into period_reports (id, mg) values(:id, :mg)';
execute immediate stmt
      using type_otchet2, mg_;
commit;
logger.log_(time_, 'gen.gen_opl_xito5 ' || mg_);
end;

procedure gen_opl_xito5day(dat1_ in xito5.dat%type,
                             dat2_ in xito5.dat%type) is
    stmt varchar2(2500);
type_otchet  constant number := 4; --тип отчета - оплата по операциям (XITO10)
type_otchet2 constant number := 10; --тип отчета - пеня (Ф.8.1)
time_ date;
  begin
time_ := sysdate;
    --ЗА ДЕНЬ
    --Оплата по операциям (для оборотки)
for a in to_char(dat1_, 'YYYYMMDD') .. to_char(dat2_, 'YYYYMMDD') loop
delete from xito5 where to_char(dat, 'YYYYMMDD') = a;
insert into xito5
(ska, pn, trest, reu, other, nal, ink, oper, nkom, nink, dat)
select sum(ska) ska,
       sum(pn) pn,
       trest,
       reu,
       other,
       nal,
       ink,
       oper,
       nkom,
       nink,
       to_date(a, 'YYYYMMDD') as dat
from v_xito5_allday v
where v.md = to_date(a, 'YYYYMMDD')
group by trest,
         reu,
         other,
         nal,
         ink,
         oper,
         nkom,
         nink,
         to_date(a, 'YYYYMMDD'),
         to_char(sysdate, 'DD/MM/YYYY HH24:MI');
delete from period_reports p
where p.id = type_otchet
  and to_char(dat, 'YYYYMMDD') = a; --обновляем период для отчета
stmt := 'insert into period_reports (id, dat) values(:id, :dat)';
execute immediate stmt
        using type_otchet, to_date(a, 'YYYYMMDD');
stmt := 'insert into period_reports (id, dat) values(:id, :dat)';

delete from period_reports p
where p.id = type_otchet2
  and to_char(dat, 'YYYYMMDD') = a; --обновляем период для отчета
execute immediate stmt
        using type_otchet2, to_date(a, 'YYYYMMDD');
commit;
end loop;
logger.log_(time_,
                'gen.gen_opl_xito5day ' || to_char(dat1_) || ' ' ||
                to_char(dat2_));
end;

procedure gen_opl_xito5day_(dat1_ in xito5_.dat%type,
                              dat2_ in xito5_.dat%type) is
    stmt varchar2(2500);
type_otchet constant number := 5; --тип отчета - оплата по операциям (XITO10)
time_ date;
  begin
    --ЗА ДЕНЬ
    --Оплата по операциям (по инкассациям)
time_ := sysdate;
for a in to_char(dat1_, 'YYYYMMDD') .. to_char(dat2_, 'YYYYMMDD') loop
delete from xito5_ where to_char(dat, 'YYYYMMDD') = a;
insert into xito5_
(ska, pn, trest, reu, from_reu, other, nal, ink, oper, dat)
select sum(ska) ska,
       sum(pn) pn,
       trest,
       reu,
       from_reu,
       other,
       nal,
       ink,
       oper,
       to_date(a, 'YYYYMMDD') as dat
from v_xito5_allday_ v
where v.md = to_date(a, 'YYYYMMDD')
group by trest,
         reu,
         from_reu,
         other,
         nal,
         ink,
         oper,
         to_date(a, 'YYYYMMDD'),
         to_char(sysdate, 'DD/MM/YYYY HH24:MI');
delete from period_reports p
where p.id = type_otchet
  and to_char(dat, 'YYYYMMDD') = a; --обновляем период для отчета
stmt := 'insert into period_reports (id, dat) values(:id, :dat)';
execute immediate stmt
        using type_otchet, to_date(a, 'YYYYMMDD');
commit;
end loop;
admin.send_message('Сформирована оплата Ф.3.1. за текущий период, c ' ||
                       to_char(dat1_, 'DD/MM/YYYY') || ' по ' ||
                       to_char(dat2_, 'DD/MM/YYYY'));
logger.log_(time_,
                'gen.gen_opl_xito5day_ ' || to_char(dat1_) || ' ' ||
                to_char(dat2_));
end;

procedure gen_opl_xito10day(dat1_ in xxito10.dat%type,
                              dat2_ in xxito10.dat%type) is
    time_ date;
mg_ params.period%type;
l_xxito14_period_var number;
  begin
    --За день
    --Оплата по предприятиям/трестам/услугам
time_ := sysdate;
select p.period into mg_ from params p;
for a in to_char(dat1_, 'YYYYMMDD') .. to_char(dat2_, 'YYYYMMDD') loop
      -- xxito11 детализация до кода операции
delete from xxito11 t where t.dat = to_date(a, 'YYYYMMDD')
                        and t.mg=mg_;
insert into xxito11
(usl,
 oper,
 org,
 summa,
 trest,
 reu,
 dat,
 var,
 forreu,
 oborot,
 dopl,
 mg)
select v.usl,
       v.oper,
       v.org,
       sum(v.summar) as summar,
       v.trest,
       v.reu,
       to_date(a, 'YYYYMMDD') as dat,
       v.var,
       v.forreu,
       v.oborot,
       v.dopl,
       mg_
from (select t.lsk, t.usl, t.org, substr(t.nkom, 1, 2) AS reu, s.trest, 0 as var,
             s.reu as forreu, to_number(substr(op.oigu, 1, 1)) AS oborot,
             t.summa as summar, t.oper, t.dat_ink as dat, t.usl as usl_b, t.org as org_b, t.dopl
      from kwtp_day t, kart k, s_reu_trest s, oper op
      where t.lsk=k.lsk and k.reu=s.reu and t.oper=op.oper and t.priznak = 1
        and t.dat_ink between init.g_dt_start and init.g_dt_end) v
where v.dat = to_date(a, 'YYYYMMDD')
group by usl,
         oper,
         org,
         trest,
         reu,
         to_date(a, 'YYYYMMDD'),
         to_char(sysdate, 'DD/MM/YYYY HH24:MI'),
         var,
         forreu,
         oborot,
         dopl;

-- xxito12 детализация до статуса жилья, адреса
delete from xxito12 t where t.dat = to_date(a, 'YYYYMMDD')
                        and t.mg=mg_;
insert into xxito12
(usl,
 org,
 summa,
 trest,
 reu,
 dat,
 var,
 forreu,
 kul,
 nd,
 status,
 dopl,
 mg)
select v.usl,
       v.org,
       sum(v.summar) as summar,
       v.trest,
       v.reu,
       to_date(a, 'YYYYMMDD') as dat,
       v.var,
       v.forreu,
       k.kul,
       k.nd,
       decode(k.status, 1, 0, 1),
       v.dopl,
       mg_
from (select t.lsk, t.usl, t.org, substr(t.nkom, 1, 2) AS reu, s.trest, 0 as var,
             s.reu as forreu, to_number(substr(op.oigu, 1, 1)) AS oborot,
             t.summa as summar, t.oper, t.dat_ink as dat, t.usl as usl_b, t.org as org_b, t.dopl
      from kwtp_day t, kart k, s_reu_trest s, oper op
      where t.lsk=k.lsk and k.reu=s.reu and t.oper=op.oper and t.priznak = 1
        and t.dat_ink between init.g_dt_start and init.g_dt_end) v, kart k
where v.dat = to_date(a, 'YYYYMMDD')
  and k.lsk = v.lsk
  and v.oborot = 1
group by v.usl,
         v.org,
         v.trest,
         v.reu,
         to_date(a, 'YYYYMMDD'),
         to_char(sysdate, 'DD/MM/YYYY HH24:MI'),
         v.var,
         v.forreu,
         k.kul,
         k.nd,
         decode(k.status, 1, 0, 1),
         v.dopl;
-- xxito14
-- способ получения периода оплаты (1-полыс, 0-остальные) ред.08.04.20
l_xxito14_period_var:=utils.get_int_param('XXITO14_PERIOD_VAR');

delete from xxito14_lsk t where t.dat = to_date(a, 'YYYYMMDD')
                            and t.mg=mg_;
insert into xxito14_lsk
(lsk, usl, org, summa, dat, var, status, dopl, oper, cd_tp, mg)
select v.lsk,
       v.usl,
       v.org,
       sum(v.summar) as summar,
       to_date(a, 'YYYYMMDD') as dat,
       v.var,
       decode(k.status, 1, 0, 1),
       v.dopl,
       v.oper,
       v.cd_tp,
       mg_
from (select t.lsk, t.usl, t.org, substr(t.nkom, 1, 2) AS reu, s2.trest, 0 as var,
             s2.reu as forreu, to_number(substr(op.oigu, 1, 1)) AS oborot,
             t.summa as summar, t.sum_distr, t.fk_distr,
             t.oper, t.dat_ink as dat, t.usl as usl_b, t.org as org_b,
             decode(l_xxito14_period_var,1,nvl(s.dopl,t.dopl),t.dopl) as dopl,
             t.priznak as cd_tp
      from kwtp_day t
               left join c_kwtp_mg s on s.id=t.kwtp_id
               join kart k on t.lsk=k.lsk
               join s_reu_trest s2 on k.reu=s2.reu
               join oper op on t.oper=op.oper
      where t.dat_ink = to_date(a, 'YYYYMMDD')) v, kart k
where k.lsk = v.lsk
  and v.oborot = 1
group by v.lsk,
         v.usl,
         v.org,
         v.fk_distr,
         to_date(a, 'YYYYMMDD'),
         v.var,
         decode(k.status, 1, 0, 1),
         v.dopl,
         v.oper,
         v.cd_tp;

delete from xxito14 t where t.dat = to_date(a, 'YYYYMMDD')
                        and t.mg=mg_;
insert into xxito14
(usl,
 org,
 summa,
 sum_distr,
 fk_distr,
 trest,
 reu,
 dat,
 var,
 forreu,
 kul,
 nd,
 status,
 dopl,
 oper,
 cd_tp,
 mg)
select v.usl,
       v.org,
       sum(v.summar) as summar,
       sum(v.sum_distr) as sum_distr,
       v.fk_distr,
       v.trest,
       v.reu,
       to_date(a, 'YYYYMMDD') as dat,
       v.var,
       v.forreu,
       k.kul,
       k.nd,
       decode(k.status, 1, 0, 1),
       v.dopl,
       v.oper,
       v.cd_tp,
       mg_
from (select t.lsk, t.usl, t.org, substr(t.nkom, 1, 2) AS reu, s2.trest, 0 as var,
             s2.reu as forreu, to_number(substr(op.oigu, 1, 1)) AS oborot,
             t.summa as summar, t.sum_distr, t.fk_distr,
             t.oper, t.dat_ink as dat, t.usl as usl_b, t.org as org_b,
             decode(l_xxito14_period_var,1,nvl(s.dopl,t.dopl),t.dopl) as dopl,
             t.priznak as cd_tp
      from kwtp_day t
               left join c_kwtp_mg s on s.id=t.kwtp_id
               join kart k on t.lsk=k.lsk
               join s_reu_trest s2 on k.reu=s2.reu
               join oper op on t.oper=op.oper
      where t.dat_ink = to_date(a, 'YYYYMMDD')) v, kart k
where k.lsk = v.lsk
  and v.oborot = 1
group by v.usl,
         v.org,
         v.trest,
         v.reu,
         v.fk_distr,
         to_date(a, 'YYYYMMDD'),
         to_char(sysdate, 'DD/MM/YYYY HH24:MI'),
         v.var,
         v.forreu,
         k.kul,
         k.nd,
         decode(k.status, 1, 0, 1),
         v.dopl,
         v.oper,
         v.cd_tp;
-- xxito10 детализация до кода организации
delete from xxito10 t where t.dat = to_date(a, 'YYYYMMDD')
                        and t.mg=mg_;
insert into xxito10
(usl, org, summa, trest, reu, dat, var, forreu, oborot, dopl, mg)
select v.usl,
       v.org,
       sum(v.summa),
       v.trest,
       v.reu,
       to_date(a, 'YYYYMMDD') as dat,
       v.var,
       v.forreu,
       v.oborot,
       dopl,
       mg_
from xxito11 v
where v.dat = to_date(a, 'YYYYMMDD') and
        v.mg=mg_
group by v.usl,
         v.org,
         v.trest,
         v.reu,
         to_date(a, 'YYYYMMDD'),
         to_char(sysdate, 'DD/MM/YYYY HH24:MI'),
         v.var,
         v.forreu,
         v.oborot,
         dopl;

logger.ins_period_rep('7', null, to_date(a, 'YYYYMMDD'), 0);
logger.ins_period_rep('2', null, to_date(a, 'YYYYMMDD'), 0);
logger.ins_period_rep('15', null, to_date(a, 'YYYYMMDD'), 0);
logger.ins_period_rep('17', null, to_date(a, 'YYYYMMDD'), 0);
logger.ins_period_rep('23', null, to_date(a, 'YYYYMMDD'), 0);
logger.ins_period_rep('35', null, to_date(a, 'YYYYMMDD'), 0);
logger.ins_period_rep('61', null, to_date(a, 'YYYYMMDD'), 0);
logger.ins_period_rep('65', null, to_date(a, 'YYYYMMDD'), 0);
commit;
end loop;

logger.log_(time_,
                'gen_opl_xito10day ' || to_char(dat1_) || ' ' ||
                to_char(dat2_));
admin.send_message('Сформирована оплата Ф.2.1.Ф.2.2.Ф.2.3. за текущий период, c ' ||
                       to_char(dat1_, 'DD/MM/YYYY') || ' по ' ||
                       to_char(dat2_, 'DD/MM/YYYY'));
end;

procedure gen_opl_xito10 is
    time_ date;
mg_ params.period%type;
l_xxito14_period_var number;
  begin
select period into mg_ from params p;

--ЗА МЕСЯЦ
--Оплата по предприятиям/трестам/услугам
time_ := sysdate;
delete from xxito11 t where t.mg = mg_;
insert into xxito11
(usl,
 oper,
 org,
 summa,
 trest,
 reu,
 mg,
 var,
 forreu,
 oborot,
 dopl,
 dat)
select v.usl,
       v.oper,
       v.org,
       sum(v.summar) as summar,
       v.trest,
       v.reu,
       mg_,
       v.var,
       v.forreu,
       v.oborot,
       v.dopl,
       v.dat
from (select t.lsk, t.usl, t.org, substr(t.nkom, 1, 2) AS reu, s.trest, 0 as var,
             s.reu as forreu, to_number(substr(op.oigu, 1, 1)) AS oborot,
             t.summa as summar, t.oper, t.dat_ink as dat, t.usl as usl_b, t.org as org_b, t.dopl
      from kwtp_day t, kart k, s_reu_trest s, oper op
      where t.lsk=k.lsk and k.reu=s.reu and t.oper=op.oper and t.priznak = 1
        and t.dat_ink between init.g_dt_start and init.g_dt_end) v
group by usl,
         oper,
         org,
         trest,
         reu,
         mg_,
         to_char(sysdate, 'DD/MM/YYYY HH24:MI'),
         var,
         forreu,
         oborot,
         dopl,
         dat;

delete from xxito12 t where t.mg = mg_;
insert into xxito12
(usl,
 org,
 summa,
 trest,
 reu,
 mg,
 var,
 forreu,
 kul,
 nd,
 status,
 dopl,
 dat)
select v.usl,
       v.org,
       sum(v.summar) as summar,
       v.trest,
       v.reu,
       mg_,
       v.var,
       k.reu,
       k.kul,
       k.nd,
       decode(k.status, 1, 0, 1),
       v.dopl,
       v.dat
from (select t.lsk, t.usl, t.org, substr(t.nkom, 1, 2) AS reu, s.trest, 0 as var,
             s.reu as forreu, to_number(substr(op.oigu, 1, 1)) AS oborot,
             t.summa as summar, t.oper, t.dat_ink as dat, t.usl as usl_b, t.org as org_b, t.dopl
      from kwtp_day t, kart k, s_reu_trest s, oper op
      where t.lsk=k.lsk and k.reu=s.reu and t.oper=op.oper and t.priznak = 1
        and t.dat_ink between init.g_dt_start and init.g_dt_end) v, kart k
where k.lsk = v.lsk
  and v.oborot = 1
group by v.usl,
         v.org,
         v.trest,
         v.reu,
         mg_,
         to_char(sysdate, 'DD/MM/YYYY HH24:MI'),
         v.var,
         k.reu,
         k.kul,
         k.nd,
         decode(k.status, 1, 0, 1),
         v.dopl,
         v.dat;

-- xxito14
-- способ получения периода оплаты (1-полыс, 0-остальные) ред.08.04.20
l_xxito14_period_var:=utils.get_int_param('XXITO14_PERIOD_VAR');

delete from xxito14_lsk t where t.mg = mg_;
insert into xxito14_lsk
(lsk, usl, org, summa, mg, var, status, dopl, oper, cd_tp, dat)
select k.lsk,
       v.usl,
       v.org,
       sum(v.summar) as summar,
       mg_ as mg,
       v.var,
       decode(k.status, 1, 0, 1),
       v.dopl,
       v.oper,
       v.cd_tp,
       v.dat
from (select t.lsk, t.usl, t.org, substr(t.nkom, 1, 2) AS reu, s2.trest, 0 as var,
             s2.reu as forreu, to_number(substr(op.oigu, 1, 1)) AS oborot,
             t.summa as summar, t.sum_distr, t.fk_distr,
             t.oper, t.dat_ink as dat, t.usl as usl_b, t.org as org_b,
             decode(l_xxito14_period_var,1,nvl(s.dopl,t.dopl),t.dopl) as dopl,
             t.priznak as cd_tp
      from kwtp_day t
               left join c_kwtp_mg s on s.id=t.kwtp_id
               join kart k on t.lsk=k.lsk
               join s_reu_trest s2 on k.reu=s2.reu
               join oper op on t.oper=op.oper
      where t.dat_ink between init.g_dt_start and init.g_dt_end) v, kart k
where k.lsk = v.lsk
  and v.oborot = 1
group by k.lsk,
         v.usl,
         v.org,
         v.var,
         decode(k.status, 1, 0, 1),
         v.dopl,
         v.oper,
         v.cd_tp,
         v.dat;

delete from xxito14 t where t.mg = mg_;
insert into xxito14
(usl,
 org,
 summa,
 sum_distr,
 fk_distr,
 trest,
 reu,
 mg,
 var,
 forreu,
 kul,
 nd,
 status,
 dopl,
 oper,
 cd_tp,
 dat)
select v.usl,
       v.org,
       sum(v.summar) as summar,
       sum(v.sum_distr) as sum_distr,
       v.fk_distr as fk_distr,
       v.trest,
       v.reu,
       mg_,
       v.var,
       k.reu,
       k.kul,
       k.nd,
       decode(k.status, 1, 0, 1),
       v.dopl,
       v.oper,
       v.cd_tp,
       v.dat
from (select t.lsk, t.usl, t.org, substr(t.nkom, 1, 2) AS reu, s2.trest, 0 as var,
             s2.reu as forreu, to_number(substr(op.oigu, 1, 1)) AS oborot,
             t.summa as summar, t.sum_distr, t.fk_distr,
             t.oper, t.dat_ink as dat, t.usl as usl_b, t.org as org_b,
             decode(l_xxito14_period_var,1,nvl(s.dopl,t.dopl),t.dopl) as dopl,
             t.priznak as cd_tp
      from kwtp_day t
               left join c_kwtp_mg s on s.id=t.kwtp_id
               join kart k on t.lsk=k.lsk
               join s_reu_trest s2 on k.reu=s2.reu
               join oper op on t.oper=op.oper
      where t.dat_ink between init.g_dt_start and init.g_dt_end) v, kart k
where k.lsk = v.lsk
  and v.oborot = 1
group by v.usl,
         v.org,
         v.trest,
         v.reu,
         v.fk_distr,
         mg_,
         to_char(sysdate, 'DD/MM/YYYY HH24:MI'),
         v.var,
         k.reu,
         k.kul,
         k.nd,
         decode(k.status, 1, 0, 1),
         v.dopl,
         v.oper,
         v.cd_tp,
         v.dat;

delete from xxito10 t where t.mg = mg_;
insert into xxito10
(usl, org, summa, trest, reu, mg, var, forreu, oborot, dopl, dat)
select v.usl,
       v.org,
       sum(v.summa),
       v.trest,
       v.reu,
       mg_,
       v.var,
       v.forreu,
       v.oborot,
       v.dopl,
       v.dat
from xxito11 v
where v.mg = mg_
group by v.usl,
         v.org,
         v.trest,
         v.reu,
         mg_,
         to_char(sysdate, 'DD/MM/YYYY HH24:MI'),
         v.var,
         v.forreu,
         v.oborot,
         v.dopl,
         v.dat;
logger.log_(time_, 'gen.gen_opl_xito10 ' || mg_);

logger.ins_period_rep('7', mg_, null, 0);
logger.ins_period_rep('2', mg_, null, 0);
logger.ins_period_rep('15', mg_, null, 0);
logger.ins_period_rep('17', mg_, null, 0);
logger.ins_period_rep('23', mg_, null, 0);
logger.ins_period_rep('35', mg_, null, 0);
logger.ins_period_rep('59', mg_, null, 0);
logger.ins_period_rep('61', mg_, null, 0);
logger.ins_period_rep('65', mg_, null, 0);
commit;
end;

/* убрал 25.03.21 - нафига здесь нужны льготы???  procedure gen_c_charges(lsk_ in kart.lsk%type) is
    cnt_ number;
    time_ date;
  begin
    time_ := sysdate;
    --формирование начисления за месяц.
    if lsk_ is null then
      --все лицевые с промежуточным коммитом, по каждому лицевому...
--      cnt_ := c_charges.gen_charges(null, null, null, null, 2, 1);
      c_charges.gen_chrg_all(0, null, null, null);
      --Загрузка субсидии по электроэнергии
      agent.load_subs_el;
    else
      --по одному лицевому, без коммита
      cnt_ := c_charges.gen_charges(lsk_, lsk_, null, null, 0, 0);
    end if;

    --льготы
    if lsk_ is null then
      execute immediate 'truncate table privs';
      insert into privs
        (lsk, summa, nomer, usl_id, lg_id, main)
        select c.lsk,
               sum(c.summa) as summa,
               c.kart_pr_id,
               c.usl,
               c.spk_id,
               c.main
          from c_charge c
         where c.type = 4
           and c.summa <> 0
         group by c.lsk, c.kart_pr_id, c.usl, c.spk_id, c.main;
    else
      delete from privs c
       where c.lsk=lsk_;
      insert into privs
        (lsk, summa, nomer, usl_id, lg_id, main)
        select c.lsk,
               sum(c.summa) as summa,
               c.kart_pr_id,
               c.usl,
               c.spk_id,
               c.main
          from c_charge c
         where c.type = 4
           and c.summa <> 0
           and c.lsk=lsk_
         group by c.lsk, c.kart_pr_id, c.usl, c.spk_id, c.main;
    end if;
    commit;
    if lsk_ is null then
    logger.log_(time_,
                'gen.gen_c_charges (Начисление по Л/C)');
    end if;
  end;
*/

procedure gen_lg
  -- Формирование льготников
   is
    type_otchet  constant number := 8; --тип возмещение по льготникам (Ф.8.1.)
type_otchet2 constant number := 11; --тип льготники для статистики (Ф.9.1.)
type_otchet3 constant number := 20; --тип возмещение по льготникам (Ф.7.5.)
time_ date;
mg_ params.period%type;
  begin
select period into mg_ from params p;
time_ := sysdate;

    --льготники для списка льготников
delete from xito_lg4 x where x.mg = mg_;
insert into xito_lg4
(lsk,
 reu,
 kul,
 nd,
 kw,
 trest,
 usl,
 lg_id,
 org,
 nomer,
 cnt_main,
 cnt,
 summa,
 mg)
select lsk,
       reu,
       kul,
       nd,
       kw,
       trest,
       usl,
       lg_id,
       org,
       nomer,
       cnt_main,
       cnt,
       summa,
       period
from v_gen_lg4;

delete from xito_lg3 x where x.mg = mg_;
--    /*+ INDEX (t PRIVS_I) */
insert into xito_lg3
(reu,
 trest,
 kul,
 nd,
 usl_id,
 lg_id,
 org_id,
 summa,
 cnt_main,
 cnt,
 mg)
select a.reu,
       a.trest,
       a.kul,
       a.nd,
       a.usl,
       a.lg_id,
       a.org,
       sum(a.summa) summa,
       sum(a.cnt_main) cnt_main,
       sum(a.cnt) cnt,
       a.period
from (select *
      from v_gen_lg3
      union all
      select * from v_gen_lg3_c) a
group by a.reu,
         a.trest,
         a.kul,
         a.nd,
         a.usl,
         a.lg_id,
         a.org,
         a.period;

delete from xito_lg2 x where x.mg = mg_;
/*+ INDEX (t PRIVS_I) */
insert into xito_lg2
(reu,
 trest,
 kul,
 nd,
 uslm_id,
 lg_id,
 org_id,
 summa,
 cnt_main,
 cnt,
 mg)
select a.reu,
       a.trest,
       a.kul,
       a.nd,
       a.uslm,
       a.lg_id,
       a.org,
       sum(summa),
       sum(cnt_main),
       sum(cnt),
       a.period
from (select *
      from v_gen_lg2
      union all
      select * from v_gen_lg2_c) a
group by a.reu,
         a.trest,
         a.kul,
         a.nd,
         a.uslm,
         a.lg_id,
         a.org,
         a.period;

delete from xito_lg1 x where x.mg = mg_;
insert into xito_lg1
(reu, trest, kul, nd, lg_id, summa, cnt_main, cnt, mg)
select reu,
       trest,
       kul,
       nd,
       lg_id,
       sum(summa) as summa,
       sum(cnt_main),
       sum(cnt),
       period
from (select s.reu,
             s.trest,
             k.kul,
             k.nd,
             x.lg_id,
             sum(x.summa) as summa,
             sum(x.cnt_main) as cnt_main,
             count(*) as cnt,
             p.period
      from (select lsk, lg_id, nomer, cnt_main, sum(summa) as summa
            from (select t.lsk,
                         t.lg_id,
                         t.nomer,
                         t.main as cnt_main,
                         t.summa
                  from privs t
                  where t.main not in (2)
                  union all
                  select t.lsk,
                         t.lg_id,
                         0 as nomer,
                         t.main as cnt_main,
                         t.summa
                  from t_corrects_lg t,
                       kart          e,
                       usl           u,
                       s_reu_trest   s,
                       params        p
                  where e.lsk = t.lsk
                    and t.usl = u.usl
                    and e.reu = s.reu
                    and t.mg = p.period)
            group by lsk, lg_id, nomer, cnt_main) x,
           kart k,
           s_reu_trest s,
           params p
      where x.lsk = k.lsk
        and k.reu = s.reu
      group by s.reu, s.trest, k.kul, k.nd, x.lg_id, p.period

      union all

      select s.reu,
             s.trest,
             k.kul,
             k.nd,
             x.lg_id,
             sum(x.summa) as summa,
             0 as cnt_main,
             0 as cnt,
             p.period
      from (select lsk, lg_id, nomer, cnt_main, sum(summa) as summa
            from (select t.lsk,
                         t.lg_id,
                         t.nomer,
                         t.main as cnt_main,
                         t.summa
                  from privs t
                  where t.main in (2))
            group by lsk, lg_id, nomer, cnt_main) x,
           kart k,
           s_reu_trest s,
           params p
      where x.lsk = k.lsk
        and k.reu = s.reu
      group by s.reu, s.trest, k.kul, k.nd, x.lg_id, p.period)
group by reu, trest, kul, nd, lg_id, period;

delete from period_reports p
where p.id = type_otchet
  and p.mg = mg_; --обновляем период для отчета
insert into period_reports (id, mg) values (type_otchet, mg_);

delete from period_reports p
where p.id = type_otchet2
  and p.mg = mg_; --обновляем период для отчета
insert into period_reports (id, mg) values (type_otchet2, mg_);

delete from period_reports p
where p.id = type_otchet3
  and p.mg = mg_; --обновляем период для отчета
insert into period_reports (id, mg) values (type_otchet3, mg_);
commit;
logger.log_(time_, 'gen.gen_lg ' || mg_);
end gen_lg;

-- загрузка входящего сальдо по внешним лиц.счетам, где установлен формат обмена, с получением вх.сальдо (Кис.ФКР)
procedure load_ext_saldo(p_tp in number default 0 -- Загружать сальдо вн.лиц.счетов, (0-как на начало месяца (ФКР Кис), 1-как за прошлый период (ФКР Полыс.))
  ) is
    l_mg    params.period%type;
l_mg_back    params.period%type;
  begin
select period, period3 into l_mg, l_mg_back from v_params p;
delete from saldo_usl t where t.mg=l_mg
                          and exists (select * from kart k join kart_ext e on k.lsk=e.lsk
                                                           join t_org o on k.reu=o.reu and o.is_exchange_ext=1 and o.ext_lsk_format_tp=1
                                      where e.lsk=t.lsk);
-- вх.сальдо на тек.период
insert into saldo_usl(lsk,
                      usl,
                      org,
                      summa,
                      mg,
                      uslm)
select k.lsk, o.usl_for_create_ext_lsk as usl, o.id as org,
       decode(p_tp, 0, e.insal, e.outsal) as summa, l_mg as mg, u.uslm
from kart k join kart_ext e on k.lsk=e.lsk and e.v=1-- только по действующим лиц.счетам, ред. 04.08.21
            join t_org o on k.reu=o.reu and o.is_exchange_ext=1 and o.ext_lsk_format_tp=1
            join usl u on o.usl_for_create_ext_lsk=u.usl;

delete from c_chargepay2 t where t.mgfrom=l_mg_back and t.mgto=l_mg_back
                             and exists (select * from kart k join kart_ext e on k.lsk=e.lsk
                                                              join t_org o on k.reu=o.reu and o.is_exchange_ext=1 and o.ext_lsk_format_tp=1
                                         where e.lsk=t.lsk);
-- движение (прошлый период)
insert into c_chargepay2(lsk,
                         type,
                         summa,
                         mg,
                         mgfrom,
                         mgto)
select k.lsk, 0 as type,
       e.outsal as summa, -- исх.сальдо, так как принимать оплату потом по нему
       l_mg_back as mg, l_mg_back as mgfrom, l_mg_back as mgto
from kart k join kart_ext e on k.lsk=e.lsk and e.v=1-- только по действующим лиц.счетам, ред. 04.08.21
            join t_org o on k.reu=o.reu and o.is_exchange_ext=1 and o.ext_lsk_format_tp=1
            join usl u on o.usl_for_create_ext_lsk=u.usl;

logger.log_(null, 'gen.load_ext_saldo');
end;

procedure gen_saldo(lsk_ in kart.lsk%type)
  --Формирование исходящего сальдо по концу месяца mg_ - текущий месяц, по Л/С
    --Внимание! следующий код при выполнении формирования за месяц может вести к нестабильности
    --получения счетов пользователями из за выполнения truncate-ов
   is
    stmt   varchar2(2000);
mg_    params.period%type;
mg1_   params.period%type;
init_mg_   params.period%type;
old_mg_   params.period%type;
cnt_ number;
time1_ date;
time_ date;
  begin

    -- вначале провести корректировки в kwtp_day
c_gen_pay.dist_pay_del_corr(lsk_);
c_gen_pay.dist_pay_add_corr(var_ => 0, p_lsk=>lsk_);

select period into mg_ from params p;

--Вычисляем следующий месяц
mg1_ := to_char(add_months(to_date(mg_ || '01', 'YYYYMMDD'), 1),
                    'YYYYMM');
    --Вычисляем прeдыдущий месяц
old_mg_ := to_char(add_months(to_date(mg_ || '01', 'YYYYMMDD'), -1),
                    'YYYYMM');

if lsk_ is null then
      time1_ := sysdate;
      --Вычисляем после-следующий месяц, для partitions
trunc_part('saldo_usl', mg1_);
--Подготовка временных таблиц...
execute immediate 'TRUNCATE TABLE t_charges_for_saldo';
execute immediate 'TRUNCATE TABLE t_changes_for_saldo';
execute immediate 'TRUNCATE TABLE t_payment_for_saldo';
execute immediate 'TRUNCATE TABLE t_privs_for_saldo';
execute immediate 'TRUNCATE TABLE t_penya_for_saldo';
execute immediate 'TRUNCATE TABLE t_subsidii_for_saldo';
else
delete from saldo_usl c
where mg = mg1_
  and c.lsk=lsk_;
delete from t_charges_for_saldo c
where c.lsk=lsk_;
delete from t_changes_for_saldo c
where c.lsk=lsk_;
delete from t_payment_for_saldo c
where c.lsk=lsk_;
delete from t_privs_for_saldo c
where c.lsk=lsk_;
delete from t_penya_for_saldo c
where c.lsk=lsk_;
delete from t_subsidii_for_saldo c
where c.lsk=lsk_;
/*      делать не здесь!!! -- загрузка входящего сальдо по внешним лиц.счетам, где установлен формат обмена, с получением вх.сальдо (Кис.ФКР)
      delete from saldo_usl t where t.mg=mg_
             and exists (select * from kart k join kart_ext e on k.lsk=e.lsk
                                join t_org o on k.reu=o.reu and o.is_exchange_ext=1 and o.ext_lsk_format_tp=1
                         where e.lsk=t.lsk and k.lsk=lsk_)
             and t.lsk=lsk_;
      insert into saldo_usl(lsk,
                            usl,
                            org,
                            summa,
                            mg,
                            uslm)
      select k.lsk, o.usl_for_create_ext_lsk as usl, o.id as org,
             e.insal, mg_ as mg, u.uslm
       from kart k join kart_ext e on k.lsk=e.lsk and k.lsk=lsk_
                                join t_org o on k.reu=o.reu and o.is_exchange_ext=1 and o.ext_lsk_format_tp=1
                                join usl u on o.usl_for_create_ext_lsk=u.usl;                                                     */
end if;



--Текущее начисление
if lsk_ is null then
      time_ := sysdate;
insert into t_charges_for_saldo
(lsk, summa, org, usl)
select
    p.lsk, sum(p.summa) as summa, t.fk_org2, p.usl
from c_charge p, t_org t, params m
where p.type = 1
  and p.org=t.id
group by p.lsk, p.usl, t.fk_org2;
commit;
logger.log_(time_, 'INSERT INTO t_charges_for_saldo (USL)');
else
insert into t_charges_for_saldo
(lsk, summa, org, usl)
select
    p.lsk, sum(p.summa) as summa, t.fk_org2, p.usl
from c_charge p, t_org t, params m
where p.type = 1
  and p.lsk=lsk_
  and p.org=t.id
group by p.lsk, p.usl, t.fk_org2;
end if;

--## надо быть готовым к отсуствию орг в c_change при выполнении перерасчетов суммами
--
--перерасчеты
if lsk_ is null then
      time_ := sysdate;
      --1 часть - для формирования сальдо
insert into t_changes_for_saldo
(lsk, summa, org, usl, type)
select t.lsk, t.summa, t.org, t.usl, t.type
from v_changes_for_saldo t;
--2 часть - для формирования задолжности по периодам
commit;
logger.log_(time_, 'INSERT INTO t_changes_for_saldo (USL)');
else
insert into t_changes_for_saldo
(lsk, summa, org, usl, type)
select lsk, sum(summa) as summa, org, usl, type
from (
         select /*+ INDEX (k A_NABOR2_I)*/
             p.lsk, p.summa, p.usl, t.fk_org2 as org, decode(p.type,1,1,2,1,3,3,0) as type
         from a_nabor2 k, c_change p, t_org t, params m
         where k.lsk = p.lsk and k.lsk=lsk_
           and p.mg2 between k.mgFrom and k.mgTo
           and k.usl = p.usl
           and k.org=t.id
           and p.org is null  -- где не указан код орг и старые периоды
           and exists             --и где найдена услуга в архивном справочнике
             (select /*+ INDEX (n A_NABOR2_I)*/* from a_nabor2 n where n.lsk=k.lsk and p.mg2 between n.mgFrom and n.mgTo
                                                                   and n.usl=k.usl)
           and p.mg2 < m.period
           and to_char(p.dtek, 'YYYYMM') = m.period
         union all
         select
             p.lsk, p.summa, p.usl, t.fk_org2, decode(p.type,1,1,2,1,3,3,0) as type
         from nabor k, c_change p, t_org t, params m
         where k.lsk = p.lsk and k.lsk=lsk_
           and k.usl = p.usl
           and k.org=t.id     --не должно быть такого, так как не понятно где брать орг
           and p.org is null  -- где не указан код орг и старые периоды
           and not exists             --и где НЕ найдена услуга в архивном справочнике
             (select /*+ INDEX (n A_NABOR2_I)*/* from a_nabor2 n where n.lsk=k.lsk and p.mg2 between n.mgFrom and n.mgTo and n.usl=k.usl)
           and p.mg2 < m.period
           and to_char(p.dtek, 'YYYYMM') = m.period
         union all
         select
             p.lsk, p.summa, p.usl, t.fk_org2, decode(p.type,1,1,2,1,3,3,0) as type
         from nabor k, c_change p, t_org t, params m
         where k.lsk = p.lsk and k.lsk=lsk_
           and k.usl = p.usl
           and k.org=t.id
           and p.org is null  -- где не указан код орг и новые периоды
           and p.mg2 >= m.period
           and p.dtek between k.dt1 and k.dt2
           and to_char(p.dtek, 'YYYYMM') = m.period
         union all
         select
             p.lsk, p.summa, p.usl, nvl(t.fk_org2, 0) as org, decode(p.type,1,1,2,1,3,3,0) as type
         from kart r, c_change p, t_org t, params m
         where r.lsk = p.lsk and r.lsk=lsk_
           and p.org=t.id
           and p.org is not null  -- где указан код орг и не важно какой период
           and to_char(p.dtek, 'YYYYMM') = m.period)
group by lsk, org, usl, type;
end if;

--оплата
if lsk_ is null then
      time_ := sysdate;
insert into t_payment_for_saldo
(lsk, summa, org, usl)
select t.lsk, sum(t.summa) as summa, t.org, t.usl
from kwtp_day t
where t.priznak=1 and t.dat_ink between init.g_dt_start and init.g_dt_end
group by t.lsk, t.org, t.usl;
commit;
logger.log_(time_, 'INSERT INTO t_payment_for_saldo (USL)');
else
insert into t_payment_for_saldo
(lsk, summa, org, usl)
select t.lsk, sum(t.summa) as summa, t.org, t.usl
from kwtp_day t
where t.priznak=1 and t.lsk=lsk_ and t.dat_ink between init.g_dt_start and init.g_dt_end
group by t.lsk, t.org, t.usl;
end if;

--льготы
if lsk_ is null then
      time_ := sysdate;
insert into t_privs_for_saldo
(lsk, summa, org, usl)
select lsk, summa, org, usl from v_privs_for_saldo;
commit;
logger.log_(time_, 'INSERT INTO t_privs_for_saldo (USL)');
else
insert into t_privs_for_saldo
(lsk, summa, org, usl)
select lsk, summa, org, usl
from v_privs_for_saldo c
where c.lsk=lsk_;
end if;

--пеня
if lsk_ is null then
      time_ := sysdate;
insert into t_penya_for_saldo
(lsk, summa, org, usl)
select p.lsk, sum(p.summa) as summa, p.org, p.usl
from kwtp_day p
where p.priznak=0 and p.dat_ink between init.g_dt_start and init.g_dt_end
group by p.lsk, p.usl, p.org;
commit;
logger.log_(time_, 'INSERT INTO t_penya_for_saldo (USL)');
else
insert into t_penya_for_saldo
(lsk, summa, org, usl)
select p.lsk, sum(p.summa) as summa, p.org, p.usl
from kwtp_day p
where p.priznak=0 and p.lsk=lsk_
  and p.dat_ink between init.g_dt_start and init.g_dt_end
group by p.lsk, p.usl, p.org;
end if;

if lsk_ is null then
      time_ := sysdate;
      --Формирование исходящего сальдо USL
      --учитываем субсидию в оборотах
insert into saldo_usl
(lsk, org, usl, uslm, summa, mg)
select t.lsk,
       t.org,
       t.usl,
       u.uslm,
       sum(t.summa) as summa,
       mg1_ as mg
from (select lsk, org, usl, summa
      from saldo_usl
      where mg = mg_
      union all
      select c.lsk, c.org, c.usl, c.summa
      from t_charges_for_saldo c
      union all
      select c.lsk, c.org, c.usl, c.summa
      from t_changes_for_saldo c
      union all
      select c.lsk, c.org, c.usl, c.summa * -1
      from t_privs_for_saldo c
      union all
      select c.lsk, c.org, c.usl, c.summa * -1
      from t_subsidii_for_saldo c
      where c.usl <> '024' --эл.эн.субс не отображается на сальдо
      union all
      select c.lsk, c.org, c.usl, c.summa * -1
      from t_payment_for_saldo c) t,
     kart k,
     usl u
where k.lsk = t.lsk
  and t.usl = u.usl
group by t.lsk, t.org, t.usl, u.uslm
having sum(t.summa)<>0; --исключить нулевое сальдо

if utils.get_int_param('GEN_CHK_C_DEB_USL') = 1 then
       --по-периодный способ распределения оплаты
       --(сформировать задолжность)
       c_dist_pay.gen_deb_usl_all;
end if;
logger.log_(time1_, 'gen_saldo');
else
insert into saldo_usl
(lsk, org, usl, uslm, summa, mg)
select t.lsk,
       t.org,
       t.usl,
       u.uslm,
       sum(t.summa) as summa,
       mg1_ as mg
from (select lsk, org, usl, summa
      from saldo_usl
      where mg = mg_
      union all
      select c.lsk, c.org, c.usl, c.summa
      from t_charges_for_saldo c
      union all
      select c.lsk, c.org, c.usl, c.summa
      from t_changes_for_saldo c
      union all
      select c.lsk, c.org, c.usl, c.summa * -1
      from t_privs_for_saldo c
      union all
      select c.lsk, c.org, c.usl, c.summa * -1
      from t_subsidii_for_saldo c
      where c.usl <> '024' --эл.эн.субс не отображается на сальдо
      union all
      select c.lsk, c.org, c.usl, c.summa * -1
      from t_payment_for_saldo c) t,
     usl u
where t.usl = u.usl
  and t.lsk = lsk_
group by t.lsk, t.org, t.usl, u.uslm
having sum(t.summa)<>0; --исключить нулевое сальдо
end if;
end gen_saldo;

procedure gen_saldo_houses
  -- Формирование сальдо по домам mg_ - текущий месяц
    -- ВЫПОЛНЯТЬ ПОСЛЕ ФОРМИРОВАНИЯ GEN_PREP_OPL!!!
   is
    stmt varchar2(5000);
mg1_ varchar2(6);
time_ date;
mg_ params.period%type;
  begin
select period into mg_ from params p;
time_ := sysdate;
    --Вычисляем следующий месяц
mg1_ := to_char(add_months(to_date(mg_ || '01', 'YYYYMMDD'), 1),
                    'YYYYMM');

prep_template_tree_objects; -- формирование объектов для меню OLAP, воткнул пока сюда.ред.29.07.2019
    --Добавляем во временную таблицу
execute immediate 'TRUNCATE TABLE t_saldo_lsk';
insert into t_saldo_lsk
(lsk, org, usl, uslm, status)
select distinct k.lsk, a.org, a.usl, u.uslm, k.status
from (select a.lsk, a.org, a.usl
      from saldo_usl a, v_params v
      where a.mg = v.period1
      union all
      select t.lsk, t.org, t.usl
      from t_charges_for_saldo t
      union all
      select t.lsk, t.org, t.usl
      from t_chpenya_for_saldo t -- текущая пеня, по услугам, готовится в пакете C_PENYA
      union all
      select t.lsk, t.org, t.usl
      from t_changes_for_saldo t
      union all
      select t.lsk, t.org, t.usl
      from t_subsidii_for_saldo t
      union all
      select t.lsk, t.org, t.usl
      from t_payment_for_saldo t
      union all
      select t.lsk, t.org, t.usl
      from t_privs_for_saldo t
      union all
      select t.lsk, t.org, t.usl from t_penya_for_saldo t
      union all
      select t.lsk, t.org, t.usl from xitog3_lsk t, v_params v
      where t.mg = v.period3 -- предыдущий период
     ) a,
     kart k,
     usl u
where k.lsk = a.lsk
  and a.usl = u.usl;
commit;

delete from xitog3_lsk t where t.mg = mg_;
insert into xitog3_lsk
(lsk,
 org,
 usl,
 uslm,
 status,
 indebet,
 inkredit,
 charges,
 pinsal,   -- входящее сальдо по пене
 poutsal, -- исходящее сальдо по пене
 pcur,     -- текущее начисление пени
 changes,
 ch_full,
 changes2,
 changes3,
 subsid,
 privs,
 payment,
 pn,
 outdebet,
 outkredit,
 mg)
select t.lsk,
       t.org,
       t.usl,
       t.uslm,
       nvl(t.status, 0),
       sum(a.summa) as indebet, -- вх.дебет
       sum(b.summa) as inkredit,-- вх.кредит
       sum(nvl(e.summa, 0) + nvl(f.summa, 0) + nvl(w.summa, 0)  + nvl(w2.summa, 0)-
           nvl(o.summa, 0) - nvl(g.summa, 0)) as charges, -- начисление
       sum(m.summa) as pinsal, -- входящее сальдо по пене
       sum(nvl(m.summa, 0) + nvl(p.summa, 0) - nvl(j.summa, 0)) as poutsal, -- исходящее сальдо по пене
       sum(p.summa) as pcur, -- текущее начисление пени
       sum(f.summa) as changes,  -- скидки
       sum(e.summa) as ch_full, -- полное начисление
       sum(w.summa) as changes2, -- доборы, возвраты
       sum(w2.summa) as changes3, -- корректировки сальдо перерасчетами
       sum(g.summa) as subsid, -- субсидии
       sum(o.summa) as privs, -- льготы
       sum(h.summa) as payment, -- оплата
       sum(j.summa) as pn, -- оплаченная пеня
       sum(k.summa) as outdebet, -- исх.дебет
       sum(l.summa) as outkredit, -- исх.кредит
       mg_ as mg
from t_saldo_lsk t,
     (select t.lsk, t.org, t.usl, t.uslm, sum(summa) as summa
      from saldo_usl t
      where sign(summa) >= 0
        and mg = mg_
      group by t.lsk, t.org, t.usl, t.uslm) a,
     (select t.lsk, t.org, t.usl, t.uslm, sum(summa) as summa
      from saldo_usl t
      where sign(summa) < 0
        and mg = mg_
      group by t.lsk, t.org, t.usl, t.uslm) b,
     (select t.lsk, t.org, t.usl, t.uslm, sum(summa) as summa
      from saldo_usl t
      where sign(summa) >= 0
        and mg = mg1_
      group by t.lsk, t.org, t.usl, t.uslm) k,
     (select t.lsk, t.org, t.usl, t.uslm, sum(summa) as summa
      from saldo_usl t
      where sign(summa) < 0
        and mg = mg1_
      group by t.lsk, t.org, t.usl, t.uslm) l,
     t_charges_for_saldo e,
     t_privs_for_saldo o,
     (select t.lsk, t.org, t.usl, t.summa
      from t_changes_for_saldo t
      where t.type in (0)) f, -- скидки
     (select t.lsk, t.org, t.usl, t.summa
      from t_changes_for_saldo t
      where t.type in (1)) w, -- доборы, возвраты
     (select t.lsk, t.org, t.usl, t.summa
      from t_changes_for_saldo t
      where t.type in (3)) w2, -- корректировки сальдо перерасчетами
     t_subsidii_for_saldo g,
     t_payment_for_saldo h,
     t_penya_for_saldo j, -- оплаченная пеня
     (select t.lsk, t.org, t.usl,
             sum(t.poutsal) as summa from xitog3_lsk t, v_params v
      where t.mg = v.period3
      group by t.lsk, t.org, t.usl) m, --вх.сальдо по пене
     t_chpenya_for_saldo p -- текушая пеня
where t.lsk = a.lsk(+)
  and t.org = a.org(+)
  and t.usl = a.usl(+)

  and t.lsk = b.lsk(+)
  and t.org = b.org(+)
  and t.usl = b.usl(+)

  and t.lsk = k.lsk(+)
  and t.org = k.org(+)
  and t.usl = k.usl(+)

  and t.lsk = l.lsk(+)
  and t.org = l.org(+)
  and t.usl = l.usl(+)

  and t.lsk = e.lsk(+)
  and t.org = e.org(+)
  and t.usl = e.usl(+)

  and t.lsk = o.lsk(+)
  and t.org = o.org(+)
  and t.usl = o.usl(+)

  and t.lsk = f.lsk(+)
  and t.org = f.org(+)
  and t.usl = f.usl(+)

  and t.lsk = w.lsk(+)
  and t.org = w.org(+)
  and t.usl = w.usl(+)

  and t.lsk = w2.lsk(+)
  and t.org = w2.org(+)
  and t.usl = w2.usl(+)

  and t.lsk = g.lsk(+)
  and t.org = g.org(+)
  and t.usl = g.usl(+)

  and t.lsk = h.lsk(+)
  and t.org = h.org(+)
  and t.usl = h.usl(+)

  and t.lsk = j.lsk(+)
  and t.org = j.org(+)
  and t.usl = j.usl(+)

  and t.lsk = p.lsk(+)
  and t.org = p.org(+)
  and t.usl = p.usl(+)

  and t.lsk = m.lsk(+)
  and t.org = m.org(+)
  and t.usl = m.usl(+)

group by t.lsk, t.org, t.usl, t.uslm, nvl(t.status, 0)
having
        nvl(sum(a.summa), 0) <> 0 or
        nvl(sum(b.summa), 0) <> 0 or
        sum(nvl(e.summa, 0) + nvl(f.summa, 0) + nvl(w.summa, 0)  + nvl(w2.summa, 0)-
            nvl(o.summa, 0) - nvl(g.summa, 0)) <> 0 or
        sum(nvl(p.summa, 0)) <> 0 or
        sum(nvl(f.summa, 0)) <> 0 or
        sum(nvl(e.summa, 0)) <> 0 or
        sum(nvl(w.summa, 0)) <> 0 or
        sum(nvl(w2.summa, 0)) <> 0 or
        nvl(sum(g.summa), 0) <> 0 or
        nvl(sum(o.summa), 0) <> 0 or
        nvl(sum(h.summa), 0) <> 0 or
        nvl(sum(j.summa), 0) <> 0 or
        nvl(sum(k.summa), 0) <> 0 or
        nvl(sum(l.summa), 0) <> 0 or
        nvl(sum(m.summa), 0) <> 0;


-- выбираем уникальные дома + орг. + усл. для формирования отчетности по сальдо
delete from t_saldo_lsk2;
insert into t_saldo_lsk2
(lsk, org, usl)
select distinct t.lsk, t.org, t.usl from xitog3_lsk t;

commit;

delete from xitog3 where mg = mg_;
insert into xitog3 t
(reu,
    trest,
    kul,
    nd,
    org,
    usl,
    uslm,
    status,
    indebet,
    inkredit,
    charges,
    pinsal,
    poutsal,
    pcur,
    changes,
    ch_full,
    changes2,
    changes3,
    subsid,
    privs,
    payment,
    pn,
    outdebet,
    outkredit,
    fk_lsk_tp,
    mg)
select s.reu,
       s.trest,
       k.kul,
       k.nd,
       x.org,
       x.usl,
       x.uslm,
       k.status,
       sum(x.indebet),
       sum(x.inkredit),
       sum(x.charges),
       sum(x.pinsal),
       sum(x.poutsal),
       sum(x.pcur),
       sum(x.changes),
       sum(x.ch_full),
       sum(x.changes2),
       sum(x.changes3),
       sum(x.subsid),
       sum(x.privs),
       sum(x.payment),
       sum(x.pn),
       sum(x.outdebet),
       sum(x.outkredit),
       k.fk_tp,
       x.mg
from xitog3_lsk x, kart k, s_reu_trest s
where x.mg = mg_
  and x.lsk = k.lsk
  and k.reu = s.reu
group by s.reu,
         s.trest,
         k.kul,
         k.nd,
         x.org,
         x.usl,
         x.uslm,
         k.status,
         x.mg,
         k.fk_tp;

-- выбираем уникальные дома + орг. + усл. для формирования отчетности по сальдо
delete from t_saldo_reu_kul_nd_st;
insert into t_saldo_reu_kul_nd_st
(reu, kul, nd, status, org, usl, fk_lsk_tp)
select distinct t.reu, t.kul, t.nd, t.status, t.org, t.usl, nvl(t.fk_lsk_tp,0) as fk_lsk_tp
from xitog3 t;

logger.ins_period_rep('1', mg_, null, 0);
logger.ins_period_rep('21', mg_, null, 0);
logger.ins_period_rep('14', mg_, null, 0);
logger.ins_period_rep('81', mg_, null, 0);
logger.ins_period_rep('89', mg_, null, 0);
logger.ins_period_rep('90', mg_, null, 0);
logger.ins_period_rep('100', mg_, null, 0);

p_tree_adr.tree_adr_load(); -- здесь сделал обновление объектов для перерасчета todo перенести?
commit;
logger.log_(time_, 'gen_saldo_houses');
end gen_saldo_houses;

procedure gen_xito13 is
    type_otchet constant number := 19; --начисление по услугам
time_ date;
  begin
time_ := sysdate;
    --ЗА МЕСЯЦ
    --Начисление по услугам
delete from xito13 x where x.mg = (select p.period from params p);
insert into xito13
(usl, uslm, reu, kul, nd, trest, mg, summa)
select c.usl,
       u.uslm,
       k.reu,
       k.kul,
       k.nd,
       s.trest,
       p.period,
       sum(c.summa)
from c_charge c, kart k, s_reu_trest s, params p, usl u
where c.lsk = k.lsk
  and c.type = 1
  and k.reu = s.reu
  and c.usl = u.usl
group by c.usl, u.uslm, k.reu, k.kul, k.nd, s.trest, p.period;

delete from period_reports p
where p.id = type_otchet
  and p.mg = (select p.period from params p); --обновляем период для отчета
insert into period_reports
(id, mg)
values
    (type_otchet, (select p.period from params p));
commit;
logger.log_(time_, 'gen.gen_xito13');
end;

procedure gen_debits_lsk_month(dat_ in date) is
    mg_  params.period%type;
mg1_ params.period%type;
mg3_ params.period%type;
var_ params.kan_var%type;
time_ date;
  begin
time_ := sysdate;
    --задолжники по лицевым
    --формировать после архивных карточек
    --выполнять ДО перехода, выгружать, - когда угодно
select period into mg_ from params;
--Вычисляем следующий месяц
mg1_ := to_char(add_months(to_date(mg_ || '01', 'YYYYMMDD'), 1),
                    'YYYYMM');
    --Вычисляем передыдущ месяц
mg3_ := to_char(add_months(to_date(mg_ || '01', 'YYYYMMDD'), -1),
                    'YYYYMM');
select nvl(p.kan_var, 0) into var_ from params p;
if dat_ is not null then
delete from debits_lsk_month d where d.dat = dat_;
else
delete from debits_lsk_month d where d.mg = mg_;
end if;
if var_ = 0 then
      -- раньше пользовался Киселевск
insert into debits_lsk_month
(lsk, reu, kul, name, nd, kw, fio, status, opl, cnt_month, dolg,
 nachisl, penya, payment, pay_pen, mg, dat)
select a.lsk, a.reu, a.kul, s.name, a.nd, a.kw, a.fio, a.status,
       a.opl,
       round(decode(b.summa, 0, 0, a.dolg / b.summa), 0) as cnt_month,
       a.dolg, b.summa as nachisl, a.penya, c.summa, a.pay_pen, case when dat_ is null then mg_
                                                                     else null end,
       dat_ --1 - часть - долги по текущему фонду
from (select t.k_lsk_id, t.lsk, t.reu, t.kul, t.nd,
             t.kw, t.fio, t.status, t.opl, s.dolg as dolg,
             e.penya as penya, f.pay_pen
      from arch_kart t,
           (select lsk, sum(summa) as dolg
            from saldo_usl
            where usl not in (select u.usl_id from usl_excl u)
              and mg = mg1_
            group by lsk) s,
           (select lsk, sum(penya) as penya
            from a_penya
            where mg = mg_
            group by lsk) e,
           (select lsk, sum(t.penya) as pay_pen
            from a_kwtp_mg t
            where mg = mg_
            group by lsk) f, v_lsk_tp tp
      where t.mg = mg_
        and t.lsk = s.lsk(+)
        and t.lsk = e.lsk(+)
        and t.lsk = f.lsk(+)
        and t.fk_tp=tp.id and tp.cd in ('LSK_TP_MAIN','LSK_TP_RSO')--Кис.просили исключить счета капрем 30.07.2015, но попросили включить РСО 08.07.2019
        and t.psch <> 8) a,
     (select k.k_lsk_id, sum(d.summa_it) as summa
      from arch_kart k, arch_charges d, usl u, v_lsk_tp tp
      where d.mg = mg_
        and k.mg = mg_
        and k.lsk = d.lsk
        and d.usl_id=u.usl
        and d.usl_id not in (select u.usl_id from usl_excl u)
        and k.fk_tp=tp.id and tp.cd in ('LSK_TP_MAIN','LSK_TP_RSO')--Кис.просили исключить счета капрем 30.07.2015, но попросили включить РСО 08.07.2019
        --and u.cd not in ('кап.' , 'кап/св.нор') --Кис.просили исключить капрем 30.07.2015
        and k.psch <> 8
      group by k.k_lsk_id) b,
     (select k.lsk, sum(d.summa) as summa
      from arch_kart k, a_kwtp_mg d, v_lsk_tp tp
      where d.mg = mg_
        and k.mg = mg_
        and k.lsk = d.lsk
        and k.fk_tp=tp.id and tp.cd in ('LSK_TP_MAIN','LSK_TP_RSO')--Кис.просили исключить счета капрем 30.07.2015, но попросили включить РСО 08.07.2019
      group by k.lsk) c, spul s
where a.k_lsk_id = b.k_lsk_id(+)
  and a.kul = s.id
  and a.lsk = c.lsk(+)
  and round(decode(b.summa, 0, 0, a.dolg / b.summa), 0) > 0
union all
select a.lsk, a.reu, a.kul, s.name, a.nd, a.kw, a.fio, a.status,
       a.opl,
       case when a.dolg > 0 then 2 else 0 end as cnt_month,
       a.dolg, b.summa as nachisl, a.penya, c.summa, a.pay_pen, case when dat_ is null then mg_
                                                                     else null end,
       dat_--2 - часть - долги по закрытому фонду
from (select t.lsk, t.k_lsk_id, t.reu, t.kul, t.nd,
             t.kw, t.fio, t.status, t.opl, s.dolg as dolg,
             e.penya as penya, f.pay_pen
      from arch_kart t,
           (select lsk, sum(summa) as dolg
            from saldo_usl
            where usl not in (select u.usl_id from usl_excl u)
              and mg = mg1_
            group by lsk) s,
           (select lsk, sum(penya) as penya
            from a_penya
            where mg = mg_
            group by lsk) e,
           (select lsk, sum(t.penya) as pay_pen
            from a_kwtp_mg t
            where mg = mg_
            group by lsk) f, v_lsk_tp tp
      where t.mg = mg_
        and t.lsk = s.lsk(+)
        and t.lsk = e.lsk(+)
        and t.lsk = f.lsk(+)
        and t.fk_tp=tp.id and tp.cd in ('LSK_TP_MAIN')
        and t.psch = 8) a,
     (select k.k_lsk_id, sum(d.summa_it) as summa
      from arch_kart k, arch_charges d, usl u, v_lsk_tp tp
      where d.mg = mg_
        and k.mg = mg_
        and k.lsk = d.lsk
        and d.usl_id=u.usl
        and d.usl_id not in (select u.usl_id from usl_excl u)
        and k.fk_tp=tp.id and tp.cd in ('LSK_TP_MAIN','LSK_TP_RSO')--Кис.просили исключить счета капрем 30.07.2015, но попросили включить РСО 08.07.2019
        --and u.cd not in ('кап.' , 'кап/св.нор') --Кис.просили исключить капрем 30.07.2015
        and k.psch <> 8
      group by k.k_lsk_id) b,
     (select k.lsk, sum(d.summa) as summa
      from arch_kart k, a_kwtp_mg d, v_lsk_tp tp
      where d.mg = mg_
        and k.mg = mg_
        and k.fk_tp=tp.id and tp.cd in ('LSK_TP_MAIN','LSK_TP_RSO')--Кис.просили исключить счета капрем 30.07.2015, но попросили включить РСО 08.07.2019
        and k.lsk = d.lsk
      group by k.lsk) c, spul s
where a.k_lsk_id = b.k_lsk_id(+)
  and a.kul = s.id
  and a.lsk = c.lsk(+)
  and a.dolg > 0;
elsif var_=1 then
      --ТСЖ
insert into debits_lsk_month
(lsk, reu, kul, name, nd, kw, fk_deb_org, fio, status, opl,
 cnt_month, dolg, cnt_month2, dolg2, nachisl, penya, payment, mg, dat)
select a.lsk, a.reu, a.kul, s.name, a.nd, a.kw, a.fk_deb_org, a.fio,
       a.status, a.opl,
       case when a.psch not in (8,9) and nvl(b.summa,0) <> 0 then
                a.cnt_month
            when a.psch in (8,9) and nvl(b.summa,0) = 0 and nvl(a.dolg,0)  > 0 --по закрытым например
                then 2 --ставим 2 мес.задолжности  (поправил 2 мес. для кис 07.08.2015)
            else 0
           end
               as cnt_month,
       nvl(a.dolg,0) /* долг же уже с учетом оплаты и изменений ред 22.03.12 - nvl(c.summa, 0) + nvl(e.summa, 0)*/
               as dolg, --долг с учетом оплаты
       case when nvl(b.summa,0) <> 0 and (nvl(a.dolg,0)+ nvl(c.summa, 0)) / nvl(b.summa,0) >= 1
                then a.cnt_month --(nvl(a.dolg,0)+ nvl(c.summa, 0)) / nvl(b.summa,0) ред.19.09.14
            when nvl(b.summa,0) <> 0 and (nvl(a.dolg,0)+ nvl(c.summa, 0)) / nvl(b.summa,0) < 1
                then 0
            when nvl(b.summa,0) = 0 and nvl(a.dolg,0)+ nvl(c.summa, 0) > 0 --по закрытым например
                then 1 --ставим 1 мес.задолжности
            else 0
           end as cnt_month2, nvl(a.dolg,0) + nvl(c.summa, 0) as dolg2,--долг без учета оплаты
       b.summa as nachisl, a.penya, c.summa, case when dat_ is null then mg_
                                                  else null end,
       dat_
from (select t.psch, t.k_lsk_id, t.lsk, t.fk_deb_org, t.reu, t.kul, t.nd, t.kw,
             t.fio, t.status, t.opl, e.dolg as dolg,
             e.penya as penya, e.cnt_month
      from arch_kart t,
           (select k.k_lsk_id, sum(a.summa) as dolg, sum(penya) as penya,
                   sum(case when a.penya >0 then 1 else 0 end) as cnt_month
            from kart k, a_penya a, v_lsk_tp tp
            where k.lsk=a.lsk and a.mg = mg_
              and k.fk_tp=tp.id and tp.cd in ('LSK_TP_MAIN')
            group by k.k_lsk_id) e, v_lsk_tp tp
      where t.mg = mg_
        and t.k_lsk_id = e.k_lsk_id(+)
        and t.fk_tp=tp.id and tp.cd in ('LSK_TP_MAIN')
        and t.psch <> 8
     ) a,
     (select k.k_lsk_id, sum(d.summa_it) as summa
      from kart k, arch_charges d, v_lsk_tp tp --начисление
      where k.lsk=d.lsk and d.mg = mg_
        and k.fk_tp=tp.id and tp.cd in ('LSK_TP_MAIN')
      group by k.k_lsk_id) b,
     (select k.k_lsk_id, sum(d.summa) as summa
      from kart k, a_kwtp_mg d, v_lsk_tp tp --оплата
      where k.lsk=d.lsk and d.mg = mg_
        and k.fk_tp=tp.id and tp.cd in ('LSK_TP_MAIN')
      group by k.k_lsk_id) c,
     (select k.k_lsk_id, sum(d.summa) as summa
      from kart k, a_change d, v_lsk_tp tp  --изменения
      where k.lsk=d.lsk and d.mg = mg_
        and k.fk_tp=tp.id and tp.cd in ('LSK_TP_MAIN')
      group by k.k_lsk_id) e,
     spul s
where a.k_lsk_id = b.k_lsk_id(+)
  and a.kul = s.id
  and a.k_lsk_id = c.k_lsk_id(+)
  and a.k_lsk_id = e.k_lsk_id(+)
  and nvl(a.dolg,0) > 0;
elsif var_ in (2,4) then
    --для Кис., ТСЖ, задолжность по л.с. (реально кол-во мес)
    --и задолжностью считается не текущий период, а период по которому начислена пеня
insert into debits_lsk_month
(lsk, reu, kul, name, nd, kw, fk_deb_org, fio, status, opl,
 cnt_month, dolg, cnt_month2, dolg2, nachisl, pen_in, pen_cur, penya, payment, pay_pen, mg, dat)
select a.lsk, a.reu, a.kul, s.name, a.nd, a.kw, a.fk_deb_org, a.fio,
       a.status, a.opl, a.cnt_month
                                     as cnt_month,
       nvl(a.dolg,0) /* долг же уже с учетом оплаты и изменений ред 22.03.12 - nvl(c.summa, 0) + nvl(e.summa, 0)*/
                                     as dolg, --долг с учетом оплаты
       a.cnt_month as cnt_month2, nvl(a.dolg,0) + nvl(c.summa, 0) as dolg2,--долг без учета оплаты
       nvl(b.summa,0)+nvl(e.summa,0) as nachisl, a.pen_in, nvl(a.pen_cur,0)+nvl(a.pen_cor,0) as pen_cur, a.penya, c.summa, c.penya, case when dat_ is null then mg_
                                                                                                                                         else null end,
       dat_
from (select t.psch, t.lsk, t.fk_deb_org, t.reu, t.kul, t.nd, t.kw,
             t.fio, t.status, t.opl, e.dolg,
             b.pen_in, d.pen_cur, f.pen_cor, e.penya, e.cnt_month
      from arch_kart t left join
           (select k.lsk, count(*) as cnt_month,  --исх.сальдо по пене
                   sum(a.summa) as dolg, sum(penya) as penya
            from kart k, a_penya a
            where k.lsk=a.lsk and a.mg = mg_
              and (var_=4 and nvl(a.summa,0) > 0  and a.mg1<>mg_ --кроме текущего --and nvl(a.penya,0) <> 0
                or var_<>4) --поправил по просьбе Своб., 14.04.2016
            group by k.lsk) e on t.lsk = e.lsk
                       left join
           (select a.lsk,  --вх.сальдо по пене
                   sum(penya) as pen_in
            from a_penya a
            where a.mg = mg3_ --период -1 мес
            group by a.lsk) b on t.lsk = b.lsk
                       left join
           (select a.lsk,  --тек.начисл. пеня
                   round(sum(penya),2) as pen_cur
            from a_pen_cur a
            where a.mg = mg_
            group by a.lsk) d on t.lsk = d.lsk
                       left join
           (select a.lsk,  --тек.коррект. пени
                   sum(penya) as pen_cor
            from a_pen_corr a
            where a.mg = mg_
            group by a.lsk) f on t.lsk = f.lsk
                       join v_lsk_tp tp on t.fk_tp=tp.id --and tp.cd in ('LSK_TP_MAIN','LSK_TP_RSO') убрал фильтр 12.07.2019 по просьбе Кис - не выгружалась задолжн по капрем
      where t.mg = mg_
     ) a,
     (select k.lsk, sum(d.summa_it) as summa
      from kart k, arch_charges d, v_lsk_tp tp --начисление
      where k.lsk=d.lsk and d.mg = mg_
        and k.fk_tp=tp.id --and tp.cd in ('LSK_TP_MAIN','LSK_TP_RSO') убрал фильтр 12.07.2019 по просьбе Кис - не выгружалась задолжн по капрем
      group by k.lsk) b,
     (select k.lsk, sum(d.summa) as summa, sum(d.penya) as penya
      from kart k, a_kwtp_mg d, v_lsk_tp tp --оплата
      where k.lsk=d.lsk and d.mg = mg_
        and k.fk_tp=tp.id --and tp.cd in ('LSK_TP_MAIN','LSK_TP_RSO') убрал фильтр 12.07.2019 по просьбе Кис - не выгружалась задолжн по капрем
      group by k.lsk) c,
     (select k.lsk, sum(d.summa) as summa
      from kart k, a_change d, v_lsk_tp tp  --изменения
      where k.lsk=d.lsk and d.mg = mg_
        and k.fk_tp=tp.id --and tp.cd in ('LSK_TP_MAIN','LSK_TP_RSO') убрал фильтр 12.07.2019 по просьбе Кис - не выгружалась задолжн по капрем
      group by k.lsk) e,
     spul s
where a.lsk = b.lsk(+)
  and a.kul = s.id
  and a.lsk = c.lsk(+)
  and a.lsk = e.lsk(+)
  and nvl(a.dolg,0) > 0;
elsif var_=3 then
    -- ПОЛЫС: (задолжностью считается всё что -1 месяц назад глядя на пеню), задолжность по л.с. (реально кол-во мес)
    -- и задолжностью считается не текущий период, а период по которому начислена пеня
    -- ред.09.04.2019

    -- по совокупности долга по помещению
insert into debits_lsk_month
(lsk, k_lsk_id, reu, kul, name, nd, kw, fk_deb_org, fio, status, opl,
 cnt_month, dolg, cnt_month2, dolg2, nachisl, penya, payment, mg, dat, var)
select /*+ USE_HASH(a, b, c, e, s)*/a.lsk, a.k_lsk_id, a.reu, a.kul, s.name, a.nd, a.kw, a.fk_deb_org, a.fio,
                                    a.status, a.opl,
                                    a.cnt_month as cnt_month,
                                    nvl(a.dolg,0) /* долг же уже с учетом оплаты и изменений ред 22.03.12 - nvl(c.summa, 0) + nvl(e.summa, 0)*/
                                                as dolg, --долг с учетом оплаты
                                    a.cnt_month as cnt_month2, nvl(a.dolg,0) + nvl(c.summa, 0) as dolg2,--долг без учета оплаты
                                    b.summa as nachisl, a.penya, c.summa, case when dat_ is null then mg_
                                                                               else null end,
                                    dat_, 0 as var
from (select t.psch, t.k_lsk_id, t.lsk, t.fk_deb_org, t.reu, t.kul, t.nd, t.kw,
             t.fio, t.status, t.opl, e.dolg as dolg,
             e.penya as penya, e.cnt_month
      from arch_kart t, v_lsk_tp tp,
           (select k.k_lsk_id, count(distinct a.mg1) as cnt_month,
                   sum(a.summa) as dolg, sum(penya) as penya
            from kart k, a_penya a, v_lsk_tp tp
            where k.lsk=a.lsk and a.mg = mg_
              and nvl(a.summa,0) <> 0
              and a.mg1 <= mg3_ --бред
              and k.fk_tp=tp.id --and tp.cd<>'LSK_TP_ADDIT' -- кроме счетов капремонта ред.06.02.19 закомментировал по просьбе Полыс.
            group by k.k_lsk_id) e
      where t.mg = mg_
        and t.k_lsk_id = e.k_lsk_id(+)
        and t.psch <> 8  --закомментировал 22.11.2017 убрал коммент 29.11.2017
        and t.fk_tp=tp.id
        and tp.cd='LSK_TP_MAIN' --долг показать по основному лиц.счету
     ) a,
     (select k.k_lsk_id, sum(d.summa_it) as summa
      from kart k, arch_charges d --начисление
      where k.lsk=d.lsk and d.mg = mg_
      group by k.k_lsk_id) b,
     (select k.k_lsk_id, sum(d.summa) as summa
      from kart k, a_kwtp_mg d --оплата
      where k.lsk=d.lsk and d.mg = mg_
      group by k.k_lsk_id) c,
     (select k.k_lsk_id, sum(d.summa) as summa
      from kart k, a_change d  --изменения
      where k.lsk=d.lsk and d.mg = mg_
      group by k.k_lsk_id) e,
     spul s
where a.k_lsk_id = b.k_lsk_id(+)
  and a.kul = s.id
  and a.k_lsk_id = c.k_lsk_id(+)
  and a.k_lsk_id = e.k_lsk_id(+)
  and nvl(a.dolg,0) > 0;

-- по лиц.счетам
insert into debits_lsk_month
(lsk, reu, kul, name, nd, kw, fk_deb_org, fio, status, opl,
 cnt_month, dolg, cnt_month2, dolg2, nachisl, penya, payment, mg, dat, var)
select /*+ USE_HASH(a, b, c, e, s)*/a.lsk, a.reu, a.kul, s.name, a.nd, a.kw, a.fk_deb_org, a.fio,
                                    a.status, a.opl,
                                    a.cnt_month as cnt_month,
                                    nvl(a.dolg,0) /* долг же уже с учетом оплаты и изменений ред 22.03.12 - nvl(c.summa, 0) + nvl(e.summa, 0)*/
                                                as dolg, --долг с учетом оплаты
                                    a.cnt_month as cnt_month2, nvl(a.dolg,0) + nvl(c.summa, 0) as dolg2,--долг без учета оплаты
                                    b.summa as nachisl, a.penya, c.summa, case when dat_ is null then mg_
                                                                               else null end,
                                    dat_, 1 as var
from (select t.psch, t.lsk, t.fk_deb_org, t.reu, t.kul, t.nd, t.kw,
             t.fio, t.status, t.opl, e.dolg as dolg,
             e.penya as penya, e.cnt_month
      from arch_kart t, v_lsk_tp tp,
           (select k.lsk, count(distinct a.mg1) as cnt_month,
                   sum(a.summa) as dolg, sum(penya) as penya
            from kart k, a_penya a, v_lsk_tp tp
            where k.lsk=a.lsk and a.mg = mg_
              and nvl(a.summa,0) <> 0
              and a.mg1 <= mg3_ --бред
              and k.fk_tp=tp.id
            group by k.lsk) e
      where t.mg = mg_
        and t.lsk = e.lsk(+)
        and t.fk_tp=tp.id
     ) a,
     (select d.lsk, sum(d.summa_it) as summa
      from arch_charges d --начисление
      where d.mg = mg_
      group by d.lsk) b,
     (select d.lsk, sum(d.summa) as summa
      from a_kwtp_mg d --оплата
      where d.mg = mg_
      group by d.lsk) c,
     (select d.lsk, sum(d.summa) as summa
      from a_change d  --изменения
      where d.mg = mg_
      group by d.lsk) e,
     spul s
where a.lsk = b.lsk(+)
  and a.kul = s.id
  and a.lsk = c.lsk(+)
  and a.lsk = e.lsk(+)
  and nvl(a.dolg,0) > 0;

-- тоже самое что выше, только развернуто по периодам

-- по совокупности долга по помещению
insert into debits_lsk_month
(lsk, k_lsk_id, reu, kul, name, nd, kw, fk_deb_org, fio, status, opl,
 cnt_month, dolg, cnt_month2, dolg2, nachisl, penya, payment, mg, dat, period_deb, deb_month, var)
select /*+ USE_HASH(a, b, c, e, s)*/a.lsk, a.k_lsk_id, a.reu, a.kul, s.name, a.nd, a.kw, a.fk_deb_org, a.fio,
                                    a.status, a.opl,
                                    a.cnt_month as cnt_month,
                                    nvl(a.dolg,0) /* долг же уже с учетом оплаты и изменений ред 22.03.12 - nvl(c.summa, 0) + nvl(e.summa, 0)*/
                                                as dolg, --долг с учетом оплаты
                                    a.cnt_month as cnt_month2, nvl(a.dolg,0) + nvl(c.summa, 0) as dolg2,--долг без учета оплаты
                                    b.summa as nachisl, a.penya, c.summa, case when dat_ is null then mg_
                                                                               else null end,
                                    dat_, a.period_deb, a.deb_month,
                                    2 as var
from (select t.psch, t.k_lsk_id, t.lsk, t.fk_deb_org, t.reu, t.kul, t.nd, t.kw,
             t.fio, t.status, t.opl, e.dolg as dolg,
             e.penya as penya, e.cnt_month, e2.mg1 as period_deb, e2.dolg as deb_month
      from arch_kart t, v_lsk_tp tp,
           (select k.k_lsk_id, count(distinct a.mg1) as cnt_month,
                   sum(a.summa) as dolg, sum(penya) as penya
            from kart k, a_penya a
            where k.lsk=a.lsk and a.mg = mg_
              and nvl(a.summa,0) <> 0
              and a.mg1 <= mg3_
            group by k.k_lsk_id) e,
           (select k.k_lsk_id, a.mg1, sum(a.summa) as dolg
            from kart k, a_penya a
            where k.lsk=a.lsk and a.mg = mg_
              and nvl(a.summa,0) <> 0
              and a.mg1 <= mg3_
            group by k.k_lsk_id, a.mg1) e2
      where t.mg = mg_
        and t.k_lsk_id = e.k_lsk_id(+)
        and t.k_lsk_id = e2.k_lsk_id(+)
        and t.psch <> 8
        and t.fk_tp=tp.id
        and tp.cd='LSK_TP_MAIN' --долг показать по основному лиц.счету
     ) a,
     (select k.k_lsk_id, sum(d.summa_it) as summa
      from kart k, arch_charges d --начисление
      where k.lsk=d.lsk and d.mg = mg_
      group by k.k_lsk_id) b,
     (select k.k_lsk_id, sum(d.summa) as summa
      from kart k, a_kwtp_mg d --оплата
      where k.lsk=d.lsk and d.mg = mg_
      group by k.k_lsk_id) c,
     (select k.k_lsk_id, sum(d.summa) as summa
      from kart k, a_change d  --изменения
      where k.lsk=d.lsk and d.mg = mg_
      group by k.k_lsk_id) e,
     spul s
where a.k_lsk_id = b.k_lsk_id(+)
  and a.kul = s.id
  and a.k_lsk_id = c.k_lsk_id(+)
  and a.k_lsk_id = e.k_lsk_id(+)
  and nvl(a.dolg,0) > 0;

-- по лиц.счетам
insert into debits_lsk_month
(lsk, reu, kul, name, nd, kw, fk_deb_org, fio, status, opl,
 cnt_month, dolg, cnt_month2, dolg2, nachisl, penya, payment, mg, dat, period_deb, deb_month, var)
select /*+ USE_HASH(a, b, c, e, s)*/a.lsk, a.reu, a.kul, s.name, a.nd, a.kw, a.fk_deb_org, a.fio,
                                    a.status, a.opl,
                                    a.cnt_month as cnt_month,
                                    nvl(a.dolg,0) /* долг же уже с учетом оплаты и изменений ред 22.03.12 - nvl(c.summa, 0) + nvl(e.summa, 0)*/
                                                as dolg, --долг с учетом оплаты
                                    a.cnt_month as cnt_month2, nvl(a.dolg,0) + nvl(c.summa, 0) as dolg2,--долг без учета оплаты
                                    b.summa as nachisl, a.penya, c.summa, case when dat_ is null then mg_
                                                                               else null end,
                                    dat_, a.period_deb, a.deb_month,
                                    3 as var
from (select t.psch, t.lsk, t.fk_deb_org, t.reu, t.kul, t.nd, t.kw,
             t.fio, t.status, t.opl, e.dolg as dolg,
             e.penya as penya, e.cnt_month, e2.mg1 as period_deb, e2.dolg as deb_month
      from arch_kart t, v_lsk_tp tp,
           (select k.lsk, count(distinct a.mg1) as cnt_month,
                   sum(a.summa) as dolg, sum(penya) as penya
            from kart k, a_penya a, v_lsk_tp tp
            where k.lsk=a.lsk and a.mg = mg_
              and nvl(a.summa,0) <> 0
              and a.mg1 <= mg3_
              and k.fk_tp=tp.id
            group by k.lsk) e,
           (select k.lsk, a.mg1, sum(a.summa) as dolg
            from kart k, a_penya a, v_lsk_tp tp
            where k.lsk=a.lsk and a.mg = mg_
              and nvl(a.summa,0) <> 0
              and a.mg1 <= mg3_
              and k.fk_tp=tp.id
            group by k.lsk, a.mg1) e2
      where t.mg = mg_
        and t.lsk = e.lsk(+)
        and t.lsk = e2.lsk(+)
        and t.fk_tp=tp.id
     ) a,
     (select d.lsk, sum(d.summa_it) as summa
      from arch_charges d --начисление
      where d.mg = mg_
      group by d.lsk) b,
     (select d.lsk, sum(d.summa) as summa
      from a_kwtp_mg d --оплата
      where d.mg = mg_
      group by d.lsk) c,
     (select d.lsk, sum(d.summa) as summa
      from a_change d  --изменения
      where d.mg = mg_
      group by d.lsk) e,
     spul s
where a.lsk = b.lsk(+)
  and a.kul = s.id
  and a.lsk = c.lsk(+)
  and a.lsk = e.lsk(+)
  and nvl(a.dolg,0) > 0;
end if;


if dat_ is not null then
      logger.ins_period_rep('54', null, dat_, 0);
logger.ins_period_rep('69', null, dat_, 0);
logger.ins_period_rep('82', null, dat_, 0);
logger.ins_period_rep('80', null, dat_, 0);
logger.ins_period_rep('98', null, dat_, 0);
else
      logger.ins_period_rep('54', mg_, null, 0);
logger.ins_period_rep('69', mg_, null, 0);
logger.ins_period_rep('82', mg_, null, 0);
logger.ins_period_rep('80', mg_, null, 0);
logger.ins_period_rep('98', mg_, null, 0);
end if;
commit;
logger.log_(time_, 'gen.gen_debits_lsk_month');
end;

procedure load_saldo(mg_ in varchar2)
  --Первоначальная загрузка сальдо mg_ - текущий месяц
   is
    stmt varchar2(2000);
time_ date;
  begin
time_ := sysdate;
stmt  := 'TRUNCATE TABLE saldo';
execute immediate stmt;
stmt := 'INSERT INTO saldo (lsk, org, uslm, summa, mg)
               SELECT lsk, kod, USLM, SUM(SWX) summa, :mg1
               FROM SWX t
               GROUP BY lsk, kod, USLM';
execute immediate stmt
      using mg_;
commit;
logger.log_(time_, 'gen.load_saldo');
end load_saldo;

/*  procedure distrib_vols
  --Распределение кубов по домам, в конце месяца
   is
    time_ date;
  begin
  --устарело - закрыто!!!!
  --ред.06.11.12

    time_ := sysdate;
    --Чтоб сработал триггер, где и произойдёт распределение
    update c_vvod t set t.kub = t.kub, t.vol_add = t.vol_add
       where kub <> 0;
    commit;
    logger.log_(time_, 'gen.distrib_vols');
  end distrib_vols;
*/

/*procedure prepare_arch_lsk(lsk_     in kart.lsk%type,
                             lsk_end_ in kart.lsk%type) is
  bill_pen_ number;
  begin
    for c in (select distinct c_lsk_id
                from kart k
               where k.lsk between lsk_ and lsk_end_) loop
      --подготовка счета индивидуально по c_lsk_id
      --начисление
      gen_c_charges(c.c_lsk_id);
      --сальдо
      gen_saldo(c.c_lsk_id);
      --архив
      prepare_arch(c.c_lsk_id);
      select nvl(p.bill_pen,0) into bill_pen_ from params p;
      if bill_pen_ = 1 then
        --пеня по тек. дате
        c_cpenya.gen_penya(c.c_lsk_id, 0);
      else
        --пеня по конечной дате месяца
        c_cpenya.gen_penya(c.c_lsk_id, 1);
      end if;
    end loop;
  end;
  */

-- расчет движения, вызывается из Delphi
procedure prepare_arch_lsk(lsk_     in kart.lsk%type,
                             var_     in number) is
  bill_pen_ number;
l_Java_deb_pen number;
l_klsk_id number;
l_dummy number;
  begin
    --подготовка счета индивидуально по lsk
if var_ = 0 then
      --начисление
select k.k_lsk_id into l_klsk_id from kart k where k.lsk=lsk_;
l_dummy:=p_java.gen(p_tp        => 0,
           p_house_id  => null,
           p_vvod_id   => null,
           p_usl_id    => null,
           p_klsk_id   => l_klsk_id,
           p_debug_lvl => 0,
           p_gen_dt    => nvl(init.dtek_, gdt(32,0,0)), -- не заполнен dtek_, вернуть последний день текущего периода
           p_stop      => 0);
      --сальдо
gen_saldo(lsk_);
--архив
prepare_arch(lsk_);
end if;

l_Java_deb_pen := utils.get_int_param('JAVA_DEB_PEN');
if l_Java_deb_pen =0 then
      --движение (по старому), по новому - вызывается из Delphi
      c_cpenya.gen_charge_pay(lsk_, 0);
      --пеня в любом случае
select nvl(p.bill_pen,0) into bill_pen_ from params p;
if bill_pen_ = 1 then
        --пеня по тек. дате
        c_cpenya.gen_penya(lsk_, 0, 0);
else
        --пеня по конечной дате месяца
        c_cpenya.gen_penya(lsk_, 1, 0);
end if;
end if;
commit;
end;

procedure prepare_arch_k_lsk(k_lsk_id_     in kart.k_lsk_id%type,
                             pen_last_month_ in number,
                             var_     in number) is
  l_Java_deb_pen number;
l_dummy number;
l_datpen date;
cnt_ number;
  begin
l_Java_deb_pen := utils.get_int_param('JAVA_DEB_PEN');

--    if l_Java_deb_pen =1 then
      -- новый вариант
      -- начисление
l_dummy:=p_java.gen(p_tp        => 0,
           p_klsk_id   => k_lsk_id_,
           p_debug_lvl => 0,
           p_gen_dt    => init.get_date,
           p_stop      => 0);

      --пеня + движение
if pen_last_month_ = 0 then --текущая пеня
        l_datpen:=init.get_date();
else --пеня по концу месяца
        l_datpen:=init.get_cur_dt_end;
end if;
l_dummy:=p_java.gen(p_tp        => 1,
           p_klsk_id   => k_lsk_id_,
           p_debug_lvl => 0,
           p_gen_dt    => l_datpen,
           p_stop      => 0);
if var_ = 0 then
          --включить еще формирование архива
          for c in (select distinct c_lsk_id, k.lsk
                from kart k
                     where k.k_lsk_id = k_lsk_id_) loop
            --сальдо
            gen_saldo(c.lsk);
            --архив
prepare_arch(c.lsk);
end loop;
end if;
/*    else
      -- старый вариант
      --подготовка счета индивидуально k_lsk
      for c in (select distinct c_lsk_id, k.lsk
                  from kart k
                 where k.k_lsk_id = k_lsk_id_) loop
        if var_ = 0 then
        --включить еще формирование архива
          --начисление, по одному лицевому, без коммита
          cnt_ := c_charges.gen_charges(c.lsk, c.lsk, null, null, 0, 0);
          --сальдо
          gen_saldo(c.lsk);
          --архив
          prepare_arch(c.lsk);
        end if;
        --движение
        c_cpenya.gen_charge_pay(c.lsk, 1);
        --пеня
        c_cpenya.gen_penya(c.lsk, nvl(pen_last_month_,0), 0);
      end loop;
    end if;*/
commit;
end;

/*  не используется??? ред. 25.06.20
  procedure prepare_arch_adr(kul_ in kart.kul%type,
                             nd_  in kart.nd%type,
                             kw_  in kart.kw%type,
                             var_ in number) is
  begin
    --подготовка счета индивидуально по адресу
    for c in (select distinct c_lsk_id
                from kart k
               where k.kul = kul_
                 and k.nd = nd_
                 and k.kw = kw_) loop
      if var_ = 0 then
        --начисление
        gen_c_charges(c.c_lsk_id);
        --сальдо
        gen_saldo(c.c_lsk_id);
        --архив
        prepare_arch(c.c_lsk_id);
        --пеня
        c_cpenya.gen_charge_pay(c.c_lsk_id, 1);
        c_cpenya.gen_penya(c.c_lsk_id, 1, 0);
      elsif var_ = 1 then
        --только пеня
        c_cpenya.gen_charge_pay(c.c_lsk_id, 1);
        c_cpenya.gen_penya(c.c_lsk_id, 0, 0);
      end if;
    end loop;
    commit;
  end;
*/

procedure prepare_arch_all is
  begin
Raise_application_error(-20000, 'Процедура prepare_arch_all не работает!');
--подготовка всех счетов (по окончанию месяца)
for c in (select k.lsk from kart k
      where not exists (--только те л/c, где нужен перерасчет
           select *
                  from t_objxpar x, u_list s, u_listtp tp where
                  s.fk_listtp=tp.id and tp.cd='Параметры лиц.счета'
                  and x.fk_list=s.id and s.cd='gen_bill'
                  and x.fk_lsk=k.lsk
                  and x.n1=0)
                  )
    loop
      prepare_arch(c.lsk);
end loop;
end;

procedure prepare_arch(lsk_ in kart.lsk%type) is
    cnt_ number;
mg_  varchar2(6);
mg1_  varchar2(6);
old_mg_  varchar2(6);
    -- Архив
time_ date;
  begin
time_ := sysdate;
    -- Создаем архивы
    --текущий месяц
select p.period into mg_ from params p;
--месяц вперед
mg1_:=to_char(add_months(to_date(mg_, 'YYYYMM'), 1), 'YYYYMM');
    --месяц назад
old_mg_:=to_char(add_months(to_date(mg_||'01','YYYYMMDD'),-1), 'YYYYMM');
    --архив расценок
if lsk_ is null then
      trunc_part('a_prices', mg_);
insert into a_prices
(usl, summa, summa2, summa3, fk_org, mg)
select usl, summa, summa2, summa3, fk_org, p.period from prices c, params p;
end if;

--архив платежей
if lsk_ is null then
      trunc_part('a_kwtp', mg_);
insert into a_kwtp
(lsk,
 summa,
 penya,
 oper,
 dopl,
 nink,
 nkom,
 dtek,
 nkvit,
 dat_ink,
 ts,
 id,
 iscorrect,
 num_doc,
 dat_doc,
 fk_pdoc,
 annul,
 mg)
select c.lsk,
       c.summa,
       c.penya,
       c.oper,
       c.dopl,
       c.nink,
       c.nkom,
       c.dtek,
       c.nkvit,
       c.dat_ink,
       c.ts,
       c.id,
       c.iscorrect,
       c.num_doc,
       c.dat_doc,
       c.fk_pdoc,
       c.annul,
       p.period
from c_kwtp c, params p
where
    c.dat_ink between init.g_dt_start and init.g_dt_end;
else
delete from a_kwtp a
where a.lsk=lsk_ and a.mg = mg_;
insert into a_kwtp
(lsk,
 summa,
 penya,
 oper,
 dopl,
 nink,
 nkom,
 dtek,
 nkvit,
 dat_ink,
 ts,
 id,
 iscorrect,
 num_doc,
 dat_doc,
 fk_pdoc,
 annul,
 mg)
select c.lsk,
       c.summa,
       c.penya,
       c.oper,
       c.dopl,
       c.nink,
       c.nkom,
       c.dtek,
       c.nkvit,
       c.dat_ink,
       c.ts,
       c.id,
       c.iscorrect,
       c.num_doc,
       c.dat_doc,
       c.fk_pdoc,
       c.annul,
       p.period
from c_kwtp c, params p
where c.dat_ink between init.g_dt_start and init.g_dt_end
  and c.lsk = lsk_; --по дате (здесь dtek!!!)
end if;

if lsk_ is null then
      trunc_part('a_kwtp_mg', mg_);
insert into a_kwtp_mg
(id,
 lsk,
 summa,
 penya,
 oper,
 dopl,
 nink,
 nkom,
 dtek,
 nkvit,
 dat_ink,
 ts,
 c_kwtp_id,
 mg)
select c.id,
       c.lsk,
       c.summa,
       c.penya,
       c.oper,
       c.dopl,
       c.nink,
       c.nkom,
       c.dtek,
       c.nkvit,
       c.dat_ink,
       c.ts,
       c.c_kwtp_id,
       p.period
from c_kwtp_mg c, params p
where c.dat_ink between init.g_dt_start and init.g_dt_end;
else
delete from a_kwtp_mg a
where a.lsk=lsk_ and a.mg = mg_;
insert into a_kwtp_mg
(id,
 lsk,
 summa,
 penya,
 oper,
 dopl,
 nink,
 nkom,
 dtek,
 nkvit,
 dat_ink,
 ts,
 c_kwtp_id,
 mg)
select c.id,
       c.lsk,
       c.summa,
       c.penya,
       c.oper,
       c.dopl,
       c.nink,
       c.nkom,
       c.dtek,
       c.nkvit,
       c.dat_ink,
       c.ts,
       c.c_kwtp_id,
       p.period
from c_kwtp_mg c, params p
where c.lsk = lsk_
  and c.dat_ink between init.g_dt_start and init.g_dt_end;
end if;

--архив распределённой оплаты
if lsk_ is null then
        trunc_part('a_kwtp_day', mg_);
insert into a_kwtp_day
(kwtp_id, summa, lsk, oper, dopl, nkom, nink, dat_ink, priznak, usl, org, fk_distr, sum_distr, id, mg, dtek)
select
    t.kwtp_id, t.summa, t.lsk, t.oper, t.dopl, t.nkom, t.nink, t.dat_ink, t.priznak,
    t.usl, t.org, t.fk_distr, t.sum_distr, t.id,
    p.period, t.dtek
from kwtp_day t, params p
where t.dat_ink between init.g_dt_start and init.g_dt_end;
else
delete from a_kwtp_day a
where a.lsk=lsk_ and a.mg = mg_;
insert into a_kwtp_day
(kwtp_id, summa, lsk, oper, dopl, nkom, nink, dat_ink, priznak, usl, org, fk_distr, sum_distr, id, mg, dtek)
select
    t.kwtp_id, t.summa, t.lsk, t.oper, t.dopl, t.nkom, t.nink, t.dat_ink, t.priznak,
    t.usl, t.org, t.fk_distr, t.sum_distr, t.id,
    p.period, t.dtek
from kwtp_day t, params p
where t.lsk=lsk_
  and t.dat_ink between init.g_dt_start and init.g_dt_end;
end if;

--архив начисления
if lsk_ is null then
      --trunc_part('a_charge', mg_);
delete from a_charge2 a
where mg_ between a.mgFrom and a.mgTo;
insert into a_charge2
(id,
 lsk,
 usl,
 org,
 summa,
 kart_pr_id,
 spk_id,
 type,
 test_opl,
 test_cena,
 test_tarkoef,
 test_spk_koef,
 main,
 lg_doc_id,
 npp,
 sch,
 mgFrom,
 mgTo,
 kpr, kprz, kpro, kpr2, opl
)
select a_charge_id.nextval,
       c.lsk,
       c.usl,
       c.org,
       c.summa,
       c.kart_pr_id,
       c.spk_id,
       c.type,
       c.test_opl,
       c.test_cena,
       c.test_tarkoef,
       c.test_spk_koef,
       c.main,
       c.lg_doc_id,
       c.npp,
       c.sch,
       p.period, -- одинаковые периоды!
       p.period,
       c.kpr, c.kprz, c.kpro, c.kpr2, c.opl
from kart k, c_charge c, params p
where k.lsk=c.lsk;
else
delete /*+ INDEX (a A_CHARGE2_I)*/ from a_charge2 a
where a.lsk=lsk_ and mg_ between a.mgFrom and a.mgTo;
insert into a_charge2
(id,
 lsk,
 usl,
 org,
 summa,
 kart_pr_id,
 spk_id,
 type,
 test_opl,
 test_cena,
 test_tarkoef,
 test_spk_koef,
 main,
 lg_doc_id,
 npp,
 sch,
 mgFrom,
 mgTo,
 kpr, kprz, kpro, kpr2, opl)
select a_charge_id.nextval,
       c.lsk,
       c.usl,
       c.org,
       c.summa,
       c.kart_pr_id,
       c.spk_id,
       c.type,
       c.test_opl,
       c.test_cena,
       c.test_tarkoef,
       c.test_spk_koef,
       c.main,
       c.lg_doc_id,
       c.npp,
       c.sch,
       p.period, --одинак. период!
       p.period,
       c.kpr, c.kprz, c.kpro, c.kpr2, c.opl
from kart k, c_charge c, params p
where k.lsk=c.lsk and c.lsk = lsk_;
end if;

--архив изменения начисления
if lsk_ is null then
      trunc_part('a_change', mg_);
insert into a_change
(lsk,
 usl,
 summa,
 proc,
 mgchange,
 nkom,
 org,
 type,
 dtek,
 ts,
 user_id,
 doc_id,
 cnt_days,
 show_bill,
 id,
 mg,
 mg2,
 vol,
 tp)
select c.lsk,
       c.usl,
       c.summa,
       c.proc,
       c.mgchange,
       c.nkom,
       c.org,
       c.type,
       c.dtek,
       c.ts,
       c.user_id,
       c.doc_id,
       c.cnt_days,
       c.show_bill,
       c.id,
       p.period,
       c.mg2,
       c.vol,
       c.tp
from kart k, c_change c, params p
where k.lsk=c.lsk and to_char(c.dtek, 'YYYYMM') = p.period; --по дате
else
delete from a_change a
where a.lsk=lsk_ and a.mg = mg_;
insert into a_change
(lsk,
 usl,
 summa,
 proc,
 mgchange,
 nkom,
 org,
 type,
 dtek,
 ts,
 user_id,
 doc_id,
 cnt_days,
 show_bill,
 id,
 mg,
 mg2,
 vol,
 tp)
select c.lsk,
       c.usl,
       c.summa,
       c.proc,
       c.mgchange,
       c.nkom,
       c.org,
       c.type,
       c.dtek,
       c.ts,
       c.user_id,
       c.doc_id,
       c.cnt_days,
       c.show_bill,
       c.id,
       p.period,
       c.mg2,
       c.vol,
       c.tp
from kart k, c_change c, params p
where to_char(c.dtek, 'YYYYMM') = p.period
  and k.lsk=c.lsk and c.lsk = lsk_; --по дате
end if;

--архив документов по изменению начисления
if lsk_ is null then
      trunc_part('a_change_docs', mg_);
insert into a_change_docs
(id, mgchange, dtek, ts, user_id, text, param_json, mg, cd_tp, description)
select c.id, c.mgchange, c.dtek, c.ts, c.user_id, c.text, c.param_json, p.period, c.cd_tp, c.description
from c_change_docs c, params p
where to_char(c.dtek, 'YYYYMM') = p.period; --по дате
end if;

if lsk_ is null then
      --архив домов
      trunc_part('a_houses', mg_);
insert into a_houses
(id, kul, nd, uch, house_type, fk_pasp_org, psch, mg, fk_other_org,
 fk_typespay)
select c.id, c.kul, c.nd, c.uch, c.house_type, c.fk_pasp_org, c.psch,
       p.period, c.fk_other_org, c.fk_typespay
from c_houses c, params p;

--архив вводов
trunc_part('a_vvod', mg_);
insert into a_vvod
(house_id,
 id,
 kub,
 edt_norm,
 usl,
 kub_man,
 kpr,
 kub_sch,
 sch_cnt,
 sch_kpr,
 cnt_lsk,
 vvod_num,
 vol_add,
 sch_add,
 kub_fact,
 kub_norm,
 kub_nrm_fact,
 kub_sch_fact,
 vol_add_fact,
 itg_fact,
 opl_add,
 use_sch,
 dist_tp,
 opl_ar,
 kub_ar,
 kub_ar_fact,
 kub_dist,
 nrm,
 kub_fact_upnorm,
 ishotpipeinsulated,
 istowelheatexist,
 wo_limit,
 mg)
select c.house_id,
       c.id,
       c.kub,
       c.edt_norm,
       c.usl,
       c.kub_man,
       c.kpr,
       c.kub_sch,
       c.sch_cnt,
       c.sch_kpr,
       c.cnt_lsk,
       c.vvod_num,
       c.vol_add,
       c.sch_add,
       c.kub_fact,
       c.kub_norm,
       c.kub_nrm_fact,
       c.kub_sch_fact,
       c.vol_add_fact,
       c.itg_fact,
       c.opl_add,
       c.use_sch,
       c.dist_tp,
       c.opl_ar,
       c.kub_ar,
       c.kub_ar_fact,
       c.kub_dist,
       c.nrm,
       c.kub_fact_upnorm,
       c.ishotpipeinsulated,
       c.istowelheatexist,
       c.wo_limit,
       p.period
from c_vvod c, params p;
elsif lsk_ is not null then
      --архив домов
delete from a_houses c where c.mg=mg_ and
    exists (select * from kart k where k.house_id=c.id
                                   and k.lsk=lsk_);
insert into a_houses
(id, kul, nd, uch, house_type, fk_pasp_org, psch, mg, fk_other_org)
select c.id, c.kul, c.nd, c.uch, c.house_type, c.fk_pasp_org, c.psch, p.period, c.fk_other_org
from c_houses c, params p
where exists (select * from kart k where k.house_id=c.id
                                     and k.lsk=lsk_);

--архив вводов
delete from a_vvod c where c.mg=mg_ and
    exists (select * from kart k where k.house_id=c.house_id
                                   and k.lsk=lsk_);
insert into a_vvod
(house_id,
 id,
 kub,
 edt_norm,
 usl,
 kub_man,
 kpr,
 kub_sch,
 sch_cnt,
 sch_kpr,
 cnt_lsk,
 vvod_num,
 vol_add,
 sch_add,
 kub_fact,
 kub_norm,
 kub_nrm_fact,
 kub_sch_fact,
 vol_add_fact,
 itg_fact,
 opl_add,
 use_sch,
 dist_tp,
 opl_ar,
 kub_ar,
 kub_ar_fact,
 nrm,
 kub_fact_upnorm,
 ishotpipeinsulated,
 istowelheatexist,
 mg)
select c.house_id,
       c.id,
       c.kub,
       c.edt_norm,
       c.usl,
       c.kub_man,
       c.kpr,
       c.kub_sch,
       c.sch_cnt,
       c.sch_kpr,
       c.cnt_lsk,
       c.vvod_num,
       c.vol_add,
       c.sch_add,
       c.kub_fact,
       c.kub_norm,
       c.kub_nrm_fact,
       c.kub_sch_fact,
       c.vol_add_fact,
       c.itg_fact,
       c.opl_add,
       c.use_sch,
       c.dist_tp,
       c.opl_ar,
       c.kub_ar,
       c.kub_ar_fact,
       c.nrm,
       c.kub_fact_upnorm,
       c.ishotpipeinsulated,
       c.istowelheatexist,
       p.period
from c_vvod c, params p
where exists (select * from kart k where k.house_id=c.house_id
                                     and k.lsk=lsk_);
end if;

if lsk_ is null then
      --справочник льгот
      trunc_part('a_spk_usl', mg_);
insert into a_spk_usl
(spk_id, usl_id, koef, mg)
select c.spk_id, c.usl_id, c.koef, p.period
from c_spk_usl c, params p;

end if;

--карточки проживающих
if lsk_ is null then
delete from a_kart_pr2 a
where mg_ between a.mgFrom and a.mgTo;
insert into a_kart_pr2
(id, lsk, fio, status, dat_rog, pol, dok, dok_c, dok_n,
 dok_d, dok_v, dok_snils, dok_div, dok_inn, dat_prop, dat_ub, relat_id,
 status_datb, status_dat, status_chng, k_fam, k_im, k_ot,
 fk_doc_tp, fk_nac, b_place, fk_frm_cntr, fk_frm_regn,
 fk_frm_distr, frm_town, frm_dat, fk_frm_kul, frm_nd,
 frm_kw, w_place, fk_ub, fk_to_cntr, fk_to_regn,
 fk_to_distr, to_town, fk_to_kul, to_nd, to_kw,
 fk_citiz, fk_milit, fk_milit_regn, priv_proc, dok_death_c, dok_death_n, mgFrom, mgTo)
select c.id, c.lsk, c.fio, c.status, c.dat_rog, c.pol, c.dok, c.dok_c, c.dok_n,
       c.dok_d, c.dok_v, c.dok_snils, c.dok_div, c.dok_inn, c.dat_prop, c.dat_ub, c.relat_id,
       c.status_datb, c.status_dat, c.status_chng, c.k_fam, c.k_im, c.k_ot,
       c.fk_doc_tp, c.fk_nac, c.b_place, c.fk_frm_cntr, c.fk_frm_regn,
       c.fk_frm_distr, c.frm_town, c.frm_dat, c.fk_frm_kul, c.frm_nd,
       c.frm_kw, c.w_place, c.fk_ub, c.fk_to_cntr, c.fk_to_regn,
       c.fk_to_distr, c.to_town, c.fk_to_kul, c.to_nd, c.to_kw,
       c.fk_citiz, c.fk_milit, c.fk_milit_regn, c.priv_proc, c.dok_death_c, c.dok_death_n, p.period as mgFrom, p.period as mgTo
from c_kart_pr c, params p;
else
delete from a_kart_pr2 a
where a.lsk=lsk_ and mg_ between a.mgFrom and a.mgTo;
insert into a_kart_pr2
(id, lsk, fio, status, dat_rog, pol, dok, dok_c, dok_n,
 dok_d, dok_v, dok_snils, dok_div, dok_inn, dat_prop, dat_ub, relat_id,
 status_datb, status_dat, status_chng, k_fam, k_im, k_ot,
 fk_doc_tp, fk_nac, b_place, fk_frm_cntr, fk_frm_regn,
 fk_frm_distr, frm_town, frm_dat, fk_frm_kul, frm_nd,
 frm_kw, w_place, fk_ub, fk_to_cntr, fk_to_regn,
 fk_to_distr, to_town, fk_to_kul, to_nd, to_kw,
 fk_citiz, fk_milit, fk_milit_regn, priv_proc, dok_death_c, dok_death_n, mgFrom, mgTo)
select c.id, c.lsk, c.fio, c.status, c.dat_rog, c.pol, c.dok, c.dok_c, c.dok_n,
       c.dok_d, c.dok_v, c.dok_snils, c.dok_div, c.dok_inn, c.dat_prop, c.dat_ub, c.relat_id,
       c.status_datb, c.status_dat, c.status_chng, c.k_fam, c.k_im, c.k_ot,
       c.fk_doc_tp, c.fk_nac, c.b_place, c.fk_frm_cntr, c.fk_frm_regn,
       c.fk_frm_distr, c.frm_town, c.frm_dat, c.fk_frm_kul, c.frm_nd,
       c.frm_kw, c.w_place, c.fk_ub, c.fk_to_cntr, c.fk_to_regn,
       c.fk_to_distr, c.to_town, c.fk_to_kul, c.to_nd, c.to_kw,
       c.fk_citiz, c.fk_milit, c.fk_milit_regn, c.priv_proc, c.dok_death_c, c.dok_death_n, p.period as mgFrom, p.period as mgTo
from c_kart_pr c, params p
where c.lsk = lsk_;
end if;

if lsk_ is null then
      --документы по льготам проживающих
      trunc_part('a_lg_docs', mg_);
insert /*+ APPEND */ into a_lg_docs
(id, c_kart_pr_id, doc, dat_begin, dat_end, main, mg)
select c.id,
       c.c_kart_pr_id,
       c.doc,
       c.dat_begin,
       c.dat_end,
       c.main,
       p.period
from c_lg_docs c, params p;

--льготы проживающих
trunc_part('a_lg_pr', mg_);
insert into a_lg_pr
(c_lg_docs_id, spk_id, type, mg)
select c.c_lg_docs_id, c.spk_id, c.type, p.period
from c_lg_pr c, params p;
else
      --документы по льготам проживающих
delete from a_lg_docs c where
        c.mg = mg_ and
    exists (select * from c_kart_pr p
            where p.lsk=lsk_
              and p.id=c.c_kart_pr_id);
insert into a_lg_docs
(id, c_kart_pr_id, doc, dat_begin, dat_end, main, mg)
select c.id,
       c.c_kart_pr_id,
       c.doc,
       c.dat_begin,
       c.dat_end,
       c.main,
       p.period
from c_lg_docs c, params p
where exists (select * from c_kart_pr p
              where p.lsk=lsk_
                and p.id=c.c_kart_pr_id);

--льготы проживающих
delete from a_lg_pr c where
        c.mg = mg_ and
    exists (select * from c_lg_docs d, c_kart_pr p
            where p.lsk=lsk_
              and p.id=d.c_kart_pr_id and d.id=c.c_lg_docs_id);
insert into a_lg_pr
(c_lg_docs_id, spk_id, type, mg)
select c.c_lg_docs_id, c.spk_id, c.type, p.period
from c_lg_pr c, params p where
    exists (select * from c_lg_docs d, c_kart_pr p
            where p.lsk=lsk_
              and p.id=d.c_kart_pr_id and d.id=c.c_lg_docs_id);
end if;

--пеня
if lsk_ is null then
      trunc_part('a_penya', mg_);
insert into a_penya
(summa, penya, mg1, mg, lsk, days)
select c.summa, c.penya, c.mg1, p.period, c.lsk, c.days
from c_penya c, params p;
else
delete from a_penya a
where a.lsk = lsk_
  and a.mg = mg_;
insert into a_penya
(summa, penya, mg1, mg, lsk, days)
select c.summa, c.penya, c.mg1, p.period, c.lsk, c.days
from c_penya c, params p
where c.lsk = lsk_;
end if;
--корректировки пени
if lsk_ is null then
delete from a_pen_corr a where a.mg=mg_;
insert into a_pen_corr
(id, lsk, penya, dopl, dtek, ts, fk_user, mg, fk_doc, usl, org)
select c.id, c.lsk, c.penya, c.dopl, c.dtek, c.ts, c.fk_user, p.period, c.fk_doc, c.usl, c.org
from c_pen_corr c, params p;
else
delete from a_pen_corr a
where a.lsk = lsk_
  and a.mg = mg_;
insert into a_pen_corr
(id, lsk, penya, dopl, dtek, ts, fk_user, mg, fk_doc, usl, org)
select c.id, c.lsk, c.penya, c.dopl, c.dtek, c.ts, c.fk_user, p.period, c.fk_doc, c.usl, c.org
from c_pen_corr c, params p
where c.lsk = lsk_;
end if;
--текущее начисление пени
if lsk_ is null then
      trunc_part('a_pen_cur', mg_);
insert into a_pen_cur
(lsk, mg1, curdays, summa2, penya, fk_stav, dt1, dt2, mg)
select c.lsk, c.mg1, c.curdays, c.summa2, c.penya, c.fk_stav, c.dt1, c.dt2, p.period as mg
from c_pen_cur c, params p;
else
delete from a_pen_cur a
where a.lsk = lsk_
  and a.mg = mg_;
insert into a_pen_cur
(lsk, mg1, curdays, summa2, penya, fk_stav, dt1, dt2, mg)
select c.lsk, c.mg1, c.curdays, c.summa2, c.penya, c.fk_stav, c.dt1, c.dt2, p.period as mg
from c_pen_cur c, params p
where c.lsk = lsk_;
end if;

--наборы услуг
if lsk_ is null then
--      delete /*+ INDEX (a A_NABOR2_I)*/ from a_nabor2 a
--       where mg_ between a.mgFrom and a.mgTo;
delete from a_nabor2 a
where mg_ between a.mgFrom and a.mgTo;
insert into a_nabor2
(id,
 lsk,
 usl,
 org,
 koeff,
 norm,
 fk_vvod,
 vol,
 vol_add,
 mgFrom,
 mgTo,
 fk_tarif,
 kf_kpr,
 nrm_kpr,
 nrm_kpr2,
 kf_kpr_wrz,
 kf_kpr_wro,
 kf_kpr_wrz_sch,
 kf_kpr_wro_sch,
 limit)
select a_nabor_id.nextval,
       c.lsk,
       c.usl,
       c.org,
       c.koeff,
       c.norm,
       c.fk_vvod,
       c.vol,
       c.vol_add,
       p.period, --одинак.период
       p.period,
       c.fk_tarif,
       c.kf_kpr,
       c.nrm_kpr,
       c.nrm_kpr2,
       c.kf_kpr_wrz,
       c.kf_kpr_wro,
       c.kf_kpr_wrz_sch,
       c.kf_kpr_wro_sch,
       c.limit
from nabor c, params p;
--сжать наборы
--    compress_nabor(null); -пока убрал 07.07.2015
else
--      delete /*+ INDEX (a A_NABOR2_I)*/ from a_nabor2 a
--       where a.lsk = lsk_
--         and mg_ between a.mgFrom and a.mgTo;
delete from a_nabor2 a
where a.lsk = lsk_
  and mg_ between a.mgFrom and a.mgTo;
insert into a_nabor2
(id,
 lsk,
 usl,
 org,
 koeff,
 norm,
 fk_vvod,
 vol,
 vol_add,
 mgFrom,
 mgTo,
 fk_tarif,
 kf_kpr,
 nrm_kpr,
 nrm_kpr2,
 kf_kpr_wrz,
 kf_kpr_wro,
 kf_kpr_wrz_sch,
 kf_kpr_wro_sch,
 limit)
select a_nabor_id.nextval,
       c.lsk,
       c.usl,
       c.org,
       c.koeff,
       c.norm,
       c.fk_vvod,
       c.vol,
       c.vol_add,
       p.period,
       p.period,
       c.fk_tarif,
       c.kf_kpr,
       c.nrm_kpr,
       c.nrm_kpr2,
       c.kf_kpr_wrz,
       c.kf_kpr_wro,
       c.kf_kpr_wrz_sch,
       c.kf_kpr_wro_sch,
       c.limit
from nabor c, params p
where c.lsk = lsk_;
end if;

-- Создаём архив начисления (без льгот и субсидий) по Л/C
if lsk_ is null then
      trunc_part('arch_charges', mg_);
insert into arch_charges
(lsk, usl_id, summa, summa_it, mg)
select n.lsk, n.usl, a.summa, a.summa, p.period
from nabor n,
     params p,
     (select lsk, usl, sum(summa) as summa
      from c_charge c
      where c.type = 1
      group by lsk, usl) a
where n.lsk = a.lsk(+)
  and n.usl = a.usl(+)
  and a.summa <> 0;
/* Убрал ненужное
                        union all
                        select lsk, usl, -1 * summa
                          from c_charge c
                         where c.type = 2
                        union all
                        select lsk, usl, -1 * summa
                          from c_charge c
                         where c.type = 4*/
else
delete from arch_charges a
where a.lsk = lsk_
  and a.mg = mg_;
insert into arch_charges
(lsk, usl_id, summa, summa_it, mg)
select n.lsk, n.usl, b.summa, a.summa, p.period
from nabor n,
     params p,
     (select lsk, usl, sum(summa) as summa
      from c_charge c
      where c.lsk=lsk_
        and c.type = 1
      group by lsk, usl) a,
     (select lsk, usl, sum(summa) as summa
      from (select lsk, usl, summa as summa
            from c_charge c
            where c.lsk=lsk_
              and c.type = 1
            union all
            select lsk, usl, -1 * summa
            from c_charge c
            where c.lsk=lsk_
              and c.type = 2
            union all
            select lsk, usl, -1 * summa
            from c_charge c
            where c.lsk=lsk_
              and c.type = 4)
      group by lsk, usl) b
where n.lsk = a.lsk(+)
  and n.usl = a.usl(+)
  and n.lsk = b.lsk(+)
  and n.usl = b.usl(+)
  and (a.summa <> 0 or b.summa <> 0)
  and n.lsk=lsk_;
end if;

--Добавляем закрытые лицевые и лицевые с пустым начисл. (чтоб тоже печатались в счетах)
if lsk_ is null then
insert into arch_charges
(lsk, usl_id, summa, mg, summa_it)
select lsk, '003', 0, p.period, 0
from kart t, params p
where not exists (select *
                  from arch_charges a
                  where a.mg = mg_
                    and a.lsk = t.lsk);
end if;

-- Создаём архивные карточки лицевых
if lsk_ is null then
      trunc_part('arch_kart', mg_);
insert into arch_kart --данный sql выполняется строго после insert into arch_charges!
(lsk, kul, nd, kw, fio, kpr, kpr_wr, kpr_ot,
 kpr_cem, kpr_s, opl, ppl, pldop, ki, psch,
 psch_dt, status, kwt, lodpl, bekpl, balpl,
 komn, et, kfg, kfot, phw, mhw,
 pgw, mgw, pel, mel, sub_nach, subsidii,
 sub_data, polis, sch_el, reu, text, schel_dt,
 eksub1, eksub2, kran, kran1, el, el1, sgku,
 doppl, subs_cor, house_id, c_lsk_id,
 mg1, mg2, kan_sch, subs_inf,
 k_lsk_id, dog_num, schel_end, fk_deb_org,
 subs_cur, k_fam, k_im, k_ot, memo, fk_distr,
 law_doc, law_doc_dt, prvt_doc, prvt_doc_dt,
 fk_pasp_org, fk_err, mg, dolg, cpn, penya,
 kpr_wrp, pn_dt, fk_tp, fact_meter_tp, for_bill, sel1, vvod_ot, entr, pot, mot, parent_lsk, kpr_own, fk_klsk_premise)
select
    k.lsk, k.kul, k.nd, k.kw, k.fio, k.kpr, k.kpr_wr, k.kpr_ot,
    k.kpr_cem, k.kpr_s, k.opl, k.ppl, k.pldop, k.ki, k.psch,
    k.psch_dt, k.status, k.kwt, k.lodpl, k.bekpl, k.balpl,
    k.komn, k.et, k.kfg, k.kfot, k.phw, k.mhw,
    k.pgw, k.mgw, k.pel, k.mel, k.sub_nach, k.subsidii,
    k.sub_data, k.polis, k.sch_el, k.reu, k.text, k.schel_dt,
    k.eksub1, k.eksub2, k.kran, k.kran1, k.el, k.el1, k.sgku,
    k.doppl, k.subs_cor, k.house_id, k.c_lsk_id,
    k.mg1, k.mg2, k.kan_sch, k.subs_inf,
    k.k_lsk_id, k.dog_num, k.schel_end, k.fk_deb_org,
    k.subs_cur, k.k_fam, k.k_im, k.k_ot, k.memo, k.fk_distr,
    k.law_doc, k.law_doc_dt, k.prvt_doc, k.prvt_doc_dt,
    k.fk_pasp_org, k.fk_err, p.period, a.dolg, k.cpn,
    nvl(d.penya,0)-nvl(b.penya,0) as penya,
    k.kpr_wrp, pn_dt, k.fk_tp, k.fact_meter_tp,
    case when nvl(e.summa,0) <> 0 or nvl(b.penya,0) <> 0 or nvl(b.dolg/*b.penya*/,0) <> 0 then 1 --добавил пеню 09.03.2016, счет будет выбираться для печати если есть долг или текущ начисление))
         else 0 end as for_bill, k.sel1, k.vvod_ot, k.entr, k.pot, k.mot, k.parent_lsk, k.kpr_own,
    k.fk_klsk_premise
from kart k, params p,
     (select t.k_lsk_id, nvl(sum(summa),0) as dolg from saldo_usl s, kart t where
             t.lsk=s.lsk and s.mg=mg1_
      group by t.k_lsk_id) a,
     (select c.lsk, sum(c.penya) as penya, sum(c.summa) as dolg
      from c_penya c
      group by c.lsk) b,
     (select c.lsk, sum(c.penya) as penya
      from a_penya c
      where c.mg=old_mg_
      group by c.lsk) d,
     (select a.lsk, sum(a.summa) as summa
      from scott.c_charge a where
              nvl(a.summa,0) <>0
                              and a.type=1
      group by a.lsk) e --полное начисление (по тарифу)
where k.k_lsk_id=a.k_lsk_id(+)
  and k.lsk=b.lsk(+)
  and k.lsk=d.lsk(+)
  and k.lsk=e.lsk(+);
--обновление № листов (важен порядок вызова процедур!)
upd_arch_kart2(/*null, */p_mg => mg_);
else
      upd_acrh_kart(p_lsk => lsk_,
                    p_mg => mg_,
                    p_mg1 => mg1_,
                    p_old_mg => old_mg_);
end if;

-- Создаём архив изменений начисления по Л/C
if lsk_ is null then
      trunc_part('arch_changes', mg_);
insert into arch_changes
(lsk, usl_id, summa, mg, id, show_bill, proc)
select c.lsk, c.usl, sum(c.summa), p.period, c.type, c.show_bill, sum(proc) as proc
from kart k, c_change c, params p --раз.изм.текущие
where k.lsk = c.lsk
  and to_char(c.dtek, 'YYYYMM') = p.period --по дате
group by c.lsk, c.usl, p.period, c.type, c.show_bill;
else
delete from arch_changes a
where a.lsk=lsk_ and a.mg = mg_;
insert into arch_changes
(lsk, usl_id, summa, mg, id, show_bill, proc)
select c.lsk, c.usl, sum(c.summa), p.period, c.type, c.show_bill, sum(proc) as proc --раз.изм.текущие
from kart k, c_change c, params p
where k.lsk = c.lsk
  and to_char(c.dtek, 'YYYYMM') = p.period --по дате
  and k.lsk=lsk_
group by c.lsk, c.usl, p.period, c.type, c.show_bill;
end if;

-- Создаём архив субсидий по Л/C
if lsk_ is null then
      trunc_part('arch_subsidii', mg_);
insert into arch_subsidii
(lsk, usl_id, summa, mg)
select lsk, usl, sum(summa), p.period
from c_charge c, params p
where c.type = 2
group by lsk, usl, p.period;
else
delete from arch_subsidii a
where a.lsk=lsk_ and a.mg = mg_;
insert into arch_subsidii
(lsk, usl_id, summa, mg)
select lsk, usl, sum(summa), p.period
from c_charge c, params p
where c.type = 2
  and c.lsk=lsk_
group by lsk, usl, p.period;
end if;

-- Создаём архив льгот по Л/C
if lsk_ is null then
      trunc_part('arch_privs', mg_);
insert into arch_privs
(lsk, summa, usl_id, lg_id, mg, cnt_main, cnt)
select lsk,
       sum(summa),
       usl_id,
       lg_id,
       p.period,
       sum(c.main),
       count(*)
from privs c, params p
group by c.lsk, c.usl_id, p.period, lg_id;
else
delete from arch_privs a
where a.lsk=lsk_ and a.mg = mg_;
insert into arch_privs
(lsk, summa, usl_id, lg_id, mg, cnt_main, cnt)
select lsk,
       sum(summa),
       usl_id,
       lg_id,
       p.period,
       sum(c.main),
       count(*)
from privs c, params p
where c.lsk=lsk_
group by c.lsk, c.usl_id, p.period, lg_id;
end if;

-- Создаём архив подготовительных объемов для начисления
if lsk_ is null then
      --trunc_part('a_charge_prep', mg_);
delete from a_charge_prep2 a
where mg_ between a.mgFrom and a.mgTo;
insert into a_charge_prep2
(lsk, usl, vol, kpr, kprz, kpro, sch, dt1, dt2, tp, vol_nrm, vol_sv_nrm, kpr2, opl, fk_spk, mgFrom, mgTo)
select
    lsk, usl, vol, kpr, kprz, kpro, sch, dt1, dt2, tp, vol_nrm, vol_sv_nrm, kpr2, opl, fk_spk, mg_ as mgFrom, mg_ as mgTo
from c_charge_prep t;
else
delete from a_charge_prep2 a
where a.lsk=lsk_ and mg_ between a.mgFrom and a.mgTo;
insert into a_charge_prep2
(lsk, usl, vol, kpr, kprz, kpro, sch, dt1, dt2, tp, vol_nrm, vol_sv_nrm, kpr2, opl, fk_spk, mgFrom, mgTo)
select
    lsk, usl, vol, kpr, kprz, kpro, sch, dt1, dt2, tp, vol_nrm, vol_sv_nrm, kpr2, opl, fk_spk, mg_ as mgFrom, mg_ as mgTo
from c_charge_prep t
where t.lsk=lsk_;
end if;

--обновляем период для отчета
if lsk_ is null then
    logger.ins_period_rep('12', mg_, null, 0); --тип отчета (архивы)
logger.ins_period_rep('52', mg_, null, 0); --списки по субсидии
logger.ins_period_rep('56', mg_, null, 0); --Список льготников
logger.ins_period_rep('58', mg_, null, 0); --Список квартиросъемщиков, имеющих счетчики учета воды
logger.ins_period_rep('60', mg_, null, 0); --тип статистика по программам/пакетам (Э+)
logger.ins_period_rep('62', mg_, null, 0);
logger.ins_period_rep('63', mg_, null, 0);
logger.ins_period_rep('64', mg_, null, 0);

logger.ins_period_rep('66', mg_, null, 0);
logger.ins_period_rep('67', mg_, null, 0);
logger.ins_period_rep('68', mg_, null, 0);
logger.ins_period_rep('73', mg_, null, 0);
logger.ins_period_rep('74', mg_, null, 0);
logger.ins_period_rep('75', mg_, null, 0);
logger.ins_period_rep('77', mg_, null, 0);
logger.ins_period_rep('79', mg_, null, 0);
logger.ins_period_rep('80', mg_, null, 0);

logger.ins_period_rep('84', mg_, null, 0);
logger.ins_period_rep('85', mg_, null, 0);
logger.ins_period_rep('86', mg_, null, 0);
logger.ins_period_rep('88', mg_, null, 0);
logger.ins_period_rep('91', mg_, null, 0);
logger.ins_period_rep('92', mg_, null, 0);
logger.ins_period_rep('93', mg_, null, 0);
logger.ins_period_rep('94', mg_, null, 0);

logger.log_(time_, 'gen.prepare_arch');
end if;
commit;
end prepare_arch;

procedure upd_acrh_kart(p_lsk in kart.lsk%type,
     p_mg in params.period%type,
     p_mg1 in params.period%type,
     p_old_mg in params.period%type
     ) is
  p_rec arch_kart%rowtype;
  begin
    begin
select a.* into p_rec
from arch_kart a
where a.lsk = p_lsk
  and a.mg = p_mg and rownum=1; --rownum=1 - потому что по каким то причинам, (у П. бывают по две записи в arch_kart!!!)
exception
    --обработка ситуации, когда еще не было записей в архиве
      when no_data_found then
        null;
end;
delete from arch_kart a
where a.lsk = p_lsk
  and a.mg = p_mg;
insert into arch_kart --данный sql выполняется строго после insert into arch_charges!
(lsk, kul, nd, kw, fio, kpr, kpr_wr, kpr_ot,
 kpr_cem, kpr_s, opl, ppl, pldop, ki, psch,
 psch_dt, status, kwt, lodpl, bekpl, balpl,
 komn, et, kfg, kfot, phw, mhw,
 pgw, mgw, pel, mel, sub_nach, subsidii,
 sub_data, polis, sch_el, reu, text, schel_dt,
 eksub1, eksub2, kran, kran1, el, el1, sgku,
 doppl, subs_cor, house_id, c_lsk_id,
 mg1, mg2, kan_sch, subs_inf,
 k_lsk_id, dog_num, schel_end, fk_deb_org,
 subs_cur, k_fam, k_im, k_ot, memo, fk_distr,
 law_doc, law_doc_dt, prvt_doc, prvt_doc_dt,
 fk_pasp_org, fk_err, mg, dolg, cpn, penya,
 kpr_wrp, pn_dt, fk_tp, for_bill, prn_num, prn_new, sel1, vvod_ot, entr, pot, mot, kpr_own, fk_klsk_premise)
select
    k.lsk, k.kul, k.nd, k.kw, k.fio, k.kpr, k.kpr_wr, k.kpr_ot,
    k.kpr_cem, k.kpr_s, k.opl, k.ppl, k.pldop, k.ki, k.psch,
    k.psch_dt, k.status, k.kwt, k.lodpl, k.bekpl, k.balpl,
    k.komn, k.et, k.kfg, k.kfot, k.phw, k.mhw,
    k.pgw, k.mgw, k.pel, k.mel, k.sub_nach, k.subsidii,
    k.sub_data, k.polis, k.sch_el, k.reu, k.text, k.schel_dt,
    k.eksub1, k.eksub2, k.kran, k.kran1, k.el, k.el1, k.sgku,
    k.doppl, k.subs_cor, k.house_id, k.c_lsk_id,
    k.mg1, k.mg2, k.kan_sch, k.subs_inf,
    k.k_lsk_id, k.dog_num, k.schel_end, k.fk_deb_org,
    k.subs_cur, k.k_fam, k.k_im, k.k_ot, k.memo, k.fk_distr,
    k.law_doc, k.law_doc_dt, k.prvt_doc, k.prvt_doc_dt,
    k.fk_pasp_org, k.fk_err, p.period, a.dolg, k.cpn,
    nvl(d.penya,0)-nvl(b.penya,0) as penya,
    k.kpr_wrp, pn_dt, k.fk_tp,
    --case when nvl(e.summa,0) <> 0 or nvl(b.dolg/*b.penya*/,0) <> 0 then 1 --счет будет выбираться для печати если есть долг или текущ начисление))
    case when nvl(e.summa,0) <> 0 or nvl(b.penya,0) <> 0 or nvl(b.dolg/*b.penya*/,0) <> 0 then 1 --добавил пеню 09.03.2016, счет будет выбираться для печати если есть долг или текущ начисление))
         else 0 end as for_bill, p_rec.prn_num, p_rec.prn_new, k.sel1, k.vvod_ot, k.entr, k.pot, k.mot, k.kpr_own,
    k.fk_klsk_premise
from kart k, params p,
     (select t.k_lsk_id, nvl(sum(summa),0) as dolg from saldo_usl s, kart t where
             t.lsk=s.lsk and s.mg=p_mg1
      group by t.k_lsk_id) a,
     (select sum(c.penya) as penya, sum(c.summa) as dolg
      from c_penya c
      where c.lsk = p_lsk) b,
     (select sum(c.penya) as penya
      from a_penya c
      where c.mg=p_old_mg
        and c.lsk = p_lsk) d,
     (select a.lsk, sum(a.summa) as summa
      from scott.c_charge a where
              nvl(a.summa,0) <>0
                              and a.type=1
      group by a.lsk) e --полное начисление (по тарифу)
where k.k_lsk_id=a.k_lsk_id(+)
  and k.lsk = p_lsk
  and k.lsk=e.lsk(+);
end;

procedure upd_arch_kart2(/*p_klsk in number, */p_mg in params.period%type) is
 i number;
k_lsk_old number;
l_mg params.period%type;
begin
--обновление порядк номера листа для печати
--выполняется только для всех счетов!!! (для лиц. счета - не имеет смысла)

if p_mg is null then
select p.period into l_mg from params p;
else
    l_mg:=p_mg;
end if;

-- TODO навести порядок с тем что p_klsk больше не используется
--if p_klsk is null then
update arch_kart t set t.prn_num=null
where t.mg=l_mg;
--end if;
-- в kart_detail prn_num используется в арх.справке, для ускорения запроса
update kart_detail t set t.prn_num=null;

i:=0;
  --if p_klsk is null then
for c in (select t.rowid as rd, t.k_lsk_id, tp.cd as lsk_tp
      from arch_kart t, spul s, v_lsk_tp tp where t.mg=l_mg
      and t.fk_tp=tp.id --and t.for_bill=1 ред.05.06.2019 - нарушается порядок вывода лиц.счетов (смотреть переписку с Кис. от 05.06.2019 в skype)
      and t.kul=s.id
      order by s.name, scott.utils.f_ord_digit(t.nd),
       scott.utils.f_ord3(t.nd) desc,
       scott.utils.f_ord_digit(t.kw),
       scott.utils.f_ord3(t.kw) desc,
        t.k_lsk_id, tp.npp
    )
  loop
    -- порядок печати счета
    i:=i+1;
update arch_kart t set t.prn_num=i
where t.rowid=c.rd;

end loop;
--end if;
-- в kart_detail prn_num используется в арх.справке, для ускорения запроса
update kart_detail t set t.prn_num=(select k.prn_num from arch_kart k where k.mg=l_mg and k.lsk=t.lsk);
end;

procedure gen_stat_debits is
    stmt varchar2(2500);
type_otchet constant number := 22; --тип отчета
dat_   date;
time1_ date;
  begin
time1_ := sysdate;
select period_debits into dat_ from params;
delete from debits_kw where dat = (select period_debits from params);
delete from debits_houses
where dat = (select period_debits from params);
delete from debits_trest
where dat = (select period_debits from params);

insert into debits_kw
(reu, kul, nd, kw, lsk, summa, mg, dat, kol_month)
select k.reu, kul, nd, kw, d.lsk, summa, mg, period_debits, kol
from kart k,
     params p,
     debits d,
     (select lsk, count(*) kol
      from debits
      where summap <> 0
      group by lsk) u
where d.lsk = u.lsk(+)
  and d.lsk = k.lsk;

insert into debits_houses
(reu, kul, nd, summa, mg, dat, kol_month)
select reu, kul, nd, sum(summa), mg, period_debits, kol_month
from debits_kw, params
where dat = period_debits
group by reu, kul, nd, mg, period_debits, kol_month;

insert into debits_trest
(reu, summa, mg, dat, kol_month)
select reu, sum(summa), mg, period_debits, kol_month
from debits_houses, params
where dat = period_debits
group by reu, mg, period_debits, kol_month;

delete from period_reports p
where p.id = type_otchet
  and to_char(dat, 'YYYYMMDD') = to_char(dat_, 'YYYYMMDD'); --обновляем период для отчета

stmt := 'insert into period_reports (id, dat,signed) values(:id, :dat,1)';
execute immediate stmt
      using type_otchet, dat_;
logger.log_(time1_, 'gen.oraparser stat_debits');
commit;
end;

procedure go_next_month_year is
  begin
    --Переход на следующий месяц - год
c_charges.trg_proc_next_month:=1;
go_nye_phase1;
go_nye_phase2;
-- go_nye_phase3; отключил, с 01.03.23
c_charges.trg_proc_next_month:=0;
end go_next_month_year;

-- I - этап (сам переход)
procedure go_nye_phase1 is
    type_otchet_  constant number := 50; --для таблицы long_table
type_otchet2_ constant number := 51; --новые архивы
cnt_ number;
mg_ params.period%type;
  begin
logger.log_(null, 'Начало I фазы перехода...');
select period into mg_ from params;
--Проверка
--наличие оплаты непроинкассированной
select nvl(count(*),0)
into cnt_
from c_kwtp c
where to_char(c.dtek, 'YYYYMM') = mg_
  and nvl(c.nink, 0) = 0;
if cnt_ <> 0 then
        raise_application_error(-20001,
                                'Стоп, есть не проинкассированные средства, в сумме:' ||
                                to_char(cnt_));
return;
end if;

/* достало 31.12.21 Клиенты сами несут ответственность, что они не сделали итоговое
    if init.get_state = 0 then
      raise_application_error(-20001,
                              'Стоп, не выполнено итоговое формирование за месяц, переход не возможен!');
      return;
    end if;
*/
--повторное формирование движения нужно, так как некоторые РКЦ принимают оплату уже будущим периодом
--(до очистки итоговых таблиц!)
-- ред.24.04.2019 - не понял, зачем это нужно? - отключил пока
--logger.log_(null, 'Повторное формирование движения, начало');
--c_cpenya.gen_charge_pay_pen;
--logger.log_(null, 'Повторное формирование движения, окончание');
---

-- чистим итоговые таблицы
logger.log_(null, 'Очистка итоговых таблиц, начало');
gen.gen_clear_tables;
logger.log_(null, 'Очистка итоговых таблиц, окончание');

    --периоды пени (добавляем, в случае их отсутствия)
insert into c_spr_pen
(mg, dat, fk_lsk_tp, reu)
select t.period1, add_months(p.dat,1), p.fk_lsk_tp, p.reu
from c_spr_pen p, v_params t
where p.mg=t.period and not exists
    (select * from c_spr_pen p1,
                   v_params m where p1.mg=m.period1
                                and p1.fk_lsk_tp=p.fk_lsk_tp
    );

--периоды новых архивов фиксируем здесь
delete from period_reports p
where p.id in (type_otchet2_)
  and p.mg = mg_; --обновляем период для отчета
insert into period_reports
(id, mg, signed)
select type_otchet2_, mg_, 1 from dual;

--узнаем, будет ли переход года
--если будет, сбрасывам номера квитанций и инкассаций
select case when substr(mg_,1,4) <>
                 substr(to_char(add_months(
                                        to_date(mg_ || '01', 'YYYYMMDD'), 1), 'YYYYMM'),1,4) then 1
            else 0 end into cnt_
from dual;
if cnt_ = 1 then
update c_comps t set t.nink=1, t.nkvit=1;
end if;
-- меняем отчетный период
update params
set period    = to_char(add_months(to_date(period || '01', 'YYYYMMDD'),
                                   1),
                        'YYYYMM'),
    period_pl = to_char(add_months(to_date(period || '01', 'YYYYMMDD'),
                                   1),
                        'YYYYMM');
select period into mg_ from params;
--обязательно меняем отчетный период глобальных переменных (чтоб их!)
init.g_dt_start:=to_date(mg_||'01', 'YYYYMMDD');
init.g_dt_end:=last_day(init.g_dt_start);

    --нулим текущий период компьютеров
update c_comps t set t.period=null
where t.period is not null;
delete from period_reports p
where p.id in (type_otchet_)
  and p.mg = (select period from params); --обновляем период для таблицы long_table

--в форму по тарифам добавить период
logger.ins_period_rep('78', mg_, null, 0);
    --новый период для сверки инкассаций
logger.ins_period_rep('36', mg_, null, 0);
    -- обновляем текущие даты формирования
init.set_date_for_gen;
commit;  --здесь коммит перехода, I - фазы
logger.log_(null, 'Окончание I - фазы перехода...');
end;

-- II - этап
procedure go_nye_phase2 is
    part_  number;
type_otchet_  constant number := 50; --для таблицы long_table
  begin
logger.log_(null, 'Начало II - фазы перехода...');
select p.part into part_ from params p;
-- добавляем, удаляем партиции
if nvl(part_, 0) = 1 then
      gen.gen_del_add_partitions;
end if;

--обновляем последний период оплаты, раз изменений
--на 12 мес.вперёд
update params p set p.period_forwrd=
                        (select to_char(add_months(to_date(p.period||'01','YYYYMMDD'),12),'YYYYMM') as mg
                         from params p);

insert into period_reports
(id, mg)
values
    (type_otchet_,
     (select to_char(add_months(to_date(period || '01', 'YYYYMMDD'), 60),
                     'YYYYMM')
      from params)); -- +60 месяцев
--установить новые месяцы в long_table, чтобы каждый раз не вычислять
insert into long_table
select "MG" from (
                     select to_char(add_months(to_date(p.period||'01','YYYYMMDD'),-1*a.lvl+1),'YYYYMM')  as mg
                     from (select level as lvl
                           from dual
                                    connect by level < 1200) a, params p
                 ) a
where to_char(mg) >= utils.get_str_param('FIRST_MONTH')
  and not exists
    (select * from long_table s where s.mg=a.mg);


/*  ред.24.04.2019 - не понятно зачем это здесь, для кого? пока закомментировал
  logger.log_(null, 'Переход-распределение оплаты, принятой будущим периодом, начало');
     if utils.get_int_param('DIST_PAY_TP') = 0 then
     --по-сальдовый способ распределения оплаты
       c_gen_pay.dist_pay_lsk_force;
     else
     --по-периодный способ распределения оплаты
       null; --пока не написал
     end if;
    logger.log_(null, 'Переход-распределение оплаты, принятой будущим периодом, окончание');
  */
logger.log_(null, 'Переход-установка признаков закрытых домов начало');
update c_houses t set t.psch=1
where nvl(t.psch,0)=0 and
    not exists (select * from kart k where k.house_id=t.id
                                       and k.psch not in (8,9));
commit;
logger.log_(null, 'Переход-установка признаков закрытых домов окончание');
    --обнуляем vol_add по услуге с fk_calc=23 (Эл.энерг.МОП), ред.26.03.13
logger.log_(null, 'Переход-начало обнуления расхода по fc_calc=23 услуге');
update nabor n set n.vol_add=null where exists
                                            (select * from usl u where u.usl=n.usl and u.fk_calc_tp=23);
commit;
logger.log_(null, 'Переход-Окончание обнуления расхода по fc_calc=23 услуге');

    --устанавливаем кол-во прожив, с учетом выбывших и зарегистрированных
    --(например человек прописался в конце прошлого месяца)

    --изменяем признаки счетчиков, в л.с. где они должны поменяться по срокам
logger.log_(null, 'Переход-utils.upd_krt_sch_state начало');
utils.upd_krt_sch_state(null);
logger.log_(null, 'Переход-utils.upd_krt_sch_state окончание');
    --изменяем признаки проживающих, в л.с. где они должны поменяться по срокам
logger.log_(null, 'Переход-utils.upd_c_kart_pr_state начало');
utils.upd_c_kart_pr_state(null);
logger.log_(null, 'Переход-utils.upd_c_kart_pr_state окончание');
    --сброс признака итогового формирования отчетов
init.set_state(0);
commit; --коммит по окончанию I фазы
--после I фазы, открываем базу
admin.set_state_base(0);
    --выполнение автоначисление счетчиков
    --logger.log_(null, 'Переход-gen.auto_charge начало');
    -- ред.24.04.2019 WTF??? Что это?? для кого это???
    --auto_charge;
    --logger.log_(null, 'Переход-gen.auto_charge окончание');
    --подготовка сальдо, для возможности распределения начисленной пени по услугам
gen_saldo(null);
--подготовка послепереходной информации
-- ред.24.04.2019 - убрал нафиг
--l_cnt:=c_charges.gen_charges(null, null, null, null, 1, 0);

--сформировать оборотную ведомость обязательно!
--иначе будут некорректно распределяться платежи в c_dist_pay (xitog3_lsk нужен текущего периода)
-- ред.24.04.2019 - убрал нафиг
--gen_saldo(null);
--gen_saldo_houses;

--ЗАЧЕМ здесь выполнять формирование движения и пени??
--если при вызове л.с, и прочем они всё равно рассчитываются
--(тем более что помечены как для пересчета выше)
--ред.22.11.12

--движение по лицевым за новый период
--c_cpenya.gen_charge_pay(null, 1);
--пеня за новый период
--for c in (select distinct lsk from kart) loop
--  c_cpenya.gen_penya(c.lsk, 0, 0);
--end loop;

commit;
-- ПОСЛЕ КОММИТА ОБНОВИТЬ PARAMS В JAVA Обязательно! Иначе может формировать Итоговое некорректно! ред.04.06.2019
logger.log_(null, 'Переход: обновление Params в Java - начало');
p_java.reloadParams;
logger.log_(null, 'Переход: обновление Params в Java - окончание');

logger.log_(null, 'Окончание II - фазы перехода...');
end;

-- III - этап
procedure go_nye_phase3 is
  l_p_mg1 params.period%type;
l_p_mg2 params.period%type;
l_cd_org t_org.cd%type;
  begin
logger.log_(null, 'Начало III - фазы перехода...');
if utils.get_int_param('HAVE_LK') = 1 then
       --Если присутствует функция личного кабинета
       --то выполнение перехода в нём
        logger.log_(null, 'Начало выполнения перехода в ЛК...');
select o.cd into l_cd_org
from scott.t_org o, scott.t_org_tp tp
where tp.id=o.fk_orgtp and tp.cd='РКЦ';
execute immediate 'begin proc.go_next_month_year@apex(:cd_org_); end;'
        using l_cd_org;
logger.log_(null, 'Окончание выполнения перехода в ЛК...');
        /*logger.log_(null, 'Начало выгрузки архивов в ЛК...'); ред. 01.04.22 - убрал выгрузку архивов во время перехода. Пусть делается в обмене с ЛК
        --отправляем архивы
        execute immediate
          'select min(t.mg), max(t.mg) from t_mg@Apex t, t_org@Apex o, scott.params p
             where t.mg<>p.period and t.fk_org=o.id
             and o.cd=:cd_org_'
          into l_p_mg1, l_p_mg2 using l_cd_org;
        ext_pkg.exp_base(1, l_p_mg1, l_p_mg2);
        logger.log_(null, 'Окончание выгрузки архивов в ЛК...');*/
end if;
logger.log_(null, 'Окончание III - фазы перехода...');
end;

procedure gen_clear_dates is
  begin
  --Выполняется после смены периода в params (во время перехода)
  --отменить не возможно!
  --чистка статусов вр.зарегистр c просроченной датой
null;
end;

procedure gen_clear_tables is
  begin
    --Выполнять после сброса таблиц в архив!!!!

    --чистка кубов и корректировки субс по л/счетам и по домам
p_vvod.g_tp:=3;--установить глобальную переменную - признак очистки по переходу
update kart k set k.mhw = 0, k.mgw = 0, k.mel = 0, k.mot = 0, k.subs_cor = 0;
p_vvod.g_tp:=0;

update c_vvod c
set c.kub     = 0,
    c.kub_man = 0,
    c.kpr     = 0,
    c.kub_sch = 0,
    c.sch_cnt = 0,
    c.sch_kpr = 0,
    c.cnt_lsk = 0,
    c.vol_add = 0,
    c.vol_add_fact = 0,
    c.kub_fact = 0,
    c.itg_fact = 0,
    c.opl_add = 0,
    c.kub_norm = 0,
    c.kub_nrm_fact = 0,
    c.kub_sch_fact = 0,
    c.opl_ar = 0,
    c.kub_ar = 0,
    c.kub_ar_fact = 0,
    c.kub_dist = 0,
    c.kub_fact_upnorm=0,
    c.nrm = null;

--чистка таблиц от информации текущего месяца
-- сначала c_change, затем c_change_docs, иначе блокируется! и индекс на c_change.doc_id нужен!!!
delete from c_change t where t.dtek between init.g_dt_start and init.g_dt_end;
delete from c_change_docs t where t.dtek between init.g_dt_start and init.g_dt_end;
delete from c_kwtp t where t.dat_ink between init.g_dt_start and init.g_dt_end;
--корректировки пени
delete from c_pen_corr t;
-- чистка прошлых периодов
delete from xxito10 t
where dat <
      (select add_months(to_date(period, 'YYYYMM'), -1) from params)
  and t.mg is null;
delete from xito5 t
where dat <
      (select add_months(to_date(period, 'YYYYMM'), -1) from params)
  and t.mg is null;
delete from xito5_ t
where dat <
      (select add_months(to_date(period, 'YYYYMM'), -1) from params)
  and t.mg is null;
delete from xxito11 t
where dat <
      (select add_months(to_date(period, 'YYYYMM'), -1) from params)
  and t.mg is null;
delete from xxito12 t
where dat <
      (select add_months(to_date(period, 'YYYYMM'), -1) from params)
  and t.mg is null;

/*delete from xxito14 t -- ред. 10.03.20 не надо чистить эту таблицу, так как данные по дням используются при запросе за месяц!
     where dat <
           (select add_months(to_date(period, 'YYYYMM'), -1) from params)
           and t.mg is null;*/

delete from debits_kw
where dat <
      (select add_months(to_date(period, 'YYYYMM'), -1) from params);
delete from debits_houses
where dat <
      (select add_months(to_date(period, 'YYYYMM'), -1) from params);
delete from debits_trest
where dat <
      (select add_months(to_date(period, 'YYYYMM'), -1) from params);
--удалить даты их периодов отчёта, ранее -1 мес
delete from period_reports s
where dat <
      (select add_months(to_date(period, 'YYYYMM'), -1) from params)
  and not exists (select * from reports r where r.id=s.id
                                            and r.cd in ('2', '3', '4', '5', '7', '14', '15', '17', '35', '61'));
-- удалить определенные типы из t_objxpar, позднее 3 лет - нельзя удалять показания счетчиков, люди их смотрят и раньше 5 лет! ред.27.12.22
--delete from t_objxpar t where exists
--                                  (select * from u_list u, params p where u.id=t.fk_list
--                                                                      and t.mg < to_char(add_months(to_date(p.period, 'YYYYMM'), -24),'YYYYMM')
--                                                                      and u.cd in ('ins_vol_sch','ins_sch'));
--поставил коммит, ред.10.01.13
commit;
end gen_clear_tables;

procedure gen_del_add_partitions is
    l_period1 char(6); -- след период
l_period2 char(6); -- след период +1 месяц
  begin
select t.period1, utils.add_months_pr(t.period1,1)
into l_period1, l_period2
from v_params t;

--make_part('c_chargepay', 'arch', 'MG' || l_period1, l_period1);

make_part('saldo_usl',
             'data',
             'MG' || l_period2, l_period2);
make_part('arch_changes', 'arch', 'MG' || l_period1, l_period1);
make_part('arch_charges', 'arch', 'MG' || l_period1, l_period1);
make_part('arch_kart', 'arch', 'MG' || l_period1, l_period1);
make_part('arch_kwtp', 'arch', 'MG' || l_period1, l_period1);
make_part('arch_privs', 'arch', 'MG' || l_period1, l_period1);
make_part('arch_subsidii', 'arch', 'MG' || l_period1, l_period1);
make_part('a_prices', 'arch', 'MG' || l_period1, l_period1);
make_part('a_change', 'arch', 'MG' || l_period1, l_period1);
make_part('a_change_docs', 'arch', 'MG' || l_period1, l_period1);
--a_charge теперь партицировано по LSK!
--make_part('a_charge', 'arch', 'MG' || l_period1, l_period1);
make_part('a_houses', 'arch', 'MG' || l_period1, l_period1);
make_part('a_kart_pr', 'arch', 'MG' || l_period1, l_period1);
make_part('a_kwtp', 'arch', 'MG' || l_period1, l_period1);
make_part('a_kwtp_mg', 'arch', 'MG' || l_period1, l_period1);
make_part('a_lg_docs', 'arch', 'MG' || l_period1, l_period1);
make_part('a_lg_pr', 'arch', 'MG' || l_period1, l_period1);

--a_nabor теперь партицирован по LSK!
--make_part('a_nabor', 'arch', 'MG' || l_period1, l_period1);
make_part('a_penya', 'arch', 'MG' || l_period1, l_period1);
make_part('a_spk_usl', 'arch', 'MG' || l_period1, l_period1);
make_part('a_vvod', 'arch', 'MG' || l_period1, l_period1);
make_part('xitog3', 'arch', 'MG' || l_period1, l_period1);
make_part('statistics', 'arch', 'MG' || l_period1, l_period1);
make_part('statistics_lsk', 'arch', 'MG' || l_period1, l_period1);
make_part('xitog3_lsk', 'arch', 'MG' || l_period1, l_period1);
make_part('xito_lg4', 'arch', 'MG' || l_period1, l_period1);
make_part('t_corrects_payments', 'arch', 'MG' || l_period1, l_period1);
make_part('expkartw', 'arch', 'MG' || l_period1, l_period1);
make_part('expprivs', 'arch', 'MG' || l_period1, l_period1);
make_part('xxito10', 'arch', 'MG' || l_period1, l_period1);
make_part('xxito11', 'arch', 'MG' || l_period1, l_period1);
make_part('xxito12', 'arch', 'MG' || l_period1, l_period1);
make_part('xxito14', 'arch', 'MG' || l_period1, l_period1);
make_part('xxito14_lsk', 'arch', 'MG' || l_period1, l_period1);

--a_charge_prep теперь партицирован по LSK!
--make_part('a_charge_prep', 'arch', 'MG' || l_period1, l_period1);
make_part('a_kwtp_day', 'arch', 'MG' || l_period1, l_period1);
make_part('a_pen_cur', 'arch', 'MG' || l_period1, l_period1);
make_part('log_actions', 'arch', 'MG' || l_period2, l_period2);
end;

procedure make_part(tablename_ in varchar2,
                      tabspc_    in varchar2,
                      partname_  in varchar2,
                      mg_        in varchar2) is
    stmt varchar2(500);
i    number;
  begin
    --удаляем партиции начиная от +12 месяцев вперёд и до текущего периода
   -- dbms_output.enable;
--    i := 12;
--    loop
--      exit when i = -1;
      --begin
      /*  stmt := 'alter table ' || tablename_ || '
              drop partition MG' ||
                to_char(add_months(to_date(mg_, 'YYYYMM'), i), 'YYYYMM') || '' ||
                ' update global indexes';*/
   -- dbms_output.put_line(stmt);
      --  execute immediate stmt;
      --exception
      --  when others then
      --    null;
      --end;
--      i := i - 1;
--    end loop;
    --создаем партицию
stmt := 'alter table ' || tablename_ || '
          add partition ' || partname_ || '
          values less than(''' || mg_ || ''')
          tablespace ' || tabspc_ || '' || '';
    begin
execute immediate stmt;
exception
      when others then
        logger.log_(null, 'ВНИМАНИЕ! ОШИБКА СОЗДАНИЯ ПАРТИЦИИ tablename='||tablename_||' partname='||partname_);
logger.log_(null, 'ВНИМАНИЕ! ОШИБКА СОЗДАНИЯ ПАРТИЦИИ ErrorCode='||sqlcode||' ErrorMessage='||SQLERRM);
end;

end;

-- создание партиции расширенное
procedure make_part2(tablename_ in varchar2,
                      tabspc_    in varchar2,
                      mg_        in varchar2,
                      p_drop in number) is
    stmt varchar2(500);
  begin
    --удаляем партицию
if p_drop=1 then
      begin
stmt := ' alter table ' || tablename_ || '
              drop partition MG' ||mg_||' update global indexes';
execute immediate stmt;
exception
        when others then
          null;
end;
end if;
--создаем партицию
stmt := 'alter table ' || tablename_ || '
          add partition MG'||mg_||'
          values less than(''' || mg_ || ''')
          tablespace ' || tabspc_ || '' || '';
execute immediate stmt;
end;

procedure drop_part(tablename_ in varchar2, mg_ in varchar2) is
    stmt varchar2(500);
  begin
    --удаляем партицию
    begin
stmt := ' alter table ' || tablename_ || '
              drop partition MG' || mg_;
execute immediate stmt;
exception
      when others then
        null;
end;
end;

procedure trunc_part(tablename_ in varchar2, mg_ in varchar2) is
    stmt  varchar2(500);
mg1_  varchar2(6);
part_ number;
  begin
select p.part into part_ from params p;
--чистим партицию
--особенность -  месяц партиции +1 от текущего
if nvl(part_, 0) = 1 then
      mg1_ := to_char(add_months(to_date(mg_, 'YYYYMM'), 1), 'YYYYMM');
stmt := ' alter table ' || tablename_ || '
            truncate partition MG' || mg1_ ||
              ' update global indexes';
execute immediate stmt;
elsif tablename_='c_chargepay' then
      --stmt := ' delete from ' || tablename_ || ' where ' || mg_ ||' between t.mgFrom and t.mgTo';
      --execute immediate stmt;
      --commit;
      Raise_application_error(-20000, 'Ошибка! Не верное вхождение по c_chargepay!');
else
      stmt := ' delete from ' || tablename_ || ' where mg=' || mg_;
execute immediate stmt;
commit;
end if;
end;






-- ред.24.04.2019 WTF??? Что это?? для кого это???
/*  procedure auto_charge is
    cnt_sch_ number;
    part_ number;
  begin
    Raise_application_error(-20000, 'WTF???');
  --автоначисление счетчиков
  --средний расход за прошлые 3 месяца начислить, у кого не начислено
  --выполнять сразу после перехода!

  --вначале заполняем аудит
  part_:=0;
  loop
    insert into log_actions
      (text, ts, fk_user_id, lsk, fk_type_act)
      select 'Автоначисление индивидуального прибора учета г.в., по-среднему за последние 3 месяца' as text,
       sysdate as ts, t.id, k.lsk as lsk, 2 as fk_type_act
      from kart k, t_user t where t.cd=user
        and
         nvl(decode(part_,0, k.mgw, k.mhw),0) = 0 and
         exists
         (select * from nabor n, usl u where k.lsk=n.lsk and n.sch_auto = 1
           and n.usl=u.usl and case when part_=0 and u.fk_calc_tp in (4, 18) then 1
                                              when part_=1 and u.fk_calc_tp in (3, 17) then 1
                                              else 0
                                              end =1)
           and
           case when part_=0 and k.psch in (1,3) then 1
                when part_=1 and k.psch in (1,2) then 1
                else 0
                end =1;

    exit when part_=1;
    part_:=part_+1;
  end loop;

  select nvl(p.cnt_sch,0) into cnt_sch_ from params p;
  if cnt_sch_ = 0 then
    --х.в.
    update kart k set k.mhw=
      (select round(avg(t.mhw),3) from arch_kart t, params p where t.k_lsk_id=k.k_lsk_id
             and t.mg between utils.add_months2(p.period,-3) and utils.add_months2(p.period,-1)
       and t.psch in (1,2))
       where
       nvl(k.mhw,0) = 0 and
       exists
       (select * from nabor n, usl u where k.lsk=n.lsk and n.sch_auto = 1
         and n.usl=u.usl and u.fk_calc_tp in (3, 17)
         )
         and k.psch in (1,2);
    --г.в.
    update kart k set k.mgw=
      (select round(avg(t.mgw),3) from arch_kart t, params p where t.k_lsk_id=k.k_lsk_id
             and t.mg between utils.add_months2(p.period,-3) and utils.add_months2(p.period,-1)
      and t.psch in (1,3))
       where
       nvl(k.mgw,0) = 0 and
       exists
       (select * from nabor n, usl u where k.lsk=n.lsk and n.sch_auto = 1
         and n.usl=u.usl and u.fk_calc_tp in (4, 18)
         )
         and k.psch in (1,3);
    else
    --х.в.
    --mhw посчитается само
    update kart k set k.phw=
      nvl(k.phw,0)+(select nvl(round(avg(t.mhw),3),0) from arch_kart t, params p where t.k_lsk_id=k.k_lsk_id
             and t.mg between utils.add_months2(p.period,-3) and utils.add_months2(p.period,-1)
      and t.psch in (1,2))
       where
       nvl(k.mhw,0) = 0 and
       exists
       (select * from nabor n, usl u where k.lsk=n.lsk and n.sch_auto = 1
         and n.usl=u.usl and u.fk_calc_tp in (3, 17)
         )
         and k.psch in (1,2);
    --г.в.
    --mgw посчитается само
    update kart k set k.pgw=
      nvl(k.pgw,0)+(select nvl(round(avg(t.mgw),3),0) from arch_kart t, params p where t.k_lsk_id=k.k_lsk_id
      and t.mg between utils.add_months2(p.period,-3) and utils.add_months2(p.period,-1)
      and t.psch in (1,3))
      where
       nvl(k.mgw,0) = 0 and
       exists
       (select * from nabor n, usl u where k.lsk=n.lsk and n.sch_auto = 1
         and n.usl=u.usl and u.fk_calc_tp in (4, 18)
         )
         and k.psch in (1,3);
  end if;


  logger.log_(null, 'gen.auto_charge (автоначисление счетчиков)');
  commit;
  end;
*/

-- подготовить дерево объектов для древовидного меню в OLAP отчетах -- TODO: перекинуть вызов в Java из gen_saldo_houses!
procedure prep_template_tree_objects is
  l_max_id number;
l_main_id number;
begin
  -- удалить объекты сессии, старше 3 дней
delete from tree_objects t
where exists (select *
              from t_sess s
              where s.fk_ses = t.fk_user
                and s.dat_create < (sysdate - 3));
-- удалить объекты, по которым нет сессий
delete from tree_objects t
where exists (select * from t_sess s where s.fk_ses = t.fk_user);
-- удалить сессии, старше 3 дней
delete from t_sess t where t.dat_create < (sysdate - 3);

delete from tree_objects t where t.fk_user = -1;
-- город
insert into tree_objects
(id, obj_level, fk_user, sel)
values
    (0, 0, -1, 1);
-- ЖЭО
insert into tree_objects
(main_id, id, obj_level, trest, fk_user, sel)
select 0, rownum as rn, 1, trest, -1, 1
from (select distinct trest
      from s_reu_trest t
      where t.trest is not null);

select max(id)
into l_max_id
from tree_objects t
where t.obj_level = 1
  and t.fk_user = -1;

-- УК
for c in (select main_id, reu
              from (select distinct s.reu, t.id as main_id
                       from s_reu_trest s, tree_objects t, t_org o
                      where s.trest = t.trest and s.reu=o.reu
                        and t.fk_user = -1
                        and o.org_tp_gis in (1) and s.reu is not null
                        and t.obj_level = 1)) loop
    l_max_id := l_max_id + 1;
insert into tree_objects
(main_id, id, obj_level, reu, fk_user, sel)
values
    (c.main_id, l_max_id, 2, c.reu, -1, 1);
end loop;

-- добавить корневой узел РСО
l_max_id := l_max_id + 1;
l_main_id:=l_max_id;
insert into tree_objects
(main_id, id, obj_level, fk_user, sel)
values
    (0, l_max_id, -1, -1, 1); -- obj_level пришлось сделать -1, так как вот так потом фильтруется датасет: DM_Olap.Uni_tree_objects.Filter := 'obj_level<=' + inttostr(max_level_);
-- и после фильтрации не виден уровень ред.15.08.2019
-- РСО
for c in (select distinct s.reu
                       from t_org s
                      where s.org_tp_gis in (2,3) and s.reu is not null) loop
    l_max_id := l_max_id + 1;
insert into tree_objects
(main_id, id, obj_level, reu, fk_user, sel)
values
    (l_main_id, l_max_id, 2, c.reu, -1, 1);
end loop;

-- Дом
for c in (select main_id, reu, kul, nd, fk_house, mg1, mg2, psch
              from (select k.reu, k.kul, k.nd, t.id as main_id, k.house_id as fk_house, min(k.mg1) as mg1, max(k.mg2) as mg2, c.psch
                       from kart k, tree_objects t, c_houses c
                      where k.reu = t.reu
                        and t.fk_user = -1
                        and t.obj_level = 2
                        and k.house_id = c.id
                      group by k.reu, k.kul, k.nd, t.id, k.house_id, c.psch) a) loop

    l_max_id := l_max_id + 1;
insert into tree_objects
(main_id, id, obj_level, reu, kul, nd, fk_user, fk_house, sel, mg1, mg2, psch)
values
    (c.main_id, l_max_id, 3, c.reu, c.kul, c.nd, -1, c.fk_house, 1, c.mg1, c.mg2, c.psch);
end loop;

for c in (select distinct t.*
              from tree_objects t
              join t_org o
                on t.reu = o.reu
               and o.org_tp_gis = 1 -- УК содержащее фонд, имеющее открытые лиц.счета
              join kart k on k.reu=o.reu and k.psch not in (8,9)
             where t.fk_user = -1
               and t.obj_level = 2) loop

    -- добавить в УК РСО организации, обслуживающие фонд
    for c2 in (select /*+ USE_HASH(k, tp, k2, tp2) */distinct k.reu
                 from kart k
                 join v_lsk_tp tp
                   on k.fk_tp = tp.id
                  and tp.cd <> 'LSK_TP_MAIN'
                 join kart k2
                   on k2.k_lsk_id = k.k_lsk_id
                 join v_lsk_tp tp2
                   on k2.fk_tp = tp2.id
                  and tp2.cd = 'LSK_TP_MAIN'
                where k2.reu = c.reu and k.reu <> c.reu) loop

      l_max_id := l_max_id + 1;
insert into tree_objects
(main_id, id, obj_level, reu, for_reu, fk_user, sel, tp_show)
values
    (c.id, l_max_id, 2, c2.reu, c.reu, -1, 1, 1);
l_main_id:=l_max_id;
      -- добавить дома по РСО
for c3 in (select a.reu, a.kul, a.nd, a.fk_house, a.mg1, a.mg2, a.psch
                   from (select /*+ USE_HASH(k, tp, k2, tp2, h) */k.reu, k.kul, k.nd, k.house_id as fk_house, min(k.mg1) as mg1, max(k.mg2) as mg2, h.psch
                           from kart k
                           join v_lsk_tp tp
                             on k.fk_tp = tp.id
                            and tp.cd <> 'LSK_TP_MAIN'
                           join kart k2
                             on k2.k_lsk_id = k.k_lsk_id
                           join v_lsk_tp tp2
                             on k2.fk_tp = tp2.id
                            and tp2.cd = 'LSK_TP_MAIN'
                           join c_houses h on k.house_id=h.id
                           where k2.reu = c.reu and k.reu = c2.reu
                           group by k.reu, k.kul, k.nd, k.house_id, h.psch) a) loop

        l_max_id := l_max_id + 1;
insert into tree_objects
(main_id, id, obj_level, reu, for_reu, kul, nd, fk_user, fk_house, sel, mg1, mg2, psch)
values
    (l_main_id, l_max_id, 3, c3.reu, c.reu, c3.kul, c3.nd, -1, c3.fk_house, 1, c3.mg1, c3.mg2, c3.psch);
end loop;
end loop;
end loop;
end;

end gen;
/

create or replace package body stat is
    procedure rep_stat(reu_ in varchar2,
                       p_for_reu in varchar2, -- для статы, УК содержащая фонд
                       kul_ in varchar2,
                       nd_ in varchar2,
                       trest_ in varchar2,
                       mg_ in varchar2,
                       mg1_ in varchar2,
                       dat_ in date,
                       dat1_ in date,
                       var_ in number, --уровень информации
                       det_ in number, --детализация информации
                       org_ in number,
                       oper_ in varchar2,
                       сd_ in varchar2, --CD отчета
                       spk_id_ in number,
                       p_house in number, --ID дома
                       p_out_tp in number, --тип выгрузки (null- в реф-курсор, 1-в текстовый файл в дир по умолчанию)
                       prep_refcursor in out rep_refcursor) is

        sqlstr_       varchar2(2000);
sqlstr2_      varchar2(2000);
sqlstr3_      varchar2(2000);
sql_det       varchar2(2000);
l_sql         varchar2(2000); --для хранения полного текста запроса
period_       varchar2(55);
uslg_         usl.uslg%type;
mg2_          params.period%type;
l_mg_next     params.period%type;
dat2_         date;
dat3_         date;
n1_           number;
n2_           number;
kpr1_         number;
kpr2_         number;
show_sal_     number;
cur_pay_      number;
gndr_         number;
prop_         number;
show_fond_    number;
fk_ses_       number;
l_dt          date;
l_dt1         date;
l_in_period   number;
l_out_period  number;
l_cur_period  params.period%type;
l_prev_period params.period%type;
l_cnt         number;
l_sel         varchar2(256);
l_sel_id      number;
l_period_tp   number;
l_char_dat_mg varchar2(6);
l_str_dat     varchar2(10);
l_str_dat1    varchar2(10);
l_rep_prop_tp number;
    begin
select userenv('sessionid') into fk_ses_ from dual;
select period into l_cur_period from params;

l_char_dat_mg := to_char(dat_, 'YYYYMM');

        --вычислить первую и последнюю даты заданных периодов, для оптимизации запросов.
if mg_ is not null then l_dt := to_date(mg_ || '01', 'YYYYMMDD'); end if;
if mg1_ is not null then l_dt1 := last_day(to_date(mg1_ || '01', 'YYYYMMDD')); end if;
--Вычисляем передыдущ месяц
if dat_ is not null then
            l_prev_period := to_char(add_months(dat_, -1), 'YYYYMM');
else
            l_prev_period := to_char(add_months(to_date(mg_ || '01', 'YYYYMMDD'), -1), 'YYYYMM');
end if;
-- конвертировать в формат строки дату
if dat_ is not null then l_str_dat := to_char(dat_, 'DD.MM.YYYY'); end if;
if dat1_ is not null then l_str_dat1 := to_char(dat1_, 'DD.MM.YYYY'); end if;

--Вычисляем следующий месяц
if mg_ is not null then l_mg_next := to_char(add_months(to_date(mg_ || '01', 'YYYYMMDD'), 1), 'YYYYMM'); end if;
--узнать находится ли заданные даты отчета в текущем периоде
select case
           when not l_dt between to_date(p.period || '01', 'YYYYMMDD') and last_day(to_date(p.period || '01', 'YYYYMMDD')) or
                not l_dt1 between to_date(p.period || '01', 'YYYYMMDD') and last_day(to_date(p.period || '01', 'YYYYMMDD'))
               then 1
           else 0 end,
       case
           when l_dt between to_date(p.period || '01', 'YYYYMMDD') and last_day(to_date(p.period || '01', 'YYYYMMDD')) or
                l_dt1 between to_date(p.period || '01', 'YYYYMMDD') and last_day(to_date(p.period || '01', 'YYYYMMDD'))
               then 1
           else 0 end
into l_out_period, l_in_period
from params p;

if dat_ is not null and dat1_ is not null then
            sqlstr_ := 's.dat between TO_DATE(''' || to_char(dat_, 'DDMMYYYY') || ''',''DDMMYYYY'') and TO_DATE(''' ||
                       to_char(dat1_, 'DDMMYYYY') || ''',''DDMMYYYY'')';
period_ := 'с ' || to_char(dat_, 'DD.MM.YYYY') || ' по ' || to_char(dat1_, 'DD.MM.YYYY');
sqlstr3_ := 'd.dat between TO_DATE(''' || to_char(dat_, 'DDMMYYYY') || ''',''DDMMYYYY'') and TO_DATE(''' ||
                        to_char(dat1_, 'DDMMYYYY') || ''',''DDMMYYYY'')';
sqlstr2_ := 's.period between ''' || to_char(dat_, 'YYYYMM') || ''' and ''' || to_char(dat_, 'YYYYMM') ||
                        '''';
l_period_tp := 0;
elsif dat_ is not null and dat1_ is null then
            sqlstr_ := 's.dat = TO_DATE('' ' || to_char(dat_, 'DDMMYYYY') || ' '',''DDMMYYYY'')';
period_ := 'с ' || to_char(dat_, 'DD.MM.YYYY') || ' по ' || to_char(dat1_, 'DD.MM.YYYY');
sqlstr3_ := 'd.dat between TO_DATE(''' || to_char(dat_, 'DDMMYYYY') || ''',''DDMMYYYY'') and TO_DATE(''' ||
                        to_char(dat1_, 'DDMMYYYY') || ''',''DDMMYYYY'')';
sqlstr2_ := 's.period between ''' || to_char(dat_, 'YYYYMM') || ''' and ''' || to_char(dat_, 'YYYYMM') ||
                        '''';
l_period_tp := 1;
else
            if mg_ = mg1_ then
                period_ := utils.month_name(substr(mg_, 5, 2)) || ' ' || substr(mg_, 1, 4) || 'г.';
else
                period_ := 'с ' || utils.month_name1(substr(mg_, 5, 2)) || ' ' || substr(mg_, 1, 4) || 'г.' || ' по ' ||
                           utils.month_name(substr(mg1_, 5, 2)) || ' ' || substr(mg_, 1, 4) || 'г.';
end if;
sqlstr_ := 's.mg between ''' || mg_ || ''' and ''' || mg1_ || '''';
sqlstr2_ := 's.period between ''' || mg_ || ''' and ''' || mg1_ || '''';
sqlstr3_ := 'd.mg between ''' || mg_ || ''' and ''' || mg1_ || '''';
l_period_tp := 2;
end if;


if сd_ = '22' then
            --Статистика по долгам
            if var_ = 3 then
                --По дому
                open prep_refcursor for 'select t.trest||'' ''||t.name_tr as predp, d.reu,
     d.reu||d.kul||d.nd||'' ''||k.name||'', ''||NVL(LTRIM(d.nd,''0''),''0'') AS predpr_det,
     LTRIM(d.kw,''0'') AS kw, substr(d.mg, 1, 4)||''-''||substr(d.mg, 5, 2) AS mg, d.summa,d.dat,SUBSTR(''000''||d.kol_month,-3) AS kol_month
     FROM DEBITS_KW d, S_REU_TREST t, SPUL k
     WHERE d.reu=t.reu
     AND d.kul=k.id
     AND d.reu=:reu_
     AND d.kul=:kul_
     AND d.nd=:nd_
     AND  d.dat BETWEEN :dat_ AND :dat1_  ORDER BY d.mg DESC' using reu_, kul_, nd_,dat_,dat1_;
elsif var_ = 2 then
                --По РЭУ
                open prep_refcursor for 'select t.trest||'' ''||t.name_tr as predp, d.reu,
     d.reu||d.kul||d.nd||'' ''||k.name||'', ''||NVL(LTRIM(d.nd,''0''),''0'') AS predpr_det,
     NULL AS kw, substr(d.mg, 1, 4)||''-''||substr(d.mg, 5, 2) AS mg, summa,d.dat,SUBSTR(''000''||d.kol_month,-3) AS kol_month
     FROM DEBITS_HOUSES d, S_REU_TREST t, SPUL k
     WHERE d.reu=t.reu
     AND d.kul=k.id
     AND t.reu=:reu_
     AND d.dat BETWEEN :dat_ AND :dat1_ ORDER BY d.mg DESC' using reu_,dat_,dat1_;
elsif var_ = 1 then
                --По ЖЭО
                open prep_refcursor for 'select t.trest||'' ''||t.name_tr as predp, d.reu, null as predpr_det,
     NULL AS kw, substr(d.mg, 1, 4)||''-''||substr(d.mg, 5, 2) AS mg, d.summa,d.dat,SUBSTR(''000''||d.kol_month,-3) AS kol_month
     FROM DEBITS_TREST d, S_REU_TREST t
     WHERE d.reu=t.reu
     AND t.trest=:trest_
     AND d.dat BETWEEN :dat_ AND :dat1_ ORDER BY d.mg DESC' using trest_,dat_,dat1_;
elsif var_ = 0 then
                --По МП УЕЗЖКУ (все тресты)
                open prep_refcursor for 'select t.trest||'' ''||t.name_tr as predp, d.reu, null as predpr_det,
     NULL AS kw,substr(d.mg, 1, 4)||''-''||substr(d.mg, 5, 2) AS mg,summa,d.dat,SUBSTR(''000''||d.kol_month,-3) AS kol_month
     FROM DEBITS_TREST d, S_REU_TREST t
     WHERE d.reu=t.reu
     AND d.dat BETWEEN :dat_ AND :dat1_ ORDER BY d.mg DESC' using dat_,dat1_;
end if;

elsif сd_ = '13' then
            --Статистика по услугам
            l_sel_id := utils.gets_list_param('REP_TP_SCH_SEL');
if det_ = 3 then
                kpr1_ := utils.gets_int_param('REP_RNG_KPR1');
kpr2_ := utils.gets_int_param('REP_RNG_KPR2');
                --детализация до квартир
open prep_refcursor for select s.lsk,
                               s.org,
                               coalesce(r.fk_org_dst, s.org) as fk_org2,
                               u.uslm,
                               s.usl,
                               o3.id || ' ' || o3.name as name_org,
                               u.usl || ' ' || u.nm as name_usl,
                               o2.name as name_org_main,
                               s.kul,
                               t.trest,
                               s.reu,
                               t.name_reu as reu_name,
                               ot.name_short || '.' || initcap(o.name) || ', ' || initcap(k.name) as name,
                               ot.name_short || '.' || initcap(o.name) || ', ' || initcap(k.name) ||
                               ', ' || nvl(ltrim(s.nd, '0'), '0') || '-' ||
                               nvl(ltrim(s.kw, '0'), '0') as predpr_det,
                               det.ord1,
                               k1.fio,
                               s.status,
                               decode(s.status, 2, 'Приват', 'Муницип') as status_name,
                               s.psch as psch,
                               s.sch as sch,
                               decode(s.sch,0,'Норматив',1,'Счетчик','Нет') as sch_name,
                               s.val_group,
                               s.val_group2,
                               s.cnt as cnt,
                               s.klsk as klsk,
                               s.kpr as kpr,
                               decode(s.is_empt, 1, 'да', 0, 'нет', null) as is_empt,
                               s.kpr_ot as kpr_ot,
                               s.kpr_wr as kpr_wr,
                               s.cnt_lg as cnt_lg,
                               s.cnt_subs as cnt_subs,
                               s.cnt_room,
                               s.uch,
                               substr(s.mg, 1, 4) || '-' || substr(s.mg, 5, 2) as mg1,
                               u.npp,
                               null as name_gr,
                               null as odpu_ex,
                               0 as odpu_kub,
                               0 as kub_dist,
                               0 as kub_fact,
                               0 as kub_fact_upnorm,
                               tp.name as lsk_tp,
                               s.opl,
                               s.is_vol,
                               s.chng_vol,
                               null as ishotpipe,
                               null as istowel,
                               null as kr_soi,
                               null as fact_cons,
                               decode(s.psch, 0, 'Открытые Л/С', 1, 'Закрытые Л/С', 2,
                                      'Старый фонд') as psch_name
                        from statistics_lsk s
                                 join usl u on s.usl = u.usl
                                 join s_reu_trest t on s.reu = t.reu
                                 join spul k on s.kul = k.id
                                 join t_org o on k.fk_settlement = o.id
                                 join t_org_tp ot on o.fk_orgtp = ot.id
                                 join kart k1 on s.lsk = k1.lsk
                                 join kart_detail det on k1.lsk = det.lsk
                                 left join v_lsk_tp tp on s.fk_tp = tp.id
                                 join t_org o3 on s.org = o3.id
                                 left join redir_pay r on s.org = r.fk_org_src and s.mg between r.mg1 and r.mg2
                                 left join t_org o2 on coalesce(r.fk_org_dst, s.org) = o2.id
                        where exists
                            (select su.fk_reu
                             from scott.c_users_perm su
                                      join scott.u_list ut
                                           on ut.cd = 'доступ к отчётам'
                                               and su.fk_perm_tp = ut.id
                                      join scott.t_user us
                                           on lower(us.cd) = lower(user)
                                               and su.user_id = us.id
                             where su.fk_reu = s.reu
                            )
                          and exists(select *
                                     from list_c i,
                                          spr_params p
                                     where i.fk_ses = fk_ses_
                                       and p.id = i.fk_par
                                       and p.cd = 'REP_USL'
                                       and i.sel_cd = s.usl
                                       and i.sel = 1)
                          and exists(select *
                                     from list_c i,
                                          spr_params p
                                     where i.fk_ses = fk_ses_
                                       and p.id = i.fk_par
                                       and p.cd = 'REP_STATUS'
                                       and i.sel_id = s.status
                                       and i.sel = 1)
                          and ((var_ = 3 and s.reu = reu_ and case
                                                                  when s.for_reu is null and mg_ < '201907'
                                                                      then 1 -- до 201907 поле for_reu - не заполнялось ред.12.11.20 - Кис. жаловались, что выходят лишние дома
                                                                  when p_for_reu is null then 1
                                                                  when s.for_reu = p_for_reu then 1
                                                                  else 0 end = 1 and s.kul = kul_ and
                                s.nd = nd_) or (var_ = 2 and s.reu = reu_ and case
                                                                                  when s.for_reu is null and mg_ < '201907'
                                                                                      then 1 -- до 201907 поле for_reu - не заполнялось ред.12.11.20 - Кис. жаловались, что выходят лишние дома
                                                                                  when p_for_reu is null
                                                                                      then 1
                                                                                  when s.for_reu = p_for_reu
                                                                                      then 1
                                                                                  else 0 end = 1) or
                               (var_ = 1 and t.trest = trest_) or var_ = 0)
                          and exists(select *
                                     from statistics_lsk st,
                                          usl ut
                                     where st.lsk = s.lsk
                                       and st.mg = s.mg
                                       and st.usl = ut.usl
                                       and ut.uslm = u.uslm
                                       and (kpr1_ is not null and st.kpr >= kpr1_ or kpr1_ is null)
                                       and (kpr2_ is not null and st.kpr <= kpr2_ or kpr2_ is null))
                          and (l_sel_id = 0 or l_sel_id <> 0 and l_sel_id = s.fk_tp)
                          and s.mg between mg_ and mg1_
                        union all
                        select null as lsk,
                               null as org,
                               null as fk_org2,
                               '000' as uslm,
                               '000' as usl,
                               'Итого' as name_org,
                               'Итого' as name_usl,
                               'Итого' as name_org_main,
                               s.kul,
                               t.trest,
                               s.reu,
                               t.name_reu as reu_name,
                               ot.name_short || '.' || initcap(o.name) || ', ' || initcap(k.name) as name,
                               ot.name_short || '.' || initcap(o.name) || ', ' || initcap(k.name) ||
                               ', ' || nvl(ltrim(s.nd, '0'), '0') || '-' ||
                               nvl(ltrim(s.kw, '0'), '0') as predpr_det,
                               det.ord1,
                               k1.fio,
                               s.status,
                               decode(s.status, 2, 'Приват', 'Муницип') as status_name,
                               s.psch as psch,
                               s.sch as sch,
                               decode(s.sch,0,'Норматив',1,'Счетчик','Нет') as sch_name,
                               s.val_group,
                               s.val_group2,
                               s.cnt as cnt,
                               s.klsk as klsk,
                               s.kpr as kpr,
                               decode(s.is_empt, 1, 'да', 0, 'нет', null) as is_empt,
                               s.kpr_ot as kpr_ot,
                               s.kpr_wr as kpr_wr,
                               s.cnt_lg as cnt_lg,
                               s.cnt_subs as cnt_subs,
                               s.cnt_room,
                               s.uch,
                               substr(s.mg, 1, 4) || '-' || substr(s.mg, 5, 2) as mg1,
                               null as npp,
                               null as name_gr,
                               null as odpu_ex,
                               0 as odpu_kub,
                               0 as kub_dist,
                               0 as kub_fact,
                               0 as kub_fact_upnorm,
                               tp.name as lsk_tp,
                               s.opl,
                               s.is_vol,
                               s.chng_vol,
                               null as ishotpipe,
                               null as istowel,
                               null as kr_soi,
                               null as fact_cons,
                               null as psch_name
                        from statistics_lsk s
                                 join s_reu_trest t on s.reu = t.reu
                                 join spul k on s.kul = k.id
                                 join t_org o on k.fk_settlement = o.id
                                 join t_org_tp ot on o.fk_orgtp = ot.id
                                 left join v_lsk_tp tp on s.fk_tp = tp.id
                                 join kart_detail det on s.lsk = det.lsk
                                 join kart k1 on s.lsk = k1.lsk
                        where
                            exists
                                (select su.fk_reu
                                 from scott.c_users_perm su
                                          join scott.u_list ut
                                               on ut.cd = 'доступ к отчётам'
                                                   and su.fk_perm_tp = ut.id
                                          join scott.t_user us
                                               on lower(us.cd) = lower(user)
                                                   and su.user_id = us.id
                                 where su.fk_reu = s.reu
                                )
                          and s.usl is null
                          and exists(select *
                                     from list_c i,
                                          spr_params p
                                     where i.fk_ses = fk_ses_
                                       and p.id = i.fk_par
                                       and p.cd = 'REP_USL'
                                       and i.sel_cd = '0' --Включить ли ИТОГ?
                                       and i.sel = 1)
                          and exists(select *
                                     from list_c i,
                                          spr_params p
                                     where i.fk_ses = fk_ses_
                                       and p.id = i.fk_par
                                       and p.cd = 'REP_STATUS'
                                       and i.sel_id = s.status
                                       and i.sel = 1)
                          and ((var_ = 3 and s.reu = reu_ and s.kul = kul_ and s.nd = nd_) or
                               (var_ = 2 and s.reu = reu_) or (var_ = 1 and t.trest = trest_) or
                               var_ = 0)
                          --неоднозначность какая то... если по услугам то фильтр по кол-ву прожив один, а если по итогам - по другому принципу...
                          and (kpr1_ is not null and s.kpr >= kpr1_ or kpr1_ is null)
                          and (kpr2_ is not null and s.kpr <= kpr2_ or kpr2_ is null)
                          and (l_sel_id = 0 or l_sel_id <> 0 and l_sel_id = s.fk_tp)
                          and s.mg between mg_ and mg1_
                        order by ord1; --не убирай порядок сортировки!
elsif det_ = 2 then -- -------------------------------------------------------------------------------------------------
            -- ВНИМАНИЕ! В КИС  A_NABOR2 - ПАРТИЦИРОВАННАЯ ТАБЛИЦА ПО MGFROM С ВЛОЖЕННОЙ СУБПАРТИЦИЕЙ ПО MGTO!!!
            -- -------------------------------------------------------------------------------------------------
            -- из за того, что тормозил внутренний запрос, пришлось вынести в temporary table ред.26.10.2017
delete from temp_stat2;
insert into temp_stat2 (kub, kub_dist, kub_fact_upnorm, kub_fact, uslm, uslm_group1, mg, reu, kul, nd, dist_tp,
                        odpu_ex, ishotpipeinsulated, istowelheatexist, kr_soi, fact_cons)
select       --для отображения объемов по ОДПУ
    distinct d.kub,
             d.kub_dist,
             d.kub_fact_upnorm,
             decode(d.dist_tp, 4, null, d.kub_fact) as kub_fact, -- не показывать объем, если 4 (нет ОДПУ) ред.05.03.2017
             u.uslm,
             u.uslm_group1,
             d.mg,
             h.reu,
             h.kul,
             h.nd,
             d.dist_tp as dist_tp,
             case
                 when d.dist_tp <> 4 and nvl(d.kub, 0) = 0 then 'есть, нет объема'
                 when d.dist_tp <> 4 and nvl(d.kub, 0) <> 0 then 'есть'
                 else 'нет' end as odpu_ex,
             nvl(d.ishotpipeinsulated, 0) as ishotpipeinsulated,
             nvl(d.istowelheatexist, 0) as istowelheatexist,
             case
                 when d.usl not in ('053', '054') then d.kub - (d.kub_norm + d.kub_sch + d.kub_ar)
                 else 0 end as kr_soi,                           -- кр на сои (только для первой строки) и не для 053 и 054 услуг
             d.kub_norm + d.kub_sch + d.kub_ar as fact_cons      -- факт.потребление (только для первой строки)
from a_vvod d,
     arch_kart h,
     a_nabor2 n,
     s_reu_trest s,
     usl u
where h.house_id = d.house_id
  and h.mg = d.mg
  and d.mg between mg_ and mg1_
  and d.usl = u.usl
  and d.id = n.fk_vvod
  and d.usl = n.usl
  and h.lsk = n.lsk
  and d.mg between n.mgfrom and n.mgto
  and to_date(d.mg || '01', 'YYYYMMDD') between n.dt1 and n.dt2
  and h.psch not in (8, 9)
  and h.reu = s.reu
  and case
          when var_ = 3 and h.reu = reu_ and h.kul = kul_ and h.nd = nd_ then 1
          when var_ = 2 and h.reu = reu_ then 1
          when var_ = 1 and s.trest = trest_ then 1
          when var_ = 0 then 1 end = 1
  and exists (select su.fk_reu
              from scott.c_users_perm su
                       join scott.u_list ut
                            on ut.cd = 'доступ к отчётам'
                                and su.fk_perm_tp = ut.id
                       join scott.t_user us
                            on lower(us.cd) = lower(user)
                                and su.user_id = us.id
              where su.fk_reu = h.reu
    );

delete from temp_stat3;
insert into temp_stat3 (mg, reu, kul, nd, name)
select distinct h.mg, h.reu, h.kul, h.nd, o.name
from arch_kart h
         join a_houses a on h.house_id = a.id and h.mg = a.mg and h.mg between mg_ and mg1_ and
                            h.psch not in (8, 9)
         join s_reu_trest e on h.reu = e.reu
         left join t_org o on a.fk_other_org = o.id
where case
          when var_ = 3 and h.reu = reu_ and h.kul = kul_ and h.nd = nd_ then 1
          when var_ = 2 and h.reu = reu_ then 1
          when var_ = 1 and e.trest = trest_ then 1
          when var_ = 0 then 1 end = 1;
--детализация до домов
open prep_refcursor for select null as lsk,
                               s.org,
                               coalesce(r.fk_org_dst, s.org) as fk_org2,
                               u.uslm,
                               s.usl,
                               o3.id || ' ' || o3.name as name_org,
                               u.usl || ' ' || u.nm as name_usl,
                               o2.name as name_org_main,
                               s.kul,
                               t.trest,
                               s.reu,
                               t.name_reu as reu_name,
                               ot.name_short || '.' || initcap(o.name) || ', ' || initcap(k.name) as name,
                               ot.name_short || '.' || initcap(o.name) || ', ' || initcap(k.name) ||
                               ', ' || nvl(ltrim(s.nd, '0'), '0') as predpr_det,
                               utils.f_order(s.nd, 6) as ord1,
                               utils.f_order2(s.nd) as ord3,
                               null as fio,
                               s.status,
                               decode(s.status, 2, 'Приват', 'Муницип') as status_name,
                               s.psch as psch,
                               s.sch as sch,
                               decode(s.sch,0,'Норматив',1,'Счетчик','Нет') as sch_name,
                               s.val_group,
                               s.val_group2,
                               s.cnt as cnt,
                               s.klsk as klsk,
                               s.kpr as kpr,
                               decode(s.is_empt, 1, 'да', 0, 'нет', null) as is_empt,
                               s.kpr_ot as kpr_ot,
                               s.kpr_wr as kpr_wr,
                               s.cnt_lg as cnt_lg,
                               s.cnt_subs as cnt_subs,
                               s.cnt_room,
                               s.uch,
                               substr(s.mg, 1, 4) || '-' || substr(s.mg, 5, 2) as mg1,
                               u.npp,
                               h3.name as name_gr,
                               nvl(h5.odpu_ex, 'нет') as odpu_ex,
                               decode(s.fr, 1, h2.kub, 0) as odpu_kub,
                               decode(s.fr, 1, h2.kub_dist, 0) as kub_dist,
                               decode(s.fr, 1, h2.kub_fact, 0) as kub_fact,
                               decode(s.fr, 1, h2.kub_fact_upnorm, 0) as kub_fact_upnorm,
                               tp.name as lsk_tp,
                               s.opl,
                               s.is_vol,
                               s.chng_vol,
                               decode(h5.ishotpipeinsulated, 1, 'да', 'нет') as ishotpipe,
                               decode(h5.istowelheatexist, 1, 'да', 'нет') as istowel,
                               decode(s.fr, 1, h2.kr_soi, 0) as kr_soi,
                               decode(s.fr, 1, h2.fact_cons, 0) as fact_cons,
                               decode(s.psch, 0, 'Открытые Л/С', 1, 'Закрытые Л/С', 2,
                                      'Старый фонд') as psch_name
                        from statistics s
                                 join usl u on s.usl = u.usl
                                 join s_reu_trest t on s.reu = t.reu
                                 join spul k on s.kul = k.id
                                 join t_org o on k.fk_settlement = o.id
                                 join t_org_tp ot on o.fk_orgtp = ot.id
                                 left join temp_stat2 h2
                                           on s.mg = h2.mg and s.reu = h2.reu and s.kul = h2.kul and
                                              s.nd = h2.nd and u.uslm =
                                                               h2.uslm --  left join temp_stat2 h4 on s.mg=h4.mg and s.reu=h4.reu and s.kul=h4.kul and s.nd=h4.nd
                            --          and nvl(u.parent_usl, s.usl)=h4.usl
                                 left join (select distinct t.uslm_group1,
                                                            t.reu,
                                                            t.kul,
                                                            t.nd,
                                                            t.mg,
                                                            t.ishotpipeinsulated,
                                                            t.istowelheatexist,
                                                            t.odpu_ex
                                            from temp_stat2 t) h5
                                           on s.mg = h5.mg and s.reu = h5.reu and s.kul = h5.kul and
                                              s.nd = h5.nd and u.uslm_group1 = h5.uslm_group1
                                 left join temp_stat3 h3
                                           on s.mg = h3.mg and s.reu = h3.reu and s.kul = h3.kul and s.nd = h3.nd
                                 left join v_lsk_tp tp on s.fk_tp = tp.id
                                 join t_org o3 on s.org = o3.id
                                 left join redir_pay r on s.org = r.fk_org_src and s.mg between r.mg1 and r.mg2
                                 left join t_org o2 on coalesce(r.fk_org_dst, s.org) = o2.id
                        where exists
                            (select su.fk_reu
                             from scott.c_users_perm su
                                      join scott.u_list ut
                                           on ut.cd = 'доступ к отчётам'
                                               and su.fk_perm_tp = ut.id
                                      join scott.t_user us
                                           on lower(us.cd) = lower(user)
                                               and su.user_id = us.id
                             where su.fk_reu = s.reu
                            ) and exists(select *
                                         from list_c i,
                                              spr_params p
                                         where i.fk_ses = fk_ses_
                                           and p.id = i.fk_par
                                           and p.cd = 'REP_USL'
                                           and i.sel_cd = s.usl
                                           and i.sel = 1)
                          and exists(select *
                                     from list_c i,
                                          spr_params p
                                     where i.fk_ses = fk_ses_
                                       and p.id = i.fk_par
                                       and p.cd = 'REP_STATUS'
                                       and i.sel_id = s.status
                                       and i.sel = 1)
                          and ((var_ = 3 and s.reu = reu_ and case
                                                                  when s.for_reu is null and mg_ < '201907'
                                                                      then 1 -- до 201907 поле for_reu - не заполнялось ред.12.11.20 - Кис. жаловались, что выходят лишние дома
                                                                  when p_for_reu is null then 1
                                                                  when s.for_reu = p_for_reu then 1
                                                                  else 0 end = 1 and s.kul = kul_ and
                                s.nd = nd_) or (var_ = 2 and s.reu = reu_ and case
                                                                                  when s.for_reu is null and mg_ < '201907'
                                                                                      then 1 -- до 201907 поле for_reu - не заполнялось ред.12.11.20 - Кис. жаловались, что выходят лишние дома
                                                                                  when p_for_reu is null
                                                                                      then 1
                                                                                  when s.for_reu = p_for_reu
                                                                                      then 1
                                                                                  else 0 end = 1) or
                               (var_ = 1 and t.trest = trest_) or var_ = 0)
                          and (l_sel_id = 0 or l_sel_id <> 0 and l_sel_id = s.fk_tp)
                          and s.mg between mg_ and mg1_
                        union all
                        select null as lsk,
                               null as org,
                               null as fk_org2,
                               '000' as uslm,
                               '000' as usl,
                               'Итого' as name_org,
                               'Итого' as name_usl,
                               'Итого' as name_org_main,
                               s.kul,
                               t.trest,
                               s.reu,
                               null as reu_name,
                               ot.name_short || '.' || initcap(o.name) || ', ' || initcap(k.name) as name,
                               ot.name_short || '.' || initcap(o.name) || ', ' || initcap(k.name) ||
                               ', ' || nvl(ltrim(s.nd, '0'), '0') as predpr_det,
                               utils.f_order(s.nd, 6) as ord1,
                               utils.f_order2(s.nd) as ord3,
                               null as fio,
                               s.status,
                               decode(s.status, 2, 'Приват', 'Муницип') as status_name,
                               s.psch as psch,
                               s.sch as sch,
                               decode(s.sch,0,'Норматив',1,'Счетчик','Нет') as sch_name,
                               s.val_group,
                               s.val_group2,
                               s.cnt as cnt,
                               s.klsk as klsk,
                               s.kpr as kpr,
                               decode(s.is_empt, 1, 'да', 0, 'нет', null) as is_empt,
                               s.kpr_ot as kpr_ot,
                               s.kpr_wr as kpr_wr,
                               s.cnt_lg as cnt_lg,
                               s.cnt_subs as cnt_subs,
                               s.cnt_room,
                               s.uch,
                               substr(s.mg, 1, 4) || '-' || substr(s.mg, 5, 2) as mg1,
                               null as npp,
                               hl.name as name_gr,
                               null as odpu_ex,
                               0 as odpu_kub,
                               0 as kub_dist,
                               0 as kub_fact,
                               0 as kub_fact_upnorm,
                               tp.name as lsk_tp,
                               s.opl,
                               s.is_vol,
                               s.chng_vol,
                               null as ishotpipe,
                               null as istowel,
                               null as kr_soi,
                               null as fact_cons,
                               null as psch_name
                        from statistics s,
                             s_reu_trest t,
                             spul k,
                             t_org o,
                             t_org_tp ot,
                             (select t.reu, t.kul, t.nd, u.name
                              from t_housexlist t,
                                   u_list u
                              where t.fk_list = u.id) hl,
                             v_lsk_tp tp
                        where s.reu = t.reu
                          and s.fk_tp = tp.id(+)
                          and s.usl is null
                          and s.kul = k.id
                          and k.fk_settlement = o.id
                          and o.fk_orgtp = ot.id
                          and exists
                            (select su.fk_reu
                             from scott.c_users_perm su
                                      join scott.u_list ut
                                           on ut.cd = 'доступ к отчётам'
                                               and su.fk_perm_tp = ut.id
                                      join scott.t_user us
                                           on lower(us.cd) = lower(user)
                                               and su.user_id = us.id
                             where su.fk_reu = s.reu
                            )
                          and exists(select *
                                     from list_c i,
                                          spr_params p
                                     where i.fk_ses = fk_ses_
                                       and p.id = i.fk_par
                                       and p.cd = 'REP_USL'
                                       and i.sel_cd = '0' --Включить ли ИТОГ?
                                       and i.sel = 1)
                          and exists(select *
                                     from list_c i,
                                          spr_params p
                                     where i.fk_ses = fk_ses_
                                       and p.id = i.fk_par
                                       and p.cd = 'REP_STATUS'
                                       and i.sel_id = s.status
                                       and i.sel = 1)

                          and s.reu = hl.reu(+)
                          and s.kul = hl.kul(+)
                          and s.nd = hl.nd(+)
                          and ((var_ = 3 and s.reu = reu_ and p_for_reu is null and s.kul = kul_ and
                                s.nd = nd_) or (var_ = 2 and s.reu = reu_ and p_for_reu is null) or
                               (var_ = 1 and t.trest = trest_) or var_ = 0)
                          and (l_sel_id = 0 or l_sel_id <> 0 and l_sel_id = s.fk_tp)
                          and s.mg between mg_ and mg1_
                        order by name, ord1;
elsif det_ in (0, 1) then
                --детализация до УК
                open prep_refcursor for select null as lsk,
                                               s.org,
                                               coalesce(r.fk_org_dst, s.org) as fk_org2,
                                               u.uslm,
                                               s.usl,
                                               o3.id || ' ' || o3.name as name_org,
                                               u.usl || ' ' || u.nm as name_usl,
                                               o2.name as name_org_main,
                                               t.trest,
                                               s.reu,
                                               t.name_reu as reu_name,
                                               null as name,
                                               null as predpr_det,
                                               null as fio,
                                               s.status,
                                               decode(s.status, 2, 'Приват', 'Муницип') as status_name,
                                               s.psch as psch,
                                               s.sch as sch,
                                               decode(s.sch,0,'Норматив',1,'Счетчик','Нет') as sch_name,
                                               s.val_group,
                                               s.val_group2,
                                               s.cnt as cnt,
                                               s.klsk as klsk,
                                               s.kpr as kpr,
                                               decode(s.is_empt, 1, 'да', 0, 'нет', null) as is_empt,
                                               s.kpr_ot as kpr_ot,
                                               s.kpr_wr as kpr_wr,
                                               s.cnt_lg as cnt_lg,
                                               s.cnt_subs as cnt_subs,
                                               s.cnt_room,
                                               s.uch,
                                               substr(s.mg, 1, 4) || '-' || substr(s.mg, 5, 2) as mg1,
                                               u.npp,
                                               null as name_gr,
                                               null as odpu_ex,
                                               0 as odpu_kub,
                                               0 as kub_dist,
                                               0 as kub_fact,
                                               0 as kub_fact_upnorm,
                                               tp.name as lsk_tp,
                                               s.opl,
                                               s.is_vol,
                                               s.chng_vol,
                                               null as ishotpipe,
                                               null as istowel,
                                               null as kr_soi,
                                               null as fact_cons,
                                               decode(s.psch, 0, 'Открытые Л/С', 1, 'Закрытые Л/С', 2,
                                                      'Старый фонд') as psch_name
                                        from statistics_trest s
                                                 join usl u on s.usl = u.usl
                                                 join s_reu_trest t on s.reu = t.reu
                                                 join v_lsk_tp tp on s.fk_tp = tp.id
                                                 join t_org o3 on s.org = o3.id
                                                 left join redir_pay r on s.org = r.fk_org_src and s.mg between r.mg1 and r.mg2
                                                 left join t_org o2 on coalesce(r.fk_org_dst, s.org) = o2.id
                                        where exists(select *
                                                     from list_c i,
                                                          spr_params p
                                                     where i.fk_ses = fk_ses_
                                                       and p.id = i.fk_par
                                                       and p.cd = 'REP_USL'
                                                       and i.sel_cd = s.usl
                                                       and i.sel = 1)
                                          and exists(select *
                                                     from list_c i,
                                                          spr_params p
                                                     where i.fk_ses = fk_ses_
                                                       and p.id = i.fk_par
                                                       and p.cd = 'REP_STATUS'
                                                       and i.sel_id = s.status
                                                       and i.sel = 1)
                                          and exists
                                            (select su.fk_reu
                                             from scott.c_users_perm su
                                                      join scott.u_list ut
                                                           on ut.cd = 'доступ к отчётам'
                                                               and su.fk_perm_tp = ut.id
                                                      join scott.t_user us
                                                           on lower(us.cd) = lower(user)
                                                               and su.user_id = us.id
                                             where su.fk_reu = s.reu
                                            )
                                          and ((var_ = 2 and s.reu = reu_) or (var_ = 1 and t.trest = trest_) or var_ = 0)
                                          and (l_sel_id = 0 or l_sel_id <> 0 and l_sel_id = s.fk_tp)
                                          and s.mg between mg_ and mg1_
                                        union all
                                        select null as lsk,
                                               null as org,
                                               null as fk_org2,
                                               '000' as uslm,
                                               '000' as usl,
                                               'Итого' as name_org,
                                               'Итого' as name_usl,
                                               'Итого' as name_org_main,
                                               t.trest,
                                               s.reu,
                                               t.name_reu as reu_name,
                                               null as name,
                                               null as predpr_det,
                                               null as fio,
                                               s.status,
                                               decode(s.status, 2, 'Приват', 'Муницип') as status_name,
                                               s.psch as psch,
                                               s.sch as sch,
                                               decode(s.sch,0,'Норматив',1,'Счетчик','Нет') as sch_name,
                                               s.val_group,
                                               s.val_group2,
                                               s.cnt as cnt,
                                               s.klsk as klsk,
                                               s.kpr as kpr,
                                               decode(s.is_empt, 1, 'да', 0, 'нет', null) as is_empt,
                                               s.kpr_ot as kpr_ot,
                                               s.kpr_wr as kpr_wr,
                                               s.cnt_lg as cnt_lg,
                                               s.cnt_subs as cnt_subs,
                                               s.cnt_room,
                                               s.uch,
                                               substr(s.mg, 1, 4) || '-' || substr(s.mg, 5, 2) as mg1,
                                               null as npp,
                                               null as name_gr,
                                               null as odpu_ex,
                                               0 as odpu_kub,
                                               0 as kub_dist,
                                               0 as kub_fact,
                                               0 as kub_fact_upnorm,
                                               tp.name as lsk_tp,
                                               s.opl,
                                               s.is_vol,
                                               s.chng_vol,
                                               null as ishotpipe,
                                               null as istowel,
                                               null as kr_soi,
                                               null as fact_cons,
                                               null as psch_name
                                        from statistics_trest s,
                                             s_reu_trest t,
                                             v_lsk_tp tp
                                        where s.reu = t.reu
                                          and s.fk_tp = tp.id(+)
                                          and s.usl is null
                                          and exists(select *
                                                     from list_c i,
                                                          spr_params p
                                                     where i.fk_ses = fk_ses_
                                                       and p.id = i.fk_par
                                                       and p.cd = 'REP_USL'
                                                       and i.sel_cd = '0' --Включить ли ИТОГ?
                                                       and i.sel = 1)
                                          and exists(select *
                                                     from list_c i,
                                                          spr_params p
                                                     where i.fk_ses = fk_ses_
                                                       and p.id = i.fk_par
                                                       and p.cd = 'REP_STATUS'
                                                       and i.sel_id = s.status
                                                       and i.sel = 1)
                                          and exists
                                            (select su.fk_reu
                                             from scott.c_users_perm su
                                                      join scott.u_list ut
                                                           on ut.cd = 'доступ к отчётам'
                                                               and su.fk_perm_tp = ut.id
                                                      join scott.t_user us
                                                           on lower(us.cd) = lower(user)
                                                               and su.user_id = us.id
                                             where su.fk_reu = s.reu
                                            )
                                          and ((var_ = 2 and s.reu = reu_) or (var_ = 1 and t.trest = trest_) or var_ = 0)
                                          and (l_sel_id = 0 or l_sel_id <> 0 and l_sel_id = s.fk_tp)
                                          and s.mg between mg_ and mg1_
                                        order by npp;
end if;
elsif сd_ = '18' then
            --Статистика по льготникам
            if var_ = 3 then
                -- по Дому
                open prep_refcursor for 'select t.trest||'' ''||t.name_tr as predp, k.name||'', ''||NVL(LTRIM(s.nd,''0''),''0'') as predpr_det,
       NVL(LTRIM(s.kw,''0''),''0'') AS kw, g.name AS spk_name, DECODE(s.main,1,''Носитель'',''Пользующ'') AS main, u.nm AS usl_name, s.cnt
       FROM STATISTICS_LG_LSK s, S_REU_TREST t, SPUL k, SPRORG p, SPK g, USL u
       WHERE ' || sqlstr_ || ' AND s.reu=t.reu AND s.kul=k.id AND s.ORG=p.kod AND s.spk_id=g.id AND s.USL=u.USL AND
       s.reu=:reu_ AND s.kul=:kul_ AND s.nd=:nd_
       ORDER BY utils.f_order(s.kw,7)' using reu_, kul_, nd_;
elsif var_ = 2 then
                -- по РЭУ
                open prep_refcursor for 'select t.trest||'' ''||t.name_tr as predp, s.reu,  k.name||'', ''||NVL(LTRIM(s.nd,''0''),''0'') as predpr_det,
       NULL AS kw, g.name AS spk_name, DECODE(s.main,1,''Носитель'',''Пользующ'') AS main, u.nm AS usl_name, s.cnt
       FROM STATISTICS_LG s, S_REU_TREST t, SPUL k, SPRORG p, SPK g, USL u
       WHERE ' || sqlstr_ || ' AND s.reu=t.reu AND s.kul=k.id AND s.ORG=p.kod AND s.spk_id=g.id AND s.USL=u.USL AND
       s.reu=:reu_
       ORDER BY k.name, utils.f_order(s.nd,6)' using reu_;
elsif var_ = 1 then
                -- по ЖЭО
                open prep_refcursor for 'select t.trest||'' ''||t.name_tr as predp, s.reu, null as predpr_det,
       null as kw, g.name as spk_name, decode(s.main,1,''Носитель'',''Пользующ'') as main, u.nm as usl_name, s.cnt
       from statistics_lg_trest s, s_reu_trest t, sprorg p, spk g, usl u
       where ' || sqlstr_ || ' and s.reu=t.reu and s.org=p.kod and s.spk_id=g.id and s.usl=u.usl and t.trest=:trest_
       order by s.reu' using trest_;
null;
elsif var_ = 0 then
                -- по МП УЕЗЖКУ
                open prep_refcursor for 'select t.trest||'' ''||t.name_tr as predp, s.reu, null as predpr_det,
       NULL AS kw, p.name AS orgname, g.name AS spk_name, DECODE(s.main,1,''Носитель'',''Пользующ'') AS main, u.nm AS usl_name, s.cnt
       FROM STATISTICS_LG_TREST s, S_REU_TREST t, SPRORG p, SPK g, USL u
       WHERE ' || sqlstr_ || ' AND s.reu=t.reu AND s.ORG=p.kod AND s.spk_id=g.id AND s.USL=u.USL
       ORDER BY t.trest';
end if;
elsif сd_ = '14' then
            show_sal_ := utils.gets_bool_param('REP_SHOW_SAL');
show_fond_ := utils.gets_list_param('REP_FOND');
l_sel_id := utils.gets_list_param('REP_TP_SCH_SEL');

kpr1_ := utils.gets_int_param('REP_RNG_KPR1'); --kpr1, kpr2 - используется в других ведомостях, но неправильно!!!
kpr2_ := utils.gets_int_param('REP_RNG_KPR2'); --только в оборотке этой - корректно! исправить потом Lev, 29.10.2015
if det_ <> 3 and (kpr1_ is not null or kpr2_ is not null) then
                raise_application_error(-20000,
                                        'Внимание! Попытка использовать не действующий на данном уровне детализации параметр - кол-во проживающих!');
end if;

--Оборотка
if det_ = 3 then
                --детализация до квартир
                open prep_refcursor for select x.mg,
                                               substr(x.mg, 1, 4) || '-' || substr(x.mg, 5, 2) as mg1,
                                               h.lsk,
                                               h.name_tr as predpr,
                                               h.name_reu as reu,
                                               h.adr as predpr_det,
                                               decode(h.type, 0, 'Прочие', 'Основные') as type,
                                               decode(h.status, 2, 'Приват', 'Муницип') as status,
                                               d.kod as org,
                                               d.kod || ' ' || d.name as org_name,
                                               c.usl,
                                               c.usl || ' ' || c.nm as usl_name,
                                               c.uslm,
                                               null as name_gr,
                                               case
                                                   when show_sal_ = 0 and x.mg > mg_ and mg_ <> mg1_ then 0
                                                   else i.indebet end as indebet,
                                               case
                                                   when show_sal_ = 0 and x.mg > mg_ and mg_ <> mg1_ then 0
                                                   else i.inkredit end as inkredit,
                                               case
                                                   when show_sal_ = 0 and x.mg < mg1_ and mg_ <> mg1_ then 0
                                                   else i.outdebet end as outdebet,
                                               case
                                                   when show_sal_ = 0 and x.mg < mg1_ and mg_ <> mg1_ then 0
                                                   else i.outkredit end as outkredit,
                                               i.charges as charges,
                                               case
                                                   when show_sal_ = 0 and x.mg > mg_ and mg_ <> mg1_ then 0
                                                   else i.pinsal end as pinsal,
                                               case
                                                   when show_sal_ = 0 and x.mg < mg1_ and mg_ <> mg1_ then 0
                                                   else i.poutsal end as poutsal,
                                               i.changes as changes,
                                               i.changes2 as changes2,
                                               i.changes3 as changes3,
                                               nvl(i.changes, 0) + nvl(i.changes2, 0) + nvl(i.changes3, 0) as changeall,
                                               i.subsid as subsid,
                                               i.privs as privs,
                                               i.payment as payment,
                                               i.pcur as pcur,
                                               i.pn as pn,
                                               null as odpu_ex,
                                               null as other_name,
                                               null as val_group2,
                                               a.fk_tp as fk_lsk_tp,
                                               h.psch as psch,
                                               d.grp,
                                               null as ishotpipe,
                                               null as istowel,
                                               a.fio,
                                               decode(h.psch, 0, 'Открытые Л/С', 1, 'Закрытые Л/С', 2,
                                                      'Старый фонд') as psch_name
                                        from (select e.lsk,
                                                     case
                                                         when k.psch in (8)
                                                             then 2 -- для отображения признаков открытого, старого, закрытого фонда
                                                         when k.psch in (9) then 1
                                                         else 0 end as psch,
                                                     e.usl,
                                                     e.org,
                                                     k.nd,
                                                     k.kw,
                                                     k.status,
                                                     k.house_id,
                                                     u.uslm,
                                                     g.type,
                                                     ot.name_short || '.' || initcap(o.name) || ', ' ||
                                                     initcap(s.name) || ', ' || nvl(ltrim(k.nd, '0'), '0') || '-' ||
                                                     nvl(ltrim(k.kw, '0'), '0') as adr,
                                                     ot.name_short || '.' || initcap(o.name) || ', ' || initcap(s.name) as street1,
                                                     s.name_reu,
                                                     s.name_tr
                                              from t_saldo_lsk2 e,
                                                   kart k,
                                                   spul s,
                                                   sprorg g,
                                                   s_reu_trest s,
                                                   usl u,
                                                   t_org o,
                                                   t_org_tp ot
                                              where e.lsk = k.lsk
                                                and k.reu = s.reu
                                                and e.org = g.kod
                                                and k.kul = s.id
                                                and e.usl = u.usl
                                                and s.fk_settlement = o.id
                                                and o.fk_orgtp = ot.id
                                                and exists
                                                  (select su.fk_reu
                                                   from scott.c_users_perm su
                                                            join scott.u_list ut
                                                                 on ut.cd = 'доступ к отчётам'
                                                                     and su.fk_perm_tp = ut.id
                                                            join scott.t_user us
                                                                 on lower(us.cd) = lower(user)
                                                                     and su.user_id = us.id
                                                   where su.fk_reu = k.reu
                                                  )
                                                and exists(select *
                                                           from list_c i,
                                                                spr_params p
                                                           where i.fk_ses = fk_ses_
                                                             and p.id = i.fk_par
                                                             and p.cd = 'REP_USL2'
                                                             and i.sel_cd = e.usl
                                                             and i.sel = 1)
                                                and (show_fond_ = 1 and k.psch not in (8, 9) or
                                                     show_fond_ = 2 and k.psch in (8, 9) or show_fond_ = 0 or
                                                     show_fond_ is null)
                                                and decode(var_, 3, reu_, k.reu) = k.reu
                                                and decode(var_, 3, kul_, k.kul) = k.kul
                                                and decode(var_, 3, nd_, k.nd) = k.nd
                                                and decode(var_, 2, reu_, k.reu) = k.reu
                                                and decode(var_, 1, trest_, s.trest) = s.trest) h
                                                 join xitog3_lsk i
                                                      on h.lsk = i.lsk(+) and h.org = i.org(+) and h.usl = i.usl(+)
                                                 join arch_kart a
                                                      on i.lsk = a.lsk and a.kpr >= coalesce(kpr1_, a.kpr) and
                                                         a.kpr <= coalesce(kpr2_, a.kpr) and i.mg = a.mg
                                                 join (select * from period_reports t where id = 14) x on x.mg = i.mg
                                                 join sprorg d on h.org = d.kod
                                                 join usl c on h.usl = c.usl
                                                 join kart_detail det on h.lsk = det.lsk
                                        where (l_sel_id = 0 or l_sel_id <> 0 and l_sel_id = a.fk_tp)
                                          and x.mg between mg_ and mg1_
                                        order by det.ord1;

elsif det_ = 2 then

delete from temp_stat2;
insert into temp_stat2 (kub, kub_dist, kub_fact_upnorm, kub_fact, uslm, uslm_group1, mg, reu, kul, nd, dist_tp,
                        odpu_ex, ishotpipeinsulated, istowelheatexist, kr_soi, fact_cons)
select       --для отображения объемов по ОДПУ
    distinct d.kub,
             d.kub_dist,
             d.kub_fact_upnorm,
             decode(d.dist_tp, 4, null, d.kub_fact) as kub_fact, -- не показывать объем, если 4 (нет ОДПУ) ред.05.03.2017
             u.uslm,
             u.uslm_group1,
             d.mg,
             h.reu,
             h.kul,
             h.nd,
             d.dist_tp as dist_tp,
             case
                 when d.dist_tp <> 4 and nvl(d.kub, 0) = 0 then 'есть, нет объема'
                 when d.dist_tp <> 4 and nvl(d.kub, 0) <> 0 then 'есть'
                 else 'нет' end as odpu_ex,
             nvl(d.ishotpipeinsulated, 0) as ishotpipeinsulated,
             nvl(d.istowelheatexist, 0) as istowelheatexist,
             case
                 when d.usl not in ('053', '054') then d.kub - (d.kub_norm + d.kub_sch + d.kub_ar)
                 else 0 end as kr_soi,                           -- кр на сои (только для первой строки) и не для 053 и 054 услуг
             d.kub_norm + d.kub_sch + d.kub_ar as fact_cons      -- факт.потребление (только для первой строки)
from a_vvod d,
     arch_kart h,
     a_nabor2 n,
     s_reu_trest s,
     usl u
where h.house_id = d.house_id
  and h.mg = d.mg
  and d.mg between mg_ and mg1_
  and d.usl = u.usl
  and d.id = n.fk_vvod
  and d.usl = n.usl
  and h.lsk = n.lsk
  and d.mg between n.mgfrom and n.mgto
  and h.psch not in (8, 9)
  and h.reu = s.reu
  and case
          when var_ = 3 and h.reu = reu_ and h.kul = kul_ and h.nd = nd_ then 1
          when var_ = 2 and h.reu = reu_ then 1
          when var_ = 1 and s.trest = trest_ then 1
          when var_ = 0 then 1 end = 1;

-- детализация до домов
open prep_refcursor for select /*+ USE_HASH(h, i, h3, h2, hl, st )*/ i.mg,
                                                                     substr(i.mg, 1, 4) || '-' || substr(i.mg, 5, 2) as mg1,
                                                                     null as lsk,
                                                                     t.name_tr as predpr,
                                                                     h.name_reu as reu,
                                                                     ot.name_short || '.' ||
                                                                     initcap(o.name) || ', ' ||
                                                                     initcap(k.name) || ', ' ||
                                                                     nvl(ltrim(h.nd, '0'), '0') as predpr_det,
                                                                     utils.f_order(h.nd, 6) as ord1,
                                                                     decode(h.type, 0, 'прочие', 'основные') as type,
                                                                     decode(h.status, 2, 'Приват', 'Муницип') as status,
                                                                     d.kod as org,
                                                                     d.kod || ' ' || d.name as org_name,
                                                                     c.usl,
                                                                     c.usl || ' ' || c.nm as usl_name,
                                                                     c.uslm,
                                                                     hl.name as name_gr,
                                                                     case
                                                                         when show_sal_ = 0 and i.mg > mg_ and mg_ <> mg1_
                                                                             then 0
                                                                         else i.indebet end as indebet,
                                                                     case
                                                                         when show_sal_ = 0 and i.mg > mg_ and mg_ <> mg1_
                                                                             then 0
                                                                         else i.inkredit end as inkredit,
                                                                     case
                                                                         when show_sal_ = 0 and i.mg < mg1_ and mg_ <> mg1_
                                                                             then 0
                                                                         else i.outdebet end as outdebet,
                                                                     case
                                                                         when show_sal_ = 0 and i.mg < mg1_ and mg_ <> mg1_
                                                                             then 0
                                                                         else i.outkredit end as outkredit,
                                                                     i.charges as charges,
                                                                     case
                                                                         when show_sal_ = 0 and i.mg > mg_ and mg_ <> mg1_
                                                                             then 0
                                                                         else i.pinsal end as pinsal,
                                                                     case
                                                                         when show_sal_ = 0 and i.mg < mg1_ and mg_ <> mg1_
                                                                             then 0
                                                                         else i.poutsal end as poutsal,
                                                                     i.changes as changes,
                                                                     i.changes2 as changes2,
                                                                     i.changes3 as changes3,
                                                                     nvl(i.changes, 0) + nvl(i.changes2, 0) + nvl(i.changes3, 0) as changeall,
                                                                     i.subsid as subsid,
                                                                     i.privs as privs,
                                                                     i.payment as payment,
                                                                     i.pcur as pcur,
                                                                     i.pn as pn,
                                                                     h5.odpu_ex,
                                                                     h3.other_name,
                                                                     st.val_group2,
                                                                     h.fk_lsk_tp,
                                                                     null as psch,
                                                                     d.grp,
                                                                     decode(h5.ishotpipeinsulated, 1, 'да', 'нет') as ishotpipe,
                                                                     decode(h5.istowelheatexist, 1, 'да', 'нет') as istowel,
                                                                     null as fio,
                                                                     null as psch_name
                        from (select distinct e.reu,
                                              e.kul,
                                              e.nd,
                                              e.usl,
                                              e.org,
                                              e.status,
                                              e.fk_lsk_tp,
                                              o.type,
                                              s.trest,
                                              s.name_reu,
                                              nvl(u.parent_usl, e.usl) as parent_usl,
                                              u.uslm,
                                              u.uslm_group1
                              from t_saldo_reu_kul_nd_st e,
                                   sprorg o,
                                   usl u,
                                   s_reu_trest s
                              where e.org = o.kod
                                and e.reu = s.reu
                                and e.usl = u.usl
                                and exists
                                  (select su.fk_reu
                                   from scott.c_users_perm su
                                            join scott.u_list ut
                                                 on ut.cd = 'доступ к отчётам'
                                                     and su.fk_perm_tp = ut.id
                                            join scott.t_user us
                                                 on lower(us.cd) = lower(user)
                                                     and su.user_id = us.id
                                   where su.fk_reu = e.reu
                                  )
                                and exists(select *
                                           from list_c i,
                                                spr_params p
                                           where i.fk_ses = fk_ses_
                                             and p.id = i.fk_par
                                             and p.cd = 'REP_USL2'
                                             and i.sel_cd = e.usl
                                             and i.sel = 1)
                                and decode(var_, 3, reu_, e.reu) = e.reu
                                and decode(var_, 3, kul_, e.kul) = e.kul
                                and decode(var_, 3, nd_, e.nd) = e.nd
                                and decode(var_, 2, reu_, e.reu) = e.reu
                                and decode(var_, 1, trest_, s.trest) = s.trest) h
                                 join xitog3 i on h.reu = i.reu and h.kul = i.kul and h.nd = i.nd and
                                                  h.org = i.org and h.usl = i.usl and
                                                  h.status = i.status and h.fk_lsk_tp = i.fk_lsk_tp and
                                                  i.mg between mg_ and mg1_
                                 left join (select distinct --t.mg,  -- ред.06.11.2019 - перевел на c_houses
                                                            --k.reu,
                                                            t.kul,
                                                            t.nd,
                                                            o.name as other_name
                                            from c_houses t,
                                                 t_org o,                -- ред.06.11.2019 - перевел на c_houses
                                                 (select --max(t.reu) as reu,
                                                         t.house_id
                                                  from kart t
                                                  where t.psch not in (8, 9)
                                                  group by t.house_id) k --работает в полыс, не убирать!
                                            where t.fk_other_org = o.id
                                              and t.id = k.house_id) h3 on --i.reu=h3.reu and
                                    i.kul = h3.kul and i.nd = h3.nd-- and i.mg=h3.mg -- ред.06.11.2019 - перевел на c_houses
                                 left join (select distinct t.uslm_group1,
                                                            t.reu,
                                                            t.kul,
                                                            t.nd,
                                                            t.mg,
                                                            t.ishotpipeinsulated,
                                                            t.istowelheatexist,
                                                            t.odpu_ex
                                            from temp_stat2 t) h5
                                           on i.mg = h5.mg and i.reu = h5.reu and i.kul = h5.kul and
                                              i.nd = h5.nd and h.uslm_group1 = h5.uslm_group1
                                 join sprorg d on h.org = d.kod
                                 join usl c on h.usl = c.usl
                                 join org l on l.id = 1
                                 join s_reu_trest t on h.reu = t.reu
                                 join spul k on h.kul = k.id
                                 join t_org o on k.fk_settlement = o.id
                                 join t_org_tp ot on o.fk_orgtp = ot.id
                                 left join (select t.reu, t.kul, t.nd, u.name
                                            from t_housexlist t,
                                                 u_list u
                                            where t.fk_list = u.id) hl
                                           on h.reu = hl.reu and h.kul = hl.kul and h.nd = hl.nd
                                 left join (select t.reu,
                                                   t.kul,
                                                   t.nd,
                                                   t.mg,
                                                   t.usl,
                                                   max(t.val_group2) as val_group2
                                            from statistics t --подключил стату здесь, чтобы были видны нормативы в оборотке...
                                            where t.mg between mg_ and mg1_
                                            group by t.reu, t.kul, t.nd, t.mg, t.usl) st
                                           on i.reu = st.reu and i.kul = st.kul and i.nd = st.nd and
                                              i.mg = st.mg and h.parent_usl = st.usl
                        where (decode(l_sel_id, 0, h.fk_lsk_tp, l_sel_id) = h.fk_lsk_tp)

                        order by i.mg,
                                 ot.name_short || '.' || initcap(o.name) || ', ' || initcap(k.name),
                                 utils.f_order(h.nd, 6);
elsif det_ in (0, 1) then
                -- до УК
                open prep_refcursor for select /*+ USE_HASH(h,i,o,u,x,d,c,l,t,hl)*/ x.mg,
                                                                                    substr(x.mg, 1, 4) || '-' || substr(x.mg, 5, 2) as mg1,
                                                                                    null as lsk,
                                                                                    t.trest || ' ' || t.name_tr as predpr,
                                                                                    h.name_reu as reu,
                                                                                    null as predpr_det,
                                                                                    decode(h.type, 0, 'прочие', 'основные') as type,
                                                                                    decode(h.status, 2, 'Приват', 'Муницип') as status,
                                                                                    d.kod as org,
                                                                                    d.kod || ' ' || d.name as org_name,
                                                                                    c.usl,
                                                                                    c.usl || ' ' || c.nm as usl_name,
                                                                                    c.uslm,
                                                                                    hl.name as name_gr,
                                                                                    case
                                                                                        when show_sal_ = 0 and x.mg > mg_ and mg_ <> mg1_
                                                                                            then 0
                                                                                        else sum(i.indebet) end as indebet,
                                                                                    case
                                                                                        when show_sal_ = 0 and x.mg > mg_ and mg_ <> mg1_
                                                                                            then 0
                                                                                        else sum(i.inkredit) end as inkredit,
                                                                                    case
                                                                                        when show_sal_ = 0 and x.mg < mg1_ and mg_ <> mg1_
                                                                                            then 0
                                                                                        else sum(i.outdebet) end as outdebet,
                                                                                    case
                                                                                        when show_sal_ = 0 and x.mg < mg1_ and mg_ <> mg1_
                                                                                            then 0
                                                                                        else sum(i.outkredit) end as outkredit,
                                                                                    sum(o.charges) as charges,
                                                                                    case
                                                                                        when show_sal_ = 0 and x.mg > mg_ and mg_ <> mg1_
                                                                                            then 0
                                                                                        else sum(o.pinsal) end as pinsal,
                                                                                    case
                                                                                        when show_sal_ = 0 and x.mg < mg1_ and mg_ <> mg1_
                                                                                            then 0
                                                                                        else sum(o.poutsal) end as poutsal,
                                                                                    sum(o.changes) as changes,
                                                                                    sum(o.changes2) as changes2,
                                                                                    sum(o.changes3) as changes3,
                                                                                    sum(nvl(o.changes, 0) + nvl(o.changes2, 0) + nvl(o.changes3, 0)) as changeall,
                                                                                    sum(o.changes) as changes,
                                                                                    sum(o.subsid) as subsid,
                                                                                    sum(o.privs) as privs,
                                                                                    sum(o.payment) as payment,
                                                                                    sum(o.pcur) as pcur,
                                                                                    sum(o.pn) as pn,
                                                                                    null as odpu_ex,
                                                                                    null as other_name,
                                                                                    null as val_group2,
                                                                                    h.fk_lsk_tp,
                                                                                    null as psch,
                                                                                    d.grp,
                                                                                    null as ishotpipe,
                                                                                    null as istowel,
                                                                                    null as fio,
                                                                                    null as psch_name
                                        from (select distinct e.reu,
                                                              e.kul,
                                                              e.nd,
                                                              e.org,
                                                              e.usl,
                                                              u.uslm,
                                                              e.status,
                                                              o.type,
                                                              s.trest,
                                                              s.name_reu,
                                                              e.fk_lsk_tp
                                              from t_saldo_reu_kul_nd_st e,
                                                   usl u,
                                                   sprorg o,
                                                   s_reu_trest s
                                              where e.org = o.kod
                                                and e.reu = s.reu
                                                and e.usl = u.usl
                                                and exists
                                                  (select su.fk_reu
                                                   from scott.c_users_perm su
                                                            join scott.u_list ut
                                                                 on ut.cd = 'доступ к отчётам'
                                                                     and su.fk_perm_tp = ut.id
                                                            join scott.t_user us
                                                                 on lower(us.cd) = lower(user)
                                                                     and su.user_id = us.id
                                                   where su.fk_reu = e.reu
                                                  )
                                                and exists(select *
                                                           from list_c i,
                                                                spr_params p
                                                           where i.fk_ses = fk_ses_
                                                             and p.id = i.fk_par
                                                             and p.cd = 'REP_USL2'
                                                             and i.sel_cd = e.usl
                                                             and i.sel = 1)
                                                and decode(var_, 2, reu_, e.reu) = e.reu
                                                and decode(var_, 1, trest_, s.trest) = s.trest) h,
                                             xitog3 i,
                                             (select reu,
                                                     kul,
                                                     nd,
                                                     status,
                                                     org,
                                                     usl,
                                                     mg,
                                                     fk_lsk_tp,
                                                     sum(charges) as charges,
                                                     sum(pinsal) as pinsal,
                                                     sum(poutsal) as poutsal,
                                                     sum(changes) as changes,
                                                     sum(changes2) as changes2,
                                                     sum(changes3) as changes3,
                                                     sum(subsid) as subsid,
                                                     sum(privs) as privs,
                                                     sum(payment) as payment,
                                                     sum(pcur) as pcur,
                                                     sum(pn) as pn
                                              from xitog3 t
                                              where t.mg between mg_ and mg1_
                                              group by reu, kul, nd, status, org, usl, mg, fk_lsk_tp) o,
                                             (select * from period_reports t where id = 14) x,
                                             sprorg d,
                                             usl c,
                                             org l,
                                             s_reu_trest t,
                                             (select t.reu, t.kul, t.nd, u.name
                                              from t_housexlist t,
                                                   u_list u
                                              where t.fk_list = u.id) hl
                                        where h.reu = i.reu(+)
                                          and h.kul = i.kul(+)
                                          and h.nd = i.nd(+)
                                          and h.org = i.org(+)
                                          and h.usl = i.usl(+)
                                          and h.status = i.status(+)
                                          and h.fk_lsk_tp = i.fk_lsk_tp(+)
                                          and h.reu = o.reu(+)
                                          and h.kul = o.kul(+)
                                          and h.nd = o.nd(+)
                                          and h.org = o.org(+)
                                          and h.usl = o.usl(+)
                                          and h.status = o.status(+)
                                          and h.fk_lsk_tp = o.fk_lsk_tp(+)
                                          and l.id = 1
                                          and h.org = d.kod
                                          and h.usl = c.usl
                                          and h.reu = t.reu
                                          and x.mg = i.mg
                                          and x.mg = o.mg
                                          and x.mg between mg_ and mg1_
                                          and h.reu = hl.reu(+)
                                          and h.kul = hl.kul(+)
                                          and h.nd = hl.nd(+)
                                          and (decode(l_sel_id, 0, h.fk_lsk_tp, l_sel_id) = h.fk_lsk_tp)
                                        group by hl.name, decode(h.type, 0, 'прочие', 'основные'), x.mg,
                                                 substr(x.mg, 1, 4) || '-' || substr(x.mg, 5, 2),
                                                 t.trest || ' ' || t.name_tr, h.name_reu, h.status, h.type, d.kod,
                                                 c.usl, c.uslm, h.fk_lsk_tp, d.grp, d.kod || ' ' || d.name, c.usl,
                                                 c.usl || ' ' || c.nm
                                        order by x.mg; --        USING show_sal_, mg_, mg_, mg1_, show_sal_, mg_, mg_, mg1_, show_sal_, mg1_, mg_, mg1_, show_sal_,
--          mg1_, mg_, mg1_, fk_ses_, var_, reu_, var_, trest_, var_, mg_, mg1_;
else
                open prep_refcursor for 'select null as predpr, null as reu, null as predpr_det, null as type,
          null as lsk,
          NULL AS STATUS, NULL AS ORG, NULL AS nm1, NULL AS name_gr, NULL AS indebet, NULL AS inkredit,
          NULL AS CHARGES, NULL AS POUTSAL, NULL AS CHANGES, NULL AS CHANGES2, NULL AS CHANGEALL, NULL AS subsid, NULL AS PRIVS, NULL AS payment,
          NULL AS pn, NULL AS outdebet, NULL AS outkredit, null fk_lsk_tp
          FROM dual';
end if;
elsif сd_ = '35' then
            -- Оплата OLAP
            if det_ = 3 then
                --детализация до квартир
                open prep_refcursor for select null as predp,
                                               o.oper || ' ' || o.naim as opername,
                                               null as reu,
                                               --'ЖЭО:'||k.reu||'-'||l.name||', '||NVL(LTRIM(k.nd,'0'),'0')||'-'||NVL(LTRIM(k.kw,'0'),'0') -- было до 20.10.20
                                               ot.name_short || '.' || initcap(o2.name) || ', ' || initcap(l.name) ||
                                               ', ' || nvl(ltrim(k.nd, '0'), '0') || '-' ||
                                               nvl(ltrim(k.kw, '0'), '0') as predpr_det,
                                               null as kw,
                                               s.var,
                                               s.dopl,
                                               substr(s.dopl, 1, 4) || '-' || substr(s.dopl, 5, 2) as dopl_name,
                                               s.mg,
                                               substr(s.mg, 1, 4) || '-' || substr(s.mg, 5, 2) as mg_name,
                                               s.dat,
                                               to_char(r.kod) || ' ' || r.name as org_name,
                                               v.name as var_name,
                                               u.nm,
                                               u.nm1,
                                               sum(s.summa) as summa,
                                               decode(s.cd_tp, 0, 'Пеня', 'Оплата') as cd_tp
                                        from kart k,
                                             xxito14_lsk s,
                                             s_reu_trest t,
                                             sprorg r,
                                             variant_xxito10 v,
                                             spul l,
                                             oper o,
                                             usl u,
                                             t_org o2,
                                             t_org_tp ot
                                        where exists
                                            (select su.fk_reu
                                             from scott.c_users_perm su
                                                      join scott.u_list ut
                                                           on ut.cd = 'доступ к отчётам'
                                                               and su.fk_perm_tp = ut.id
                                                      join scott.t_user us
                                                           on lower(us.cd) = lower(user)
                                                               and su.user_id = us.id
                                             where su.fk_reu = k.reu
                                            )
                                          and k.lsk = s.lsk
                                          and s.usl = u.usl
                                          and s.oper = o.oper
                                          and s.org = r.kod
                                          and s.var = v.id
                                          and k.kul = l.id
                                          and k.reu = t.reu

                                          and (dat_ is null and dat1_ is null and s.mg between mg_ and mg1_ or
                                               dat_ is not null and dat1_ is null and s.dat = dat_ or
                                               dat_ is not null and dat1_ is not null and s.dat between dat_ and dat1_)

                                          and decode(var_, 3, reu_, k.reu) = k.reu
                                          and decode(var_, 3, kul_, k.kul) = k.kul
                                          and decode(var_, 3, nd_, k.nd) = k.nd
                                          and decode(var_, 2, reu_, k.reu) = k.reu
                                          and decode(var_, 1, trest_, t.trest) = t.trest
                                          and l.fk_settlement = o2.id
                                          and o2.fk_orgtp = ot.id
                                        group by o.oper || ' ' || o.naim,
                                                 ot.name_short || '.' || initcap(o2.name) || ', ' || initcap(l.name) ||
                                                 ', ' || nvl(ltrim(k.nd, '0'), '0') || '-' ||
                                                 nvl(ltrim(k.kw, '0'), '0'), s.var, s.dopl,
                                                 substr(s.dopl, 1, 4) || '-' || substr(s.dopl, 5, 2), s.mg,
                                                 substr(s.mg, 1, 4) || '-' || substr(s.mg, 5, 2), s.dat,
                                                 to_char(r.kod) || ' ' || r.name, v.name, u.nm, u.nm1,
                                                 decode(s.cd_tp, 0, 'Пеня', 'Оплата')
                                        order by s.dopl desc;
elsif det_ in (2) then
                -- детализация до домов
                open prep_refcursor for select t.name_tr as predp,
                                               o.oper || ' ' || o.naim as opername,
                                               t.name_reu as reu,
                                               -- ' ЖЭО:'||s.forreu||'-'||k.name||', '||NVL(LTRIM(s.nd,'0'),'0') -- было до 20.10.20
                                               ot.name_short || '.' || initcap(o2.name) || ', ' || initcap(k.name) ||
                                               ', ' || nvl(ltrim(s.nd, '0'), '0') as predpr_det,
                                               null as kw,
                                               s.var,
                                               s.dopl,
                                               substr(s.dopl, 1, 4) || '-' || substr(s.dopl, 5, 2) as dopl_name,
                                               s.mg,
                                               substr(s.mg, 1, 4) || '-' || substr(s.mg, 5, 2) as mg_name,
                                               s.dat,
                                               to_char(r.kod) || ' ' || r.name org_name,
                                               v.name as var_name,
                                               u.nm,
                                               u.nm1,
                                               sum(s.summa) as summa,
                                               decode(s.cd_tp, 0, 'Пеня', 'Оплата') as cd_tp
                                        from xxito14 s,
                                             s_reu_trest t,
                                             sprorg r,
                                             variant_xxito10 v,
                                             spul k,
                                             oper o,
                                             usl u,
                                             t_org o2,
                                             t_org_tp ot
                                        where exists
                                            (select su.fk_reu
                                             from scott.c_users_perm su
                                                      join scott.u_list ut
                                                           on ut.cd = 'доступ к отчётам'
                                                               and su.fk_perm_tp = ut.id
                                                      join scott.t_user us
                                                           on lower(us.cd) = lower(user)
                                                               and su.user_id = us.id
                                             where su.fk_reu = s.forreu
                                            )
                                          and s.usl = u.usl
                                          and s.oper = o.oper
                                          and s.forreu = t.reu
                                          and s.org = r.kod
                                          and s.var = v.id
                                          and s.kul = k.id

                                          and (dat_ is null and dat1_ is null and s.mg between mg_ and mg1_ or
                                               dat_ is not null and dat1_ is null and s.dat = dat_ or
                                               dat_ is not null and dat1_ is not null and s.dat between dat_ and dat1_)

                                          and decode(var_, 3, reu_, s.forreu) = s.forreu
                                          and decode(var_, 2, reu_, s.forreu) = s.forreu
                                          and decode(var_, 1, trest_, t.trest) = t.trest
                                          and k.fk_settlement = o2.id
                                          and o2.fk_orgtp = ot.id

                                        group by t.name_tr, o.oper || ' ' || o.naim, t.name_reu,
                                                 ot.name_short || '.' || initcap(o2.name) || ', ' || initcap(k.name) ||
                                                 ', ' || nvl(ltrim(s.nd, '0'), '0'), s.var, s.dopl,
                                                 substr(s.dopl, 1, 4) || '-' || substr(s.dopl, 5, 2), s.mg,
                                                 substr(s.mg, 1, 4) || '-' || substr(s.mg, 5, 2), s.dat,
                                                 to_char(r.kod) || ' ' || r.name, v.name, u.nm, u.nm1,
                                                 decode(s.cd_tp, 0, 'Пеня', 'Оплата')
                                        order by s.dopl desc;
elsif det_ in (0, 1) then
                -- детализация до ЖЭО
                open prep_refcursor for select t.name_tr as predp,
                                               o.oper || ' ' || o.naim as opername,
                                               t.name_reu as reu,
                                               null as predpr_det,
                                               null as kw,
                                               s.var,
                                               s.dopl,
                                               substr(s.dopl, 1, 4) || '-' || substr(s.dopl, 5, 2) as dopl_name,
                                               s.mg,
                                               substr(s.mg, 1, 4) || '-' || substr(s.mg, 5, 2) as mg_name,
                                               s.dat,
                                               to_char(r.kod) || ' ' || r.name org_name,
                                               v.name as var_name,
                                               u.nm,
                                               u.nm1,
                                               sum(s.summa) as summa,
                                               decode(s.cd_tp, 0, 'Пеня', 'Оплата') as cd_tp

                                        from xxito14 s,
                                             s_reu_trest t,
                                             sprorg r,
                                             variant_xxito10 v,
                                             oper o,
                                             usl u
                                        where exists
                                            (select su.fk_reu
                                             from scott.c_users_perm su
                                                      join scott.u_list ut
                                                           on ut.cd = 'доступ к отчётам'
                                                               and su.fk_perm_tp = ut.id
                                                      join scott.t_user us
                                                           on lower(us.cd) = lower(user)
                                                               and su.user_id = us.id
                                             where su.fk_reu = s.forreu
                                            )
                                          and s.usl = u.usl
                                          and s.oper = o.oper
                                          and s.forreu = t.reu
                                          and s.org = r.kod
                                          and s.var = v.id

                                          and (dat_ is null and dat1_ is null and s.mg between mg_ and mg1_ or
                                               dat_ is not null and dat1_ is null and s.dat = dat_ or
                                               dat_ is not null and dat1_ is not null and s.dat between dat_ and dat1_)

                                          and decode(var_, 3, reu_, s.forreu) = s.forreu
                                          and decode(var_, 2, reu_, s.forreu) = s.forreu
                                          and decode(var_, 1, trest_, t.trest) = t.trest

                                        group by t.name_tr, o.oper || ' ' || o.naim, t.name_reu, s.var, s.dopl,
                                                 substr(s.dopl, 1, 4) || '-' || substr(s.dopl, 5, 2), s.mg,
                                                 substr(s.mg, 1, 4) || '-' || substr(s.mg, 5, 2), s.dat,
                                                 to_char(r.kod) || ' ' || r.name, v.name, u.nm, u.nm1,
                                                 decode(s.cd_tp, 0, 'Пеня', 'Оплата')
                                        order by s.dopl desc;
end if;
elsif сd_ = '36' then
            -- Сверка инкассаций
            if var_ = 3 then
                --По дому
                raise_application_error(-20001, 'Не существует уровня детализации!');
elsif var_ = 2 then
                --По РЭУ
                raise_application_error(-20001, 'Не существует уровня детализации!');
elsif var_ = 1 then
                --По ЖЭО
                raise_application_error(-20001, 'Не существует уровня детализации!');
elsif var_ = 0 then
                --(все тресты)
                open prep_refcursor for 'select sum(summa) as summa, sum(penya) as penya, opername,
               nink, nkom, dat_ink, dtek from (
               select t.summa as summa, t.penya as penya, o.oper||'' ''||o.naim as opername,
               t.nink, t.nkom, t.dat_ink, t.dtek
                 from c_kwtp_mg t, oper o
               where t.oper=o.oper and t.dat_ink between :dt1 and :dt2
               and :l_in_period=1
               union all
               select t.summa as summa, t.penya as penya, o.oper||'' ''||o.naim as opername,
               t.nink, t.nkom, t.dat_ink, t.dtek
                 from a_kwtp_mg t, oper o, params p
               where t.oper=o.oper and t.dat_ink between :dt1 and :dt2
               and :l_out_period=1 and t.mg <> p.period
               union all
               select t.summa as summa, t.penya as penya, o.oper||'' ''||o.naim as opername,
               t.nink, t.nkom, t.dat_ink, t.dtek
                 from c_kwtp_mg t, oper o
               where t.oper=o.oper and t.dat_ink is null
               and :l_in_period=1
               union all
               select t.summa as summa, t.penya as penya, o.oper||'' ''||o.naim as opername,
               t.nink, t.nkom, t.dat_ink, t.dtek
                 from a_kwtp_mg t, oper o, params p
               where t.oper=o.oper and t.dat_ink is null
               and :l_out_period=1 and t.mg <> p.period
               union all
               select t.summa, null as penya, o.oper||'' ''||o.naim as opername, null as nink,
                null as nkom, t.dat as dat_ink, t.dat
                from t_corrects_payments t, oper o where
                t.mg between :mg and :mg1 and o.oper=''99''
                ) a
               group by a.dat_ink, a.nkom, opername, a.nink, a.dtek
               order by a.dat_ink, a.nkom, opername, a.nink, a.dtek' using l_dt, l_dt1, l_in_period, l_dt, l_dt1, l_out_period, l_in_period, l_out_period, mg_, mg1_;
end if;
elsif сd_ = '37' then
            -- Сверка перерасчетов начисления
            if var_ = 3 then
                --По дому
                raise_application_error(-20001, 'Не существует уровня детализации!');
elsif var_ = 2 then
                --По РЭУ
                raise_application_error(-20001, 'Не существует уровня детализации!');
elsif var_ = 1 then
                --По ЖЭО
                raise_application_error(-20001, 'Не существует уровня детализации!');
elsif var_ = 0 then
                --По городу
                open prep_refcursor for select d.id,
                                               s.name_reu,
                                               u.nm,
                                               t.name as type_ch,
                                               ot.name_short || '.' || initcap(o.name) || ', ' || initcap(l.name) ||
                                               ', ' || nvl(ltrim(k.nd, '0'), '0') || '-' ||
                                               nvl(ltrim(k.kw, '0'), '0') as adr,
                                               c.mgchange,
                                               substr(c.mgchange, 1, 4) || '-' || substr(c.mgchange, 5, 2) as mg1,
                                               c.dtek,
                                               u.name,
                                               sum(c.summa) as summa,
                                               max(c.proc) as proc
                                        from kart k,
                                             spul l,
                                             c_change_docs d,
                                             c_change c,
                                             s_reu_trest s,
                                             t_user u,
                                             usl u,
                                             c_change_tp t,
                                             t_org o,
                                             t_org_tp ot
                                        where k.lsk = c.lsk
                                          and k.kul = l.id
                                          and k.reu = s.reu
                                          and d.id = c.doc_id
                                          and c.type = t.id
                                          and d.user_id = u.id
                                          and c.usl = u.usl
                                          and l.fk_settlement = o.id
                                          and o.fk_orgtp = ot.id
                                        group by d.id, l.name || ', ' || nvl(ltrim(k.nd, '0'), '0') || '-' ||
                                                       nvl(ltrim(k.kw, '0'), '0'), s.name_reu, u.nm, t.name, c.mgchange,
                                                 c.dtek, u.name
                                        order by d.id;
end if;
elsif сd_ = '54' then --Задолжники OLAP
        --:cur_pay_=1 -- с учетом текущей оплаты, 0 - без учета

            cur_pay_ := utils.gets_bool_param('REP_CUR_PAY');
kpr1_ := utils.gets_int_param('REP_RNG_KPR1');
kpr2_ := utils.gets_int_param('REP_RNG_KPR2');
n1_ := utils.gets_list_param('REP_DEB_VAR');
l_sel_id := utils.gets_list_param('REP_TP_SCH_SEL');

if n1_ = 0 then
                n2_ := utils.gets_int_param('REP_DEB_MONTH');
else
                n2_ := utils.gets_int_param('REP_DEB_SUMMA');
end if;

if var_ = 3 then
                --По дому ред.09.04.2019 Долги в совокупности! (debits_lsk_month.var=0)
                open prep_refcursor for select s.lsk,
                                               decode(k.psch, 9, 'Закрытые Л/С', 8, 'Старый фонд', 'Открытые Л/С') as psch,
                                               t.name_tr,
                                               t.name_reu,
                                               trim(s.name) as street,
                                               ltrim(s.nd, '0') as nd,
                                               ltrim(s.kw, '0') as kw,
                                               trim(s.name) || ', ' || ltrim(s.nd, '0') || '-' || ltrim(s.kw, '0') as adr,
                                               s.fio,
                                               case when cur_pay_ = 1 then s.cnt_month else s.cnt_month2 end as cnt_month,
                                               case when cur_pay_ = 1 then s.dolg else s.dolg2 end as dolg,
                                               g.name as deb_org,
                                               s.penya,
                                               s.nachisl,
                                               s.payment,
                                               s.dat,
                                               a.name as st_name
                                        from kart k,
                                             debits_lsk_month s,
                                             s_reu_trest t,
                                             t_org g,
                                             status a
                                        where decode(l_sel_id, 0, 0, 1) = s.var
                                          and decode(l_sel_id, 0, k.fk_tp, l_sel_id) = k.fk_tp-- либо совокупно по помещению, либо по конкретному типу лс
                                          and k.lsk = s.lsk
                                          and s.status = a.id
                                          and s.reu = t.reu
                                          and s.reu = reu_
                                          and s.kul = kul_
                                          and s.nd = nd_
                                          and s.mg between mg_ and mg1_
                                          and s.fk_deb_org = g.id(+)
                                          and ((cur_pay_ = 1 and s.cnt_month > 0) or (cur_pay_ = 0 and s.cnt_month2 > 0))
                                          and exists
                                            (select su.fk_reu
                                             from scott.c_users_perm su
                                                      join scott.u_list ut
                                                           on ut.cd = 'доступ к отчётам'
                                                               and su.fk_perm_tp = ut.id
                                                      join scott.t_user us
                                                           on lower(us.cd) = lower(user)
                                                               and su.user_id = us.id
                                             where su.fk_reu = s.reu
                                            )
                                          and exists(select *
                                                     from kart k
                                                     where k.lsk = s.lsk
                                                       and (kpr1_ is not null and k.kpr >= kpr1_ or kpr1_ is null)
                                                       and (kpr2_ is not null and k.kpr <= kpr2_ or kpr2_ is null))
                                          and ((n1_ = 0 and s.cnt_month >= n2_) or (n1_ = 1 and s.dolg >= n2_))
                                        order by s.name, utils.f_order(s.nd, 6), utils.f_order(s.kw, 7); --    using cur_pay_, cur_pay_, reu_, kul_, nd_, cur_pay_, cur_pay_,
--    kpr1_, kpr1_, kpr1_, kpr2_, kpr2_, kpr2_, n1_, n2_, n1_, n2_;
elsif var_ = 2 then
                --По ЖЭО ред.09.04.2019
                open prep_refcursor for select s.lsk,
                                               decode(k.psch, 9, 'Закрытые Л/С', 8, 'Старый фонд', 'Открытые Л/С') as psch,
                                               t.name_tr,
                                               t.name_reu,
                                               trim(s.name) as street,
                                               ltrim(s.nd, '0') as nd,
                                               ltrim(s.kw, '0') as kw,
                                               trim(s.name) || ', ' || ltrim(s.nd, '0') || '-' || ltrim(s.kw, '0') as adr,
                                               s.fio,
                                               case when cur_pay_ = 1 then s.cnt_month else s.cnt_month2 end as cnt_month,
                                               case when cur_pay_ = 1 then s.dolg else s.dolg2 end as dolg,
                                               g.name as deb_org,
                                               s.penya,
                                               s.nachisl,
                                               s.payment,
                                               s.dat,
                                               a.name as st_name
                                        from kart k,
                                             debits_lsk_month s,
                                             s_reu_trest t,
                                             t_org g,
                                             status a
                                        where decode(l_sel_id, 0, 0, 1) = s.var
                                          and decode(l_sel_id, 0, k.fk_tp, l_sel_id) = k.fk_tp-- либо совокупно по помещению, либо по конкретному типу лс
                                          and k.lsk = s.lsk
                                          and s.status = a.id
                                          and s.reu = t.reu
                                          and s.reu = reu_
                                          and s.mg between mg_ and mg1_
                                          and s.fk_deb_org = g.id(+)
                                          and ((cur_pay_ = 1 and s.cnt_month > 0) or (cur_pay_ = 0 and s.cnt_month2 > 0))
                                          and exists
                                            (select su.fk_reu
                                             from scott.c_users_perm su
                                                      join scott.u_list ut
                                                           on ut.cd = 'доступ к отчётам'
                                                               and su.fk_perm_tp = ut.id
                                                      join scott.t_user us
                                                           on lower(us.cd) = lower(user)
                                                               and su.user_id = us.id
                                             where su.fk_reu = s.reu
                                            )
                                          and exists(select *
                                                     from kart k
                                                     where k.lsk = s.lsk
                                                       and (kpr1_ is not null and k.kpr >= kpr1_ or kpr1_ is null)
                                                       and (kpr2_ is not null and k.kpr <= kpr2_ or kpr2_ is null))
                                          and ((n1_ = 0 and s.cnt_month >= n2_) or (n1_ = 1 and s.dolg >= n2_))
                                        order by s.name, utils.f_order(s.nd, 6), utils.f_order(s.kw, 7); --    using cur_pay_, cur_pay_, reu_, cur_pay_, cur_pay_,
--    kpr1_, kpr1_, kpr1_, kpr2_, kpr2_, kpr2_, n1_, n2_, n1_, n2_;
elsif var_ = 1 then
                --По фонду ред.09.04.2019
                open prep_refcursor for select s.lsk,
                                               decode(k.psch, 9, 'Закрытые Л/С', 8, 'Старый фонд', 'Открытые Л/С') as psch,
                                               t.name_tr,
                                               t.name_reu,
                                               trim(s.name) as street,
                                               ltrim(s.nd, '0') as nd,
                                               ltrim(s.kw, '0') as kw,
                                               trim(s.name) || ', ' || ltrim(s.nd, '0') || '-' || ltrim(s.kw, '0') as adr,
                                               s.fio,
                                               case when cur_pay_ = 1 then s.cnt_month else s.cnt_month2 end as cnt_month,
                                               case when cur_pay_ = 1 then s.dolg else s.dolg2 end as dolg,
                                               g.name as deb_org,
                                               s.penya,
                                               s.nachisl,
                                               s.payment,
                                               s.dat,
                                               a.name as st_name
                                        from kart k,
                                             debits_lsk_month s,
                                             s_reu_trest t,
                                             t_org g,
                                             status a
                                        where decode(l_sel_id, 0, 0, 1) = s.var
                                          and decode(l_sel_id, 0, k.fk_tp, l_sel_id) = k.fk_tp-- либо совокупно по помещению, либо по конкретному типу лс
                                          and k.lsk = s.lsk
                                          and s.status = a.id
                                          and s.reu = t.reu
                                          and t.trest = trest_
                                          and s.mg between mg_ and mg1_
                                          and s.fk_deb_org = g.id(+)
                                          and ((cur_pay_ = 1 and s.cnt_month > 0) or (cur_pay_ = 0 and s.cnt_month2 > 0))
                                          and exists
                                            (select su.fk_reu
                                             from scott.c_users_perm su
                                                      join scott.u_list ut
                                                           on ut.cd = 'доступ к отчётам'
                                                               and su.fk_perm_tp = ut.id
                                                      join scott.t_user us
                                                           on lower(us.cd) = lower(user)
                                                               and su.user_id = us.id
                                             where su.fk_reu = s.reu
                                            )
                                          and exists(select *
                                                     from kart k
                                                     where k.lsk = s.lsk
                                                       and (kpr1_ is not null and k.kpr >= kpr1_ or kpr1_ is null)
                                                       and (kpr2_ is not null and k.kpr <= kpr2_ or kpr2_ is null))
                                          and ((n1_ = 0 and s.cnt_month >= n2_) or (n1_ = 1 and s.dolg >= n2_))
                                        order by s.name, utils.f_order(s.nd, 6), utils.f_order(s.kw, 7); --    using cur_pay_, cur_pay_, trest_, cur_pay_, cur_pay_,
--    kpr1_, kpr1_, kpr1_, kpr2_, kpr2_, kpr2_, n1_, n2_, n1_, n2_;
elsif var_ = 0 then
                --По городу ред.09.04.2019
                open prep_refcursor for select s.lsk,
                                               decode(k.psch, 9, 'Закрытые Л/С', 8, 'Старый фонд', 'Открытые Л/С') as psch,
                                               t.name_tr,
                                               t.name_reu,
                                               trim(s.name) as street,
                                               ltrim(s.nd, '0') as nd,
                                               ltrim(s.kw, '0') as kw,
                                               trim(s.name) || ', ' || ltrim(s.nd, '0') || '-' || ltrim(s.kw, '0') as adr,
                                               s.fio,
                                               case when cur_pay_ = 1 then s.cnt_month else s.cnt_month2 end as cnt_month,
                                               case when cur_pay_ = 1 then s.dolg else s.dolg2 end as dolg,
                                               g.name as deb_org,
                                               s.penya,
                                               s.nachisl,
                                               s.payment,
                                               s.dat,
                                               a.name as st_name
                                        from kart k,
                                             debits_lsk_month s,
                                             s_reu_trest t,
                                             t_org g,
                                             status a
                                        where decode(l_sel_id, 0, 0, 1) = s.var
                                          and decode(l_sel_id, 0, k.fk_tp, l_sel_id) = k.fk_tp-- либо совокупно по помещению, либо по конкретному типу лс
                                          and k.lsk = s.lsk
                                          and s.status = a.id
                                          and s.reu = t.reu
                                          and s.mg between mg_ and mg1_
                                          and s.fk_deb_org = g.id(+)
                                          and ((cur_pay_ = 1 and s.cnt_month > 0) or (cur_pay_ = 0 and s.cnt_month2 > 0))
                                          and exists
                                            (select su.fk_reu
                                             from scott.c_users_perm su
                                                      join scott.u_list ut
                                                           on ut.cd = 'доступ к отчётам'
                                                               and su.fk_perm_tp = ut.id
                                                      join scott.t_user us
                                                           on lower(us.cd) = lower(user)
                                                               and su.user_id = us.id
                                             where su.fk_reu = s.reu
                                            )
                                          and exists(select *
                                                     from kart k
                                                     where k.lsk = s.lsk
                                                       and (kpr1_ is not null and k.kpr >= kpr1_ or kpr1_ is null)
                                                       and (kpr2_ is not null and k.kpr <= kpr2_ or kpr2_ is null))
                                          and ((n1_ = 0 and s.cnt_month >= n2_) or (n1_ = 1 and s.dolg >= n2_))
                                        order by s.name, utils.f_order(s.nd, 6), utils.f_order(s.kw, 7); --    using cur_pay_, cur_pay_, cur_pay_, cur_pay_,
--    kpr1_, kpr1_, kpr1_, kpr2_, kpr2_, kpr2_, n1_, n2_, n1_, n2_;
end if;
elsif сd_ = '56' then
            raise_application_error(-20000, 'Раздел закрыт 03.06.2020');
            --Списки льготников
if var_ = 3 then
                --По дому
                open prep_refcursor for 'select substr(s.mg, 1, 4)||''-''||substr(s.mg, 5, 2) as mg, s.lsk,
    initcap(trim(e.name) || '', '' || ltrim(s.nd, ''0'') || ''-'' || ltrim(s.kw, ''0'')) as adr,
       s.opl, s.kpr, c.kpr_cem, c1.kpr_s,
       decode(lag(s.lsk, 1) over (order by s.lsk), s.lsk, 0, c2.cnt) as cnt,
       decode(lag(s.lsk, 1) over (order by s.lsk), s.lsk, 0, c2.cnt_main) as cnt_main,
       p.fio, m.name as lg_name, u.nm2 as usl_name, b.summa, nvl(d.doc, '' '') as doc
  from arch_kart s, spul e, a_kart_pr p, spk m, a_lg_docs d,
       usl u, (select sum(s.summa) as summa, s.lg_doc_id, s.spk_id, s.usl, s.kart_pr_id
                 from a_charge s
                where ' || sqlstr_ || ' and s.spk_id=:spk_id_ and s.type = 4 and s.summa <> 0
                group by s.lg_doc_id, s.spk_id, s.usl, s.kart_pr_id) b,
              (select lsk, sum(kpr_cem) as kpr_cem from
                (select distinct s.lsk, s.kart_pr_id, 1 as kpr_cem
                 from a_charge s, usl m
                where ' || sqlstr_ || ' and s.usl=m.usl and s.spk_id=:spk_id_ and m.usl_type=1
                 and s.type = 4 and s.summa <> 0)
                 group by lsk
                ) c,
              (select lsk, sum(kpr_s) as kpr_s from
                (select distinct s.lsk, s.kart_pr_id, 1 as kpr_s
                 from a_charge s, usl m
                where ' || sqlstr_ || ' and s.usl=m.usl and s.spk_id=:spk_id_ and m.usl_type=0
                 and s.type = 4 and s.summa <> 0)
                 group by lsk
                ) c1,
              (select lsk, count(*) as cnt, sum(cnt_main) as cnt_main from (
                select distinct lsk, kart_pr_id, main as cnt_main from a_charge s
                where ' || sqlstr_ || ' and s.spk_id=:spk_id_
                and s.type=4 and s.summa <> 0  --кол-во носителей льг.
                )
                group by lsk
                ) c2
 where s.lsk = p.lsk and s.reu=:reu_ and s.kul=:kul_ and s.nd=:nd_
   and s.kul = e.id
   and ' || sqlstr_ || '
   and s.lsk = c.lsk(+)
   and s.lsk = c1.lsk(+)
   and s.lsk = c2.lsk(+)
   and p.mg=s.mg
   and d.mg=s.mg
   and b.spk_id=m.id
   and p.id = b.kart_pr_id
   and b.usl = u.usl
   and d.id=b.lg_doc_id
   order by s.lsk, p.id' using spk_id_, spk_id_, spk_id_, spk_id_, reu_, kul_, nd_;
elsif var_ = 2 then
                --По ЖЭО
                open prep_refcursor for 'select substr(s.mg, 1, 4)||''-''||substr(s.mg, 5, 2) as mg, s.lsk,
    initcap(trim(e.name) || '', '' || ltrim(s.nd, ''0'') || ''-'' || ltrim(s.kw, ''0'')) as adr,
       s.opl, s.kpr, c.kpr_cem, c1.kpr_s,
       decode(lag(s.lsk, 1) over (order by s.lsk), s.lsk, 0, c2.cnt) as cnt,
       decode(lag(s.lsk, 1) over (order by s.lsk), s.lsk, 0, c2.cnt_main) as cnt_main,
       p.fio, m.name as lg_name, u.nm2 as usl_name, b.summa, nvl(d.doc, '' '') as doc
  from arch_kart s, spul e, a_kart_pr p, spk m, a_lg_docs d,
       usl u, (select sum(s.summa) as summa, s.lg_doc_id, s.spk_id, s.usl, s.kart_pr_id
                 from a_charge s
                where ' || sqlstr_ || ' and s.spk_id=:spk_id_ and s.type = 4 and s.summa <> 0
                group by s.lg_doc_id, s.spk_id, s.usl, s.kart_pr_id) b,
              (select lsk, sum(kpr_cem) as kpr_cem from
                (select distinct s.lsk, s.kart_pr_id, 1 as kpr_cem
                 from a_charge s, usl m
                where ' || sqlstr_ || ' and s.usl=m.usl and s.spk_id=:spk_id_ and m.usl_type=1
                 and s.type = 4 and s.summa <> 0)
                 group by lsk
                ) c,
              (select lsk, sum(kpr_s) as kpr_s from
                (select distinct s.lsk, s.kart_pr_id, 1 as kpr_s
                 from a_charge s, usl m
                where ' || sqlstr_ || ' and s.usl=m.usl and s.spk_id=:spk_id_ and m.usl_type=0
                 and s.type = 4 and s.summa <> 0)
                 group by lsk
                ) c1,
              (select lsk, count(*) as cnt, sum(cnt_main) as cnt_main from (
                select distinct lsk, kart_pr_id, main as cnt_main from a_charge s
                where ' || sqlstr_ || ' and s.spk_id=:spk_id_
                and s.type=4 and s.summa <> 0  --кол-во носителей льг.
                )
                group by lsk
                ) c2
 where s.lsk = p.lsk and s.reu=:reu_
   and s.kul = e.id
   and ' || sqlstr_ || '
   and s.lsk = c.lsk(+)
   and s.lsk = c1.lsk(+)
   and s.lsk = c2.lsk(+)
   and p.mg=s.mg
   and d.mg=s.mg
   and b.spk_id=m.id
   and p.id = b.kart_pr_id
   and b.usl = u.usl
   and d.id=b.lg_doc_id
   order by s.lsk, p.id' using spk_id_, spk_id_, spk_id_, spk_id_, reu_;
elsif var_ = 1 then
                --По фонду
                open prep_refcursor for 'select substr(s.mg, 1, 4)||''-''||substr(s.mg, 5, 2) as mg, s.lsk,
    initcap(trim(e.name) || '', '' || ltrim(s.nd, ''0'') || ''-'' || ltrim(s.kw, ''0'')) as adr,
       s.opl, s.kpr, c.kpr_cem, c1.kpr_s,
       decode(lag(s.lsk, 1) over (order by s.lsk), s.lsk, 0, c2.cnt) as cnt,
       decode(lag(s.lsk, 1) over (order by s.lsk), s.lsk, 0, c2.cnt_main) as cnt_main,
       p.fio, m.name as lg_name, u.nm2 as usl_name, b.summa, nvl(d.doc, '' '') as doc
  from arch_kart s, spul e, a_kart_pr p, spk m, a_lg_docs d, s_reu_trest t,
       usl u, (select sum(s.summa) as summa, s.lg_doc_id, s.spk_id, s.usl, s.kart_pr_id
                 from a_charge s
                where ' || sqlstr_ || ' and s.spk_id=:spk_id_ and s.type = 4 and s.summa <> 0
                group by s.lg_doc_id, s.spk_id, s.usl, s.kart_pr_id) b,
              (select lsk, sum(kpr_cem) as kpr_cem from
                (select distinct s.lsk, s.kart_pr_id, 1 as kpr_cem
                 from a_charge s, usl m
                where ' || sqlstr_ || ' and s.usl=m.usl and s.spk_id=:spk_id_ and m.usl_type=1
                 and s.type = 4 and s.summa <> 0)
                 group by lsk
                ) c,
              (select lsk, sum(kpr_s) as kpr_s from
                (select distinct s.lsk, s.kart_pr_id, 1 as kpr_s
                 from a_charge s, usl m
                where ' || sqlstr_ || ' and s.usl=m.usl and s.spk_id=:spk_id_ and m.usl_type=0
                 and s.type = 4 and s.summa <> 0)
                 group by lsk
                ) c1,
              (select lsk, count(*) as cnt, sum(cnt_main) as cnt_main from (
                select distinct lsk, kart_pr_id, main as cnt_main from a_charge s
                where ' || sqlstr_ || ' and s.spk_id=:spk_id_
                and s.type=4 and s.summa <> 0  --кол-во носителей льг.
                )
                group by lsk
                ) c2
 where s.lsk = p.lsk and t.trest=:trest_
   and s.kul = e.id
   and ' || sqlstr_ || '
   and s.lsk = c.lsk(+)
   and s.lsk = c1.lsk(+)
   and s.lsk = c2.lsk(+)
   and p.mg=s.mg
   and d.mg=s.mg
   and b.spk_id=m.id
   and p.id = b.kart_pr_id
   and b.usl = u.usl
   and d.id=b.lg_doc_id
   order by s.lsk, p.id' using spk_id_, spk_id_, spk_id_, spk_id_, trest_;
elsif var_ = 0 then
                --По городу
                open prep_refcursor for 'select substr(s.mg, 1, 4)||''-''||substr(s.mg, 5, 2) as mg, s.lsk,
    initcap(trim(e.name) || '', '' || ltrim(s.nd, ''0'') || ''-'' || ltrim(s.kw, ''0'')) as adr,
       s.opl, s.kpr, c.kpr_cem, c1.kpr_s,
       decode(lag(s.lsk, 1) over (order by s.lsk), s.lsk, 0, c2.cnt) as cnt,
       decode(lag(s.lsk, 1) over (order by s.lsk), s.lsk, 0, c2.cnt_main) as cnt_main,
       p.fio, m.name as lg_name, u.nm2 as usl_name, b.summa, nvl(d.doc, '' '') as doc
  from arch_kart s, spul e, a_kart_pr p, spk m, a_lg_docs d,
       usl u, (select sum(s.summa) as summa, s.lg_doc_id, s.spk_id, s.usl, s.kart_pr_id
                 from a_charge s --суммы возмещ по льготам
                where ' || sqlstr_ || ' and s.spk_id=:spk_id_ and s.type = 4 and s.summa <> 0
                group by s.lg_doc_id, s.spk_id, s.usl, s.kart_pr_id) b,
              (select lsk, sum(kpr_cem) as kpr_cem from
                (select distinct s.lsk, s.kart_pr_id, 1 as kpr_cem
                 from a_charge s, usl m --кол-во польз. льг. по жилью
                where ' || sqlstr_ || ' and s.usl=m.usl and s.spk_id=:spk_id_ and m.usl_type=1
                 and s.type = 4 and s.summa <> 0)
                 group by lsk
                ) c,
              (select lsk, sum(kpr_s) as kpr_s from
                (select distinct s.lsk, s.kart_pr_id, 1 as kpr_s
                 from a_charge s, usl m --кол-во польз. льг. по комун.усл.
                where ' || sqlstr_ || ' and s.usl=m.usl and s.spk_id=:spk_id_ and m.usl_type=0
                 and s.type = 4 and s.summa <> 0)
                 group by lsk
                ) c1,
              (select lsk, count(*) as cnt, sum(cnt_main) as cnt_main from (
                select distinct lsk, kart_pr_id, main as cnt_main from a_charge s
                where ' || sqlstr_ || ' and s.spk_id=:spk_id_
                and s.type=4 and s.summa <> 0  --кол-во носителей льг.
                )
                group by lsk
                ) c2
 where s.lsk = p.lsk
   and s.kul = e.id
   and ' || sqlstr_ || '
   and s.lsk = c.lsk(+)
   and s.lsk = c1.lsk(+)
   and s.lsk = c2.lsk(+)
   and p.mg=s.mg
   and d.mg=s.mg
   and b.spk_id=m.id
   and p.id = b.kart_pr_id
   and b.usl = u.usl
   and d.id=b.lg_doc_id
   order by s.lsk, p.id' using spk_id_, spk_id_, spk_id_, spk_id_;
end if;

elsif сd_ = '57' then
            --Список по объёмным показателям
            if var_ = 3 then
                --По дому
                open prep_refcursor for '
select t.trest||'' ''||t.name_reu as predp,
    k.name||'', ''||NVL(LTRIM(s.nd,''0''),''0'') AS predpr_det,
    LTRIM(s.kw,''0'') AS kw,
    TRIM(u.nm)||DECODE(u.ed_izm,NULL,'''','' (''||TRIM(u.ed_izm)||'')'') AS nm,
    TRIM(u.nm1)||DECODE(u.ed_izm,NULL,'''','' (''||TRIM(u.ed_izm)||'')'') AS nm1,
    p.name AS orgname, m.name AS STATUS, DECODE(s.psch,1,''Закрытые Л/С'', 2,''Старый фонд'', ''Открытые Л/С'') AS psch,
    DECODE(s.sch,1,''Счетчик'',''Норматив'') AS sch, s.val_group2 as val_group,
    sum(s.cnt) AS cnt, sum(s.klsk) AS klsk, sum(s.kpr) AS kpr, sum(s.kpr_ot) AS kpr_ot,
    sum(s.kpr_wr) AS kpr_wr, sum(s.cnt_lg) AS cnt_lg, sum(s.cnt_subs) AS cnt_subs, s.uch,
    substr(s.mg, 1, 4)||''-''||substr(s.mg, 5, 2) as mg1,
    to_number(translate(upper(s.nd),
    translate(upper(s.nd),''0123456789'','' ''), '' '')) as nd1,
    to_number(translate(upper(s.kw),
    translate(upper(s.kw),''0123456789'','' ''), '' '')) as kw1
    FROM STATISTICS_LSK s, USL u, S_REU_TREST t, SPRORG p, STATUS m, SPUL k
    WHERE s.reu=t.reu and s.psch not in (8,9)
    AND s.USL=u.USL
    AND s.ORG=p.kod
    and u.uslm in
     (''004'',''006'',''007'',''008'')
    AND s.kul=k.id
    AND s.STATUS=m.id
    AND s.reu=:reu_ and s.kul=:kul_ and s.nd=:nd_
    AND ' || sqlstr_ || '
    group by u.npp, t.trest||'' ''||t.name_reu,
    k.name||'', ''||NVL(LTRIM(s.nd,''0''),''0''),
    LTRIM(s.kw,''0''),
    TRIM(u.nm)||DECODE(u.ed_izm,NULL,'''','' (''||TRIM(u.ed_izm)||'')''),
    TRIM(u.nm1)||DECODE(u.ed_izm,NULL,'''','' (''||TRIM(u.ed_izm)||'')''),
    p.name, m.name, DECODE(s.psch,1,''Закрытые Л/С'', 2,''Старый фонд'', ''Открытые Л/С''),
    DECODE(s.sch,1,''Счетчик'',''Норматив''), s.val_group2, s.uch,
    substr(s.mg, 1, 4)||''-''||substr(s.mg, 5, 2), k.name,
    to_number(translate(upper(s.nd),
    translate(upper(s.nd),''0123456789'','' ''), '' '')),
    to_number(translate(upper(s.kw),
    translate(upper(s.kw),''0123456789'','' ''), '' ''))
    order by u.npp, k.name, to_number(translate(upper(s.nd),
    translate(upper(s.nd),''0123456789'','' ''), '' '')),
    to_number(translate(upper(s.kw),
    translate(upper(s.kw),''0123456789'','' ''), '' ''))' using reu_, kul_, nd_;
elsif var_ = 2 then
                --По ЖЭО
                open prep_refcursor for '
select t.trest||'' ''||t.name_reu as predp,
    k.name||'', ''||NVL(LTRIM(s.nd,''0''),''0'') AS predpr_det,
    LTRIM(s.kw,''0'') AS kw,
    TRIM(u.nm)||DECODE(u.ed_izm,NULL,'''','' (''||TRIM(u.ed_izm)||'')'') AS nm,
    TRIM(u.nm1)||DECODE(u.ed_izm,NULL,'''','' (''||TRIM(u.ed_izm)||'')'') AS nm1,
    p.name AS orgname, m.name AS STATUS, DECODE(s.psch,1,''Закрытые Л/С'', 2,''Старый фонд'', ''Открытые Л/С'') AS psch,
    DECODE(s.sch,1,''Счетчик'',''Норматив'') AS sch, s.val_group2 as val_group,
    sum(s.cnt) AS cnt, sum(s.klsk) AS klsk, sum(s.kpr) AS kpr, sum(s.kpr_ot) AS kpr_ot,
    sum(s.kpr_wr) AS kpr_wr, sum(s.cnt_lg) AS cnt_lg, sum(s.cnt_subs) AS cnt_subs, s.uch,
    substr(s.mg, 1, 4)||''-''||substr(s.mg, 5, 2) as mg1,
    to_number(translate(upper(s.nd),
    translate(upper(s.nd),''0123456789'','' ''), '' '')) as nd1,
    to_number(translate(upper(s.kw),
    translate(upper(s.kw),''0123456789'','' ''), '' '')) as kw1
    FROM STATISTICS_LSK s, USL u, S_REU_TREST t, SPRORG p, STATUS m, SPUL k
    WHERE s.reu=t.reu and s.psch not in (8,9)
    AND s.USL=u.USL
    AND s.ORG=p.kod
    and u.uslm in
     (''004'',''006'',''007'',''008'')
    AND s.kul=k.id
    AND s.STATUS=m.id
    AND s.reu=:reu_
    AND ' || sqlstr_ || '
    group by u.npp, t.trest||'' ''||t.name_reu,
    k.name||'', ''||NVL(LTRIM(s.nd,''0''),''0''),
    LTRIM(s.kw,''0''),
    TRIM(u.nm)||DECODE(u.ed_izm,NULL,'''','' (''||TRIM(u.ed_izm)||'')''),
    TRIM(u.nm1)||DECODE(u.ed_izm,NULL,'''','' (''||TRIM(u.ed_izm)||'')''),
    p.name, m.name, DECODE(s.psch,1,''Закрытые Л/С'', 2,''Старый фонд'', ''Открытые Л/С''),
    DECODE(s.sch,1,''Счетчик'',''Норматив''), s.val_group2, s.uch,
    substr(s.mg, 1, 4)||''-''||substr(s.mg, 5, 2), k.name,
    to_number(translate(upper(s.nd),
    translate(upper(s.nd),''0123456789'','' ''), '' '')),
    to_number(translate(upper(s.kw),
    translate(upper(s.kw),''0123456789'','' ''), '' ''))
    order by u.npp, k.name, to_number(translate(upper(s.nd),
    translate(upper(s.nd),''0123456789'','' ''), '' '')),
    to_number(translate(upper(s.kw),
    translate(upper(s.kw),''0123456789'','' ''), '' ''))' using reu_;
elsif var_ = 1 then
                --По фонду
                open prep_refcursor for '
 select t.trest||'' ''||t.name_reu as predp,
    k.name||'', ''||NVL(LTRIM(s.nd,''0''),''0'') AS predpr_det,
    LTRIM(s.kw,''0'') AS kw,
    TRIM(u.nm)||DECODE(u.ed_izm,NULL,'''','' (''||TRIM(u.ed_izm)||'')'') AS nm,
    TRIM(u.nm1)||DECODE(u.ed_izm,NULL,'''','' (''||TRIM(u.ed_izm)||'')'') AS nm1,
    p.name AS orgname, m.name AS STATUS, DECODE(s.psch,1,''Закрытые Л/С'', 2,''Старый фонд'', ''Открытые Л/С'') AS psch,
    DECODE(s.sch,1,''Счетчик'',''Норматив'') AS sch, s.val_group2 as val_group,
    sum(s.cnt) AS cnt, sum(s.klsk) AS klsk, sum(s.kpr) AS kpr, sum(s.kpr_ot) AS kpr_ot,
    sum(s.kpr_wr) AS kpr_wr, sum(s.cnt_lg) AS cnt_lg, sum(s.cnt_subs) AS cnt_subs, s.uch,
    substr(s.mg, 1, 4)||''-''||substr(s.mg, 5, 2) as mg1,
    to_number(translate(upper(s.nd),
    translate(upper(s.nd),''0123456789'','' ''), '' '')) as nd1,
    to_number(translate(upper(s.kw),
    translate(upper(s.kw),''0123456789'','' ''), '' '')) as kw1
    FROM STATISTICS_LSK s, USL u, S_REU_TREST t, SPRORG p, STATUS m, SPUL k
    WHERE s.reu=t.reu and s.psch not in (8,9)
    AND s.USL=u.USL
    AND s.ORG=p.kod
    and u.uslm in
     (''004'',''006'',''007'',''008'')
    AND s.kul=k.id
    AND s.STATUS=m.id
    AND t.trest=:trest_
    AND ' || sqlstr_ || '
    group by u.npp, t.trest||'' ''||t.name_reu,
    k.name||'', ''||NVL(LTRIM(s.nd,''0''),''0''),
    LTRIM(s.kw,''0''),
    TRIM(u.nm)||DECODE(u.ed_izm,NULL,'''','' (''||TRIM(u.ed_izm)||'')''),
    TRIM(u.nm1)||DECODE(u.ed_izm,NULL,'''','' (''||TRIM(u.ed_izm)||'')''),
    p.name, m.name, DECODE(s.psch,1,''Закрытые Л/С'', 2,''Старый фонд'', ''Открытые Л/С''),
    DECODE(s.sch,1,''Счетчик'',''Норматив''), s.val_group2, s.uch,
    substr(s.mg, 1, 4)||''-''||substr(s.mg, 5, 2), k.name,
    to_number(translate(upper(s.nd),
    translate(upper(s.nd),''0123456789'','' ''), '' '')),
    to_number(translate(upper(s.kw),
    translate(upper(s.kw),''0123456789'','' ''), '' ''))
    order by u.npp, k.name, to_number(translate(upper(s.nd),
    translate(upper(s.nd),''0123456789'','' ''), '' '')),
    to_number(translate(upper(s.kw),
    translate(upper(s.kw),''0123456789'','' ''), '' ''))' using trest_;
elsif var_ = 0 then
                --По городу
                open prep_refcursor for '
   select t.trest||'' ''||t.name_reu as predp,
    k.name||'', ''||NVL(LTRIM(s.nd,''0''),''0'') AS predpr_det,
    LTRIM(s.kw,''0'') AS kw,
    TRIM(u.nm)||DECODE(u.ed_izm,NULL,'''','' (''||TRIM(u.ed_izm)||'')'') AS nm,
    TRIM(u.nm1)||DECODE(u.ed_izm,NULL,'''','' (''||TRIM(u.ed_izm)||'')'') AS nm1,
    p.name AS orgname, m.name AS STATUS, DECODE(s.psch,1,''Закрытые Л/С'', 2,''Старый фонд'', ''Открытые Л/С'') AS psch,
    DECODE(s.sch,1,''Счетчик'',''Норматив'') AS sch, s.val_group2 as val_group,
    sum(s.cnt) AS cnt, sum(s.klsk) AS klsk, sum(s.kpr) AS kpr, sum(s.kpr_ot) AS kpr_ot,
    sum(s.kpr_wr) AS kpr_wr, sum(s.cnt_lg) AS cnt_lg, sum(s.cnt_subs) AS cnt_subs, s.uch,
    substr(s.mg, 1, 4)||''-''||substr(s.mg, 5, 2) as mg1,
    to_number(translate(upper(s.nd),
    translate(upper(s.nd),''0123456789'','' ''), '' '')) as nd1,
    to_number(translate(upper(s.kw),
    translate(upper(s.kw),''0123456789'','' ''), '' '')) as kw1
    FROM STATISTICS_LSK s, USL u, S_REU_TREST t, SPRORG p, STATUS m, SPUL k
    WHERE s.reu=t.reu and s.psch not in (8,9)
    AND s.USL=u.USL
    AND s.ORG=p.kod
    and u.uslm in
     (''004'',''006'',''007'',''008'')
    AND s.kul=k.id
    AND s.STATUS=m.id
    AND ' || sqlstr_ || '
    group by u.npp, t.trest||'' ''||t.name_reu,
    k.name||'', ''||NVL(LTRIM(s.nd,''0''),''0''),
    LTRIM(s.kw,''0''),
    TRIM(u.nm)||DECODE(u.ed_izm,NULL,'''','' (''||TRIM(u.ed_izm)||'')''),
    TRIM(u.nm1)||DECODE(u.ed_izm,NULL,'''','' (''||TRIM(u.ed_izm)||'')''),
    p.name, m.name, DECODE(s.psch,1,''Закрытые Л/С'', 2,''Старый фонд'', ''Открытые Л/С''),
    DECODE(s.sch,1,''Счетчик'',''Норматив''), s.val_group2, s.uch,
    substr(s.mg, 1, 4)||''-''||substr(s.mg, 5, 2), k.name,
    to_number(translate(upper(s.nd),
    translate(upper(s.nd),''0123456789'','' ''), '' '')),
    to_number(translate(upper(s.kw),
    translate(upper(s.kw),''0123456789'','' ''), '' ''))
    order by u.npp, k.name, to_number(translate(upper(s.nd),
    translate(upper(s.nd),''0123456789'','' ''), '' '')),
    to_number(translate(upper(s.kw),
    translate(upper(s.kw),''0123456789'','' ''), '' ''))';
end if;
elsif сd_ = '58' then
            --Список квартиросъемщиков, имеющих счетчики учета воды
            if var_ = 3 then
                --По дому
                open prep_refcursor for '
      select s.lsk, k.name||'', ''||NVL(LTRIM(s.nd,''0''),''0'')||''-''||NVL(LTRIM(s.kw,''0''),''0'') as adr,
             case when s.psch in (1,2) then s.phw
               else null end as phw,
             case when s.psch in (1,3) then s.pgw
               else null end as pgw,
             case when s.sch_el in (1) then s.pel
               else null end as pel,
             case when nvl(s.pot,0) <> 0 then s.pot
               else null end as pot
        from arch_kart s, s_reu_trest t, spul k, v_lsk_tp tp
       where s.reu = t.reu and s.fk_tp=tp.id and tp.cd=''LSK_TP_MAIN''
         and s.reu=:reu_ and s.kul=:kul_ and s.nd=:nd_
         and (s.psch not in (8, 9, 0) or s.psch not in (8, 9) and nvl(s.pot, 0)<>0)
         and s.kul = k.id
         and (nvl(s.phw,0) <> 0 or nvl(s.pgw,0) <> 0 or nvl(s.pel,0) <> 0 or nvl(s.pot,0) <> 0)
         and ' || sqlstr_ || '
       order by k.name, utils.f_order(s.nd,6), utils.f_order2(s.nd), utils.f_order(s.kw,7)' using reu_, kul_, nd_;
elsif var_ = 2 then
                --По ЖЭО
                open prep_refcursor for '
      select s.lsk, k.name||'', ''||NVL(LTRIM(s.nd,''0''),''0'')||''-''||NVL(LTRIM(s.kw,''0''),''0'') as adr,
             case when s.psch in (1,2) then s.phw
               else null end as phw,
             case when s.psch in (1,3) then s.pgw
               else null end as pgw,
             case when s.sch_el in (1) then s.pel
               else null end as pel,
             case when nvl(s.pot,0) <> 0 then s.pot
               else null end as pot
        from arch_kart s, s_reu_trest t, spul k, v_lsk_tp tp
       where s.reu = t.reu and s.fk_tp=tp.id and tp.cd=''LSK_TP_MAIN''
         and s.reu =:reu_
         and (s.psch not in (8, 9, 0) or s.psch not in (8, 9) and nvl(s.pot, 0)<>0)
         and s.kul = k.id
         and (nvl(s.phw,0) <> 0 or nvl(s.pgw,0) <> 0 or nvl(s.pel,0) <> 0 or nvl(s.pot,0) <> 0)
         and ' || sqlstr_ || '
       order by k.name, utils.f_order(s.nd,6), utils.f_order2(s.nd), utils.f_order(s.kw,7)' using reu_;
elsif var_ = 1 then
                --По фонду
                open prep_refcursor for '
      select s.lsk, k.name||'', ''||NVL(LTRIM(s.nd,''0''),''0'')||''-''||NVL(LTRIM(s.kw,''0''),''0'') as adr,
             case when s.psch in (1,2) then s.phw
               else null end as phw,
             case when s.psch in (1,3) then s.pgw
               else null end as pgw,
             case when s.sch_el in (1) then s.pel
               else null end as pel,
             case when nvl(s.pot,0) <> 0 then s.pot
               else null end as pot
        from arch_kart s, s_reu_trest t, spul k, v_lsk_tp tp
       where s.reu = t.reu and s.fk_tp=tp.id and tp.cd=''LSK_TP_MAIN''
         and t.trest =:trest_
         and (s.psch not in (8, 9, 0) or s.psch not in (8, 9) and nvl(s.pot, 0)<>0)
         and s.kul = k.id
         and (nvl(s.phw,0) <> 0 or nvl(s.pgw,0) <> 0 or nvl(s.pel,0) <> 0 or nvl(s.pot,0) <> 0)
         and ' || sqlstr_ || '
       order by k.name, utils.f_order(s.nd,6), utils.f_order2(s.nd), utils.f_order(s.kw,7)' using trest_;
elsif var_ = 0 then
                --По городу
                open prep_refcursor for '
      select s.lsk, k.name||'', ''||NVL(LTRIM(s.nd,''0''),''0'')||''-''||NVL(LTRIM(s.kw,''0''),''0'') as adr,
             case when s.psch in (1,2) then s.phw
               else null end as phw,
             case when s.psch in (1,3) then s.pgw
               else null end as pgw,
             case when s.sch_el in (1) then s.pel
               else null end as pel,
             case when nvl(s.pot,0) <> 0 then s.pot
               else null end as pot
        from arch_kart s, s_reu_trest t, spul k, v_lsk_tp tp
       where s.reu = t.reu and s.fk_tp=tp.id and tp.cd=''LSK_TP_MAIN''
         and (s.psch not in (8, 9, 0) or s.psch not in (8, 9) and nvl(s.pot, 0)<>0)
         and s.kul = k.id
         and (nvl(s.phw,0) <> 0 or nvl(s.pgw,0) <> 0 or nvl(s.pel,0) <> 0 or nvl(s.pot,0) <> 0)
         and ' || sqlstr_ || '
       order by k.name, utils.f_order(s.nd,6), utils.f_order2(s.nd), utils.f_order(s.kw,7)';
end if;
elsif сd_ = '59' then
            --Оплата для Э+
            open prep_refcursor for 'select u.nm as name_usl, substr(s.mg, 1, 4)||''-''||substr(s.mg, 5, 2) as mg1, t.name_tr,
   r.name as name_org,
   decode(t.ink, 0, ''самост'', 1, ''не самост'') as name_status, sum(s.summa) as summa
  from rmt_xxito15 s, rmt_s_reu_trest t, rmt_usl u, rmt_sprorg r
  where s.usl in (''020'',''021'') and s.forreu=t.reu and ' || sqlstr_ || '
   and s.org=r.kod and s.priznak=1 and s.usl = u.usl
  group by u.nm, substr(s.mg, 1, 4)||''-''||substr(s.mg, 5, 2), r.name,
   t.name_tr, decode(t.ink, 0, ''самост'', 1, ''не самост'')
  order by substr(s.mg, 1, 4)||''-''||substr(s.mg, 5, 2),
   decode(t.ink, 0, ''самост'', 1, ''не самост''), r.name';

elsif сd_ = '60' then
            --Статистика по Программам - Пакетам пользователя
            if det_ = 3 then
                --По дому
                open prep_refcursor for 'select t.trest||'' ''||t.name_tr as predp,
     k.name||'', ''||nvl(ltrim(r.nd,''0''),''0'')||''-''||ltrim(r.kw,''0'') as predpr_det,
      i.name as tarif_name, u.nm,
     substr(s.mg, 1, 4)||''-''||substr(s.mg, 5, 2) as mg
      from kart r, a_nabor_progs s, spr_tarif i, s_reu_trest t, spul k, usl u
         where r.kul=k.id and r.reu=t.reu and r.lsk=s.lsk and s.usl=u.usl
         and s.fk_tarif=i.id
         and r.reu=:reu_ and r.kul=:kul_ and r.nd=:nd_ and ' || sqlstr_ using reu_, kul_, nd_;
elsif det_ = 2 then
                --По ЖЭО
                open prep_refcursor for 'select t.reu||'' ''||t.name_reu as predp,
     k.name||'', ''||nvl(ltrim(r.nd,''0''),''0'') as predpr_det,
     i.name as tarif_name, u.nm,
     substr(s.mg, 1, 4)||''-''||substr(s.mg, 5, 2) as mg
      from kart r, a_nabor_progs s, spr_tarif i, s_reu_trest t, spul k, usl u
         where r.kul=k.id and r.reu=t.reu and r.lsk=s.lsk and s.usl=u.usl
         and s.fk_tarif=i.id
         and r.reu=:reu_ and ' || sqlstr_ using reu_;
elsif det_ = 1 then
                --По Фонду
                open prep_refcursor for 'select t.trest||'' ''||t.name_tr as predp,
     k.name||'', ''||nvl(ltrim(r.nd,''0''),''0'') as predpr_det,
     i.name as tarif_name, u.nm,
     substr(s.mg, 1, 4)||''-''||substr(s.mg, 5, 2) as mg
      from kart r, a_nabor_progs s, spr_tarif i, s_reu_trest t, spul k, usl u
         where r.kul=k.id and r.reu=t.reu and r.lsk=s.lsk and s.usl=u.usl
         and s.fk_tarif=i.id
         and t.trest=:trest_ and ' || sqlstr_ using trest_;
elsif det_ = 0 then
                --По Городу
                open prep_refcursor for 'select t.trest||'' ''||t.name_tr as predp,
     k.name||'', ''||nvl(ltrim(r.nd,''0''),''0'') as predpr_det,
     i.name as tarif_name, u.nm,
     substr(s.mg, 1, 4)||''-''||substr(s.mg, 5, 2) as mg
      from kart r, a_nabor_progs s, spr_tarif i, s_reu_trest t, spul k, usl u
         where r.kul=k.id and r.reu=t.reu and r.lsk=s.lsk and s.usl=u.usl
         and s.fk_tarif=i.id
         and ' || sqlstr_;
end if;
elsif сd_ = '61' then
            --Оплата по Ф 2.4. ред. 10.03.20
            if dat_ is not null and dat1_ is not null then
                sqlstr_ := 'select * from xxito14 a where a.dat between to_date(''' || l_str_dat ||
                           ''',''DD.MM.YYYY'') and to_date(''' || l_str_dat1 || ''',''DD.MM.YYYY'') and a.mg=''' ||
                           l_char_dat_mg || '''';
elsif dat_ is not null and dat1_ is null then
                sqlstr_ := 'select * from xxito14 a where a.dat = to_date(''' || l_str_dat ||
                           ''',''DD.MM.YYYY'') and a.mg=''' || l_char_dat_mg || '''';
elsif dat_ is null and dat1_ is null then
                sqlstr_ := 'select * from xxito14 a where a.mg between ''' || mg_ || ''' and ''' || mg1_ || '''';
end if;
sqlstr_ := sqlstr_ || ' and exists
           (select * from list_c i, spr_params p where i.fk_ses=' || fk_ses_ || '
                and p.id=i.fk_par and p.cd=''REP_USL2''
                and i.sel_cd=a.usl
            and i.sel=1)
            and exists
           (select * from list_c i, spr_params p where i.fk_ses=' || fk_ses_ || '
                and p.id=i.fk_par and p.cd=''REP_ORG2''
                and i.sel_id=a.org
            and i.sel=1) ';
if var_ = 2 then
                --по РЭУ
                open prep_refcursor for 'select ''' || period_ || ''' as period, s.trest, substr(t.name_tr, 1, 15) as name_tr, s.oper,
               to_char(o.kod) || '' '' || substr(o.name, 1, 20) as name,
               substr(u.nm1, 1, 20) as nm1, sum(summa) as summa,
               decode(s.cd_tp, 0, ''Пеня'', ''Оплата'') as cd_tp
         from
         (' || sqlstr_ || ') s, s_reu_trest t, sprorg o, usl u
           where s.forreu = t.reu
             and s.org = o.kod
             and s.forreu = ' || reu_ || '
             and s.usl = u.usl
           group by s.trest, substr(t.name_tr, 1, 15), s.oper,
          to_char(o.kod) || '' '' || substr(o.name, 1, 20),
          substr(u.nm1, 1, 20),
          decode(s.cd_tp, 0, ''Пеня'', ''Оплата'')';
elsif var_ = 1 then
                --по ЖЭО

                open prep_refcursor for 'select ''' || period_ || ''' as period, s.trest, substr(t.name_tr, 1, 15) as name_tr, s.oper,
               to_char(o.kod) || '' '' || substr(o.name, 1, 20) as name,
               substr(u.nm1, 1, 20) as nm1, sum(summa) as summa,
               decode(s.cd_tp, 0, ''Пеня'', ''Оплата'') as cd_tp
         from (' || sqlstr_ || ') s, s_reu_trest t, sprorg o, usl u
           where s.forreu = t.reu
             and s.org = o.kod
             and s.trest = ' || trest_ || '
             and s.usl = u.usl
           group by s.trest, substr(t.name_tr, 1, 15), s.oper,
          to_char(o.kod) || '' '' || substr(o.name, 1, 20),
          substr(u.nm1, 1, 20),
          decode(s.cd_tp, 0, ''Пеня'', ''Оплата'')';
elsif var_ = 0 then
                --по Городу
                open prep_refcursor for 'select ''' || period_ || ''' as period, s.trest, substr(t.name_tr, 1, 15) as name_tr, s.oper,
               to_char(o.kod) || '' '' || substr(o.name, 1, 20) as name,
               substr(u.nm1, 1, 20) as nm1, sum(summa) as summa,
               decode(s.cd_tp, 0, ''Пеня'', ''Оплата'') as cd_tp
         from (' || sqlstr_ || ') s, s_reu_trest t, sprorg o, usl u
           where s.forreu = t.reu
             and s.org = o.kod
             and s.usl = u.usl
           group by s.trest, substr(t.name_tr, 1, 15), s.oper,
          to_char(o.kod) || '' '' || substr(o.name, 1, 20),
          substr(u.nm1, 1, 20),
          decode(s.cd_tp, 0, ''Пеня'', ''Оплата'')';
end if;

elsif сd_ in ('62', '63') then
            --список-оборотка для субсидирования ТСЖ
            if сd_ = '62' then uslg_ := '001'; elsif сd_ = '63' then uslg_ := '002'; end if;
--список для ТСЖ
open prep_refcursor for 'select l.name||'', ''||NVL(LTRIM(s.nd,''0''),''0'') as adr,
    ltrim(s.kw,''0'') as kw,
    s.komn, s.opl, b.opl_n, b.opl_sv, s.fio, s.kpr, b.summa_n, b.summa_sv,
     decode(s.psch, 1, gw.gw_n, 3, gw.gw_n, 0) as gw_sch_n,
     decode(s.psch, 1, gw.gw_sv, 3, gw.gw_sv, 0) as gw_sch_sv,
     decode(s.psch, 0, gw.gw_n, 2, gw.gw_n, 0) as gw_n,
     decode(s.psch, 0, gw.gw_sv, 2, gw.gw_sv, 0) as gw_sv,
     decode(s.psch, 0, gw.summa_n, 2, gw.summa_n, 0) as gw_n_summa_n,
     decode(s.psch, 0, gw.summa_sv, 2, gw.summa_sv, 0) as gw_n_summa_sv,
     decode(s.psch, 1, gw.summa_n, 3, gw.summa_n, 0) as gw_sch_summa_n,
     decode(s.psch, 1, gw.summa_sv, 3, gw.summa_sv, 0) as gw_sch_summa_sv,
     decode(s.psch, 0, gw2.cnt, 2, gw2.cnt, 0) as gw_n_corr,
     decode(s.psch, 1, gw2.cnt, 3, gw2.cnt, 0) as gw_sch_corr,
     decode(s.psch, 0, gw2.summa, 2, gw2.summa, 0) as gw_n_corr_summa,
     decode(s.psch, 1, gw2.summa, 3, gw2.summa, 0) as gw_sch_corr_summa,
     d.chng,
    c.name, c.adr as org_adr, c.inn, c.kpp, c.head_name,
    upper(utils.MONTH_NAME(substr(s.mg,5,2)))||'' ''||substr(s.mg,1,4)||''г.'' as mg_name,
    upper(s.nm) as nm
    from (select s.*, u.uslg, u.nm from arch_kart s, uslg u  where ' || sqlstr_ || '
     and exists
      (select * from a_charge a, usl u where a.usl=u.usl and
        u.uslg in (:uslg_) and a.type = 1 and a.summa <> 0
        and a.lsk=s.lsk and a.mg=s.mg
      )
    and s.reu=:reu_ and u.uslg=:uslg_
     and s.psch not in (8,9)
     and s.status not in (7)--убрал нежилые по просьбе ТСЖ Клён, ред.09.01.13
     ) s, t_org c, params p, spul l,
    (select s.lsk, u.uslg,
     sum(decode(u.usl_norm, 0, decode(s.type, 1, s.summa, 0))) as summa_n,
     sum(decode(u.usl_norm, 1, decode(s.type, 1, s.summa, 0))) as summa_sv,
     sum(decode(u.usl_norm, 0, decode(s.type, 1, s.test_opl, 0), 0)) as opl_n,
     sum(decode(u.usl_norm, 1, decode(s.type, 1, s.test_opl, 0), 0)) as opl_sv
      from (
      select s.lsk, s.usl, s.type, s.test_opl, s.summa from a_charge s, usl u where ' || sqlstr_ || ' and s.usl=u.usl and
      u.uslg=:uslg_ and s.type in (1, 2, 4)
      ) s, usl u
      where s.usl=u.usl
     group by s.lsk, u.uslg) b,
      (select s.lsk, u.uslg, sum(s.summa) as chng from a_change s, usl u where ' || sqlstr_ || ' and s.usl=u.usl and
       u.uslg=:uslg_
       group by s.lsk, u.uslg) d,
    (select s.lsk,
     sum(decode(u.usl_norm, 0, decode(s.type, 1, s.summa, 0))) as summa_n,
     sum(decode(u.usl_norm, 1, decode(s.type, 1, s.summa, 0))) as summa_sv,
     sum(decode(u.usl_norm, 0, decode(s.type, 1, s.test_opl, 0), 0)) as gw_n,
     sum(decode(u.usl_norm, 1, decode(s.type, 1, s.test_opl, 0), 0)) as gw_sv
      from (
      select s.lsk, s.usl, s.type, s.test_opl, s.summa from a_charge s, usl u where ' || sqlstr_ || ' and s.usl=u.usl and
      u.cd in (''г.вода'', ''г.вода/св.нор'') and s.type in (1, 2, 4)
      ) s, usl u
      where s.usl=u.usl
     group by s.lsk) gw,
    (select s.lsk,
      sum(s.cnt) as cnt,
      sum(s.summa) as summa
      from (
      select s.lsk, s.usl, s.test_opl as cnt, s.summa from a_charge s, usl u where ' || sqlstr_ || ' and s.usl=u.usl and
      u.cd in (''г.вода.ОДН'') and s.type in (1)
      ) s
     group by s.lsk) gw2
    where s.lsk = b.lsk(+) and s.kul=l.id and s.lsk=gw.lsk(+) and s.uslg=b.uslg(+)
     and s.lsk = d.lsk(+) and s.uslg=d.uslg(+)
     and s.reu=c.reu and s.lsk=gw2.lsk(+)
    order by l.name, s.nd, s.kw' using uslg_, reu_, uslg_, uslg_, uslg_;
elsif сd_ in ('64') then
            dat2_ := utils.gets_date_param('REP_DT_BR1');
dat3_ := utils.gets_date_param('REP_DT_BR2');
gndr_ := utils.gets_list_param('REP_GENDER');
            --Отчет по проживающим, для паспортного стола
            --список для ТСЖ
if var_ = 3 then
                --по РЭУ
                open prep_refcursor for select l.name || ', ' || nvl(ltrim(s.nd, '0'), '0') || '-' ||
                                               nvl(ltrim(s.kw, '0'), '0') as adr,
                                               p.fio,
                                               p.dat_rog,
                                               p.dok_c,
                                               p.dok_n,
                                               p.dok_v,
                                               p.dok_d,
                                               p.dok_div,
                                               p.dok_inn,
                                               p.dok_snils
                                        from arch_kart s,
                                             a_kart_pr2 p,
                                             spul l
                                        where s.mg between mg_ and mg1_
                                          and s.lsk = p.lsk
                                          and s.kul = l.id
                                          and s.mg between p.mgfrom and p.mgto
                                          and s.reu = reu_
                                          and s.kul = kul_
                                          and s.nd = nd_
                                          and p.dat_rog between dat2_ and dat3_
                                          and s.psch <> 8
                                          and ((gndr_ <> 2 and p.pol = gndr_) or gndr_ = 2)
                                          and p.status <> 4
                                        order by l.name, utils.f_order(s.nd, 6), utils.f_order(s.kw, 7);
elsif var_ = 2 then
                --по РЭУ
                open prep_refcursor for select l.name || ', ' || nvl(ltrim(s.nd, '0'), '0') || '-' ||
                                               nvl(ltrim(s.kw, '0'), '0') as adr,
                                               p.fio,
                                               p.dat_rog,
                                               p.dok_c,
                                               p.dok_n,
                                               p.dok_v,
                                               p.dok_d,
                                               p.dok_div,
                                               p.dok_inn,
                                               p.dok_snils
                                        from arch_kart s,
                                             a_kart_pr2 p,
                                             spul l
                                        where s.mg between mg_ and mg1_
                                          and s.lsk = p.lsk
                                          and s.kul = l.id
                                          and s.mg between p.mgfrom and p.mgto
                                          and s.reu = reu_
                                          and p.dat_rog between dat2_ and dat3_
                                          and s.psch <> 8
                                          and ((gndr_ <> 2 and p.pol = gndr_) or gndr_ = 2)
                                          and p.status <> 4
                                        order by l.name, utils.f_order(s.nd, 6), utils.f_order(s.kw, 7);
elsif var_ = 1 then
                --по ЖЭО
                open prep_refcursor for select l.name || ', ' || nvl(ltrim(s.nd, '0'), '0') || '-' ||
                                               nvl(ltrim(s.kw, '0'), '0') as adr,
                                               p.fio,
                                               p.dat_rog,
                                               p.dok_c,
                                               p.dok_n,
                                               p.dok_v,
                                               p.dok_d,
                                               p.dok_div,
                                               p.dok_inn,
                                               p.dok_snils
                                        from arch_kart s,
                                             a_kart_pr2 p,
                                             spul l,
                                             s_reu_trest t
                                        where s.mg between mg_ and mg1_
                                          and s.lsk = p.lsk
                                          and s.kul = l.id
                                          and s.mg between p.mgfrom and p.mgto
                                          and s.reu = t.reu
                                          and t.trest = trest_
                                          and p.dat_rog between dat2_ and dat3_
                                          and s.psch <> 8
                                          and ((gndr_ <> 2 and p.pol = gndr_) or gndr_ = 2)
                                          and p.status <> 4
                                        order by l.name, utils.f_order(s.nd, 6), utils.f_order(s.kw, 7);
elsif var_ = 0 then
                --по Городу
                open prep_refcursor for select l.name || ', ' || nvl(ltrim(s.nd, '0'), '0') || '-' ||
                                               nvl(ltrim(s.kw, '0'), '0') as adr,
                                               p.fio,
                                               p.dat_rog,
                                               p.dok_c,
                                               p.dok_n,
                                               p.dok_v,
                                               p.dok_d,
                                               p.dok_div,
                                               p.dok_inn,
                                               p.dok_snils
                                        from arch_kart s,
                                             a_kart_pr2 p,
                                             spul l
                                        where s.mg = mg_
                                          and s.lsk = p.lsk
                                          and s.kul = l.id
                                          and s.mg between p.mgfrom and p.mgto
                                          and p.dat_rog between dat2_ and dat3_
                                          and s.psch <> 8
                                          and ((gndr_ <> 2 and p.pol = gndr_) or gndr_ = 2)
                                          and p.status <> 4
                                        order by l.name, utils.f_order(s.nd, 6), utils.f_order(s.kw, 7);
end if;
elsif сd_ in ('65') then --Отчет для сверки распределения оплаты
        --список для ТСЖ
            if var_ in (1, 2, 3) then
                raise_application_error(-20001, 'Не существует уровня детализации!');
elsif var_ = 0 then
                if dat1_ is not null and dat2_ is not null then
                    mg2_ := mg_;
else
select period into mg2_ from params p;
end if;
--по Городу
open prep_refcursor for 'select s.fk_distr, decode(s.fk_distr,0,''деб.сальдо'',1,''кред.сальдо'',2,''тек.начисл.'',3,''на одну усл.'',4,
         ''ошибочн.оплата'') as type_distr, o.name as name_tr,
         u.nm as name_usl, o2.name as name_org,
         b.pay as pay_itg, s.pay, t.deb,
         decode(s.fk_distr, 0, decode(nvl(a.deb,0), 0, 0, round(t.deb/a.deb,2)*100), decode(nvl(d.chrg,0), 0, 0, round(r.chrg/d.chrg,2)*100)) as deb_proc, s.sum_distr,
         round(s.pay/b.pay,2)*100 as pay_proc, r.chrg, d.chrg as chrg1,
         decode(nvl(d.chrg,0), 0, 0, round(r.chrg/d.chrg,2)) as chrg_proc
         from
        (select s.forreu as reu, s.fk_distr, s.usl, s.org, sum(s.summa) as pay, sum(s.sum_distr) as sum_distr
         from xxito14 s where ' || sqlstr_ || ' and s.oper <> ''99'' --кроме корректировок
         group by s.forreu, s.fk_distr, s.usl, s.org) s,
         (select c.reu, c.usl, c.org, sum(c.indebet) as deb
         from xitog3 c where mg=''' || mg2_ || '''
         group by c.reu, c.usl, c.org) t,
         (select c.reu, c.usl, c.org, sum(c.charges) as chrg --текущее начисление
         from xitog3 c where mg=''' || mg2_ || '''
         group by c.reu, c.usl, c.org) r,
         (select c.reu, sum(c.charges) as chrg --текущее начисление итогом по РЭУ
         from xitog3 c where mg=''' || mg2_ || '''
         group by c.reu) d,
        (select reu, sum(c.indebet) as deb
         from xitog3 c where mg=''' || mg2_ || '''
        group by reu) a,
        (select s.forreu as reu, s.fk_distr, sum(s.summa) as pay
         from xxito14 s where ' || sqlstr_ || ' and s.oper <> ''99'' --кроме корректировок
        group by s.forreu, s.fk_distr) b,
         usl u, t_org o, t_org o2
        where s.reu=t.reu(+) and s.usl=t.usl(+) and s.org=t.org(+) and
         s.reu=r.reu(+) and s.usl=r.usl(+) and s.org=r.org(+) and s.reu=d.reu(+) and
         s.reu=a.reu(+) and s.reu=b.reu and s.fk_distr=b.fk_distr and s.usl=u.usl and s.org=o2.id
         and s.reu=o.reu
        order by s.fk_distr, o.name, u.nm, o2.name'; --0 -по дебетовому сальдо
--1 -по кредитовому сальдо
--2 -только для платежей где где отношение деб.сальдо/платеж < 1 (по текущему начислению + дебет сальдо вх)
--3 -как в Э+ (вся оплата на одну услугу)
--4 -неудачное распределение (не найдено ни в сальдо ни в начислении как распределять оплату)
--5 -корректировки оплаты
end if;
elsif сd_ in ('66') then --Реестры по задолжникам, по тарифам, для Дениса (Э+)
        --Выполнять после итогового формирования (чтоб вошла вся текущая оплата)
        --Вычисляем следующий месяц
            mg2_ := to_char(add_months(to_date(mg_ || '01', 'YYYYMMDD'), 1), 'YYYYMM');
open prep_refcursor for select k.lsk,
                               substr(trim(k.fio), 1, 25) as fio,
                               substr(l.name || ', ' || nvl(ltrim(k.nd, '0'), '0') || '-' ||
                                      nvl(ltrim(k.kw, '0'), '0'), 1, 32) as adr,
                               1 as type,
                               u.nm as type_name,
                               f.name as tarif_name,
                               mg2_ as period,
                               nvl(s.summa, 0) as summa
                        from kart k,
                             nabor n,
                             saldo_usl s,
                             spul l,
                             usl u,
                             spr_tarif f
                        where k.lsk = s.lsk
                          and k.lsk = n.lsk
                          and n.usl = u.usl
                          and n.fk_tarif = f.id
                          and k.kul = l.id
                          and k.lsk = s.lsk
                          and s.mg = mg2_
                          and s.usl = u.usl
                          and u.cd = 'каб.тел.'
                          and f.cdtp = 'ИНТ'
                        order by f.name;

elsif сd_ in ('67') then --Долги для Сбербанка-2 (для кабельного)
        --Выполнять после итогового формирования (чтоб вошла вся текущая оплата)
        --Вычисляем следующий месяц
            mg2_ := to_char(add_months(to_date(mg_ || '01', 'YYYYMMDD'), 1), 'YYYYMM');
--для Сбера
open prep_refcursor for select k.lsk,
                               '' as fio,
                               substr(o.name || ',' || l.name || ', ' || nvl(ltrim(k.nd, '0'), '0') ||
                                      '-' || nvl(ltrim(k.kw, '0'), '0'), 1, 52) as adr,
                               1 as type,
                               s.nm as type_name,
                               mg_ as period,
                               null as empty_field,
                               nvl(sum(s.summa), 0) * 100 as summa
                        from kart k,
                             t_org o,
                             t_org_tp tp,
                             (select t.*, u.nm
                              from saldo_usl t,
                                   usl u
                              where t.mg = mg2_
                                and t.usl = u.usl
                                and u.cd in ('каб.тел.', 'антен.д.нач.', 'антен.нач.')) s,
                             spul l --лицевые по которым есть сальдо
                        where k.psch not in (8, 9)
                          and k.lsk = s.lsk
                          and k.kul = l.id
                          and tp.cd = 'Город'
                          and o.fk_orgtp = tp.id
                        group by k.lsk, substr(trim(k.fio), 1, 25), s.nm,
                                 substr(o.name || ',' || l.name || ', ' || nvl(ltrim(k.nd, '0'), '0') ||
                                        '-' || nvl(ltrim(k.kw, '0'), '0'), 1, 52)
                        union all
                        select k.lsk,
                               '' as fio,
                               substr(o.name || ',' || l.name || ', ' || nvl(ltrim(k.nd, '0'), '0') ||
                                      '-' || nvl(ltrim(k.kw, '0'), '0'), 1, 52) as adr,
                               1 as type,
                               u.nm as type_name,
                               mg_ as period,
                               null as empty_field,
                               0 as summa
                        from kart k,
                             t_org o,
                             t_org_tp tp,
                             nabor n,
                             spul l,
                             usl u --лицевые по которым нет сальдо
                        where k.psch not in (8, 9)
                          and k.lsk = n.lsk
                          and k.kul = l.id
                          and nvl(decode(u.sptarn, 0, nvl(n.koeff, 0), 1, nvl(n.norm, 0), 2,
                                         nvl(n.koeff, 0) * nvl(n.norm, 0), 3, nvl(n.koeff, 0) *
                                                                              nvl(n.norm, 0)), 0) <> 0
                          and n.usl = u.usl
                          and u.cd in ('каб.тел.', 'антен.д.нач.', 'антен.нач.')
                          and tp.cd = 'Город'
                          and o.fk_orgtp = tp.id
                          and not exists(select t.*, u.nm
                                         from saldo_usl t,
                                              usl u
                                         where t.mg = mg2_
                                           and t.usl = u.usl
                                           and u.cd in ('каб.тел.', 'антен.д.нач.', 'антен.нач.')
                                           and t.lsk = k.lsk)
                        group by k.lsk, substr(trim(k.fio), 1, 25), u.nm,
                                 substr(o.name || ',' || l.name || ', ' || nvl(ltrim(k.nd, '0'), '0') ||
                                        '-' || nvl(ltrim(k.kw, '0'), '0'), 1, 52);
--       having sum(summa) > 0; ред 03.10.2011
elsif сd_ in ('68') then
            open prep_refcursor for select 'USL' as tp_cd, null as lsk, t.usl as s1, t.nm as s2, null as s3, null as n1
                                    from usl t
                                    union all
                                    select 'ORG' as tp_cd, null as lsk, t.cd as s1, t.name as s2, null as s3, null as n1
                                    from t_org t
                                    union all
                                    select 'STREET' as tp_cd,
                                           null as lsk,
                                           t.id as s1,
                                           t.name as s2,
                                           null as s3,
                                           null as n1
                                    from spul t
                                    union all
                                    select 'ADR' as tp_cd, t.lsk, t.kul as s1, t.nd as s2, t.kw as s3, null as n1
                                    from kart t
                                    union all
                                    select 'VOL' as tp_cd,
                                           n.lsk,
                                           n.usl as s1,
                                           o.cd as s2,
                                           null as s3,
                                           round(nvl(decode(u.sptarn, 0, nvl(n.koeff, 0), 1, nvl(n.norm, 0), 2,
                                                            nvl(n.koeff, 0) * nvl(n.norm, 0), 3,
                                                            nvl(n.koeff, 0) * nvl(n.norm, 0), 4,
                                                            nvl(n.koeff, 0) * nvl(n.norm, 0)), 0), 8) as n1
                                    from nabor n,
                                         t_org o,
                                         usl u
                                    where n.org = o.id
                                      and n.usl = u.usl
                                      and round(nvl(decode(u.sptarn, 0, nvl(n.koeff, 0), 1, nvl(n.norm, 0), 2,
                                                           nvl(n.koeff, 0) * nvl(n.norm, 0), 3, nvl(n.koeff, 0) *
                                                                                                nvl(n.norm, 0), 4,
                                                           nvl(n.koeff, 0) * nvl(n.norm, 0)), 0), 8) <> 0
                                      and u.usl in ('045', '046');
elsif сd_ in ('69') then --Задолжники FR, вне зависимости от организатора задолжника

        --(не смог сделать по другому, так как в одной квартире могут быть разные орг. а задолжность
        --по членам семьи - не делится)
            kpr1_ := utils.gets_int_param('REP_RNG_KPR1');
kpr2_ := utils.gets_int_param('REP_RNG_KPR2');

n1_ := utils.gets_list_param('REP_DEB_VAR');
if n1_ = 0 then
                n2_ := utils.gets_int_param('REP_DEB_MONTH');
else
                n2_ := utils.gets_int_param('REP_DEB_SUMMA');
end if;

if var_ = 3 then
                --по Дому
                open prep_refcursor for 'select s.lsk, t.name_reu, trim(s.name) as street_name,
      ltrim(s.nd,''0'') as nd, ltrim(s.kw,''0'') as kw, s.fio, s.cnt_month, s.dolg, s.pen_in, s.pen_cur, s.penya,
      case when s.dat is null and s.mg is not null then last_day(to_date(s.mg||''01'',''YYYYMMDD''))
           else s.dat
           end as dat
      from debits_lsk_month s, s_reu_trest t
      where s.reu=t.reu
      and ' || sqlstr_ || '
      and s.reu=:reu_ AND s.kul=:kul_ AND s.nd=:nd_
      and exists
      (select su.fk_reu
      from scott.c_users_perm su
            join scott.u_list ut
                 on ut.cd = ''доступ к отчётам''
                     and su.fk_perm_tp = ut.id
            join scott.t_user us
                 on lower(us.cd) = lower(user)
                     and su.user_id = us.id
      where su.fk_reu = s.reu
      )
      and exists
      (select * from kart k where k.lsk=s.lsk
      and (:kpr1_ is not null and k.kpr >=:kpr1_ or :kpr1_ is null)
      and (:kpr2_ is not null and k.kpr <=:kpr2_ or :kpr2_ is null))
      and
      ((:n1_=0 and s.cnt_month >= :n2_) or
      (:n1_=1 and s.dolg >= :n2_))
      order by t.name_reu, s.name, s.nd, s.kw' using reu_, kul_, nd_, kpr1_, kpr1_, kpr1_, kpr2_, kpr2_, kpr2_, n1_, n2_, n1_, n2_;

elsif var_ = 2 then
                --по РЭУ
                open prep_refcursor for 'select s.lsk, t.name_reu, trim(s.name) as street_name,
      ltrim(s.nd,''0'') as nd, ltrim(s.kw,''0'') as kw, s.fio, s.cnt_month, s.dolg, s.pen_in, s.pen_cur, s.penya,
      case when s.dat is null and s.mg is not null then last_day(to_date(s.mg||''01'',''YYYYMMDD''))
           else s.dat
           end as dat
      from debits_lsk_month s, s_reu_trest t
      where s.reu=t.reu
      and exists
      (select su.fk_reu
      from scott.c_users_perm su
            join scott.u_list ut
                 on ut.cd = ''доступ к отчётам''
                     and su.fk_perm_tp = ut.id
            join scott.t_user us
                 on lower(us.cd) = lower(user)
                     and su.user_id = us.id
      where su.fk_reu = s.reu
      )
      and ' || sqlstr_ || '
      and s.reu=:reu_
      and exists
      (select * from kart k where k.lsk=s.lsk
      and (:kpr1_ is not null and k.kpr >=:kpr1_ or :kpr1_ is null)
      and (:kpr2_ is not null and k.kpr <=:kpr2_ or :kpr2_ is null))
      and
      ((:n1_=0 and s.cnt_month >= :n2_) or
      (:n1_=1 and s.dolg >= :n2_))
      order by t.name_reu, s.name, s.nd, s.kw' using reu_, kpr1_, kpr1_, kpr1_, kpr2_, kpr2_, kpr2_, n1_, n2_, n1_, n2_;

elsif var_ = 1 then
                --по ЖЭО
                open prep_refcursor for 'select s.lsk, t.name_reu, trim(s.name) as street_name,
      ltrim(s.nd,''0'') as nd, ltrim(s.kw,''0'') as kw, s.fio, s.cnt_month, s.dolg, s.pen_in, s.pen_cur, s.penya,
      case when s.dat is null and s.mg is not null then last_day(to_date(s.mg||''01'',''YYYYMMDD''))
           else s.dat
           end as dat
      from debits_lsk_month s, s_reu_trest t
      where s.reu=t.reu
      and ' || sqlstr_ || '
      and exists
      (select su.fk_reu
      from scott.c_users_perm su
            join scott.u_list ut
                 on ut.cd = ''доступ к отчётам''
                     and su.fk_perm_tp = ut.id
            join scott.t_user us
                 on lower(us.cd) = lower(user)
                     and su.user_id = us.id
      where su.fk_reu = s.reu
      )
      and s.reu=:trest_
      and exists
      (select * from kart k where k.lsk=s.lsk
      and (:kpr1_ is not null and k.kpr >=:kpr1_ or :kpr1_ is null)
      and (:kpr2_ is not null and k.kpr <=:kpr2_ or :kpr2_ is null))
      and
      ((:n1_=0 and s.cnt_month >= :n2_) or
      (:n1_=1 and s.dolg >= :n2_))
      order by t.name_reu, s.name, s.nd, s.kw' using trest_, kpr1_, kpr1_, kpr1_, kpr2_, kpr2_, kpr2_, n1_, n2_, n1_, n2_;

elsif var_ = 0 then
                --по Городу
                open prep_refcursor for 'select s.lsk, t.name_reu, trim(s.name) as street_name,
      ltrim(s.nd,''0'') as nd, ltrim(s.kw,''0'') as kw, s.fio, s.cnt_month, s.dolg, s.pen_in, s.pen_cur, s.penya,
      case when s.dat is null and s.mg is not null then last_day(to_date(s.mg||''01'',''YYYYMMDD''))
           else s.dat
           end as dat
      from debits_lsk_month s, s_reu_trest t
      where s.reu=t.reu
      and ' || sqlstr_ || '
      and exists
      (select su.fk_reu
      from scott.c_users_perm su
            join scott.u_list ut
                 on ut.cd = ''доступ к отчётам''
                     and su.fk_perm_tp = ut.id
            join scott.t_user us
                 on lower(us.cd) = lower(user)
                     and su.user_id = us.id
      where su.fk_reu = s.reu
      )
      and exists
      (select * from kart k where k.lsk=s.lsk
      and (:kpr1_ is not null and k.kpr >=:kpr1_ or :kpr1_ is null)
      and (:kpr2_ is not null and k.kpr <=:kpr2_ or :kpr2_ is null))
      and
      ((:n1_=0 and s.cnt_month >= :n2_) or
      (:n1_=1 and s.dolg >= :n2_))
      order by t.name_reu, s.name, s.nd, s.kw' using kpr1_, kpr1_, kpr1_, kpr2_, kpr2_, kpr2_, n1_, n2_, n1_, n2_;

end if;

elsif сd_ in ('73', '74') then
            dat2_ := nvl(utils.gets_date_param('REP_DT_PROP1'), to_date('19000101', 'YYYYMMDD'));
dat3_ := nvl(utils.gets_date_param('REP_DT_PROP2'), to_date('29000101', 'YYYYMMDD'));
prop_ := nvl(utils.gets_list_param('REP_PROP_VAR'), 0);
l_rep_prop_tp := utils.get_int_param('REP_PROP_TP');
            --Отчет по прописанным/выписанным, для паспортного стола
if l_rep_prop_tp = 0 then
                --отчет для ТСЖ
                if var_ = 3 then
                    --по Дому
                    open prep_refcursor for select s.lsk,
                                                   p.dat_rog,
                                                   p.dok_death_c,
                                                   p.dok_death_n,
                                                   u.name as reason,
                                                   l.name || ', ' || nvl(ltrim(s.nd, '0'), '0') || '-' ||
                                                   nvl(ltrim(s.kw, '0'), '0') as adr,
                                                   p.fio,
                                                   r.name as rel,
                                                   p.dat_prop as dt1,
                                                   p.dat_ub as dt2
                                            from arch_kart s,
                                                 a_kart_pr2 p,
                                                 spul l,
                                                 relations r,
                                                 u_list u
                                            where s.mg = mg_
                                              and s.lsk = p.lsk
                                              and s.kul = l.id
                                              and s.mg between p.mgfrom and p.mgto
                                              and p.relat_id = r.id(+)
                                              and s.reu = reu_
                                              and s.kul = kul_
                                              and s.nd = nd_
                                              and decode(prop_, 0, p.dat_prop, p.dat_ub) between dat2_ and dat3_
                                              and s.psch <> 8
                                              and p.fk_ub = u.id(+)
                                            order by l.name, utils.f_order(s.nd, 6), utils.f_order(s.kw, 7);
elsif var_ = 2 then
                    --по РЭУ
                    open prep_refcursor for select s.lsk,
                                                   p.dat_rog,
                                                   p.dok_death_c,
                                                   p.dok_death_n,
                                                   u.name as reason,
                                                   l.name || ', ' || nvl(ltrim(s.nd, '0'), '0') || '-' ||
                                                   nvl(ltrim(s.kw, '0'), '0') as adr,
                                                   p.fio,
                                                   r.name as rel,
                                                   p.dat_prop as dt1,
                                                   p.dat_ub as dt2
                                            from arch_kart s,
                                                 a_kart_pr2 p,
                                                 spul l,
                                                 relations r,
                                                 u_list u
                                            where s.mg = mg_
                                              and s.lsk = p.lsk
                                              and s.kul = l.id
                                              and s.mg between p.mgfrom and p.mgto
                                              and p.relat_id = r.id(+)
                                              and s.reu = reu_
                                              and decode(prop_, 0, p.dat_prop, p.dat_ub) between dat2_ and dat3_
                                              and s.psch <> 8
                                              and p.fk_ub = u.id(+)
                                            order by l.name, utils.f_order(s.nd, 6), utils.f_order(s.kw, 7);
elsif var_ = 1 then
                    --по ЖЭО
                    open prep_refcursor for select s.lsk,
                                                   p.dat_rog,
                                                   p.dok_death_c,
                                                   p.dok_death_n,
                                                   u.name as reason,
                                                   l.name || ', ' || nvl(ltrim(s.nd, '0'), '0') || '-' ||
                                                   nvl(ltrim(s.kw, '0'), '0') as adr,
                                                   p.fio,
                                                   r.name as rel,
                                                   p.dat_prop as dt1,
                                                   p.dat_ub as dt2
                                            from arch_kart s,
                                                 a_kart_pr2 p,
                                                 spul l,
                                                 relations r,
                                                 s_reu_trest t,
                                                 u_list u
                                            where s.mg = mg_
                                              and s.lsk = p.lsk
                                              and s.kul = l.id
                                              and s.mg between p.mgfrom and p.mgto
                                              and p.relat_id = r.id(+)
                                              and s.reu = t.reu
                                              and t.trest = trest_
                                              and decode(prop_, 0, p.dat_prop, p.dat_ub) between dat2_ and dat3_
                                              and s.psch <> 8
                                              and p.fk_ub = u.id(+)
                                            order by l.name, utils.f_order(s.nd, 6), utils.f_order(s.kw, 7);
elsif var_ = 0 then
                    --по Городу
                    open prep_refcursor for select s.lsk,
                                                   p.dat_rog,
                                                   p.dok_death_c,
                                                   p.dok_death_n,
                                                   u.name as reason,
                                                   l.name || ', ' || nvl(ltrim(s.nd, '0'), '0') || '-' ||
                                                   nvl(ltrim(s.kw, '0'), '0') as adr,
                                                   p.fio,
                                                   r.name as rel,
                                                   p.dat_prop as dt1,
                                                   p.dat_ub as dt2
                                            from arch_kart s,
                                                 a_kart_pr2 p,
                                                 spul l,
                                                 relations r,
                                                 u_list u
                                            where s.mg = mg_
                                              and s.lsk = p.lsk
                                              and s.kul = l.id
                                              and s.mg between p.mgfrom and p.mgto
                                              and p.relat_id = r.id(+)
                                              and decode(prop_, 0, p.dat_prop, p.dat_ub) between dat2_ and dat3_
                                              and s.psch <> 8
                                              and p.fk_ub = u.id(+)
                                            order by l.name, utils.f_order(s.nd, 6), utils.f_order(s.kw, 7);
end if;
else
                -- отчет для Кис.
                if var_ = 3 then
                    --по Дому
                    open prep_refcursor for select s.lsk,
                                                   p.dat_rog,
                                                   p.dok_death_c,
                                                   p.dok_death_n,
                                                   u.name as reason,
                                                   l.name || ', ' || nvl(ltrim(s.nd, '0'), '0') || '-' ||
                                                   nvl(ltrim(s.kw, '0'), '0') as adr,
                                                   p.fio,
                                                   r.name as rel,
                                                   p.dat_prop as dt1,
                                                   p.dat_ub as dt2
                                            from arch_kart s,
                                                 a_kart_pr2 p,
                                                 spul l,
                                                 relations r,
                                                 u_list u
                                            where s.mg = mg_
                                              and s.lsk = p.lsk
                                              and s.kul = l.id
                                              and s.mg between p.mgfrom and p.mgto
                                              and p.relat_id = r.id(+)
                                              and s.reu = reu_
                                              and s.kul = kul_
                                              and s.nd = nd_
                                              --and decode(prop_,0,p.dat_prop, p.dat_ub) between dat2_ and dat3_
                                              and exists(select *
                                                         from c_states_pr r
                                                         where case
                                                                   when prop_ = 1 and r.fk_status = 1 then 1
                                                                   when prop_ = 0 and r.fk_status = 4 then 1
                                                                   else 0 end = 1
                                                           and r.fk_kart_pr = p.id
                                                           and r.dt_upd between dat2_ and dat3_)
                                              and s.psch <> 8
                                              and p.fk_ub = u.id(+)
                                            order by l.name, utils.f_order(s.nd, 6), utils.f_order(s.kw, 7);
elsif var_ = 2 then
                    --по РЭУ
                    open prep_refcursor for select s.lsk,
                                                   p.dat_rog,
                                                   p.dok_death_c,
                                                   p.dok_death_n,
                                                   u.name as reason,
                                                   l.name || ', ' || nvl(ltrim(s.nd, '0'), '0') || '-' ||
                                                   nvl(ltrim(s.kw, '0'), '0') as adr,
                                                   p.fio,
                                                   r.name as rel,
                                                   p.dat_prop as dt1,
                                                   p.dat_ub as dt2
                                            from arch_kart s,
                                                 a_kart_pr2 p,
                                                 spul l,
                                                 relations r,
                                                 u_list u
                                            where s.mg = mg_
                                              and s.lsk = p.lsk
                                              and s.kul = l.id
                                              and s.mg between p.mgfrom and p.mgto
                                              and p.relat_id = r.id(+)
                                              and s.reu = reu_
                                              --and decode(prop_,0,p.dat_prop, p.dat_ub) between dat2_ and dat3_
                                              and exists(select *
                                                         from c_states_pr r
                                                         where case
                                                                   when prop_ = 1 and r.fk_status = 1 then 1
                                                                   when prop_ = 0 and r.fk_status = 4 then 1
                                                                   else 0 end = 1
                                                           and r.fk_kart_pr = p.id
                                                           and r.dt_upd between dat2_ and dat3_)
                                              and s.psch <> 8
                                              and p.fk_ub = u.id(+)
                                            order by l.name, utils.f_order(s.nd, 6), utils.f_order(s.kw, 7);
elsif var_ = 1 then
                    --по ЖЭО
                    open prep_refcursor for select s.lsk,
                                                   p.dat_rog,
                                                   p.dok_death_c,
                                                   p.dok_death_n,
                                                   u.name as reason,
                                                   l.name || ', ' || nvl(ltrim(s.nd, '0'), '0') || '-' ||
                                                   nvl(ltrim(s.kw, '0'), '0') as adr,
                                                   p.fio,
                                                   r.name as rel,
                                                   p.dat_prop as dt1,
                                                   p.dat_ub as dt2
                                            from arch_kart s,
                                                 a_kart_pr2 p,
                                                 spul l,
                                                 relations r,
                                                 s_reu_trest t,
                                                 u_list u
                                            where s.mg = mg_
                                              and s.lsk = p.lsk
                                              and s.kul = l.id
                                              and s.mg between p.mgfrom and p.mgto
                                              and p.relat_id = r.id(+)
                                              and s.reu = t.reu
                                              and t.trest = trest_
                                              --and decode(prop_,0,p.dat_prop, p.dat_ub) between dat2_ and dat3_
                                              and exists(select *
                                                         from c_states_pr r
                                                         where case
                                                                   when prop_ = 1 and r.fk_status = 1 then 1
                                                                   when prop_ = 0 and r.fk_status = 4 then 1
                                                                   else 0 end = 1
                                                           and r.fk_kart_pr = p.id
                                                           and r.dt_upd between dat2_ and dat3_)
                                              and s.psch <> 8
                                              and p.fk_ub = u.id(+)
                                            order by l.name, utils.f_order(s.nd, 6), utils.f_order(s.kw, 7);
elsif var_ = 0 then
                    --по Городу
                    open prep_refcursor for select s.lsk,
                                                   p.dat_rog,
                                                   p.dok_death_c,
                                                   p.dok_death_n,
                                                   u.name as reason,
                                                   l.name || ', ' || nvl(ltrim(s.nd, '0'), '0') || '-' ||
                                                   nvl(ltrim(s.kw, '0'), '0') as adr,
                                                   p.fio,
                                                   r.name as rel,
                                                   p.dat_prop as dt1,
                                                   p.dat_ub as dt2
                                            from arch_kart s,
                                                 a_kart_pr2 p,
                                                 spul l,
                                                 relations r,
                                                 u_list u
                                            where s.mg = mg_
                                              and s.lsk = p.lsk
                                              and s.kul = l.id
                                              and s.mg between p.mgfrom and p.mgto
                                              and p.relat_id = r.id(+)
                                              --and decode(prop_,0,p.dat_prop, p.dat_ub) between dat2_ and dat3_
                                              and exists(select *
                                                         from c_states_pr r
                                                         where case
                                                                   when prop_ = 1 and r.fk_status = 1 then 1
                                                                   when prop_ = 0 and r.fk_status = 4 then 1
                                                                   else 0 end = 1
                                                           and r.fk_kart_pr = p.id
                                                           and r.dt_upd between dat2_ and dat3_)
                                              and s.psch <> 8
                                              and p.fk_ub = u.id(+)
                                            order by l.name, utils.f_order(s.nd, 6), utils.f_order(s.kw, 7);
end if;

end if;
elsif сd_ in ('75') then --Долги для УК, ТСЖ (для кабельного)
        --Выполнять после итогового формирования (чтоб вошла вся текущая оплата)
        --Вычисляем следующий месяц
            mg2_ := to_char(add_months(to_date(mg_ || '01', 'YYYYMMDD'), 1), 'YYYYMM');
--для ТСЖ
open prep_refcursor for select k.lsk,
                               l.cd_kladr,
                               l.name,
                               k.nd,
                               k.kw,
                               s.nm as type_name,
                               mg_ as period,
                               nvl(sum(s.summa), 0) * 100 as summa
                        from scott.kart k,
                             (select t.*, u.nm
                              from scott.saldo_usl t,
                                   scott.usl u
                              where t.mg = mg2_
                                and t.usl = u.usl
                                and u.cd in ('каб.тел.', 'антен.д.нач.', 'антен.нач.')) s,
                             scott.spul l
                        where k.psch not in (8, 9)
                          and k.lsk = s.lsk
                          and k.kul = l.id
                        group by k.lsk, substr(trim(k.fio), 1, 25), s.nm, l.name, l.cd_kladr, k.nd, k.kw
                        union all
                        select k.lsk,
                               l.cd_kladr,
                               l.name,
                               k.nd,
                               k.kw,
                               u.nm as type_name,
                               mg_ as period,
                               0 as summa
                        from kart k,
                             nabor n,
                             spul l,
                             usl u --лицевые по которым нет сальдо
                        where k.psch not in (8, 9)
                          and k.lsk = n.lsk
                          and k.kul = l.id
                          and nvl(decode(u.sptarn, 0, nvl(n.koeff, 0), 1, nvl(n.norm, 0), 2,
                                         nvl(n.koeff, 0) * nvl(n.norm, 0), 3, nvl(n.koeff, 0) *
                                                                              nvl(n.norm, 0), 4,
                                         nvl(n.koeff, 0) * nvl(n.norm, 0)), 0) <> 0
                          and n.usl = u.usl
                          and u.cd in ('каб.тел.', 'антен.д.нач.', 'антен.нач.')
                          and not exists(select t.*, u.nm
                                         from saldo_usl t,
                                              usl u
                                         where t.mg = mg2_
                                           and t.usl = u.usl
                                           and u.cd in ('каб.тел.', 'антен.д.нач.', 'антен.нач.')
                                           and t.lsk = k.lsk)
                        group by k.lsk, substr(trim(k.fio), 1, 25), u.nm, l.name, l.cd_kladr, k.nd, k.kw;
elsif сd_ in ('77') then --Долги для прочих банков (от ТСЖ)
        --Выполнять после итогового формирования (чтоб вошла вся текущая оплата)
        --Вычисляем следующий месяц
            mg2_ := to_char(add_months(to_date(mg_ || '01', 'YYYYMMDD'), 1), 'YYYYMM');
if var_ = 3 then
                --по Дому
                open prep_refcursor for select k.fio || ';' || t.name || ',' || l.name || ',' || ltrim(k.nd, '0') ||
                                               ',' || ltrim(k.kw, '0') || ';' || k.lsk || ';' ||
                                               to_char(sum(s.summa), '999990.99') as txt,
                                               sum(s.summa) as srv_sum --служебное поле, для подсчёта итоговой суммы по файлу
                                        from kart k,
                                             saldo_usl s,
                                             usl u,
                                             spul l,
                                             t_org t,
                                             t_org_tp tp
                                        where k.lsk = s.lsk(+)
                                          and k.kul = l.id
                                          and s.mg = mg2_
                                          and s.usl = u.usl
                                          and t.fk_orgtp = tp.id
                                          and tp.cd = 'Город'
                                          and k.reu = reu_
                                          and k.kul = kul_
                                          and k.nd = nd_
                                        group by k.fio, t.name, l.name, k.nd, k.kw, k.lsk
                                        order by l.name, k.nd, k.kw;
elsif var_ = 2 then
                --по УК
                open prep_refcursor for select k.fio || ';' || t.name || ',' || l.name || ',' || ltrim(k.nd, '0') ||
                                               ',' || ltrim(k.kw, '0') || ';' || k.lsk || ';' ||
                                               to_char(sum(s.summa), '999990.99') as txt,
                                               sum(s.summa) as srv_sum --служебное поле, для подсчёта итоговой суммы по файлу
                                        from kart k,
                                             saldo_usl s,
                                             usl u,
                                             spul l,
                                             t_org t,
                                             t_org_tp tp
                                        where k.lsk = s.lsk(+)
                                          and k.kul = l.id
                                          and s.mg = mg2_
                                          and s.usl = u.usl
                                          and t.fk_orgtp = tp.id
                                          and tp.cd = 'Город'
                                          and k.reu = reu_
                                        group by k.fio, t.name, l.name, k.nd, k.kw, k.lsk
                                        order by l.name, k.nd, k.kw;
elsif var_ = 1 then
                --по Фонду
                open prep_refcursor for select k.fio || ';' || t.name || ',' || l.name || ',' || ltrim(k.nd, '0') ||
                                               ',' || ltrim(k.kw, '0') || ';' || k.lsk || ';' ||
                                               to_char(sum(s.summa), '999990.99') as txt,
                                               sum(s.summa) as srv_sum --служебное поле, для подсчёта итоговой суммы по файлу
                                        from kart k,
                                             saldo_usl s,
                                             usl u,
                                             spul l,
                                             t_org t,
                                             t_org_tp tp
                                        where k.lsk = s.lsk(+)
                                          and k.kul = l.id
                                          and s.mg = mg2_
                                          and s.usl = u.usl
                                          and t.fk_orgtp = tp.id
                                          and tp.cd = 'Город'
                                          and exists(select * from s_reu_trest r where r.reu = k.reu and r.trest = trest_)
                                        group by k.fio, t.name, l.name, k.nd, k.kw, k.lsk
                                        order by l.name, k.nd, k.kw;
elsif var_ = 0 then
                --по Городу
                open prep_refcursor for select k.fio || ';' || t.name || ',' || l.name || ',' || ltrim(k.nd, '0') ||
                                               ',' || ltrim(k.kw, '0') || ';' || k.lsk || ';' ||
                                               to_char(sum(s.summa), '999990.99') as txt,
                                               sum(s.summa) as srv_sum --служебное поле, для подсчёта итоговой суммы по файлу
                                        from kart k,
                                             saldo_usl s,
                                             usl u,
                                             spul l,
                                             t_org t,
                                             t_org_tp tp
                                        where k.lsk = s.lsk(+)
                                          and k.kul = l.id
                                          and s.mg = mg2_
                                          and s.usl = u.usl
                                          and t.fk_orgtp = tp.id
                                          and tp.cd = 'Город'
                                        group by k.fio, t.name, l.name, k.nd, k.kw, k.lsk
                                        order by l.name, k.nd, k.kw;
end if;
elsif сd_ in ('78') then --форма для контроля тарифов
--det_ - вариант (0-только по основным лс., 1 - только по дополнит лс.)
            l_sel := utils.getscd_list_param('REP_TP_SCH_SEL');
--Raise_application_error(-20000, show_fond_);
if l_cur_period = mg_ then
                --текущий период
                if var_ = 3 then
                    --по Дому
                    open prep_refcursor for select distinct null as btn,
                                                            k.house_id,
                                                            u.usl,
                                                            u.npp,
                                                            u.nm,
                                                            t.org,
                                                            g.id,
                                                            g.id || ' ' || g.name as name,
                                                            m.id || ' ' || m.name as name2,
                                                            t.koeff,
                                                            t.norm,
                                                            u.sptarn,
                                                            t.dt1,
                                                            t.dt2
                                            from kart k,
                                                 nabor t,
                                                 usl u,
                                                 t_org g,
                                                 t_org m,
                                                 v_lsk_tp tp
                                            where k.lsk = t.lsk
                                              and t.usl = u.usl
                                              and t.org = g.id
                                              and g.fk_org2 = m.id
                                              and k.house_id = p_house
                                              and k.fk_tp = tp.id
                                              and tp.cd = l_sel
                                              and k.reu = reu_
                                              and k.psch not in (8, 9)
                                            order by u.npp, t.dt1, g.id, t.koeff, t.norm;
elsif var_ = 2 then
                    --по УК
                    open prep_refcursor for select distinct null as btn,
                                                            null as house_id,
                                                            u.usl,
                                                            u.npp,
                                                            u.nm,
                                                            t.org,
                                                            g.id,
                                                            g.id || ' ' || g.name as name,
                                                            m.id || ' ' || m.name as name2,
                                                            t.koeff,
                                                            t.norm,
                                                            u.sptarn,
                                                            t.dt1,
                                                            t.dt2
                                            from kart k,
                                                 nabor t,
                                                 usl u,
                                                 t_org g,
                                                 t_org m,
                                                 v_lsk_tp tp
                                            where k.lsk = t.lsk
                                              and t.usl = u.usl
                                              and t.org = g.id
                                              and g.fk_org2 = m.id
                                              and k.reu = reu_
                                              and k.fk_tp = tp.id
                                              and tp.cd = l_sel
                                              and k.psch not in (8, 9)
                                            order by u.npp, t.dt1, g.id, t.koeff, t.norm;
elsif var_ = 1 then
                    --по Фонду
                    open prep_refcursor for select distinct
                                                null as btn,
                                                null as house_id,
                                                u.usl,
                                                u.npp,
                                                u.nm,
                                                t.org,
                                                g.id,
                                                g.id || ' ' || g.name as name,
                                                m.id || ' ' || m.name as name2,
                                                t.koeff,
                                                t.norm,
                                                u.sptarn,
                                                t.dt1,
                                                t.dt2
                                            from kart k,
                                                 nabor t,
                                                 usl u,
                                                 t_org g,
                                                 t_org m,
                                                 v_lsk_tp tp
                                            where k.lsk = t.lsk
                                              and t.usl = u.usl
                                              and t.org = g.id
                                              and g.fk_org2 = m.id
                                              and k.fk_tp = tp.id
                                              and exists(select * from s_reu_trest r where r.reu = k.reu and r.trest = trest_)
                                              and tp.cd = l_sel
                                              and k.psch not in (8, 9)
                                            order by u.npp, t.dt1, g.id, t.koeff, t.norm;
elsif var_ = 0 then
                    --по Городу
                    open prep_refcursor for select distinct null as btn,
                                                            null as house_id,
                                                            u.usl,
                                                            u.npp,
                                                            u.nm,
                                                            t.org,
                                                            g.id,
                                                            g.id || ' ' || g.name as name,
                                                            m.id || ' ' || m.name as name2,
                                                            t.koeff,
                                                            t.norm,
                                                            u.sptarn,
                                                            t.dt1,
                                                            t.dt2
                                            from kart k,
                                                 nabor t,
                                                 usl u,
                                                 t_org g,
                                                 t_org m,
                                                 v_lsk_tp tp
                                            where k.lsk = t.lsk
                                              and t.usl = u.usl
                                              and t.org = g.id
                                              and g.fk_org2 = m.id
                                              and k.fk_tp = tp.id
                                              and tp.cd = l_sel
                                              and k.psch not in (8, 9)
                                            order by u.npp, t.dt1, g.id, t.koeff, t.norm;
end if;
else
                --прошлый период
                if var_ = 3 then
                    --по Дому
                    open prep_refcursor for select distinct null as btn,
                                                            k.house_id,
                                                            u.usl,
                                                            u.npp,
                                                            u.nm,
                                                            t.org,
                                                            g.id,
                                                            g.id || ' ' || g.name as name,
                                                            m.id || ' ' || m.name as name2,
                                                            t.koeff,
                                                            t.norm,
                                                            u.sptarn,
                                                            t.dt1,
                                                            t.dt2
                                            from arch_kart k,
                                                 a_nabor2 t,
                                                 usl u,
                                                 t_org g,
                                                 t_org m,
                                                 v_lsk_tp tp
                                            where k.lsk = t.lsk
                                              and t.usl = u.usl
                                              and t.org = g.id
                                              and g.fk_org2 = m.id
                                              and k.house_id = p_house
                                              and k.mg = mg_
                                              and k.mg between t.mgfrom and t.mgto
                                              and k.fk_tp = tp.id
                                              and tp.cd = l_sel
                                              and k.psch not in (8, 9)
                                            order by u.npp, t.dt1, g.id, t.koeff, t.norm;
elsif var_ = 2 then
                    --по УК
                    open prep_refcursor for select distinct null as btn,
                                                            null as house_id,
                                                            u.usl,
                                                            u.npp,
                                                            u.nm,
                                                            t.org,
                                                            g.id,
                                                            g.id || ' ' || g.name as name,
                                                            m.id || ' ' || m.name as name2,
                                                            t.koeff,
                                                            t.norm,
                                                            u.sptarn,
                                                            t.dt1,
                                                            t.dt2
                                            from arch_kart k,
                                                 a_nabor2 t,
                                                 usl u,
                                                 t_org g,
                                                 t_org m,
                                                 v_lsk_tp tp
                                            where k.lsk = t.lsk
                                              and t.usl = u.usl
                                              and t.org = g.id
                                              and g.fk_org2 = m.id
                                              and k.reu = reu_
                                              and k.mg = mg_
                                              and k.mg between t.mgfrom and t.mgto
                                              and k.fk_tp = tp.id
                                              and tp.cd = l_sel
                                              and k.psch not in (8, 9)
                                            order by u.npp, t.dt1, g.id, t.koeff, t.norm;
elsif var_ = 1 then
                    --по Фонду
                    open prep_refcursor for select distinct null as btn,
                                                            null as house_id,
                                                            u.usl,
                                                            u.npp,
                                                            u.nm,
                                                            t.org,
                                                            g.id,
                                                            g.id || ' ' || g.name as name,
                                                            m.id || ' ' || m.name as name2,
                                                            t.koeff,
                                                            t.norm,
                                                            u.sptarn,
                                                            t.dt1,
                                                            t.dt2
                                            from arch_kart k,
                                                 a_nabor2 t,
                                                 usl u,
                                                 t_org g,
                                                 t_org m,
                                                 v_lsk_tp tp
                                            where k.lsk = t.lsk
                                              and t.usl = u.usl
                                              and t.org = g.id
                                              and g.fk_org2 = m.id
                                              and exists(select * from s_reu_trest r where r.reu = k.reu and r.trest = trest_)
                                              and k.mg = mg_
                                              and k.mg between t.mgfrom and t.mgto
                                              and k.fk_tp = tp.id
                                              and tp.cd = l_sel
                                              and k.psch not in (8, 9)
                                            order by u.npp, t.dt1, g.id, t.koeff, t.norm;
elsif var_ = 0 then
                    --по Городу
                    open prep_refcursor for select distinct null as btn,
                                                            null as house_id,
                                                            u.usl,
                                                            u.npp,
                                                            u.nm,
                                                            t.org,
                                                            g.id,
                                                            g.id || ' ' || g.name as name,
                                                            m.id || ' ' || m.name as name2,
                                                            t.koeff,
                                                            t.norm,
                                                            u.sptarn,
                                                            t.dt1,
                                                            t.dt2
                                            from arch_kart k,
                                                 a_nabor2 t,
                                                 usl u,
                                                 t_org g,
                                                 t_org m,
                                                 v_lsk_tp tp
                                            where k.lsk = t.lsk
                                              and t.usl = u.usl
                                              and t.org = g.id
                                              and g.fk_org2 = m.id
                                              and k.mg = mg_
                                              and k.mg between t.mgfrom and t.mgto
                                              and k.fk_tp = tp.id
                                              and tp.cd = l_sel
                                              and k.psch not in (8, 9)
                                            order by u.npp, t.dt1, g.id, t.koeff, t.norm;
end if;
end if;

elsif сd_ in ('79') then
            --отчет (для Полыс) по льготникам, для УСЗН
            if var_ = 3 then
                --По дому
                raise_application_error(-20001, 'Не существует уровня детализации!');
elsif var_ = 2 then
                --По РЭУ
                raise_application_error(-20001, 'Не существует уровня детализации!');
elsif var_ = 1 then
                --По ЖЭО
                raise_application_error(-20001, 'Не существует уровня детализации!');
elsif var_ = 0 then
                --(все УК)
                open prep_refcursor for select k.lsk,
                                               k.mg,
                                               s.name || ', ' || nvl(ltrim(k.nd, '0'), '0') || '-' ||
                                               nvl(ltrim(k.kw, '0'), '0') as adr,
                                               k.opl,
                                               decode(k.status, 1, '-', '+') as status,
                                               a.summa1,
                                               a.summa2,
                                               a.summa3,
                                               a.summa4,
                                               a.summa5,
                                               a.summa6,
                                               a.summa7,
                                               a.summa8,
                                               a.norm1,
                                               a.norm2,
                                               a.norm3,
                                               a.tp1,
                                               a.limit1,
                                               a.limit2,
                                               a.limit3
                                        from arch_kart k,
                                             spul s,
                                             (select k.k_lsk_id,
                                                     max(decode(t.usl, '003', t.test_cena, '004', t.test_cena, 0)) as summa1, --тек.содержание
                                                     max(decode(t.usl, '005', t.test_cena, 0)) +
                                                     max(decode(t.usl, '006', t.test_cena, 0)) +
                                                     max(decode(t.usl, '009', t.test_cena, 0)) +
                                                     max(decode(t.usl, '010', t.test_cena, 0)) as summa2,                     --лифт
                                                     max(decode(t.usl, '031', t.test_cena, '046', t.summa, 0)) as summa3,     --тбо
                                                     max(decode(t.usl, '052', t.test_cena, 0)) as summa4,                     --ассенизация
                                                     max(decode(t.usl, '054', t.test_cena, 0)) as summa5,                     --утилизация
                                                     max(decode(t.usl, '033', t.test_cena, '034', test_cena, 0)) as summa6,   --расценка кап.рем.
                                                     max(decode(t.usl, '026', test_cena, 0)) as summa7,                       --расценка найм.
                                                     max(decode(t.usl, '055', t.test_cena, 0)) as summa8,                     --текущий ремонт
                                                     max(decode(n.usl, '011', n.norm, 0)) as norm1,                           --норматив хвс
                                                     max(decode(n.usl, '015', n.norm, 0)) as norm2,                           --норматив гвс
                                                     max(decode(n.usl, '013', n.norm, 0)) as norm3,                           --норматив водоотвед
                                                     case
                                                         when nvl(max(decode(t.usl, '007', t.summa, '008', t.summa, 0)), 0) <>
                                                              0 then '+'
                                                         else '-' end as tp1,                                                 --признак наличия отопления
                                                     max(decode(n.usl, '011', d.nrm, 0)) as limit1,                           --ограничение ОДН хвс
                                                     max(decode(n.usl, '015', d.nrm, 0)) as limit2,                           --ограничение ОДН гвс
                                                     max(decode(n.usl, '053', d.nrm, 0)) as limit3                            --ограничение ОДН Эл.эн.
                                              from a_nabor2 n
                                                       join arch_kart k on n.lsk = k.lsk and k.mg = mg_ and k.psch not in (8, 9)
                                                       left join a_charge2 t
                                                                 on n.usl = t.usl and n.lsk = t.lsk and t.type = 1 and
                                                                    mg_ between t.mgfrom and t.mgto
                                                       left join scott.a_vvod d on d.mg = mg_ and n.fk_vvod = d.id
                                              where mg_ between n.mgfrom and n.mgto
                                                and to_date(mg_ || '01', 'YYYYMMDD') between n.dt1 and n.dt2
                                              group by k.k_lsk_id) a
                                        where k.kul = s.id
                                          and k.k_lsk_id = a.k_lsk_id(+)
                                          and k.mg = mg_
                                          and k.psch not in (8, 9)
                                        order by s.name, utils.f_order(k.nd, 6), utils.f_order2(k.nd),
                                                 utils.f_order(k.kw, 7), k.lsk;
end if;
elsif сd_ = '80' then --Задолжники OLAP-2 - версия для тех, кто использует c_deb_usl (полыс.)

--   cur_pay_:=utils.getS_bool_param('REP_CUR_PAY');
            if dat_ is not null then raise_application_error(-20000, 'Текущая дата не используется!'); end if;

kpr1_ := utils.gets_int_param('REP_RNG_KPR1');
kpr2_ := utils.gets_int_param('REP_RNG_KPR2');
n1_ := utils.gets_list_param('REP_DEB_VAR');
if n1_ = 0 then
                n2_ := utils.gets_int_param('REP_DEB_MONTH');
else
                n2_ := utils.gets_int_param('REP_DEB_SUMMA');
end if;

if var_ = 3 then
                --По дому
                raise_application_error(-20000, 'не используется уровень!');
elsif var_ = 2 then
                --По ЖЭО
                raise_application_error(-20000, 'не используется уровень!');
elsif var_ = 1 then
                --По фонду
                raise_application_error(-20000, 'не используется уровень!');
elsif var_ = 0 then
                --По городу
                open prep_refcursor for select null as lsk, -- 21.02.2018 договорились с Полыс. что будет выводиться только по адресу
                                               s.org,
                                               o.name as name_org,
                                               u.nm,
                                               u.nm1,
                                            /*decode(k.psch,
              9,
              'Закрытые Л/С',
              8,
              'Старый фонд',
              'Открытые Л/С')*/
                                               'Открытые Л/С' as psch,
                                               r.name_tr,
                                               r.name_reu,
                                               l.name as street,
                                               ltrim(k.nd, '0') as nd,
                                               ltrim(k.kw, '0') as kw,
                                               k.nd,
                                               k.kw,
                                               decode(det_, 3, trim(l.name) || ', ' || ltrim(k.nd, '0') || '-' ||
                                                               ltrim(k.kw, '0'), trim(l.name) || ', ' ||
                                                                                 ltrim(k.nd, '0')) --показать информацию по квартирам или по домам
                                                   as adr,
                                               k.fio,
                                               sum(s.summa) as dolg,
                                               t.cnt_month as cnt,
                                               substr(s.mg, 1, 4) || '.' || substr(s.mg, 5, 2) as mg
                                        from kart k
                                                 join (select k2.k_lsk_id, t.usl, t.org, t.mg, sum(t.summa) as summa
                                                       from kart k2
                                                                join c_deb_usl t on k2.lsk = t.lsk
                                                       where t.period = mg_
                                                         and t.mg <= l_prev_period -- не считать начисление текущего периода
                                                       group by k2.k_lsk_id, t.usl, t.org, t.mg) s
                                                      on k.k_lsk_id = s.k_lsk_id and s.summa > 0
                                                 join (select k2.k_lsk_id, sum(t.cnt_month) as cnt_month
                                                       from kart k2
                                                                join debits_lsk_month t on k2.lsk = t.lsk
                                                       where t.mg = mg_
                                                       group by k2.k_lsk_id
                                                       having ((n1_ = 0 and sum(t.cnt_month) >= n2_) or
                                                               (n1_ = 1 and sum(t.dolg) >= n2_))) t
                                                      on k.k_lsk_id = t.k_lsk_id
                                                 join t_org o on s.org = o.id
                                                 join spul l on k.kul = l.id
                                                 join s_reu_trest r on k.reu = r.reu
                                                 join usl u on s.usl = u.usl
                                                 join v_lsk_tp tp on k.fk_tp = tp.id and tp.cd = 'LSK_TP_MAIN'
                                        where (kpr1_ is not null and k.kpr >= kpr1_ or kpr1_ is null)
                                          and (kpr2_ is not null and k.kpr <= kpr2_ or kpr2_ is null)
                                          and k.psch not in (8)
                                        group by s.org, u.nm, u.nm1, o.name,
                                                 decode(k.psch, 9, 'Закрытые Л/С', 8, 'Старый фонд', 'Открытые Л/С'),
                                                 r.name_tr, r.name_reu, l.name, ltrim(k.nd, '0'), ltrim(k.kw, '0'),
                                                 k.nd, k.kw,
                                                 trim(l.name) || ', ' || ltrim(k.nd, '0') || '-' || ltrim(k.kw, '0'),
                                                 k.fio, t.cnt_month, substr(s.mg, 1, 4) || '.' || substr(s.mg, 5, 2)
                                        order by trim(l.name), utils.f_order(k.nd, 6),
                                                 utils.f_order(k.kw, 7); --    using det_, l_prev_period, n1_, n2_, n1_, n2_, kpr1_, kpr1_, kpr1_, kpr2_, kpr2_, kpr2_;

/*open prep_refcursor for
   select s.lsk,
       s.org,
       o.name as name_org,
       u.nm,
       u.nm1,
       decode(k.psch,
              9,
              'Закрытые Л/С',
              8,
              'Старый фонд',
              'Открытые Л/С') as psch,
       r.name_tr,
       r.name_reu,
       l.name as street,
       ltrim(k.nd, '0') as nd,
       ltrim(k.kw, '0') as kw,
       k.nd, k.kw,
       decode(det_, 3, trim(l.name) || ', ' || ltrim(k.nd, '0') || '-' || ltrim(k.kw, '0'),
               trim(l.name) || ', ' || ltrim(k.nd, '0')) --показать информацию по квартирам или по домам
       as adr,
       k.fio,
       sum(s.summa) as dolg,
       t.cnt_month as cnt,
       substr(s.mg,1,4)||'.'||substr(s.mg,5,2) as mg
         from kart k, c_deb_usl s, (select d.lsk, max(d.cnt_month) as cnt_month
          from debits_lsk_month d where (l_period_tp<>3 and d.dat between dat_ and dat1_
                                              or l_period_tp=3 and d.mg between mg_ and mg1_)
                                           group by d.lsk) t, t_org o, spul l, s_reu_trest r, usl u
 where --s.summa > 0
   --and
   s.usl=u.usl and --s.mg<=l_prev_period
   (n1_ <> 0 or n1_=0 and abs(months_between(
                  to_date(s.period||'01','YYYYMMDD'),
                  to_date(s.mg||'01','YYYYMMDD')
                  )) >= n2_)
   and exists (select *
          from debits_lsk_month d
         where d.k_lsk_id=k.k_lsk_id and (l_period_tp<>3 and d.dat between dat_ and dat1_
                                              or l_period_tp=3 and d.mg between mg_ and mg1_)
            and ((n1_=0 and d.cnt_month >= n2_) or
            (n1_=1 and d.dolg >= n2_))
           )
   and (kpr1_ is not null and k.kpr >=kpr1_ or kpr1_ is null)
   and (kpr2_ is not null and k.kpr <=kpr2_ or kpr2_ is null)
   and s.org = o.id
   and k.lsk = s.lsk
   and k.lsk = t.lsk
   and k.kul = l.id
   and k.reu = r.reu and (l_period_tp<>3 and s.period between to_char(dat_,'YYYYMM') and to_char(dat1_,'YYYYMM')
                                              or l_period_tp=3 and s.period between mg_ and mg1_)
 group by s.lsk,
          s.org,
          u.nm,
          u.nm1,
          o.name,
          decode(k.psch,
                 9,
                 'Закрытые Л/С',
                 8,
                 'Старый фонд',
                 'Открытые Л/С'),
          r.name_tr,
          r.name_reu,
          l.name,
          ltrim(k.nd, '0'),
          ltrim(k.kw, '0'),
          k.nd, k.kw,
          trim(l.name) || ', ' || ltrim(k.nd, '0') || '-' ||
          ltrim(k.kw, '0'),
          k.fio,
          t.cnt_month,
          substr(s.mg,1,4)||'.'||substr(s.mg,5,2)
 order by trim(l.name), utils.f_order(k.nd, 6), utils.f_order(k.kw, 7);
--    using det_, l_prev_period, n1_, n2_, n1_, n2_, kpr1_, kpr1_, kpr1_, kpr2_, kpr2_, kpr2_;
*/
end if;

elsif сd_ = '81' then
            --Отчет для Э+ (для Дениса)
            l_sql := 'select * from (select k.lsk, k.lsk_ext as lsk2, sp.name, ltrim(k.nd, ''0'') as nd,
      ltrim(k.kw, ''0'') as kw, t.cdtp, p.cena, -1*nvl(s.summa,0) as summa from kart k,
      nabor n, spr_tarif t, spr_tarif_prices p, spul sp,
      (select s.lsk, sum(summa) as summa from saldo_usl s where s.usl=''042''
       and s.mg=' || l_mg_next || '
       group by s.lsk) s,
      (select t.lsk, sum(t.summa) as summa from c_charge t where t.usl=''042''
       and t.type=1
       group by t.lsk) a
       where k.lsk=n.lsk and k.lsk=s.lsk(+)and k.lsk=a.lsk(+) and t.cdtp=''ИНТ''
      and t.id=p.fk_tarif and ' || mg_ || '
      between p.mg1 and p.mg2
      and n.fk_tarif=t.id
      and sp.id=k.kul
      and nvl(n.norm,0) <> 0
      and nvl(n.koeff,0) <> 0
      union all
      select k.lsk, k.lsk_ext as lsk2, sp.name, ltrim(k.nd, ''0'') as nd,
      ltrim(k.kw, ''0'') as kw, t.cdtp, p.cena, -1*nvl(s.summa,0) as summa from kart k,
      nabor_progs n, spr_tarif t, spr_tarif_prices p, spul sp,
      (select s.lsk, sum(summa) as summa from saldo_usl s where s.usl=''056''
       and s.mg=' || l_mg_next || '
       group by s.lsk) s
       where k.lsk=n.lsk and k.lsk=s.lsk(+)
      and t.id=p.fk_tarif and ' || mg_ || '
      between p.mg1 and p.mg2
      and t.usl=''056''
      and n.fk_tarif=t.id
      and sp.id=k.kul
--      and k.lsk=''00107244''
      ) d
      order by d.name, utils.f_order(d.nd,6), utils.f_order(d.kw,6)
      ';

if nvl(p_out_tp, 0) = 1 then
                if utils.set_base_state_gen(1) = 0 then --если выполнили БЛОКИРОВКУ формирования,
                --в противном случае выгрузить пустой отчет
                --установить состояние базы - не выполнено итоговое формирование
                    init.set_state(0);
                    --выполнить формирование сальдо
gen.gen_saldo(null);
                    --выгрузить в файл,в директорию по умолчанию
sqltofile(l_sql, 'LOAD_FILE_DIR', 'OUT' || to_char(trunc(sysdate), 'YYYYMMDD') || '.txt',
             'OUT' || to_char(trunc(sysdate), 'YYYYMMDD') || '.txt', ';');
--снимаем БЛОКИРОВКУ
l_cnt := utils.set_base_state_gen(0);
else
                    --пустой отчет
                    sqltofile('select null as lsk from dual', 'LOAD_FILE_DIR',
                              'OUT' || to_char(trunc(sysdate), 'YYYYMMDD') || '.txt',
                              'OUT' || to_char(trunc(sysdate), 'YYYYMMDD') || '.txt', ';');
end if;

else
                --отправить как реф-курсор
                open prep_refcursor for l_sql/*
       using l_mg_next, mg_, mg_, mg_*/;
end if;

elsif сd_ in ('82') then --Задолжники FR, в зависимости от организатора задолжника - для Полыс.
        --переделал, что можно вывести по нескольким орг.
            kpr1_ := utils.gets_int_param('REP_RNG_KPR1');
kpr2_ := utils.gets_int_param('REP_RNG_KPR2');

n1_ := utils.gets_list_param('REP_DEB_VAR');
if n1_ = 0 then
                n2_ := utils.gets_int_param('REP_DEB_MONTH');
else
                n2_ := utils.gets_int_param('REP_DEB_SUMMA');
end if;

if var_ = 3 then
                --по Дому
                open prep_refcursor for 'select o.name as name_deb_org, s.lsk, t.name_reu, trim(s.name) as street_name,
      ltrim(s.nd,''0'') as nd, ltrim(s.kw,''0'') as kw, pr.fio, s.cnt_month, s.dolg, s.penya,
      case when s.dat is null and s.mg is not null then to_date(s.mg||''01'',''YYYYMMDD'')
           else s.dat
           end as dat
      from debits_lsk_month s, s_reu_trest t, t_org o, c_kart_pr pr
      where s.reu=t.reu and s.var=0
      and ' || sqlstr_ || '
      and s.reu=:reu_ AND s.kul=:kul_ AND s.nd=:nd_
      and exists
      (select * from kart k where k.lsk=s.lsk
      and (:kpr1_ is not null and k.kpr >=:kpr1_ or :kpr1_ is null)
      and (:kpr2_ is not null and k.kpr <=:kpr2_ or :kpr2_ is null))
      and
      ((:n1_=0 and s.cnt_month >= :n2_) or
      (:n1_=1 and s.dolg >= :n2_))
      and exists
      (select * from c_kart_pr r, list_c i, spr_params p where i.fk_ses=:fk_ses_
            and p.id=i.fk_par and p.cd=''REP_ORG''
            and r.fk_deb_org=i.sel_id
            and s.lsk=r.lsk
            and i.sel_id=o.id
            and r.id=pr.id
            and r.status<>4
            and i.sel=1)
      order by o.name, t.name_reu, s.name, s.nd, s.kw' using reu_, kul_, nd_, kpr1_, kpr1_, kpr1_, kpr2_, kpr2_, kpr2_, n1_, n2_, n1_, n2_, fk_ses_;

elsif var_ = 2 then
                --по РЭУ
                open prep_refcursor for 'select o.name as name_deb_org, s.lsk, t.name_reu, trim(s.name) as street_name,
      ltrim(s.nd,''0'') as nd, ltrim(s.kw,''0'') as kw, pr.fio, s.cnt_month, s.dolg, s.penya,
      case when s.dat is null and s.mg is not null then to_date(s.mg||''01'',''YYYYMMDD'')
           else s.dat
           end as dat
      from debits_lsk_month s, s_reu_trest t, t_org o, c_kart_pr pr
      where s.reu=t.reu and s.var=0
      and ' || sqlstr_ || '
      and s.reu=:reu_
      and exists
      (select * from kart k where k.lsk=s.lsk
      and (:kpr1_ is not null and k.kpr >=:kpr1_ or :kpr1_ is null)
      and (:kpr2_ is not null and k.kpr <=:kpr2_ or :kpr2_ is null))
      and
      ((:n1_=0 and s.cnt_month >= :n2_) or
      (:n1_=1 and s.dolg >= :n2_))
      and exists
      (select * from c_kart_pr r, list_c i, spr_params p where i.fk_ses=:fk_ses_
            and p.id=i.fk_par and p.cd=''REP_ORG''
            and r.fk_deb_org=i.sel_id
            and s.lsk=r.lsk
            and i.sel_id=o.id
            and r.id=pr.id
            and r.status<>4
            and i.sel=1)
      order by o.name, t.name_reu, s.name, s.nd, s.kw' using reu_, kpr1_, kpr1_, kpr1_, kpr2_, kpr2_, kpr2_, n1_, n2_, n1_, n2_, fk_ses_;

elsif var_ = 1 then
                --по ЖЭО
                open prep_refcursor for 'select o.name as name_deb_org, s.lsk, t.name_reu, trim(s.name) as street_name,
      ltrim(s.nd,''0'') as nd, ltrim(s.kw,''0'') as kw, pr.fio, s.cnt_month, s.dolg, s.penya,
      case when s.dat is null and s.mg is not null then to_date(s.mg||''01'',''YYYYMMDD'')
           else s.dat and s.var=0
           end as dat
      from debits_lsk_month s, s_reu_trest t, t_org o, c_kart_pr pr
      where s.reu=t.reu
      and ' || sqlstr_ || '
      and s.reu=:trest_
      and exists
      (select * from kart k where k.lsk=s.lsk
      and (:kpr1_ is not null and k.kpr >=:kpr1_ or :kpr1_ is null)
      and (:kpr2_ is not null and k.kpr <=:kpr2_ or :kpr2_ is null))
      and
      ((:n1_=0 and s.cnt_month >= :n2_) or
      (:n1_=1 and s.dolg >= :n2_))
      and exists
      (select * from c_kart_pr r, list_c i, spr_params p where i.fk_ses=:fk_ses_
            and p.id=i.fk_par and p.cd=''REP_ORG''
            and r.fk_deb_org=i.sel_id
            and s.lsk=r.lsk
            and i.sel_id=o.id
            and r.id=pr.id
            and r.status<>4
            and i.sel=1)
      order by o.name, t.name_reu, s.name, s.nd, s.kw' using trest_, kpr1_, kpr1_, kpr1_, kpr2_, kpr2_, kpr2_, n1_, n2_, n1_, n2_, fk_ses_;

elsif var_ = 0 then
                --по Городу
                open prep_refcursor for 'select o.name as name_deb_org, s.lsk, t.name_reu, trim(s.name) as street_name,
      ltrim(s.nd,''0'') as nd, ltrim(s.kw,''0'') as kw, pr.fio, s.cnt_month, s.dolg, s.penya,
      case when s.dat is null and s.mg is not null then to_date(s.mg||''01'',''YYYYMMDD'')
           else s.dat
           end as dat
      from debits_lsk_month s, s_reu_trest t, t_org o, c_kart_pr pr
      where s.reu=t.reu and s.var=0
      and ' || sqlstr_ || '
      and exists
      (select * from kart k where k.lsk=s.lsk
      and (:kpr1_ is not null and k.kpr >=:kpr1_ or :kpr1_ is null)
      and (:kpr2_ is not null and k.kpr <=:kpr2_ or :kpr2_ is null))
      and
      ((:n1_=0 and s.cnt_month >= :n2_) or
      (:n1_=1 and s.dolg >= :n2_))
      and exists
      (select * from c_kart_pr r, list_c i, spr_params p where i.fk_ses=:fk_ses_
            and p.id=i.fk_par and p.cd=''REP_ORG''
            and r.fk_deb_org=i.sel_id
            and s.lsk=r.lsk
            and i.sel_id=o.id
            and r.id=pr.id
            and r.status<>4
            and i.sel=1)
      order by o.name, t.name_reu, s.name, s.nd, s.kw' using kpr1_, kpr1_, kpr1_, kpr2_, kpr2_, kpr2_, n1_, n2_, n1_, n2_, fk_ses_;
end if;
elsif сd_ in ('83') then --отчёт для администрации по тарифам, объемам, нормативам и прочее...
        --имеет дочерний датасет в rep_detail
            if var_ = 3 then
                --по Дому
                open prep_refcursor for 'select s.lsk, st.name as name_st, s.kpr,
         sp.name||'', ''||NVL(LTRIM(s.nd,''0''),''0'')||''-''||NVL(LTRIM(s.kw,''0''),''0'') as adr,
         s.mg
        from arch_kart s, spul sp, status st where s.mg=:p_mg
        and not exists (select * from a_nabor n, usl u where n.lsk=s.lsk and n.usl=u.usl and u.cd=''гараж'') --не гаражи
        and s.reu=:reu_ and s.kul=:kul_ and s.nd=:nd_
        and s.kul=sp.id
        and s.status=st.id
        order by s.lsk  --order by sp.name, f_ord_digit(s.nd), f_ord3(s.nd), f_ord_digit(s.kw)' using mg_, reu_, kul_, nd_;
elsif var_ = 2 then
                --по РЭУ
                open prep_refcursor for 'select s.lsk, st.name as name_st, s.kpr,
         sp.name||'', ''||NVL(LTRIM(s.nd,''0''),''0'')||''-''||NVL(LTRIM(s.kw,''0''),''0'') as adr,
         s.mg
        from arch_kart s, spul sp, status st where s.mg=:p_mg
        and not exists (select * from a_nabor n, usl u where n.lsk=s.lsk and n.usl=u.usl and u.cd=''гараж'' and n.koeff<>0) --не гаражи
        and s.reu=:reu_
        and s.kul=sp.id
        and s.status=st.id
        order by s.lsk  -- order by sp.name, utils.f_ord_digit(s.nd), utils.f_ord3(s.nd), utils.f_ord_digit(s.kw)' using mg_, reu_;
elsif var_ = 1 then
                --по ЖЭО
                open prep_refcursor for 'select s.lsk, st.name as name_st, s.kpr,
         sp.name||'', ''||NVL(LTRIM(s.nd,''0''),''0'')||''-''||NVL(LTRIM(s.kw,''0''),''0'') as adr,
         s.mg
        from arch_kart s, spul sp, status st, s_reu_trest t where s.mg=:p_mg
        and not exists (select * from a_nabor n, usl u where n.lsk=s.lsk and n.usl=u.usl and u.cd=''гараж'' and n.koeff<>0) --не гаражи
        and s.reu=t.reu
        and t.trest=:trest_
        and s.kul=sp.id
        and s.status=st.id
        order by s.lsk --order by sp.name, utils.f_ord_digit(s.nd), utils.f_ord3(s.nd), utils.f_ord_digit(s.kw)' using mg_, trest_;
elsif var_ = 0 then
                --по Городу
                open prep_refcursor for 'select s.lsk, st.name as name_st, s.kpr,
         sp.name||'', ''||NVL(LTRIM(s.nd,''0''),''0'')||''-''||NVL(LTRIM(s.kw,''0''),''0'') as adr,
         s.mg
        from arch_kart s, spul sp, status st where s.mg=:p_mg
        and not exists (select * from a_nabor n, usl u where n.lsk=s.lsk and n.usl=u.usl and u.cd=''гараж'' and n.koeff<>0) --не гаражи
        and s.kul=sp.id
        and s.status=st.id
        order by s.lsk --order by sp.name, utils.f_ord_digit(s.nd), utils.f_ord3(s.nd), utils.f_ord_digit(s.kw)' using mg_;
end if;

elsif сd_ in ('84') then
            --Новый список-оборотка для субсидирования ТСЖ
            if var_ = 2 then
                --по УК
                open prep_refcursor for select l.name || ', ' || nvl(ltrim(s.nd, '0'), '0') as adr,
                                               ltrim(s.kw, '0') as kw,
                                               s.komn,
                                               s.opl,
                                               s.fio,
                                               e.opl_n,
                                               e.opl_sv,
                                               e.opl_empt,
                                               nvl(e.opl_n, 0) + nvl(e.opl_sv, 0) + nvl(e.opl_empt, 0) as opl_itg,
                                               e.summa_n,
                                               e.summa_sv,
                                               e.summa_empt,
                                               nvl(e.summa_n, 0) + nvl(e.summa_sv, 0) + nvl(e.summa_empt, 0) +
                                               nvl(d.chng, 0) as summa_itg,

                                               nvl(gw.summa_sch_n, 0) + nvl(gw.summa_norm_n, 0) as gw_summa_norm,
                                               gw.summa_sch_sv as gw_summa_sch_sv,
                                               gw.summa_sch_empt as gw_summa_sch_empt,

                                               nvl(gw.summa_sch_n, 0) + nvl(gw.summa_norm_n, 0) +
                                               nvl(gw.summa_sch_sv, 0) + nvl(gw.summa_sch_empt, 0) +
                                               nvl(gw2.summa, 0) as gw_summa_itg,

                                               gw.vol_sch_n as gw_sch_n,
                                               gw.vol_sch_sv as gw_sch_sv,
                                               gw.vol_sch_empt as gw_sch_empt,

                                               gw.vol_norm_n as gw_vol_norm,
                                               gw2.vol_sch as odn_vol_sch,
                                               gw2.vol_norm as odn_vol_norm,

                                               nvl(gw.vol_sch_n, 0) + nvl(gw.vol_sch_sv, 0) + nvl(gw.vol_sch_empt, 0) +
                                               nvl(gw2.vol_sch, 0) as gw_vol_itg,

                                               gw2.summa as odn_summa,
                                               gw3.kpr_sch,
                                               gw3.kpr_norm,
                                               nvl(gw3.kpr_sch, 0) + nvl(gw3.kpr_norm, 0) as kpr_itg,
                                               d.chng,
                                               c.name,
                                               c.adr as org_adr,
                                               c.inn,
                                               c.kpp,
                                               c.head_name,
                                               upper(utils.month_name(substr(s.mg, 5, 2))) || ' ' ||
                                               substr(s.mg, 1, 4) || 'г.' as mg_name,
                                               s.vvod_ot
                                        from (select s.*
                                              from arch_kart s,
                                                   s_reu_trest e
                                              where s.mg between mg_ and mg1_
                                                and exists --ключевой запрос
                                                  (select *
                                                   from a_charge2 a,
                                                        usl u
                                                   where a.usl = u.usl
                                                     and u.cd in ('отоп', 'отоп/св.нор', 'отоп/0 зарег.')
                                                     and a.type = 1
                                                     and a.summa <> 0
                                                     and a.lsk = s.lsk
                                                     and s.mg between a.mgfrom and a.mgto)
                                                and e.reu = reu_
                                                and s.reu = e.reu
                                                and s.psch not in (8, 9)
                                                and s.status not in (7)--убрал нежилые по просьбе ТСЖ Клён, ред.09.01.13 В ЭТОМ ОТЧЕТЕ НЕТ НИКАКИХ MG1_ !!! только MG_!!!
                                             ) s,
                                             t_org c,
                                             params p,
                                             spul l,
                                             (select s.lsk,
                                                     u.uslg,
                                                     sum(case
                                                             when u.cd in ('отоп')/*u.usl_norm = 0 and s.kpr <> 0*/
                                                                 then s.summa
                                                             else 0 end) as summa_n,
                                                     sum(case
                                                             when u.cd in ('отоп/св.нор')/*u.usl_norm = 1 and s.kpr <> 0*/
                                                                 then s.summa
                                                             else 0 end) as summa_sv,
                                                     sum(case when u.cd in ('отоп/0 зарег.')/*s.kpr = 0*/ then s.summa else 0 end) as summa_empt,
                                                     sum(case
                                                             when u.cd in ('отоп')/*u.usl_norm = 0 and s.kpr <> 0*/
                                                                 then s.test_opl
                                                             else 0 end) as opl_n,
                                                     sum(case
                                                             when u.cd in ('отоп/св.нор')/*u.usl_norm = 1 and s.kpr <> 0*/
                                                                 then s.test_opl
                                                             else 0 end) as opl_sv,
                                                     sum(case
                                                             when u.cd in ('отоп/0 зарег.')/*s.kpr = 0*/ then s.test_opl
                                                             else 0 end) as opl_empt
                                              from a_charge2 s,
                                                   usl u
                                              where mg_ between s.mgfrom and s.mgto
                                                and s.usl = u.usl
                                                and u.cd in ('отоп', 'отоп/св.нор', 'отоп/0 зарег.')
                                                and s.type = 1 --отопление (начисление чистое, объёмы)
                                              group by s.lsk, u.uslg) e,
                                             (select s.lsk, u.uslg, sum(s.summa) as chng
                                              from a_change s,
                                                   usl u
                                              where s.mg between mg_ and mg_
                                                and s.usl = u.usl
                                                and u.cd in ('отоп', 'отоп/св.нор', 'отоп/0 зарег.') --перерасчёты по отоплению
                                              group by s.lsk, u.uslg) d,
                                             (select s.lsk,
                                                     sum(case
                                                             when s.sch <> 0 and u.cd in ('г.вода')/*u.usl_norm = 0 and nvl(s.kpr,0) <> 0*/
                                                                 then s.summa
                                                             else 0 end) as summa_sch_n,
                                                     sum(case
                                                             when s.sch <> 0 and u.cd in ('г.вода/св.нор')/*u.usl_norm = 1 and nvl(s.kpr,0) <> 0*/
                                                                 then s.summa
                                                             else 0 end) as summa_sch_sv,
                                                     sum(case
                                                             when s.sch <> 0 and u.cd in ('г.вода/0 зарег.')/*nvl(s.kpr,0) = 0*/
                                                                 then s.summa
                                                             else 0 end) as summa_sch_empt,
                                                     sum(case
                                                             when s.sch <> 0 and u.cd in ('г.вода')/*u.usl_norm = 0 and nvl(s.kpr,0) <> 0*/
                                                                 then s.test_opl
                                                             else 0 end) as vol_sch_n,
                                                     sum(case
                                                             when s.sch <> 0 and u.cd in ('г.вода/св.нор')/*u.usl_norm = 1 and nvl(s.kpr,0) <> 0*/
                                                                 then s.test_opl
                                                             else 0 end) as vol_sch_sv,
                                                     sum(case
                                                             when s.sch <> 0 and u.cd in ('г.вода/0 зарег.')/*nvl(s.kpr,0) = 0*/
                                                                 then s.test_opl
                                                             else 0 end) as vol_sch_empt,
                                                     sum(case
                                                             when s.sch = 0 and u.cd in ('г.вода')/*u.usl_norm = 0*/
                                                                 then s.summa
                                                             else 0 end) as summa_norm_n,

                                                     sum(case
                                                             when s.sch = 0 and u.cd in ('г.вода')/*u.usl_norm = 0*/
                                                                 then s.test_opl
                                                             else 0 end) as vol_norm_n
                                              from a_charge2 s,
                                                   usl u
                                              where mg_ between s.mgfrom and s.mgto
                                                and s.usl = u.usl
                                                and u.cd in ('г.вода', 'г.вода/св.нор', 'г.вода/0 зарег.')
                                                and s.type = 1 --г.вода (начисление чистое, объёмы)
                                              group by s.lsk) gw,
                                             (select s.lsk,
                                                     sum(case when s.sch <> 0 then s.kpr else 0 end) as kpr_sch,
                                                     sum(case when s.sch = 0 then s.kpr else 0 end) as kpr_norm
                                              from a_charge_prep2 s,
                                                   usl u
                                              where mg_ between s.mgfrom and s.mgto
                                                and s.usl = u.usl
                                                and u.cd in ('г.вода')
                                                and s.tp = 1 --г.вода (кол-во прожив по сч/нормативу)
                                              group by s.lsk) gw3,
                                             (select s.lsk,
                                                     sum(s.summa) as summa,
                                                     sum(case
                                                             when s.sch <> 0 and u.usl_norm = 0 /*and s.kpr <> 0 вот так странно. почему то исключались пустые квартиры (обсуждал с Ларисой 27.05.2016*/
                                                                 then s.test_opl
                                                             else 0 end) as vol_sch,
                                                     sum(case when s.sch = 0 and u.usl_norm = 0 then s.test_opl else 0 end) as vol_norm
                                              from a_charge2 s,
                                                   usl u
                                              where mg_ between s.mgfrom and s.mgto
                                                and s.usl = u.usl
                                                and u.cd in ('г.вода.ОДН')
                                                and s.type = 1 --г.вода ОДН (начисление чистое, объёмы)
                                              group by s.lsk) gw2
                                        where s.lsk = e.lsk(+)
                                          and s.kul = l.id
                                          and s.lsk = gw.lsk(+)
                                          and s.lsk = gw2.lsk(+)
                                          and s.lsk = gw3.lsk(+)
                                          and s.lsk = d.lsk(+)
                                          and s.reu = c.reu
                                        order by l.name, s.nd, s.vvod_ot, s.kw;
else
                raise_application_error(-20000, 'Нет уровня детализации!');
end if;
elsif сd_ in ('85') then
            --Справка по начислению квартплаты по отоплению (для кис.)

            if var_ = 2 then
                --по УК
                open prep_refcursor for 'select
     c.name as name_uk,
     nvl(r.name2, r2.name2) as name_org,
     nvl(r.name, r2.name) as name_kot,

     sum(nvl(e.opl,0)+nvl(e2.opl,0)) as opl_itg,

     sum(e.opl) as opl,
     sum(e.opl_empt) as opl_empt,

     sum(e2.opl) as opl_sch,
     sum(e2.opl_empt) as opl_empt_sch,
     sum(e2.vol) as vol_sch,
     sum(e2.vol_empt) as vol_empt_sch,

     sum(nvl(e.summa,0)+nvl(e2.summa,0)+nvl(d.summa,0)+nvl(d2.summa,0)) as summa_itg,
     sum(nvl(e.summa,0)+nvl(d.summa,0)) as summa,
     sum(e.summa_empt) as summa_empt,
     sum(d.summa) as chng,
     sum(nvl(e2.summa,0)+nvl(d2.summa,0)) as summa_sch,
     sum(e2.summa_empt) as summa_sch_empt,
     sum(d.summa) as chng_sch,
     c.head_name,
     upper(utils.MONTH_NAME(substr(s.mg,5,2)))||'' ''||substr(s.mg,1,4)||''г.'' as mg_name
    from (select s.* from arch_kart s, s_reu_trest e where ' || sqlstr_ || ' and exists --ключевой запрос
      (select * from a_charge a, usl u where a.usl=u.usl and
        u.cd in (''отоп'', ''отоп/0 зарег.'') and a.type = 1 and a.summa <> 0
        and a.lsk=s.lsk and a.mg=s.mg
      )
     and e.reu=:reu
     and s.reu=e.reu
     and s.psch not in (8,9)
     and s.status not in (7)--убрал нежилые --
     ) s, t_org c, params p,
    (select s.lsk,
     sum(s.summa) as summa,
     sum(case when s.kpr = 0 then s.summa else 0 end) as summa_empt,
     sum(s.test_opl) as opl,
     sum(case when s.kpr = 0 then s.test_opl else 0 end) as opl_empt
       from a_charge s, usl u where ' || sqlstr_ || ' and s.usl=u.usl and
      u.cd in (''отоп'', ''отоп/0 зарег.'') and s.type=1 --отопление (начисление чистое, объёмы)
     group by s.lsk) e,
    (select s.lsk,
     sum(s.summa) as summa,
     sum(case when s.kpr = 0 then s.summa else 0 end) as summa_empt,
     sum(s.opl) as opl,
     sum(case when s.kpr = 0 then s.opl else 0 end) as opl_empt,
     sum(s.test_opl) as vol,
     sum(case when s.kpr = 0 then s.test_opl else 0 end) as vol_empt
       from a_charge s, usl u where ' || sqlstr_ || ' and s.usl=u.usl and
      u.cd in (''отоп.гкал.'', ''отоп.гкал./0 зарег.'') and s.type=1 --отопление гКал.(начисление чистое, объёмы)
     group by s.lsk) e2,
      (select s.lsk, sum(s.summa) as summa from a_change s, usl u where ' || sqlstr_ || ' and s.usl=u.usl and
       u.cd in (''отоп'', ''отоп/0 зарег.'') --перерасчёты по отоплению
       group by s.lsk) d,
      (select s.lsk, sum(s.summa) as summa from a_change s, usl u where ' || sqlstr_ || ' and s.usl=u.usl and
       u.cd in (''отоп.гкал.'', ''отоп.гкал./0 зарег.'') --перерасчёты по отоплению (гКал)
       group by s.lsk) d2,
       (select n.lsk, o.name, o2.name as name2 from nabor n,
         t_org o, t_org o2, usl u where n.org=o.id and n.usl=u.usl and
         init.get_date() between n.dt1 and n.dt2
         and u.cd in (''отоп'') --организации, котельные (здесь по одной услуге иначе - удвоится)
         and n.org=o.id and o.fk_org2=o2.id(+)) r,
       (select n.lsk, o.name, o2.name as name2 from nabor n,
         t_org o, t_org o2, usl u where n.org=o.id and n.usl=u.usl and
         init.get_date() between n.dt1 and n.dt2
         and u.cd in (''отоп.гкал.'') --организации, котельные (здесь по одной услуге иначе - удвоится)
         and n.org=o.id and o.fk_org2=o2.id(+)) r2
    where s.lsk = e.lsk(+)
     and s.lsk = e2.lsk(+)
     and s.lsk = d.lsk(+)
     and s.lsk = d2.lsk(+)
     and s.lsk = r.lsk(+)
     and s.lsk = r2.lsk(+)
     and s.reu=c.reu
     group by
     c.name, nvl(r.name2, r2.name2), nvl(r.name, r2.name),
     c.head_name,
     upper(utils.MONTH_NAME(substr(s.mg,5,2)))||'' ''||substr(s.mg,1,4)||''г.''
     order by
     c.name, nvl(r.name2, r2.name2), nvl(r.name, r2.name)
    ' using reu_;
end if;

elsif сd_ in ('86') then
            if var_ = 2 then
                --по УК
                open prep_refcursor for 'select
     c.name as name_uk,
     nvl(r.name2, r2.name2) as name_org,
     nvl(r.name, r2.name) as name_kot,

     sum(nvl(s.opl,0)) as opl_itg,
     sum(case when s.status not in (7) then s.opl else 0 end) as opl,
     sum(case when s.status in (7) then s.opl else 0 end) as opl_ur,

     sum(e.vol) as vol_itg,
     sum(case when f.kub is null then e.vol end) as vol_nrm,
     sum(case when f.kub is null then e.vol_wo_odn end) as vol_wo_odn_nrm,
     sum(case when f.kub is null then e.vol_odn end) as vol_odn_nrm,
     sum(case when f.kub is null then e.vol_empt end) as vol_empt_nrm,
     sum(case when f.kub is null then e.vol_odn_empt end) as vol_odn_empt_nrm,

     sum(case when f.kub is not null then e.vol end) as vol_odpu,
     sum(case when f.kub is not null then e.vol_wo_odn end) as vol_wo_odn_odpu,
     sum(case when f.kub is not null then e.vol_odn end) as vol_odn_odpu,
     sum(case when f.kub is not null then e.vol_empt end) as vol_empt_odpu,
     sum(case when f.kub is not null then e.vol_odn_empt end) as vol_odn_empt_odpu,

     sum(e.summa) as summa_itg,
     sum(case when f.kub is null then e.summa end) as summa_nrm,
     sum(case when f.kub is null then e.summa_wo_odn end) as summa_wo_odn_nrm,
     sum(case when f.kub is null then e.summa_odn end) as summa_odn_nrm,
     sum(case when f.kub is null then e.summa_empt end) as summa_empt_nrm,
     sum(case when f.kub is null then e.summa_odn_empt end) as summa_odn_empt_nrm,
     sum(case when f.kub is null then e.summa_chng end) as summa_chng_nrm,
     sum(case when f.kub is null then e.summa_chng_empt end) as summa_chng_empt_nrm,

     sum(case when f.kub is not null then e.summa end) as summa_odpu,
     sum(case when f.kub is not null then e.summa_wo_odn end) as summa_wo_odn_odpu,
     sum(case when f.kub is not null then e.summa_odn end) as summa_odn_odpu,
     sum(case when f.kub is not null then e.summa_empt end) as summa_empt_odpu,
     sum(case when f.kub is not null then e.summa_odn_empt end) as summa_odn_empt_odpu,
     sum(case when f.kub is not null then e.summa_chng end) as summa_chng_odpu,
     sum(case when f.kub is not null then e.summa_chng_empt end) as summa_chng_empt_odpu,
     c.head_name,
     upper(utils.MONTH_NAME(substr(s.mg,5,2)))||'' ''||substr(s.mg,1,4)||''г.'' as mg_name
    from (select s.* from arch_kart s, s_reu_trest e where ' || sqlstr_ || ' and exists --ключевой запрос
      (select * from a_charge a, usl u where a.usl=u.usl and
        u.cd in (''г.вода'', ''г.вода/0 зарег.'') and a.type = 1 and a.summa <> 0
        and a.lsk=s.lsk and a.mg=s.mg
      )
     and e.reu=:reu
     and s.reu=e.reu
     and s.psch not in (8,9)
     and s.status not in (7)--убрал нежилые
     ) s, t_org c, params p,
    (select a.lsk,
     sum(a.test_opl) as vol, --объем всего
     sum(case when a.cd in (''г.вода'', ''г.вода/0 зарег.'') then a.test_opl else 0 end) as vol_wo_odn, --объем общий без ОДН
     sum(case when a.cd in (''г.вода.ОДН'', ''Г.в. ОДН, 0 зарег'') then a.test_opl else 0 end) as vol_odn, --объем ОДН
     sum(case when a.cd in (''г.вода/0 зарег.'') then a.test_opl else 0 end) as vol_empt, --объем по пустым кв.
     sum(case when a.cd in (''Г.в. ОДН, 0 зарег'') then a.test_opl else 0 end) as vol_odn_empt, --объем ОДН по пустым кв.
     sum(a.summa) as summa, --начисление всего
     sum(case when a.cd in (''г.вода'', ''г.вода/0 зарег.'') then a.summa else 0 end) as summa_wo_odn, --начисление общее без ОДН
     sum(case when a.cd in (''г.вода.ОДН'', ''Г.в. ОДН, 0 зарег'') then a.summa else 0 end) as summa_odn, --начисление ОДН
     sum(case when a.cd in (''г.вода/0 зарег.'') then a.summa else 0 end) as summa_empt, --начисление по пустым кв.
     sum(case when a.cd in (''Г.в. ОДН, 0 зарег'') then a.summa else 0 end) as summa_odn_empt, --начисление ОДН по пустым кв.
     sum(a.summa_chng) as summa_chng,
     sum(a.summa_chng_empt) as summa_chng_empt
       from
       (select s.lsk, s.summa, null as summa_chng, null as summa_chng_empt, s.test_opl, u.cd
         from a_charge s, usl u where ' || sqlstr_ || ' and s.usl=u.usl and
         u.cd in (''г.вода'', ''г.вода/0 зарег.'', ''г.вода.ОДН'', ''Г.в. ОДН, 0 зарег'') and s.type=1 --г.в.
        union all
        select s.lsk, s.summa, s.summa as summa_chng,
          case when u.cd in (''г.вода/0 зарег.'') then s.summa else 0 end as summa_chng_empt, null as test_opl, u.cd
         from a_change s, usl u where ' || sqlstr_ || ' and s.usl=u.usl and
         u.cd in (''г.вода'', ''г.вода/0 зарег.'', ''г.вода.ОДН'', ''Г.в. ОДН, 0 зарег'')) a--г.в. в т.ч. перерасчеты
     group by a.lsk
     ) e,
     (select n.lsk, s.kub from a_nabor n, a_vvod s, usl u where ' || sqlstr_ || ' and s.usl=u.usl and
         u.cd in (''г.вода'')
         and n.mg=s.mg
         and n.usl=s.usl
         and n.fk_vvod=s.id
         and to_date(s.mg||''01'',''YYYYMMDD'') between n.dt1 and n.dt2
         ) f,
      (select s.lsk, sum(s.summa) as summa from a_change s, usl u where ' || sqlstr_ || ' and s.usl=u.usl and
       u.cd in (''отоп'', ''отоп/0 зарег.'') --перерасчёты по отоплению
       group by s.lsk) d,
      (select s.lsk, sum(s.summa) as summa from a_change s, usl u where ' || sqlstr_ || ' and s.usl=u.usl and
       u.cd in (''отоп.гкал.'', ''отоп.гкал./0 зарег.'') --перерасчёты по отоплению (гКал)
       group by s.lsk) d2,
       (select n.lsk, o.name, o2.name as name2 from nabor n,
         t_org o, t_org o2, usl u where n.org=o.id and n.usl=u.usl
         and init.get_date() between n.dt1 and n.dt2
         and u.cd in (''отоп'') --организации, котельные (здесь по одной услуге иначе - удвоится)
         and n.org=o.id and o.fk_org2=o2.id(+)) r,
       (select n.lsk, o.name, o2.name as name2 from nabor n,
         t_org o, t_org o2, usl u where n.org=o.id and n.usl=u.usl
         and init.get_date() between n.dt1 and n.dt2
         and u.cd in (''отоп.гкал.'') --организации, котельные (здесь по одной услуге иначе - удвоится)
         and n.org=o.id and o.fk_org2=o2.id(+)) r2
    where s.lsk = e.lsk(+)
     and s.lsk = f.lsk(+)
     and s.lsk = d.lsk(+)
     and s.lsk = d2.lsk(+)
     and s.lsk = r.lsk(+)
     and s.lsk = r2.lsk(+)
     and s.reu=c.reu
     group by
     c.name, nvl(r.name2, r2.name2), nvl(r.name, r2.name),
     c.head_name,
     upper(utils.MONTH_NAME(substr(s.mg,5,2)))||'' ''||substr(s.mg,1,4)||''г.''
     order by
     c.name, nvl(r.name2, r2.name2), nvl(r.name, r2.name)' using reu_;
end if;

elsif сd_ in ('88') then
            --Реестр для фонда Капремонта

            if var_ = 0 then
                --по Городу
                if utils.get_int_param('CAP_VAR_REP1') = 0 then
                    -- версия для остальных (Полыс)
                    open prep_refcursor for select *
                                            from (select rownum as rn1, xx1.*
                                                  from (select /*+ USE_HASH(k, o2, tp, o, s, d, tp2, t, p1, p5, sl, sp, g)*/
                                                            scott.utils.month_name(substr(mg_, 5, 2)) as mon,
                                                            substr(mg_, 1, 4) as year,
                                                            k.lsk as ls,
                                                            o.name as np,
                                                            s.name as ul,
                                                            ltrim(k.nd, '0') as dom,
                                                            ltrim(k.kw, '0') as kv,
                                                            d.name_kp as st,
                                                            d.tp as naz,
                                                            k.k_fam as sur,
                                                            k.k_im as nam,
                                                            k.k_ot as mid,
                                                            -- сведения о льготах
                                                            g.name as lg,
                                                            -- площадь
                                                            k.opl as pl,
                                                            -- сальдо входящее
                                                            nvl(t.indebet, 0) + nvl(t.inkredit, 0) as sn,
                                                            -- начислено
                                                            nvl(t.charges, 0) as bil,
                                                            -- начисленная пеня (текущая)
                                                            nvl(t.pcur, 0) as pcur,
                                                            -- вх. сальдо по пене
                                                            nvl(t.pinsal, 0) as pinsal,
                                                            -- исх сальдо по пене
                                                            nvl(t.poutsal, 0) as poutsal,
                                                            -- оплачено
                                                            nvl(t.payment, 0) as pay,
                                                            -- оплачено пени
                                                            nvl(t.pn, 0) as penpay,
                                                            -- сальдо исходящее
                                                            nvl(t.outdebet, 0) + nvl(t.outkredit, 0) as sk,
                                                            -- перерасчет
                                                            nvl(t.changes, 0) as corr
                                                        from scott.arch_kart k
                                                                 join scott.t_org o2 on o2.cd = 'Фонд Капремонта МКД'
                                                                 join scott.t_org_tp tp on tp.cd = 'Город'
                                                                 join scott.t_org o on o.fk_orgtp = tp.id
                                                                 join scott.spul s on k.kul = s.id
                                                                 join scott.status d on k.status = d.id
                                                                 join v_lsk_tp tp2 on k.fk_tp = tp2.id and tp2.cd = 'LSK_TP_ADDIT'
                                                                 left join (select t.lsk,
                                                                                   t.org,
                                                                                   sum(t.charges) as charges,
                                                                                   sum(t.changes) as changes,
                                                                                   sum(t.pinsal) as pinsal,
                                                                                   sum(t.pcur) as pcur,
                                                                                   sum(t.poutsal) as poutsal,
                                                                                   sum(t.indebet) as indebet,
                                                                                   sum(t.inkredit) as inkredit,
                                                                                   sum(t.outdebet) as outdebet,
                                                                                   sum(t.outkredit) as outkredit,
                                                                                   sum(t.payment) as payment,
                                                                                   sum(t.pn) as pn
                                                                            from scott.xitog3_lsk t
                                                                                     join usl us2
                                                                                          on us2.cd in ('кап.', 'кап/св.нор') and t.usl = us2.usl and t.mg = mg_
                                                                            group by t.lsk, t.org) t
                                                                           on k.lsk = t.lsk and t.org = o2.id
                                                                 left join (select p.lsk, max(p.fk_spk) as fk_spk
                                                                            from scott.a_charge_prep2 p
                                                                            where mg_ between p.mgfrom and p.mgto
                                                                              and p.tp = 9
                                                                            group by p.lsk) sl on k.lsk = sl.lsk
                                                                 left join scott.spk sp on sl.fk_spk = sp.id
                                                                 left join scott.spk_gr g on sp.gr_id = g.id
                                                        where k.mg = mg_
                                                          and k.lsk not in ('04001692')             -- просит Полыс. не выгружать л.с. ред.21.05.20
                                                          and (k.house_id in (39986, 39966) and k.psch not in (8,
                                                                                                               9) or -- кроме двух домов выбранных Полыс ред.25.09.20
                                                               k.house_id not in (39986, 39966) or k.lsk in
                                                                                                   ('06000671') --просит Полыс выгружать ред. 02.02.21
                                                                   and (nvl(t.indebet, 0) <> 0 or
                                                                        nvl(t.inkredit, 0) <> 0 or
                                                                        nvl(t.outdebet, 0) <> 0 or
                                                                        nvl(t.outkredit, 0) <> 0 or
                                                                        nvl(t.pinsal, 0) <> 0 or
                                                                        nvl(t.poutsal, 0) <> 0 or
                                                                        k.psch not in (8, 9) or k.status =
                                                                                                1)) -- открытые или с наличием сальдо по кап. или муницип квартиры ред.25.09.20 по просьбе Полыс
                                                        order by scott.utils.f_ord_digit(k.nd),
                                                                 scott.utils.f_ord3(k.nd),
                                                                 scott.utils.f_ord_digit(k.kw),
                                                                 scott.utils.f_ord3(k.kw)) xx1) xx2
                                            where rn1 between 0 and 2000000; --сделал ограничение до 2 млн, чтоб можно было если что постранично выгружать
else
                    -- версия для Кис
                    open prep_refcursor for select *
                                            from (select rownum as rn1, xx1.*
                                                  from (select scott.utils.month_name(substr(mg_, 5, 2)) as mon,
                                                               substr(mg_, 1, 4) as year,
                                                               k.lsk as ls,
                                                               o.name as np,
                                                               s.name as ul,
                                                               ltrim(k.nd, '0') as dom,
                                                               ltrim(k.kw, '0') as kv,
                                                               d.name_kp as st,
                                                               d.tp as naz,
                                                               k.k_fam as sur,
                                                               k.k_im as nam,
                                                               k.k_ot as mid,
                                                               --сведения о льготах
                                                               g.name as lg,
                                                               --площадь
                                                               k.opl as pl,
                                                               --сальдо начальное
                                                               nvl(t.indebet, 0) + nvl(t.inkredit, 0) +
                                                               nvl(decode(tp2.cd, 'LSK_TP_ADDIT', p1.penya_in, 0), 0) as sn,
                                                               --проценты в сальдо начисленной пени
                                                               nvl(decode(tp2.cd, 'LSK_TP_ADDIT', p1.penya_in, 0), 0) as pensn,
                                                               --начислено, в т.ч. пеня
                                                               nvl(t.charges, 0) + nvl(t.pcur, 0) as bil,
                                                               --начисленная пеня
                                                               --nvl(t.poutsal,0) as penbil,
                                                               nvl(t.pcur, 0) as penbil, --ред.15.04.20 Кис: просили заменить на текущую пеню
                                                               --платеж, в т.ч. пеня
                                                               nvl(t.payment, 0) + nvl(t.pn, 0) as pay,
                                                               --оплаченная пеня
                                                               t.pn as penpay,
                                                               --сальдо конечное
                                                               nvl(t.outdebet, 0) + nvl(t.outkredit, 0) +
                                                               nvl(decode(tp2.cd, 'LSK_TP_ADDIT', p5.penya_out, 0), 0) as sk,
                                                               --проценты в сальдо уплаченной пени
                                                               nvl(decode(tp2.cd, 'LSK_TP_ADDIT', p5.penya_out, 0), 0) as pensk,
                                                               --перерасчет
                                                               nvl(t.changes, 0) as corr,
                                                               a.test_cena as tariff     --ред.17.04.20 (тариф)
                                                        from scott.arch_kart k
                                                                 join scott.t_org o2 on o2.cd = 'Фонд Капремонта МКД'
                                                                 join scott.t_org_tp tp on tp.cd = 'Город'
                                                                 join scott.t_org o on o.fk_orgtp = tp.id
                                                                 join scott.spul s on k.kul = s.id
                                                                 join scott.status d on k.status = d.id
                                                                 join v_lsk_tp tp2 on k.fk_tp = tp2.id
                                                                 join usl u on u.cd in ('кап.')
                                                                 left join (select t.lsk,
                                                                                   t.org,
                                                                                   sum(t.charges) as charges,
                                                                                   nvl(sum(t.changes), 0) +
                                                                                   nvl(sum(t.changes2), 0) +
                                                                                   nvl(sum(t.changes3), 0) as changes, --ред.15.04.20 Кис: добавил changes2, changes3 согласно ТЗ
                                                                                   sum(t.poutsal) as poutsal,
                                                                                   sum(t.indebet) as indebet,
                                                                                   sum(t.inkredit) as inkredit,
                                                                                   sum(t.outdebet) as outdebet,
                                                                                   sum(t.outkredit) as outkredit,
                                                                                   sum(t.payment) as payment,
                                                                                   sum(t.pn) as pn,
                                                                                   sum(t.pcur) as pcur
                                                                            from scott.xitog3_lsk t
                                                                                     join usl us2
                                                                                          on us2.cd in ('кап.', 'кап/св.нор') and t.usl = us2.usl and t.mg = mg_
                                                                            group by t.lsk, t.org) t
                                                                           on k.lsk = t.lsk and t.org = o2.id
                                                                 left join (select l.lsk, sum(l.penya) as penya_in --сальдо по пене входящее
                                                                            from scott.a_penya l
                                                                            where l.mg = scott.utils.add_months_pr(mg_, -1)
                                                                            group by l.lsk) p1 on k.lsk = p1.lsk
                                                                 left join (select l.lsk, sum(l.penya) as penya_out --сальдо по пене исходящее
                                                                            from scott.a_penya l
                                                                            where l.mg = mg_
                                                                            group by l.lsk) p5 on k.lsk = p5.lsk
                                                                 left join a_charge2 a on a.lsk = k.lsk and
                                                                                          mg_ between a.mgfrom and a.mgto --ред.17.04.20 (тариф)
                                                            and a.type = 1 and a.usl = u.usl
                                                                 left join (select p.lsk, max(p.fk_spk) as fk_spk
                                                                            from scott.a_charge_prep2 p
                                                                            where mg_ between p.mgfrom and p.mgto
                                                                              and p.tp = 9
                                                                            group by p.lsk) sl on k.lsk = sl.lsk
                                                                 left join scott.spk sp on sl.fk_spk = sp.id
                                                                 left join scott.spk_gr g on sp.gr_id = g.id
                                                        where k.mg = mg_
                                                          and (k.status not in
                                                               (9) --k.status not in (1,9) ред.21.04.20 согласно ТЗ
                                                                   and k.psch not in (8, 9) and
                                                               tp2.cd = 'LSK_TP_ADDIT' or
                                                               (nvl(decode(tp2.cd, 'LSK_TP_ADDIT', p1.penya_in, 0), 0) <>
                                                                0 or nvl(t.charges, 0) + nvl(t.poutsal, 0) <> 0 or
                                                                nvl(t.poutsal, 0) <> 0 or
                                                                nvl(t.payment, 0) + nvl(t.pn, 0) <> 0 or t.pn <> 0 or
                                                                nvl(t.outdebet, 0) + nvl(t.outkredit, 0) +
                                                                nvl(decode(tp2.cd, 'LSK_TP_ADDIT', p5.penya_out, 0), 0) <>
                                                                0 or
                                                                nvl(decode(tp2.cd, 'LSK_TP_ADDIT', p5.penya_out, 0), 0) <>
                                                                0 or nvl(t.changes, 0) <> 0))
                                                        order by scott.utils.f_ord_digit(k.nd),
                                                                 scott.utils.f_ord3(k.nd),
                                                                 scott.utils.f_ord_digit(k.kw),
                                                                 scott.utils.f_ord3(k.kw)) xx1) xx2
                                            where rn1 between 0 and 200000000;
end if;
end if;

elsif сd_ in ('89') then
            if var_ = 3 then
                --по Дому
                open prep_refcursor for select d.npp,
                                               h.name_reu || ' ' || l.name || h.name || ', ' || ltrim(h.nd, '0') as predpr,
                                               null as type,
                                               to_char(d.id) || ' ' || d.name as org,
                                               to_char(h.uslm) || ' ' || decode(m.frc_get_price, 1, m.nm, m.nm1) as nm1,
                                               sum(i.indebet) as indebet,
                                               sum(i.inkredit) as inkredit,
                                               sum(o.charges) as charges,
                                               sum(o.changes) as changes,
                                               sum(o.subsid) as subsid,
                                               sum(o.privs) as privs,
                                               sum(o.privs_city) as privs_city,
                                               sum(o.payment) as payment,
                                               sum(o.ch_full) as ch_full,
                                               sum(o.changes2) as changes2,
                                               sum(nvl(o.changes, 0) + nvl(o.changes2, 0)) as changesall,
                                               sum(o.pn) as pn,
                                               sum(u.outdebet) as outdebet,
                                               sum(u.outkredit) as outkredit
                                        from (select u.uslm,
                                                     e.reu,
                                                     e.kul,
                                                     e.nd,
                                                     e.status,
                                                     e.org,
                                                     e.usl,
                                                     trim(t.name_reu) as name_reu,
                                                     ot.name_short || '.' || initcap(o.name) || ', ' || initcap(s.name) as name,
                                                     e.fk_lsk_tp
                                              from t_saldo_reu_kul_nd_st e,
                                                   s_reu_trest t,
                                                   spul s,
                                                   usl u,
                                                   t_org o,
                                                   t_org_tp ot
                                              where e.usl = u.usl
                                                and e.reu = t.reu
                                                and e.reu = reu_
                                                and e.kul = kul_
                                                and e.nd = nd_
                                                and s.fk_settlement = o.id
                                                and o.fk_orgtp = ot.id
                                                and exists(select *
                                                           from list_c i,
                                                                spr_params p
                                                           where i.fk_ses = fk_ses_
                                                             and p.id = i.fk_par
                                                             and p.cd = 'REP_USL2'
                                                             and i.sel_cd = e.usl
                                                             and i.sel = 1)
                                                and exists(select *
                                                           from list_c i,
                                                                spr_params p
                                                           where i.fk_ses = fk_ses_
                                                             and p.id = i.fk_par
                                                             and p.cd = 'REP_ORG2'
                                                             and i.sel_id = e.org
                                                             and i.sel = 1)
                                                and e.kul = s.id) h,
                                             (select * from xitog3 e where e.mg = mg_) i,
                                             (select reu,
                                                     kul,
                                                     nd,
                                                     org,
                                                     usl,
                                                     status,
                                                     fk_lsk_tp,
                                                     sum(charges) as charges,
                                                     sum(changes) as changes,
                                                     sum(subsid) as subsid,
                                                     sum(privs) as privs,
                                                     sum(privs_city) as privs_city,
                                                     sum(ch_full) as ch_full,
                                                     sum(changes2) as changes2,
                                                     sum(payment) as payment,
                                                     sum(pn) as pn
                                              from xitog3 t
                                              where t.mg between mg_ and mg1_
                                              group by reu, kul, nd, org, usl, status, fk_lsk_tp) o,
                                             (select * from xitog3 e where e.mg = mg1_) u,
                                             t_org d,
                                             usl m,
                                             org l
                                        where h.reu = i.reu(+)
                                          and h.kul = i.kul(+)
                                          and h.nd = i.nd(+)
                                          and h.org = i.org(+)
                                          and h.usl = i.usl(+)
                                          and h.status = i.status(+)
                                          and h.fk_lsk_tp = i.fk_lsk_tp(+)
                                          and h.reu = o.reu(+)
                                          and h.kul = o.kul(+)
                                          and h.nd = o.nd(+)
                                          and h.org = o.org(+)
                                          and h.usl = o.usl(+)
                                          and h.status = o.status(+)
                                          and h.fk_lsk_tp = o.fk_lsk_tp(+)
                                          and h.reu = u.reu(+)
                                          and h.kul = u.kul(+)
                                          and h.nd = u.nd(+)
                                          and h.org = u.org(+)
                                          and h.usl = u.usl(+)
                                          and h.status = u.status(+)
                                          and h.fk_lsk_tp = u.fk_lsk_tp(+)
                                          and l.id = 4
                                          and h.org = d.id
                                          and h.usl = m.usl
                                        group by d.npp,
                                                 h.name_reu || ' ' || l.name || h.name || ', ' || ltrim(h.nd, '0'),
                                                 to_char(d.id) || ' ' || d.name,
                                                 to_char(h.uslm) || ' ' || decode(m.frc_get_price, 1, m.nm, m.nm1)
                                        having sum(i.indebet) <> 0
                                            or sum(i.inkredit) <> 0
                                            or sum(o.charges) <> 0
                                            or sum(o.changes) <> 0
                                            or sum(o.subsid) <> 0
                                            or sum(o.privs) <> 0
                                            or sum(o.privs_city) <> 0
                                            or sum(o.payment) <> 0
                                            or sum(o.pn) <> 0
                                            or sum(u.outdebet) <> 0
                                            or sum(u.outkredit) <> 0
                                        order by h.name_reu || ' ' || l.name || h.name || ', ' || ltrim(h.nd, '0'),
                                                 d.npp;

elsif var_ = 2 then
                --по РЭУ
                open prep_refcursor for select d.npp,
                                               l.name || h.name_reu as predpr,
                                               null as type,
                                               to_char(d.id) || ' ' || d.name as org,
                                               to_char(h.uslm) || ' ' || decode(m.frc_get_price, 1, m.nm, m.nm1) as nm1,
                                               sum(i.indebet) as indebet,
                                               sum(i.inkredit) as inkredit,
                                               sum(o.charges) as charges,
                                               sum(o.changes) as changes,
                                               sum(o.subsid) as subsid,
                                               sum(o.privs) as privs,
                                               sum(o.privs_city) as privs_city,
                                               sum(o.payment) as payment,
                                               sum(o.ch_full) as ch_full,
                                               sum(o.changes2) as changes2,
                                               sum(nvl(o.changes, 0) + nvl(o.changes2, 0)) as changesall,
                                               sum(o.pn) as pn,
                                               sum(u.outdebet) as outdebet,
                                               sum(u.outkredit) as outkredit
                                        from (select u.uslm,
                                                     e.reu,
                                                     e.kul,
                                                     e.nd,
                                                     e.status,
                                                     e.org,
                                                     e.usl,
                                                     s.name_reu,
                                                     e.fk_lsk_tp
                                              from t_saldo_reu_kul_nd_st e,
                                                   s_reu_trest s,
                                                   usl u
                                              where e.usl = u.usl
                                                and e.reu = reu_
                                                and e.reu = s.reu
                                                and exists(select *
                                                           from list_c i,
                                                                spr_params p
                                                           where i.fk_ses = fk_ses_
                                                             and p.id = i.fk_par
                                                             and p.cd = 'REP_USL2'
                                                             and i.sel_cd = e.usl
                                                             and i.sel = 1)
                                                and exists(select *
                                                           from list_c i,
                                                                spr_params p
                                                           where i.fk_ses = fk_ses_
                                                             and p.id = i.fk_par
                                                             and p.cd = 'REP_ORG2'
                                                             and i.sel_id = e.org
                                                             and i.sel = 1)) h,
                                             (select * from xitog3 e where e.mg = mg_) i,
                                             (select reu,
                                                     kul,
                                                     nd,
                                                     org,
                                                     usl,
                                                     status,
                                                     fk_lsk_tp,
                                                     sum(charges) as charges,
                                                     sum(changes) as changes,
                                                     sum(subsid) as subsid,
                                                     sum(privs) as privs,
                                                     sum(privs_city) as privs_city,
                                                     sum(ch_full) as ch_full,
                                                     sum(changes2) as changes2,
                                                     sum(nvl(t.changes, 0) + nvl(changes2, 0)) as changesall,
                                                     sum(payment) as payment,
                                                     sum(pn) as pn
                                              from xitog3 t
                                              where t.mg between mg_ and mg1_
                                              group by reu, kul, nd, org, usl, status, fk_lsk_tp) o,
                                             (select * from xitog3 e where e.mg = mg1_) u,
                                             t_org d,
                                             usl m,
                                             org l
                                        where h.reu = i.reu(+)
                                          and h.kul = i.kul(+)
                                          and h.nd = i.nd(+)
                                          and h.org = i.org(+)
                                          and h.usl = i.usl(+)
                                          and h.status = i.status(+)
                                          and h.fk_lsk_tp = i.fk_lsk_tp(+)
                                          and h.reu = o.reu(+)
                                          and h.kul = o.kul(+)
                                          and h.nd = o.nd(+)
                                          and h.org = o.org(+)
                                          and h.usl = o.usl(+)
                                          and h.status = o.status(+)
                                          and h.fk_lsk_tp = o.fk_lsk_tp(+)
                                          and h.reu = u.reu(+)
                                          and h.kul = u.kul(+)
                                          and h.nd = u.nd(+)
                                          and h.org = u.org(+)
                                          and h.usl = u.usl(+)
                                          and h.status = u.status(+)
                                          and h.fk_lsk_tp = u.fk_lsk_tp(+)
                                          and l.id = 3
                                          and h.org = d.id
                                          and h.usl = m.usl
                                        group by d.npp, l.name || h.name_reu, to_char(d.id) || ' ' || d.name,
                                                 to_char(h.uslm) || ' ' || decode(m.frc_get_price, 1, m.nm, m.nm1)
                                        having sum(i.indebet) <> 0
                                            or sum(i.inkredit) <> 0
                                            or sum(o.charges) <> 0
                                            or sum(o.changes) <> 0
                                            or sum(o.subsid) <> 0
                                            or sum(o.privs) <> 0
                                            or sum(o.privs_city) <> 0
                                            or sum(o.payment) <> 0
                                            or sum(o.pn) <> 0
                                            or sum(u.outdebet) <> 0
                                            or sum(u.outkredit) <> 0
                                        order by d.npp;
elsif var_ = 1 then
                --по ЖЭО
                open prep_refcursor for select d.npp,
                                               l.name || h.name_tr as predpr,
                                               null as type,
                                               to_char(d.id) || ' ' || d.name as org,
                                               to_char(h.uslm) || ' ' || decode(m.frc_get_price, 1, m.nm, m.nm1) as nm1,
                                               sum(i.indebet) as indebet,
                                               sum(i.inkredit) as inkredit,
                                               sum(o.charges) as charges,
                                               sum(o.changes) as changes,
                                               sum(o.subsid) as subsid,
                                               sum(o.privs) as privs,
                                               sum(o.privs_city) as privs_city,
                                               sum(o.payment) as payment,
                                               sum(o.ch_full) as ch_full,
                                               sum(o.changes2) as changes2,
                                               sum(nvl(o.changes, 0) + nvl(o.changes2, 0)) as changesall,
                                               sum(o.pn) as pn,
                                               sum(u.outdebet) as outdebet,
                                               sum(u.outkredit) as outkredit
                                        from (select u.uslm,
                                                     e.reu,
                                                     e.kul,
                                                     e.nd,
                                                     e.status,
                                                     e.org,
                                                     e.usl,
                                                     s.trest,
                                                     s.name_tr,
                                                     e.fk_lsk_tp
                                              from t_saldo_reu_kul_nd_st e,
                                                   s_reu_trest s,
                                                   usl u
                                              where e.usl = u.usl
                                                and s.trest = trest_
                                                and e.reu = s.reu
                                                and exists(select *
                                                           from list_c i,
                                                                spr_params p
                                                           where i.fk_ses = fk_ses_
                                                             and p.id = i.fk_par
                                                             and p.cd = 'REP_USL2'
                                                             and i.sel_cd = e.usl
                                                             and i.sel = 1)
                                                and exists(select *
                                                           from list_c i,
                                                                spr_params p
                                                           where i.fk_ses = fk_ses_
                                                             and p.id = i.fk_par
                                                             and p.cd = 'REP_ORG2'
                                                             and i.sel_id = e.org
                                                             and i.sel = 1)) h,
                                             (select * from xitog3 e where e.mg = mg_) i,
                                             (select reu,
                                                     kul,
                                                     nd,
                                                     org,
                                                     usl,
                                                     status,
                                                     fk_lsk_tp,
                                                     sum(charges) as charges,
                                                     sum(changes) as changes,
                                                     sum(subsid) as subsid,
                                                     sum(privs) as privs,
                                                     sum(privs_city) as privs_city,
                                                     sum(ch_full) as ch_full,
                                                     sum(changes2) as changes2,
                                                     sum(payment) as payment,
                                                     sum(pn) as pn
                                              from xitog3 t
                                              where t.mg between mg_ and mg1_
                                              group by reu, kul, nd, org, usl, status, fk_lsk_tp) o,
                                             (select * from xitog3 e where e.mg = mg1_) u,
                                             t_org d,
                                             usl m,
                                             org l
                                        where h.reu = i.reu(+)
                                          and h.kul = i.kul(+)
                                          and h.nd = i.nd(+)
                                          and h.org = i.org(+)
                                          and h.usl = i.usl(+)
                                          and h.status = i.status(+)
                                          and h.fk_lsk_tp = i.fk_lsk_tp(+)
                                          and h.reu = o.reu(+)
                                          and h.kul = o.kul(+)
                                          and h.nd = o.nd(+)
                                          and h.org = o.org(+)
                                          and h.usl = o.usl(+)
                                          and h.status = o.status(+)
                                          and h.fk_lsk_tp = o.fk_lsk_tp(+)
                                          and h.reu = u.reu(+)
                                          and h.kul = u.kul(+)
                                          and h.nd = u.nd(+)
                                          and h.org = u.org(+)
                                          and h.usl = u.usl(+)
                                          and h.status = u.status(+)
                                          and h.fk_lsk_tp = u.fk_lsk_tp(+)
                                          and l.id = 2
                                          and h.org = d.id
                                          and h.usl = m.usl
                                        group by d.npp, l.name || h.name_tr, to_char(d.id) || ' ' || d.name,
                                                 to_char(h.uslm) || ' ' || decode(m.frc_get_price, 1, m.nm, m.nm1)
                                        having sum(i.indebet) <> 0
                                            or sum(i.inkredit) <> 0
                                            or sum(o.charges) <> 0
                                            or sum(o.changes) <> 0
                                            or sum(o.subsid) <> 0
                                            or sum(o.privs) <> 0
                                            or sum(o.privs_city) <> 0
                                            or sum(o.payment) <> 0
                                            or sum(o.pn) <> 0
                                            or sum(u.outdebet) <> 0
                                            or sum(u.outkredit) <> 0
                                        order by d.npp;
elsif var_ = 0 then
                --по Городу
                open prep_refcursor for select d.npp,
                                               l.name as predpr,
                                               null as type,
                                               to_char(d.id) || ' ' || d.name as org,
                                               to_char(h.uslm) || ' ' || decode(m.frc_get_price, 1, m.nm, m.nm1) as nm1,
                                               sum(i.indebet) as indebet,
                                               sum(i.inkredit) as inkredit,
                                               sum(o.charges) as charges,
                                               sum(o.changes) as changes,
                                               sum(o.subsid) as subsid,
                                               sum(o.privs) as privs,
                                               sum(o.privs_city) as privs_city,
                                               sum(o.payment) as payment,
                                               sum(o.ch_full) as ch_full,
                                               sum(o.changes2) as changes2,
                                               sum(nvl(o.changes, 0) + nvl(o.changes2, 0)) as changesall,
                                               sum(o.pn) as pn,
                                               sum(u.outdebet) as outdebet,
                                               sum(u.outkredit) as outkredit
                                        from (select u.uslm,
                                                     e.reu,
                                                     e.kul,
                                                     e.nd,
                                                     e.status,
                                                     e.org,
                                                     e.usl,
                                                     s.trest,
                                                     e.fk_lsk_tp
                                              from t_saldo_reu_kul_nd_st e,
                                                   s_reu_trest s,
                                                   usl u
                                              where e.usl = u.usl
                                                and e.reu = s.reu
                                                and exists(select *
                                                           from list_c i,
                                                                spr_params p
                                                           where i.fk_ses = fk_ses_
                                                             and p.id = i.fk_par
                                                             and p.cd = 'REP_USL2'
                                                             and i.sel_cd = e.usl
                                                             and i.sel = 1)
                                                and exists(select *
                                                           from list_c i,
                                                                spr_params p
                                                           where i.fk_ses = fk_ses_
                                                             and p.id = i.fk_par
                                                             and p.cd = 'REP_ORG2'
                                                             and i.sel_id = e.org
                                                             and i.sel = 1)) h,
                                             (select * from xitog3 e where e.mg = mg_) i,
                                             (select reu,
                                                     kul,
                                                     nd,
                                                     org,
                                                     usl,
                                                     status,
                                                     fk_lsk_tp,
                                                     sum(charges) as charges,
                                                     sum(changes) as changes,
                                                     sum(subsid) as subsid,
                                                     sum(privs) as privs,
                                                     sum(privs_city) as privs_city,
                                                     sum(ch_full) as ch_full,
                                                     sum(changes2) as changes2,
                                                     sum(payment) as payment,
                                                     sum(pn) as pn
                                              from xitog3 t
                                              where t.mg between mg_ and mg1_
                                              group by reu, kul, nd, org, usl, status, fk_lsk_tp) o,
                                             (select * from xitog3 e where e.mg = mg1_) u,
                                             t_org d,
                                             usl m,
                                             org l
                                        where h.reu = i.reu(+)
                                          and h.kul = i.kul(+)
                                          and h.nd = i.nd(+)
                                          and h.org = i.org(+)
                                          and h.usl = i.usl(+)
                                          and h.status = i.status(+)
                                          and h.fk_lsk_tp = i.fk_lsk_tp(+)
                                          and h.reu = o.reu(+)
                                          and h.kul = o.kul(+)
                                          and h.nd = o.nd(+)
                                          and h.org = o.org(+)
                                          and h.usl = o.usl(+)
                                          and h.status = o.status(+)
                                          and h.fk_lsk_tp = o.fk_lsk_tp(+)
                                          and h.reu = u.reu(+)
                                          and h.kul = u.kul(+)
                                          and h.nd = u.nd(+)
                                          and h.org = u.org(+)
                                          and h.usl = u.usl(+)
                                          and h.status = u.status(+)
                                          and h.fk_lsk_tp = u.fk_lsk_tp(+)
                                          and l.id = 1
                                          and h.org = d.id
                                          and h.usl = m.usl
                                        group by d.npp, l.name, to_char(d.id) || ' ' || d.name,
                                                 to_char(h.uslm) || ' ' || decode(m.frc_get_price, 1, m.nm, m.nm1)
                                        having sum(i.indebet) <> 0
                                            or sum(i.inkredit) <> 0
                                            or sum(o.charges) <> 0
                                            or sum(o.changes) <> 0
                                            or sum(o.subsid) <> 0
                                            or sum(o.privs) <> 0
                                            or sum(o.privs_city) <> 0
                                            or sum(o.payment) <> 0
                                            or sum(o.pn) <> 0
                                            or sum(u.outdebet) <> 0
                                            or sum(u.outkredit) <> 0
                                        order by d.npp;
end if;
elsif сd_ in ('90') then
            --Оборотная ведомость по домам
            if var_ = 3 then
                --по дому
                open prep_refcursor for select h.name_reu as predpr,
                                               h.name as adr,
                                               sum(i.indebet) as indebet,
                                               sum(i.inkredit) as inkredit,
                                               sum(o.charges) as charges,
                                               sum(o.changes) as changes,
                                               sum(o.subsid) as subsid,
                                               sum(o.privs) as privs,
                                               sum(o.privs_city) as privs_city,
                                               sum(o.payment) as payment,
                                               sum(o.ch_full) as ch_full,
                                               sum(o.changes2) as changes2,
                                               sum(nvl(o.changes, 0) + nvl(o.changes2, 0)) as changesall,
                                               sum(o.pn) as pn,
                                               sum(u.outdebet) as outdebet,
                                               sum(u.outkredit) as outkredit
                                        from (select distinct e.reu,
                                                              e.kul,
                                                              e.nd,
                                                              e.org,
                                                              e.usl,
                                                              e.status,
                                                              u2.uslm,
                                                              trim(t.name_reu) as name_reu,
                                                              ot.name_short || '.' || initcap(o.name) || ', ' ||
                                                              initcap(s.name) || ', ' || ltrim(e.nd, '0') as name,
                                                              e.fk_lsk_tp
                                              from t_saldo_reu_kul_nd_st e,
                                                   usl u2,
                                                   s_reu_trest t,
                                                   spul s,
                                                   t_org o,
                                                   t_org_tp ot
                                              where e.usl = u2.usl
                                                and e.reu = t.reu
                                                and e.reu = reu_
                                                and e.kul = kul_
                                                and e.nd = nd_
                                                and s.fk_settlement = o.id
                                                and o.fk_orgtp = ot.id
                                                and exists(select *
                                                           from list_c i,
                                                                spr_params p
                                                           where i.fk_ses = fk_ses_
                                                             and p.id = i.fk_par
                                                             and p.cd = 'REP_USL2'
                                                             and i.sel_cd = e.usl
                                                             and i.sel = 1)
                                                and exists(select *
                                                           from list_c i,
                                                                spr_params p
                                                           where i.fk_ses = fk_ses_
                                                             and p.id = i.fk_par
                                                             and p.cd = 'REP_ORG2'
                                                             and i.sel_id = e.org
                                                             and i.sel = 1)
                                                and e.kul = s.id) h,
                                             (select * from xitog3 e where e.mg = mg_) i,
                                             (select reu,
                                                     kul,
                                                     nd,
                                                     org,
                                                     usl,
                                                     status,
                                                     fk_lsk_tp,
                                                     sum(charges) as charges,
                                                     sum(changes) as changes,
                                                     sum(subsid) as subsid,
                                                     sum(privs) as privs,
                                                     sum(privs_city) as privs_city,
                                                     sum(ch_full) as ch_full,
                                                     sum(changes2) as changes2,
                                                     sum(payment) as payment,
                                                     sum(pn) as pn
                                              from xitog3 t
                                              where t.mg between mg_ and mg1_
                                              group by reu, kul, nd, org, usl, status, fk_lsk_tp) o,
                                             (select * from xitog3 e where e.mg = mg1_) u,
                                             sprorg d,
                                             uslm m,
                                             org l,
                                             spul p
                                        where h.kul = p.id
                                          and h.reu = i.reu(+)
                                          and h.kul = i.kul(+)
                                          and h.nd = i.nd(+)
                                          and h.org = i.org(+)
                                          and h.usl = i.usl(+)
                                          and h.status = i.status(+)
                                          and h.fk_lsk_tp = i.fk_lsk_tp(+)
                                          and h.reu = o.reu(+)
                                          and h.kul = o.kul(+)
                                          and h.nd = o.nd(+)
                                          and h.org = o.org(+)
                                          and h.usl = o.usl(+)
                                          and h.status = o.status(+)
                                          and h.fk_lsk_tp = o.fk_lsk_tp(+)
                                          and h.reu = u.reu(+)
                                          and h.kul = u.kul(+)
                                          and h.nd = u.nd(+)
                                          and h.org = u.org(+)
                                          and h.usl = u.usl(+)
                                          and h.status = u.status(+)
                                          and h.fk_lsk_tp = u.fk_lsk_tp(+)
                                          and l.id = 3
                                          and h.org = d.kod
                                          and h.uslm = m.uslm
                                        group by h.name_reu, h.name
                                        having sum(i.indebet) <> 0
                                            or sum(i.inkredit) <> 0
                                            or sum(o.charges) <> 0
                                            or sum(o.changes) <> 0
                                            or sum(o.subsid) <> 0
                                            or sum(o.privs) <> 0
                                            or sum(o.privs_city) <> 0
                                            or sum(o.payment) <> 0
                                            or sum(o.pn) <> 0
                                            or sum(u.outdebet) <> 0
                                            or sum(u.outkredit) <> 0
                                        order by h.name_reu || ' ' || h.name;
elsif var_ = 2 then
                --по ЖЭО
                open prep_refcursor for select l.name || h.name_reu as predpr,
                                               ot.name_short || '.' || initcap(o2.name) || ', ' || initcap(p.name) ||
                                               ', ' || ltrim(h.nd, '0') as adr,
                                               sum(i.indebet) as indebet,
                                               sum(i.inkredit) as inkredit,
                                               sum(o.charges) as charges,
                                               sum(o.changes) as changes,
                                               sum(o.subsid) as subsid,
                                               sum(o.privs) as privs,
                                               sum(o.privs_city) as privs_city,
                                               sum(o.payment) as payment,
                                               sum(o.ch_full) as ch_full,
                                               sum(o.changes2) as changes2,
                                               sum(nvl(o.changes, 0) + nvl(o.changes2, 0)) as changesall,
                                               sum(o.pn) as pn,
                                               sum(u.outdebet) as outdebet,
                                               sum(u.outkredit) as outkredit
                                        from (select distinct u.reu,
                                                              u.kul,
                                                              u.nd,
                                                              u.org,
                                                              u.usl,
                                                              u.status,
                                                              u2.uslm,
                                                              trim(s.name_reu) as name_reu,
                                                              fk_lsk_tp
                                              from t_saldo_reu_kul_nd_st u,
                                                   usl u2,
                                                   s_reu_trest s
                                              where u.usl = u2.usl
                                                and u.reu = reu_
                                                and u.reu = s.reu
                                                and exists(select *
                                                           from list_c i,
                                                                spr_params p
                                                           where i.fk_ses = fk_ses_
                                                             and p.id = i.fk_par
                                                             and p.cd = 'REP_USL2'
                                                             and i.sel_cd = u.usl
                                                             and i.sel = 1)
                                                and exists(select *
                                                           from list_c i,
                                                                spr_params p
                                                           where i.fk_ses = fk_ses_
                                                             and p.id = i.fk_par
                                                             and p.cd = 'REP_ORG2'
                                                             and i.sel_id = u.org
                                                             and i.sel = 1)) h,
                                             (select * from xitog3 e where e.mg = mg_) i,
                                             (select reu,
                                                     kul,
                                                     nd,
                                                     org,
                                                     usl,
                                                     status,
                                                     fk_lsk_tp,
                                                     sum(charges) as charges,
                                                     sum(changes) as changes,
                                                     sum(subsid) as subsid,
                                                     sum(privs) as privs,
                                                     sum(privs_city) as privs_city,
                                                     sum(ch_full) as ch_full,
                                                     sum(changes2) as changes2,
                                                     sum(payment) as payment,
                                                     sum(pn) as pn
                                              from xitog3 t
                                              where t.mg between mg_ and mg1_
                                              group by reu, kul, nd, org, usl, status, fk_lsk_tp) o,
                                             (select * from xitog3 e where e.mg = mg1_) u,
                                             sprorg d,
                                             uslm m,
                                             org l,
                                             spul p,
                                             t_org o2,
                                             t_org_tp ot
                                        where h.kul = p.id
                                          and p.fk_settlement = o2.id
                                          and o2.fk_orgtp = ot.id
                                          and h.reu = i.reu(+)
                                          and h.kul = i.kul(+)
                                          and h.nd = i.nd(+)
                                          and h.org = i.org(+)
                                          and h.usl = i.usl(+)
                                          and h.status = i.status(+)
                                          and h.fk_lsk_tp = i.fk_lsk_tp(+)
                                          and h.reu = o.reu(+)
                                          and h.kul = o.kul(+)
                                          and h.nd = o.nd(+)
                                          and h.org = o.org(+)
                                          and h.usl = o.usl(+)
                                          and h.status = o.status(+)
                                          and h.fk_lsk_tp = o.fk_lsk_tp(+)
                                          and h.reu = u.reu(+)
                                          and h.kul = u.kul(+)
                                          and h.nd = u.nd(+)
                                          and h.org = u.org(+)
                                          and h.usl = u.usl(+)
                                          and h.status = u.status(+)
                                          and h.fk_lsk_tp = u.fk_lsk_tp(+)
                                          and l.id = 3
                                          and h.org = d.kod
                                          and h.uslm = m.uslm
                                        group by l.name || h.name_reu,
                                                 ot.name_short || '.' || initcap(o2.name) || ', ' || initcap(p.name) ||
                                                 ', ' || ltrim(h.nd, '0')
                                        having sum(i.indebet) <> 0
                                            or sum(i.inkredit) <> 0
                                            or sum(o.charges) <> 0
                                            or sum(o.changes) <> 0
                                            or sum(o.subsid) <> 0
                                            or sum(o.privs) <> 0
                                            or sum(o.privs_city) <> 0
                                            or sum(o.payment) <> 0
                                            or sum(o.pn) <> 0
                                            or sum(u.outdebet) <> 0
                                            or sum(u.outkredit) <> 0
                                        order by l.name || h.name_reu,
                                                 ot.name_short || '.' || initcap(o2.name) || ', ' || initcap(p.name) ||
                                                 ', ' || ltrim(h.nd, '0');
elsif var_ = 1 then
                --по Фонду
                open prep_refcursor for select l.name || t.name_tr as predpr,
                                               ot.name_short || '.' || initcap(o2.name) || ', ' || initcap(p.name) ||
                                               ', ' || ltrim(h.nd, '0') as adr,
                                               sum(i.indebet) as indebet,
                                               sum(i.inkredit) as inkredit,
                                               sum(o.charges) as charges,
                                               sum(o.changes) as changes,
                                               sum(o.subsid) as subsid,
                                               sum(o.privs) as privs,
                                               sum(o.privs_city) as privs_city,
                                               sum(o.payment) as payment,
                                               sum(o.ch_full) as ch_full,
                                               sum(o.changes2) as changes2,
                                               sum(nvl(o.changes, 0) + nvl(o.changes2, 0)) as changesall,
                                               sum(o.pn) as pn,
                                               sum(u.outdebet) as outdebet,
                                               sum(u.outkredit) as outkredit
                                        from (select distinct u.reu,
                                                              u.kul,
                                                              u.nd,
                                                              u.org,
                                                              u.usl,
                                                              u.status,
                                                              u2.uslm,
                                                              u.fk_lsk_tp
                                              from t_saldo_reu_kul_nd_st u,
                                                   usl u2,
                                                   s_reu_trest s
                                              where u.usl = u2.usl
                                                and u.reu = s.reu
                                                and s.trest = trest_
                                                and exists(select *
                                                           from list_c i,
                                                                spr_params p
                                                           where i.fk_ses = fk_ses_
                                                             and p.id = i.fk_par
                                                             and p.cd = 'REP_USL2'
                                                             and i.sel_cd = u.usl
                                                             and i.sel = 1)
                                                and exists(select *
                                                           from list_c i,
                                                                spr_params p
                                                           where i.fk_ses = fk_ses_
                                                             and p.id = i.fk_par
                                                             and p.cd = 'REP_ORG2'
                                                             and i.sel_id = u.org
                                                             and i.sel = 1)) h,
                                             (select * from xitog3 e where e.mg = mg_) i,
                                             (select reu,
                                                     kul,
                                                     nd,
                                                     org,
                                                     usl,
                                                     status,
                                                     fk_lsk_tp,
                                                     sum(charges) as charges,
                                                     sum(changes) as changes,
                                                     sum(subsid) as subsid,
                                                     sum(privs) as privs,
                                                     sum(privs_city) as privs_city,
                                                     sum(ch_full) as ch_full,
                                                     sum(changes2) as changes2,
                                                     sum(payment) as payment,
                                                     sum(pn) as pn
                                              from xitog3 t
                                              where t.mg between mg_ and mg1_
                                              group by reu, kul, nd, org, usl, status, fk_lsk_tp) o,
                                             (select * from xitog3 e where e.mg = mg1_) u,
                                             sprorg d,
                                             uslm m,
                                             org l,
                                             spul p,
                                             s_reu_trest t,
                                             t_org o2,
                                             t_org_tp ot
                                        where h.kul = p.id
                                          and h.reu = t.reu
                                          and p.fk_settlement = o2.id
                                          and o2.fk_orgtp = ot.id
                                          and h.reu = i.reu(+)
                                          and h.kul = i.kul(+)
                                          and h.nd = i.nd(+)
                                          and h.org = i.org(+)
                                          and h.usl = i.usl(+)
                                          and h.status = i.status(+)
                                          and h.fk_lsk_tp = i.fk_lsk_tp(+)
                                          and h.reu = o.reu(+)
                                          and h.kul = o.kul(+)
                                          and h.nd = o.nd(+)
                                          and h.org = o.org(+)
                                          and h.usl = o.usl(+)
                                          and h.status = o.status(+)
                                          and h.fk_lsk_tp = o.fk_lsk_tp(+)
                                          and h.reu = u.reu(+)
                                          and h.kul = u.kul(+)
                                          and h.nd = u.nd(+)
                                          and h.org = u.org(+)
                                          and h.usl = u.usl(+)
                                          and h.status = u.status(+)
                                          and h.fk_lsk_tp = u.fk_lsk_tp(+)
                                          and l.id = 2
                                          and h.org = d.kod
                                          and h.uslm = m.uslm
                                        group by l.name || t.name_tr,
                                                 ot.name_short || '.' || initcap(o2.name) || ', ' || initcap(p.name) ||
                                                 ', ' || ltrim(h.nd, '0')
                                        having sum(i.indebet) <> 0
                                            or sum(i.inkredit) <> 0
                                            or sum(o.charges) <> 0
                                            or sum(o.changes) <> 0
                                            or sum(o.subsid) <> 0
                                            or sum(o.privs) <> 0
                                            or sum(o.privs_city) <> 0
                                            or sum(o.payment) <> 0
                                            or sum(o.pn) <> 0
                                            or sum(u.outdebet) <> 0
                                            or sum(u.outkredit) <> 0
                                        order by l.name || t.name_tr,
                                                 ot.name_short || '.' || initcap(o2.name) || ', ' || initcap(p.name) ||
                                                 ', ' || ltrim(h.nd, '0');
elsif var_ = 0 then
                --по Городу
                open prep_refcursor for select l.name as predpr,
                                               ot.name_short || '.' || initcap(o2.name) || ', ' || initcap(p.name) ||
                                               ', ' || ltrim(h.nd, '0') as adr,
                                               sum(i.indebet) as indebet,
                                               sum(i.inkredit) as inkredit,
                                               sum(o.charges) as charges,
                                               sum(o.poutsal) as poutsal,
                                               sum(o.changes) as changes,
                                               sum(o.subsid) as subsid,
                                               sum(o.privs) as privs,
                                               sum(o.privs_city) as privs_city,
                                               sum(o.payment) as payment,
                                               sum(o.ch_full) as ch_full,
                                               sum(o.changes2) as changes2,
                                               sum(nvl(o.changes, 0) + nvl(o.changes2, 0)) as changesall,
                                               sum(o.pn) as pn,
                                               sum(u.outdebet) as outdebet,
                                               sum(u.outkredit) as outkredit
                                        from (select distinct u.reu,
                                                              u.kul,
                                                              u.nd,
                                                              u.org,
                                                              u.usl,
                                                              u.status,
                                                              u2.uslm,
                                                              u.fk_lsk_tp
                                              from t_saldo_reu_kul_nd_st u,
                                                   usl u2
                                              where u.usl = u2.usl
                                                and exists(select *
                                                           from list_c i,
                                                                spr_params p
                                                           where i.fk_ses = fk_ses_
                                                             and p.id = i.fk_par
                                                             and p.cd = 'REP_USL2'
                                                             and i.sel_cd = u.usl
                                                             and i.sel = 1)
                                                and exists(select *
                                                           from list_c i,
                                                                spr_params p
                                                           where i.fk_ses = fk_ses_
                                                             and p.id = i.fk_par
                                                             and p.cd = 'REP_ORG2'
                                                             and i.sel_id = u.org
                                                             and i.sel = 1)) h,
                                             (select * from xitog3 e where e.mg = mg_) i,
                                             (select reu,
                                                     kul,
                                                     nd,
                                                     org,
                                                     usl,
                                                     status,
                                                     fk_lsk_tp,
                                                     sum(charges) as charges,
                                                     sum(poutsal) as poutsal,
                                                     sum(changes) as changes,
                                                     sum(subsid) as subsid,
                                                     sum(privs) as privs,
                                                     sum(privs_city) as privs_city,
                                                     sum(ch_full) as ch_full,
                                                     sum(changes2) as changes2,
                                                     sum(payment) as payment,
                                                     sum(pn) as pn
                                              from xitog3 t
                                              where t.mg between mg_ and mg1_
                                              group by reu, kul, nd, org, usl, status, fk_lsk_tp) o,
                                             (select * from xitog3 e where e.mg = mg1_) u,
                                             sprorg d,
                                             uslm m,
                                             org l,
                                             spul p,
                                             t_org o2,
                                             t_org_tp ot
                                        where h.kul = p.id
                                          and p.fk_settlement = o2.id
                                          and o2.fk_orgtp = ot.id
                                          and h.reu = i.reu(+)
                                          and h.kul = i.kul(+)
                                          and h.nd = i.nd(+)
                                          and h.org = i.org(+)
                                          and h.usl = i.usl(+)
                                          and h.status = i.status(+)
                                          and h.fk_lsk_tp = i.fk_lsk_tp(+)
                                          and h.reu = o.reu(+)
                                          and h.kul = o.kul(+)
                                          and h.nd = o.nd(+)
                                          and h.org = o.org(+)
                                          and h.usl = o.usl(+)
                                          and h.status = o.status(+)
                                          and h.fk_lsk_tp = o.fk_lsk_tp(+)
                                          and h.reu = u.reu(+)
                                          and h.kul = u.kul(+)
                                          and h.nd = u.nd(+)
                                          and h.org = u.org(+)
                                          and h.usl = u.usl(+)
                                          and h.status = u.status(+)
                                          and h.fk_lsk_tp = u.fk_lsk_tp(+)
                                          and l.id = 1
                                          and h.org = d.kod
                                          and h.uslm = m.uslm
                                        group by l.name,
                                                 ot.name_short || '.' || initcap(o2.name) || ', ' || initcap(p.name) ||
                                                 ', ' || ltrim(h.nd, '0')
                                        having sum(i.indebet) <> 0
                                            or sum(i.inkredit) <> 0
                                            or sum(o.charges) <> 0
                                            or sum(o.changes) <> 0
                                            or sum(o.subsid) <> 0
                                            or sum(o.privs) <> 0
                                            or sum(o.privs_city) <> 0
                                            or sum(o.payment) <> 0
                                            or sum(o.pn) <> 0
                                            or sum(u.outdebet) <> 0
                                            or sum(u.outkredit) <> 0
                                        order by l.name,
                                                 ot.name_short || '.' || initcap(o2.name) || ', ' || initcap(p.name) ||
                                                 ', ' || ltrim(h.nd, '0');
--USING fk_ses_, fk_ses_, mg_, mg_, mg1_, mg1_;
end if;
elsif сd_ in ('91') then
            --реестр пользующихся льготой по капремонту >=70 лет ТОЛЬКО ДЛЯ ПОЛЫС, ТАК КАК У НИХ НЕТ ДОП СЧЕТОВ, - РАБОТАЕТ ПО ДРУГОМУ
            open prep_refcursor for select scott.utils.month_name(substr(k.mg, 5, 2)) as mon,
                                           substr(k.mg, 1, 4) as year,
                                           k.lsk as ls,
                                           s.name as ul,
                                           ltrim(k.nd, '0') as dom,
                                           ltrim(k.kw, '0') as kv,
                                           a.name as st,
                                           k.opl,
                                           k.k_fam as sur,
                                           k.k_im as nam,
                                           k.k_ot as mid,
                                           k.kpr,
                                           t.dat_rog
                                    from arch_kart k,
                                         a_kart_pr2 t,
                                         spul s,
                                         status a
                                    where k.psch not in (8, 9)
                                      and k.lsk = t.lsk
                                      and k.mg between t.mgfrom and t.mgto
                                      and k.mg = mg_
                                      and k.kul = s.id
                                      and k.status = a.id
                                      and a.cd = 'PRV'
                                      and exists(select min(p.id)
                                                 from a_kart_pr2 p,
                                                      relations r
                                                 where months_between(to_date(mg_ || '01', 'YYYYMMDD'), p.dat_rog) / 12 >= 70
                                                   and p.dat_rog is not null
                                                   and p.id = t.id
                                                   and mg_ between p.mgfrom and p.mgto
                                                   and p.relat_id = r.id
                                                   and r.cd in ('Квартиросъемщик', 'Собственник')
                                                   and p.status = 1
                                                 having min(p.id) = t.id)
                                      and exists(select *
                                                 from a_charge_prep2 a,
                                                      arch_kart d,
                                                      v_lsk_tp tp
                                                 where a.lsk = d.lsk
                                                   and a.tp = 9
                                                   and mg_ between a.mgfrom and a.mgto
                                                   and d.mg = mg_
                                                   and d.k_lsk_id = k.k_lsk_id
                                                   and d.fk_tp = tp.id
                                                   and tp.cd = 'LSK_TP_ADDIT') --доп счета, только льготники
                                    order by k.reu, s.name, ltrim(k.nd, '0'), ltrim(k.kw, '0');

elsif сd_ in ('92') then
            --реестр для УСЗН,-длинный, бессмысленный и беспощадный (для ТСЖ)
            open prep_refcursor for select c.name as org1,
                                           c.reu,
                                           s.kul,
                                           null as st_code,
                                           c2.name as nasp,
                                           l.cd_uszn as nylic,
                                           scott.utils.f_ord_digit(s.nd) as ndom,
                                           lower(scott.utils.f_ord3(s.nd)) as nkorp,
                                           ltrim(s.kw, '0') as nkw,
                                           null as nkomn,
                                           s.lsk as lchet,
                                           s.kpr,
                                           s.kpr_wr,
                                           s.kpr_ot,
                                           s.opl,
                                           s.opl as pl_ot,
                                           'Содержание и ремонт жилого помещения' as gu1,
                                           c1.tf1 as tf_ng1,
                                           c1.tf2 as tf_svg1,
                                           e2.summa_itg as sum_g1,
                                           'Капитальный ремонт' as gu2,
                                           c2.tf1 as tf_ng2,
                                           c2.tf2 as tf_svg2,
                                           e3.summa_itg as sum_g2,
                                           'Найм коммерческий' as gu3,
                                           null as tf_ng3,
                                           null as tf_svg3,
                                           null as sum_g3,
                                           'Найм помещения' as gu4,
                                           c4.tf1 as tf_ng4,
                                           null as tf_svg4,
                                           e4.summa_itg as sum_g4,
                                           'Электроснабжение в домах со стационарными эл.плит.' as gku1,
                                           s.lsk as lchet1,
                                           'квт.' as ed_izm1,
                                           c5.tf1 as tf_n1,
                                           c5.tf2 as tf_sv1,
                                           e5.test_opl as fakt1,
                                           case
                                               when s.kpr = 1 then 130
                                               when s.kpr in (2, 3) then 100
                                               when s.kpr = 4 then 87.5
                                               when s.kpr = 5 then 80
                                               when s.kpr >= 6 then 75 end as norm1,
                                           e5.summa_itg as sum_f1,

                                           'Электроснабжение, ОДН' as gku2,
                                           s.lsk as lchet2,
                                           'квт.' as ed_izm2,
                                           e6.nrm as norm2,
                                           c6.tf1 as tf_n2,
                                           null as tf_sv2,
                                           null as fakt2,
                                           e6.summa_itg as sum_f2,

                                           null as gku3,
                                           null as lchet3,
                                           null as ed_izm3,
                                           null as tf_n3,
                                           null as tf_sv3,
                                           null as norm3,
                                           null as fakt3,
                                           null as sum_f3,
                                           'ХВС' as gku4,
                                           s.lsk as lchet4,
                                           'м3' as ed_izm4,
                                           c7.tf1 as tf_n4,
                                           c7.tf2 as tf_sv4,
                                           e7.norm as norm4,
                                           e7.test_opl as fakt4,
                                           e7.summa_itg as sum_f4,

                                           'Холодная вода, ОДН' as gku5,
                                           s.lsk as lchet5,
                                           'м3' as ed_izm5,
                                           c8.tf1 as tf_n5,
                                           c8.tf2 as tf_sv5,
                                           e8.nrm as norm5,
                                           e9.test_opl as fakt5,
                                           e8.summa_itg as sum_f5,

                                           'ГВС' as gku6,
                                           s.lsk as lchet6,
                                           'м3' as ed_izm6,
                                           c9.tf1 as tf_n6,
                                           c9.tf2 as tf_sv6,
                                           e9.norm as norm6,
                                           e9.test_opl as fakt6,
                                           e9.summa_itg as sum_f6,

                                           'Горячая вода, ОДН' as gku7,
                                           s.lsk as lchet7,
                                           'м3' as ed_izm7,
                                           c10.tf1 as tf_n7,
                                           c10.tf2 as tf_sv7,
                                           e10.nrm as norm7,
                                           null as fakt7,
                                           e10.summa_itg as sum_f7,

                                           'Канализация' as gku8,
                                           s.lsk as lchet8,
                                           'м3' as ed_izm8,
                                           e11.test_opl as fakt8,
                                           c11.tf1 as tf_n8,
                                           c11.tf2 as tf_sv8,
                                           e11.norm as norm8,
                                           null as fakt8,
                                           e11.summa_itg as sum_f8,

                                           'Отопление' as gku9,
                                           s.lsk as lchet9,
                                           'м2' as ed_izm9,
                                           c12.tf1 as tf_n9,
                                           c12.tf2 as tf_sv9,
                                           case
                                               when s.kpr = 1 then 33
                                               when s.kpr in (2) then 21
                                               when s.kpr >= 3 then 18 end as norm9,
                                           case
                                               when to_number(s.lsk) between 1 and 157
                                                   then 0.0204 --жестко привязал по отоплению Гкал
                                               else 0.019678 end as fakt9,
                                           e12.summa_itg as sum_f9,
                                           'Газ в баллонах' as gku10,
                                           null as lchet10,
                                           null as ed_izm10,
                                           null as fakt10,
                                           null as sum_f10,

                                           'Канализование на ОДН' as gku11,
                                           s.lsk as lchet11,
                                           'м3' as ed_izm11,
                                           c13.tf1 as tf_n11,
                                           c13.tf2 as tf_sv11,
                                           e13.nrm as norm11,
                                           null as fakt11,
                                           e13.summa_itg as sum_f11,

                                           'Вывоз ТКО' as gku13,
                                           s.lsk as lchet13,
                                           'м3' as ed_izm13,
                                           c14.tf1 as tf_n13,
                                           c14.tf1 as tf_sv13,
                                           0.17275 as norm13,
                                           round(e14.test_opl * 0.17275, 4) as fakt13, -- кол-во людей * норматив
                                           e14.summa_itg as sum_f13,

                                           null as lchet12,
                                           null as ed_izm12,
                                           null as fakt12,
                                           null as sum_f12,
                                           null as gku12,
                                           null as tf_n12,
                                           null as tf_sv12,
                                           null as norm12
                                    from (select s.*
                                          from kart k,
                                               arch_kart s,
                                               s_reu_trest e
                                          where s.mg = mg_
                                            and k.lsk = s.lsk
                                            and k.sel1 = 1
                                              /*and e.reu='01'
     and s.lsk='00000001'*/
                                            and s.reu = e.reu
                                            and s.psch not in (8, 9) --только открытые
                                            and s.status not in (7)--убрал нежилые
                                         ) s
                                             join t_org c on s.reu = c.reu
                                             join t_org c2 on 1 = 1
                                             join t_org_tp tp on c2.fk_orgtp = tp.id and tp.cd = 'Город'
                                             join params p on p.period = p.period
                                             join spul l on s.kul = l.id
                                             left join (select s.lsk, sum(s.summa) as summa_itg
                                                        from a_charge2 s,
                                                             usl u
                                                        where s.usl = u.usl
                                                          and mg_ between s.mgfrom and s.mgto
                                                          and u.cd in
                                                              ('т/сод', 'т/сод/св.нор', 'лифт', 'лифт/св.нор', 'дерат.',
                                                               'дерат/св.нор', 'мус.площ.', 'мус.площ./св.нор'/*,
       'выв.мус.', 'выв.мус./св.нор'*/)
                                                          and s.type = 1 --текущее содержание, вместе с под-услугами
                                                        group by s.lsk) e2 on s.lsk = e2.lsk
                                             left join (select s.lsk, sum(s.summa) as summa_itg
                                                        from a_charge2 s,
                                                             usl u
                                                        where s.usl = u.usl
                                                          and mg_ between s.mgfrom and s.mgto
                                                          and u.cd in ('кап.', 'кап/св.нор')
                                                          and s.type = 1 --капремонт
                                                        group by s.lsk) e3 on s.lsk = e3.lsk
                                             left join (select s.lsk, sum(s.summa) as summa_itg
                                                        from a_charge2 s,
                                                             usl u
                                                        where s.usl = u.usl
                                                          and mg_ between s.mgfrom and s.mgto
                                                          and u.cd in ('найм')
                                                          and s.type = 1 --найм
                                                        group by s.lsk) e4 on s.lsk = e4.lsk
                                             left join (select s.lsk,
                                                               sum(s.test_opl) as test_opl,
                                                               sum(s.summa) as summa_itg
                                                        from a_charge2 s,
                                                             usl u
                                                        where s.usl = u.usl
                                                          and mg_ between s.mgfrom and s.mgto
                                                          and u.cd in ('эл.энерг.2', 'эл.эн.2/св.нор')
                                                          and s.type = 1 --эл.энерг
                                                        group by s.lsk) e5 on s.lsk = e5.lsk
                                             left join (select s.lsk,
                                                               sum(s.test_opl) as test_opl,
                                                               sum(s.summa) as summa_itg,
                                                               --max(d.nrm) as nrm
                                                               4.1 as nrm --вбил жёстко
                                                        from a_charge2 s
                                                                 join a_nabor2 n on s.lsk = n.lsk and
                                                                                    mg_ between s.mgfrom and s.mgto and
                                                                                    mg_ between n.mgfrom and n.mgto and
                                                                                    to_date(mg_ || '01', 'YYYYMMDD') between n.dt1 and n.dt2
                                                                 join a_vvod d
                                                                      on n.fk_vvod = d.id and n.usl = d.usl and d.mg between s.mgfrom and s.mgto
                                                                 join usl u2 on n.usl = u2.usl and u2.cd in ('эл.энерг.2')
                                                                 join usl u on s.usl = u.usl and u.cd in ('эл.эн.ОДН', 'EL_SOD') and s.type = 1 --эл.энерг
                                                        group by s.lsk) e6 on s.lsk = e6.lsk
                                             left join (select s.lsk,
                                                               sum(s.test_opl) as test_opl,
                                                               sum(s.summa) as summa_itg,
                                                               max(n.norm) as norm
                                                        from a_charge2 s,
                                                             a_nabor2 n,
                                                             usl u
                                                        where s.usl = u.usl
                                                          and s.lsk = n.lsk
                                                          and to_date(mg_ || '01', 'YYYYMMDD') between n.dt1 and n.dt2
                                                          and mg_ between s.mgfrom and s.mgto
                                                          and mg_ between n.mgfrom and n.mgto
                                                          and s.usl = n.usl
                                                          and u.cd in ('х.вода', 'х.вода/св.нор')
                                                          and s.type = 1
                                                        group by s.lsk) e7 on s.lsk = e7.lsk
                                             left join (select s.lsk,
                                                               sum(s.test_opl) as test_opl,
                                                               sum(s.summa) as summa_itg,
                                                               max(d.nrm) as nrm
                                                        from a_charge2 s
                                                                 join a_nabor2 n on s.lsk = n.lsk and
                                                                                    to_date(mg_ || '01', 'YYYYMMDD') between n.dt1 and n.dt2 and
                                                                                    mg_ between s.mgfrom and s.mgto and
                                                                                    mg_ between n.mgfrom and n.mgto
                                                                 join a_vvod d
                                                                      on n.fk_vvod = d.id and n.usl = d.usl and d.mg between s.mgfrom and s.mgto
                                                                 join usl u2 on n.usl = u2.usl and u2.cd in ('х.вода')
                                                                 join usl u
                                                                      on s.usl = u.usl and u.cd in ('х.вода.ОДН', 'HW_SOD') and s.type = 1
                                                        group by s.lsk) e8 on s.lsk = e8.lsk
                                             left join (select s.lsk,
                                                               sum(s.test_opl) as test_opl,
                                                               sum(s.summa) as summa_itg,
                                                               max(n.norm) as norm
                                                        from a_charge2 s,
                                                             a_nabor2 n,
                                                             usl u
                                                        where s.usl = u.usl
                                                          and s.lsk = n.lsk
                                                          and to_date(mg_ || '01', 'YYYYMMDD') between n.dt1 and n.dt2
                                                          and mg_ between s.mgfrom and s.mgto
                                                          and mg_ between n.mgfrom and n.mgto
                                                          and s.usl = n.usl
                                                          and u.cd in ('г.вода', 'г.вода/св.нор')
                                                          and s.type = 1
                                                        group by s.lsk) e9 on s.lsk = e9.lsk
                                             left join (select s.lsk,
                                                               sum(s.test_opl) as test_opl,
                                                               sum(s.summa) as summa_itg,
                                                               max(d.nrm) as nrm
                                                        from a_charge2 s
                                                                 join a_nabor2 n on s.lsk = n.lsk and
                                                                                    to_date(mg_ || '01', 'YYYYMMDD') between n.dt1 and n.dt2 and
                                                                                    mg_ between s.mgfrom and s.mgto and
                                                                                    mg_ between n.mgfrom and n.mgto
                                                                 join a_vvod d
                                                                      on n.fk_vvod = d.id and n.usl = d.usl and d.mg between s.mgfrom and s.mgto
                                                                 join usl u2 on n.usl = u2.usl and u2.cd in ('г.вода')
                                                                 join usl u
                                                                      on s.usl = u.usl and u.cd in ('г.вода.ОДН', 'GW_SOD') and s.type = 1 --эл.энерг
                                                        group by s.lsk) e10 on s.lsk = e10.lsk
                                             left join (select s.lsk,
                                                               sum(s.test_opl) as test_opl,
                                                               sum(s.summa) as summa_itg,
                                                               max(n.norm) as norm
                                                        from a_charge2 s,
                                                             a_nabor2 n,
                                                             usl u
                                                        where s.usl = u.usl
                                                          and s.lsk = n.lsk
                                                          and to_date(mg_ || '01', 'YYYYMMDD') between n.dt1 and n.dt2
                                                          and mg_ between s.mgfrom and s.mgto
                                                          and mg_ between n.mgfrom and n.mgto
                                                          and s.usl = n.usl
                                                          and u.cd in ('канализ', 'канализ/св.нор')
                                                          and s.type = 1
                                                        group by s.lsk) e11 on s.lsk = e11.lsk
                                             left join (select s.lsk,
                                                               sum(s.test_opl) as test_opl,
                                                               sum(s.summa) as summa_itg,
                                                               null as norm
                                                        from a_charge2 s,
                                                             a_nabor2 n,
                                                             usl u
                                                        where s.usl = u.usl
                                                          and s.lsk = n.lsk
                                                          and to_date(mg_ || '01', 'YYYYMMDD') between n.dt1 and n.dt2
                                                          and mg_ between s.mgfrom and s.mgto
                                                          and mg_ between n.mgfrom and n.mgto
                                                          and s.usl = n.usl
                                                          and u.cd in ('отоп', 'отоп/св.нор')
                                                          and s.type = 1
                                                        group by s.lsk) e12 on s.lsk = e12.lsk

                                             left join (select s.lsk,
                                                               sum(s.test_opl) as test_opl,
                                                               sum(s.summa) as summa_itg
                                                        from a_charge2 s,
                                                             usl u
                                                        where s.usl = u.usl
                                                          and mg_ between s.mgfrom and s.mgto
                                                          and u.cd in ('выв.мус.')
                                                          and s.type = 1 -- вывоз ТКО
                                                        group by s.lsk) e14 on s.lsk = e14.lsk

                                             left join (select n.lsk,
                                                               round(sum(case
                                                                             when u.usl_norm = 0 then case
                                                                                                          when n.koeff is not null and u.sptarn in (0, 2, 3, 4)
                                                                                                              then n.koeff
                                                                                                          else 1 end *
                                                                                                      r.summa
                                                                             else 0 end), 2) as tf1,
                                                               round(sum(case
                                                                             when u.usl_norm = 1 then case
                                                                                                          when n.koeff is not null and u.sptarn in (0, 2, 3, 4)
                                                                                                              then n.koeff
                                                                                                          else 1 end *
                                                                                                      r.summa
                                                                             else 0 end), 2) as tf2
                                                        from a_nabor2 n
                                                                 join usl u on n.usl = u.usl and
                                                                               to_date(mg_ || '01', 'YYYYMMDD') between n.dt1 and n.dt2
                                                                 join a_prices r on n.usl = r.usl and r.mg between n.mgfrom and n.mgto
                                                        where r.mg = mg_
                                                          and u.cd in
                                                              ('т/сод', 'т/сод/св.нор', 'лифт', 'лифт/св.нор', 'дерат.',
                                                               'дерат/св.нор', 'мус.площ.', 'мус.площ./св.нор')
                                                        group by n.lsk) c1 on s.lsk = c1.lsk
                                             left join (select n.lsk,
                                                               round(sum(case
                                                                             when u.usl_norm = 0 then case
                                                                                                          when n.koeff is not null and u.sptarn in (0, 2, 3, 4)
                                                                                                              then n.koeff
                                                                                                          else 1 end *
                                                                                                      r.summa
                                                                             else 0 end), 2) as tf1,
                                                               round(sum(case
                                                                             when u.usl_norm = 1 then case
                                                                                                          when n.koeff is not null and u.sptarn in (0, 2, 3, 4)
                                                                                                              then n.koeff
                                                                                                          else 1 end *
                                                                                                      r.summa
                                                                             else 0 end), 2) as tf2
                                                        from a_nabor2 n
                                                                 join usl u on n.usl = u.usl and
                                                                               to_date(mg_ || '01', 'YYYYMMDD') between n.dt1 and n.dt2
                                                                 join a_prices r on n.usl = r.usl and r.mg between n.mgfrom and n.mgto
                                                        where r.mg = mg_
                                                          and u.cd in ('кап.', 'кап/св.нор')
                                                        group by n.lsk) c2 on s.lsk = c2.lsk


                                             left join (select n.lsk,
                                                               round(sum(case
                                                                             when u.usl_norm = 0 then case
                                                                                                          when n.koeff is not null and u.sptarn in (0, 2, 3, 4)
                                                                                                              then n.koeff
                                                                                                          else 1 end *
                                                                                                      r.summa
                                                                             else 0 end), 2) as tf1,
                                                               round(sum(case
                                                                             when u.usl_norm = 1 then case
                                                                                                          when n.koeff is not null and u.sptarn in (0, 2, 3, 4)
                                                                                                              then n.koeff
                                                                                                          else 1 end *
                                                                                                      r.summa
                                                                             else 0 end), 2) as tf2
                                                        from a_nabor2 n
                                                                 join usl u on n.usl = u.usl and
                                                                               to_date(mg_ || '01', 'YYYYMMDD') between n.dt1 and n.dt2
                                                                 join a_prices r on n.usl = r.usl and r.mg between n.mgfrom and n.mgto
                                                        where r.mg = mg_
                                                          and u.cd in ('найм')
                                                        group by n.lsk) c4 on s.lsk = c4.lsk
                                             left join (select n.lsk,
                                                               round(sum(case
                                                                             when u.usl_norm = 0 then case
                                                                                                          when n.koeff is not null and u.sptarn in (0, 2, 3, 4)
                                                                                                              then n.koeff
                                                                                                          else 1 end *
                                                                                                      r.summa
                                                                             else 0 end), 2) as tf1,
                                                               round(sum(case
                                                                             when u.usl_norm = 1 then case
                                                                                                          when n.koeff is not null and u.sptarn in (0, 2, 3, 4)
                                                                                                              then n.koeff
                                                                                                          else 1 end *
                                                                                                      r.summa
                                                                             else 0 end), 2) as tf2
                                                        from a_nabor2 n
                                                                 join usl u on n.usl = u.usl and
                                                                               to_date(mg_ || '01', 'YYYYMMDD') between n.dt1 and n.dt2
                                                                 join a_prices r on n.usl = r.usl and r.mg between n.mgfrom and n.mgto
                                                        where r.mg = mg_
                                                          and u.cd in ('эл.энерг.2', 'эл.эн.2/св.нор')
                                                        group by n.lsk) c5 on s.lsk = c5.lsk

                                             left join (select n.lsk,
                                                               sum(r.summa) as tf1,
                                                               sum(r.summa) as tf2
                                                        from a_nabor2 n
                                                                 join usl u on n.usl = u.usl and
                                                                               to_date(mg_ || '01', 'YYYYMMDD') between n.dt1 and n.dt2
                                                                 join usl u2 on u2.cd in ('эл.эн.2/св.нор')
                                                                 join a_prices r on u2.usl = r.usl and r.mg between n.mgfrom and n.mgto
                                                        where r.mg = mg_ and u.cd in ('EL_SOD')
                                                        group by n.lsk) c6 on s.lsk = c6.lsk

                                             left join (select n.lsk,
                                                               round(sum(case
                                                                             when u.usl_norm = 0 then case
                                                                                                          when n.koeff is not null and u.sptarn in (0, 2, 3, 4)
                                                                                                              then n.koeff
                                                                                                          else 1 end *
                                                                                                      r.summa
                                                                             else 0 end), 2) as tf1,
                                                               round(sum(case
                                                                             when u.usl_norm = 1 then case
                                                                                                          when n.koeff is not null and u.sptarn in (0, 2, 3, 4)
                                                                                                              then n.koeff
                                                                                                          else 1 end *
                                                                                                      r.summa
                                                                             else 0 end), 2) as tf2
                                                        from a_nabor2 n
                                                                 join usl u on n.usl = u.usl and
                                                                               to_date(mg_ || '01', 'YYYYMMDD') between n.dt1 and n.dt2
                                                                 join a_prices r on n.usl = r.usl and r.mg between n.mgfrom and n.mgto
                                                        where r.mg = mg_
                                                          and u.cd in ('х.вода', 'х.вода/св.нор')
                                                        group by n.lsk) c7 on s.lsk = c7.lsk

                                             left join (select n.lsk,
                                                               round(sum(case
                                                                             when u.usl_norm = 0 then case
                                                                                                          when n.koeff is not null and u.sptarn in (0, 2, 3, 4)
                                                                                                              then n.koeff
                                                                                                          else 1 end *
                                                                                                      r.summa
                                                                             else 0 end), 2) as tf1,
                                                               round(sum(case
                                                                             when u.usl_norm = 1 then case
                                                                                                          when n.koeff is not null and u.sptarn in (0, 2, 3, 4)
                                                                                                              then n.koeff
                                                                                                          else 1 end *
                                                                                                      r.summa
                                                                             else 0 end), 2) as tf2
                                                        from a_nabor2 n
                                                                 join usl u on n.usl = u.usl and
                                                                               to_date(mg_ || '01', 'YYYYMMDD') between n.dt1 and n.dt2
                                                                 join a_prices r on n.usl = r.usl and r.mg between n.mgfrom and n.mgto
                                                        where r.mg = mg_
                                                          --and u.cd in ('х.вода.ОДН', 'HW_SOD')
                                                          and u.cd in ('х.вода/св.нор')
                                                        group by n.lsk) c8 on s.lsk = c8.lsk

                                             left join (select n.lsk,
                                                               round(sum(case
                                                                             when u.usl_norm = 0 then case
                                                                                                          when n.koeff is not null and u.sptarn in (0, 2, 3, 4)
                                                                                                              then n.koeff
                                                                                                          else 1 end *
                                                                                                      r.summa
                                                                             else 0 end), 2) as tf1,
                                                               round(sum(case
                                                                             when u.usl_norm = 1 then case
                                                                                                          when n.koeff is not null and u.sptarn in (0, 2, 3, 4)
                                                                                                              then n.koeff
                                                                                                          else 1 end *
                                                                                                      r.summa
                                                                             else 0 end), 2) as tf2
                                                        from a_nabor2 n
                                                                 join usl u on n.usl = u.usl and
                                                                               to_date(mg_ || '01', 'YYYYMMDD') between n.dt1 and n.dt2
                                                                 join a_prices r on n.usl = r.usl and r.mg between n.mgfrom and n.mgto
                                                        where r.mg = mg_
                                                          and u.cd in ('г.вода', 'г.вода/св.нор')
                                                        group by n.lsk) c9 on s.lsk = c9.lsk

                                             left join (select n.lsk,
                                                               round(sum(case
                                                                             when u.usl_norm = 0 then case
                                                                                                          when n.koeff is not null and u.sptarn in (0, 2, 3, 4)
                                                                                                              then n.koeff
                                                                                                          else 1 end *
                                                                                                      r.summa
                                                                             else 0 end), 2) as tf1,
                                                               round(sum(case
                                                                             when u.usl_norm = 1 then case
                                                                                                          when n.koeff is not null and u.sptarn in (0, 2, 3, 4)
                                                                                                              then n.koeff
                                                                                                          else 1 end *
                                                                                                      r.summa
                                                                             else 0 end), 2) as tf2
                                                        from a_nabor2 n
                                                                 join usl u on n.usl = u.usl and
                                                                               to_date(mg_ || '01', 'YYYYMMDD') between n.dt1 and n.dt2
                                                                 join a_prices r on n.usl = r.usl and r.mg between n.mgfrom and n.mgto
                                                        where r.mg = mg_
                                                          --and u.cd in ('г.вода.ОДН', 'GW_SOD')
                                                          and u.cd in ('г.вода/св.нор')
                                                        group by n.lsk) c10 on s.lsk = c10.lsk

                                             left join (select n.lsk,
                                                               round(sum(case
                                                                             when u.usl_norm = 0 then case
                                                                                                          when n.koeff is not null and u.sptarn in (0, 2, 3, 4)
                                                                                                              then n.koeff
                                                                                                          else 1 end *
                                                                                                      r.summa
                                                                             else 0 end), 2) as tf1,
                                                               round(sum(case
                                                                             when u.usl_norm = 1 then case
                                                                                                          when n.koeff is not null and u.sptarn in (0, 2, 3, 4)
                                                                                                              then n.koeff
                                                                                                          else 1 end *
                                                                                                      r.summa
                                                                             else 0 end), 2) as tf2
                                                        from a_nabor2 n
                                                                 join usl u on n.usl = u.usl and
                                                                               to_date(mg_ || '01', 'YYYYMMDD') between n.dt1 and n.dt2
                                                                 join a_prices r on n.usl = r.usl and r.mg between n.mgfrom and n.mgto
                                                        where r.mg = mg_
                                                          and u.cd in ('канализ', 'канализ/св.нор')
                                                        group by n.lsk) c11 on s.lsk = c11.lsk

                                             left join (select n.lsk,
                                                               round(sum(case
                                                                             when u.usl_norm = 0 then case
                                                                                                          when n.koeff is not null and u.sptarn in (0, 2, 3, 4)
                                                                                                              then n.koeff
                                                                                                          else 1 end *
                                                                                                      r.summa
                                                                             else 0 end), 2) as tf1,
                                                               round(sum(case
                                                                             when u.usl_norm = 1 then case
                                                                                                          when n.koeff is not null and u.sptarn in (0, 2, 3, 4)
                                                                                                              then n.koeff
                                                                                                          else 1 end *
                                                                                                      r.summa
                                                                             else 0 end), 2) as tf2
                                                        from a_nabor2 n
                                                                 join usl u on n.usl = u.usl and
                                                                               to_date(mg_ || '01', 'YYYYMMDD') between n.dt1 and n.dt2
                                                                 join a_prices r on n.usl = r.usl and r.mg between n.mgfrom and n.mgto
                                                        where r.mg = mg_
                                                          and u.cd in ('отоп', 'отоп/св.нор')
                                                        group by n.lsk) c12 on s.lsk = c12.lsk

                                             left join (select s.lsk,
                                                               sum(s.test_opl) as test_opl,
                                                               sum(s.summa) as summa_itg,
                                                               null as nrm
                                                        from a_charge2 s
                                                                 join a_nabor2 n on s.lsk = n.lsk and
                                                                                    mg_ between n.mgfrom and n.mgto and
                                                                                    mg_ between s.mgfrom and s.mgto and
                                                                                    to_date(mg_ || '01', 'YYYYMMDD') between n.dt1 and n.dt2
                                                                 join usl u2 on n.usl = u2.usl and u2.cd in ('канализ')
                                                                 join usl u on s.usl = u.usl and u.cd in ('KAN_SOD') and s.type = 1 --канализ МКД
                                                        group by s.lsk) e13 on s.lsk = e13.lsk

                                             left join (select n.lsk,
                                                               round(sum(case
                                                                             when u.usl_norm = 0 then case
                                                                                                          when n.koeff is not null and u.sptarn in (0, 2, 3, 4)
                                                                                                              then n.koeff
                                                                                                          else 1 end *
                                                                                                      r.summa
                                                                             else 0 end), 2) as tf1,
                                                               round(sum(case
                                                                             when u.usl_norm = 1 then case
                                                                                                          when n.koeff is not null and u.sptarn in (0, 2, 3, 4)
                                                                                                              then n.koeff
                                                                                                          else 1 end *
                                                                                                      r.summa
                                                                             else 0 end), 2) as tf2
                                                        from a_nabor2 n
                                                                 join usl u on n.usl = u.usl and
                                                                               to_date(mg_ || '01', 'YYYYMMDD') between n.dt1 and n.dt2
                                                                 join a_prices r on n.usl = r.usl and r.mg = mg_
                                                        where mg_ between n.mgfrom and n.mgto
                                                          --and u.cd in ('KAN_SOD') --канализ МКД
                                                          and u.cd in ('канализ/св.нор')
                                                        group by n.lsk) c13 on s.lsk = c13.lsk

                                             left join (select n.lsk, max(round(n.koeff * r.summa, 2)) as tf1
                                                        from a_nabor2 n
                                                                 join usl u on n.usl = u.usl and
                                                                               to_date(mg_ || '01', 'YYYYMMDD') between n.dt1 and n.dt2
                                                                 join a_prices r on n.usl = r.usl and r.mg between n.mgfrom and n.mgto
                                                        where r.mg = mg_
                                                          and u.cd in ('выв.мус.') -- вывоз ТКО
                                                        group by n.lsk) c14 on s.lsk = c14.lsk

                                    order by l.name, s.nd, s.kw;

elsif сd_ in ('94') then -- следующий номер CD смотреть так же в REP_LSK!
        --ГИС ЖКХ
        --Шаблон импорта ЛС
            l_dt1 := gdt(0, 0, 0);
open prep_refcursor for select k.house_id,
                               k.reu,
                               ltrim(k.kw, '0') as kw,
                               decode(s.cd, 'MUN', 'Да', 'PRV', 'Нет', 'Да') as status,
                               k.lsk,
                               k2.elsk,
                               tp.cd as tp,
                               k2.lsk as lsk2,
                               k2.elsk as elsk2,
                               decode(tp2.cd, 'LSK_TP_MAIN', 'ЛС УО', 'ЛС КР') as tp2,
                               s.cd as stat_cd,
                               k.k_fam,
                               k.k_im,
                               k.k_ot,
                               to_char(k.opl, '999999.99') as opl,
                               k.opl,
                               k.kpr,
                               'Нет' as cad_no,                 --Кадастр.номера пока нет
                               k.entr,                          --подъезд
                               'Да' as comm_use,--Помещение, составляющее общее имущество в многоквартирном доме
                               'Отдельная квартира' as charact, --Характеристика помещения
                               x8.d1 as ent_date,               --дата постройки подъезда
                               b.shortname || '.' || b.offname || ', ' || a.shortname || '.' || a.offname ||
                               ', ' || h.housenum || ', ' || h.buildnum as adr,
                               100 as part,                     --доля
                               h.houseguid,
                               o.oktmo,
                               x6.s1 as cond,
                               x.n1 as house_opl,
                               x3.n1 as house_opl_pasp,
                               x4.n1 as house_year,
                               x2.n1 as house_et,
                               0 as house_unet,
                               'Новокузнецк' as clk_zone,
                               'Нет' as house_cult,
                               'Нет' as house_cad_no,           -- НЕ выгружать кадастровый номер пока, система пишет: INT004072 Сведения в ГКН не найдены.
                               x7.n1 as ent_et,
                               x8.d1 as ent_dt,
                               e.serviceid,
                               e.guid as premiseguid
                        from kart k
                                 join bs.addr_tp atp on atp.cd = 'Квартира'
                                 left join exs.eolink e on k.lsk = e.lsk -- лиц.счет
                                 left join exs.eolink e2
                                           on e2.id = e.parent_id and e2.fk_objtp = atp.id -- помещение
                                 join u_list tp
                                      on k.fk_tp = tp.id and k.psch not in (8, 9) --тип лиц.счета, не закрытый
                                 join u_list tp2 on tp2.cd = 'LSK_TP_MAIN' --для счета по капремонту
                                 left join kart k2 on k.k_lsk_id = k2.k_lsk_id and k2.fk_tp = tp2.id and
                                                      k2.psch not in (8, 9) -- счет по капремонту
                                 join status s on k.status = s.id
                                 join prep_house_fias p on k.house_id = p.fk_house
                                 join fias_house h on lower(p.houseguid) = lower(h.houseguid) --дом
                                 join fias_addr a on lower(h.aoguid) = lower(a.aoguid) --улица
                                 join fias_addr b on lower(a.parentguid) = lower(b.aoguid) --город
                                 join c_houses h2 on p.fk_house = h2.id
                                 join u_list u on u.cd = 'Общая площадь здания'
                                 left join t_objxpar x on h2.k_lsk_id = x.fk_k_lsk and x.fk_list = u.id
                                 join u_list u2 on u2.cd = 'Этажность'
                                 left join t_objxpar x2 on h2.k_lsk_id = x2.fk_k_lsk and x2.fk_list = u2.id
                                 join u_list u3 on u3.cd = 'Общ.пл.жил.пом.по пасп.'
                                 left join t_objxpar x3 on h2.k_lsk_id = x3.fk_k_lsk and x3.fk_list = u3.id
                                 join u_list u4 on u4.cd = 'Год ввода в экспл.'
                                 left join t_objxpar x4 on h2.k_lsk_id = x4.fk_k_lsk and x4.fk_list = u4.id
                                 join u_list u5 on u5.cd = 'Кадаст.номер'
                                 left join t_objxpar x5 on h2.k_lsk_id = x5.fk_k_lsk and x5.fk_list = u5.id
                                 join u_list u6 on u6.cd = 'Состояние'
                                 left join t_objxpar x6 on h2.k_lsk_id = x6.fk_k_lsk and x6.fk_list = u6.id

                                 left join c_vvod d
                                           on k.house_id = d.house_id and k.entr = d.vvod_num and d.usl is null --подъезд
                                 left join t_objxpar x7 on d.fk_k_lsk = x7.fk_k_lsk and x7.fk_list = u2.id
                                 join u_list u8 on u8.cd = 'Дата постройки'
                                 left join t_objxpar x8 on d.fk_k_lsk = x8.fk_k_lsk and x8.fk_list = u8.id

                                 join t_org o on b.aoguid = o.aoguid
                        where
                          --даты установить корректные! в пределах тек периода!!!
                            l_dt1 between h.startdate and h.enddate
                          and l_dt1 between b.startdate and b.enddate
                          and k.reu = reu_
                          --and utils.f_ord3(k.kw) is null --только не квартиры с индексом!!! ред.18.11.2016 Убрал ограничение, так как в ГИС-е оно тоже снято
                        order by k.reu, k.house_id, nvl(k.entr, 1), k.kw, b.offname, a.offname,
                                 h.housenum -- строго порядок по k.reu, k.house_id, k.entr иначе будет выгружаться некорректно!
;


/*1 Этажность
2 Дата постройки
3 Кадаст.номер
4 Статус культ.насл
5 Кол-во подземных этажей
6 Кол-во этажей
7 Площадь застройки
8 Общий износ здания
9 Год ввода в экспл.
10  Год постройки
11  Общ.пл.нежил.пом.по пасп.
12  Общ.пл.жил.пом.по пасп.
13  Общая площадь здания
14  Состояние
15  Ветхий
16  Исправный
17  Аварийный*/


elsif сd_ in ('97') then -- следующий номер CD смотреть так же в REP_LSK!
-- показания счетчиков
            if var_ = 3 then -- по дому
                sqlstr_ := 'and s.kul=''' || kul_ || ''' AND s.nd=''' || nd_ || '''';
elsif var_ = 2 then
                -- по РЭУ
                sqlstr_ := 'and s.reu=''' || reu_ || '''';
elsif var_ = 1 then
                -- по УК
                sqlstr_ := 'and r.trest=''' || trest_ || '''';
elsif var_ = 0 then
                -- все УК
                sqlstr_ := '';
end if;
if det_ = 3 then
                -- детализация по квартирам
                sql_det := 'p.name||'', ''||NVL(LTRIM(s.nd,''0''),''0'')||''-''||NVL(LTRIM(s.kw,''0''),''0'')';
else
                sql_det := 'p.name||'', ''||NVL(LTRIM(s.nd,''0''),''0'')';
end if;
open prep_refcursor for 'select s.reu, max(s.lsk) as lsk, s.k_lsk_id, ' || sql_det || ' AS predpr_det,
          det.ord1, u.usl, u.usl||''-''||u.nm as name_usl, sum(m.n1) as n1, sum(a.vol) as vol, r.name_reu
          FROM kart s
          join spul p on s.kul=p.id
          join s_reu_trest r on r.reu=s.reu
          join kart_detail det on s.lsk=det.lsk and det.is_main=1 -- основной лиц.сч.
          join usl u on 1=1
          left join meter m on m.fk_klsk_obj=s.k_lsk_id and m.fk_usl=u.usl
          left join (select t.fk_k_lsk, sum(t.n1) as vol from t_objxpar t
          join u_list us on t.fk_list=us.id and us.cd=''ins_vol_sch''
          where t.mg=' || mg_ || '
          group by t.fk_k_lsk) a on m.k_lsk_id=a.fk_k_lsk
          where u.usl in (''011'',''015'',''038'') ' || sqlstr_
          || ' group by s.reu, r.name_reu, s.k_lsk_id, ' || sql_det || ', det.ord1, u.usl, u.usl||''-''||u.nm
          order by det.ord1, u.usl';

elsif сd_ = '98' then
            --Задолжники OLAP-3 - Полыс, с развернутыми периодами задолженности
            cur_pay_ := utils.gets_bool_param('REP_CUR_PAY');
kpr1_ := utils.gets_int_param('REP_RNG_KPR1');
kpr2_ := utils.gets_int_param('REP_RNG_KPR2');
n1_ := utils.gets_list_param('REP_DEB_VAR');
l_sel_id := utils.gets_list_param('REP_TP_SCH_SEL');

if n1_ = 0 then
                n2_ := utils.gets_int_param('REP_DEB_MONTH');
else
                n2_ := utils.gets_int_param('REP_DEB_SUMMA');
end if;

if var_ = 3 then
                open prep_refcursor for select s.lsk,
                                               decode(k.psch, 9, 'Закрытые Л/С', 8, 'Старый фонд', 'Открытые Л/С') as psch,
                                               t.name_tr,
                                               t.name_reu,
                                               trim(s.name) as street,
                                               ltrim(s.nd, '0') as nd,
                                               ltrim(s.kw, '0') as kw,
                                               trim(s.name) || ', ' || ltrim(s.nd, '0') || '-' || ltrim(s.kw, '0') as adr,
                                               s.fio,
                                               case when cur_pay_ = 1 then s.cnt_month else s.cnt_month2 end as cnt_month,
                                               case when cur_pay_ = 1 then s.dolg else s.dolg2 end as dolg,
                                               s.period_deb,
                                               s.deb_month,
                                               g.name as deb_org,
                                               s.penya,
                                               s.nachisl,
                                               s.payment,
                                               s.dat,
                                               a.name as st_name
                                        from kart k,
                                             debits_lsk_month s,
                                             s_reu_trest t,
                                             t_org g,
                                             status a
                                        where decode(l_sel_id, 0, 2, 3) = s.var
                                          and decode(l_sel_id, 0, k.fk_tp, l_sel_id) = k.fk_tp-- либо совокупно по помещению, либо по конкретному типу лс
                                          and k.lsk = s.lsk
                                          and s.status = a.id
                                          and s.reu = t.reu
                                          and s.reu = reu_
                                          and s.kul = kul_
                                          and s.nd = nd_
                                          and s.mg between mg_ and mg1_
                                          and s.fk_deb_org = g.id(+)
                                          and ((cur_pay_ = 1 and s.cnt_month > 0) or (cur_pay_ = 0 and s.cnt_month2 > 0))
                                          and exists(select *
                                                     from kart k
                                                     where k.lsk = s.lsk
                                                       and (kpr1_ is not null and k.kpr >= kpr1_ or kpr1_ is null)
                                                       and (kpr2_ is not null and k.kpr <= kpr2_ or kpr2_ is null))
                                          and ((n1_ = 0 and s.cnt_month >= n2_) or (n1_ = 1 and s.dolg >= n2_))
                                        order by s.name, utils.f_order(s.nd, 6), utils.f_order(s.kw, 7);
elsif var_ = 2 then
                --По ЖЭО
                open prep_refcursor for select s.lsk,
                                               decode(k.psch, 9, 'Закрытые Л/С', 8, 'Старый фонд', 'Открытые Л/С') as psch,
                                               t.name_tr,
                                               t.name_reu,
                                               trim(s.name) as street,
                                               ltrim(s.nd, '0') as nd,
                                               ltrim(s.kw, '0') as kw,
                                               trim(s.name) || ', ' || ltrim(s.nd, '0') || '-' || ltrim(s.kw, '0') as adr,
                                               s.fio,
                                               case when cur_pay_ = 1 then s.cnt_month else s.cnt_month2 end as cnt_month,
                                               case when cur_pay_ = 1 then s.dolg else s.dolg2 end as dolg,
                                               s.period_deb,
                                               s.deb_month,
                                               g.name as deb_org,
                                               s.penya,
                                               s.nachisl,
                                               s.payment,
                                               s.dat,
                                               a.name as st_name
                                        from kart k,
                                             debits_lsk_month s,
                                             s_reu_trest t,
                                             t_org g,
                                             status a
                                        where decode(l_sel_id, 0, 2, 3) = s.var
                                          and decode(l_sel_id, 0, k.fk_tp, l_sel_id) = k.fk_tp-- либо совокупно по помещению, либо по конкретному типу лс
                                          and k.lsk = s.lsk
                                          and s.status = a.id
                                          and s.reu = t.reu
                                          and s.reu = reu_
                                          and s.mg between mg_ and mg1_
                                          and s.fk_deb_org = g.id(+)
                                          and ((cur_pay_ = 1 and s.cnt_month > 0) or (cur_pay_ = 0 and s.cnt_month2 > 0))
                                          and exists(select *
                                                     from kart k
                                                     where k.lsk = s.lsk
                                                       and (kpr1_ is not null and k.kpr >= kpr1_ or kpr1_ is null)
                                                       and (kpr2_ is not null and k.kpr <= kpr2_ or kpr2_ is null))
                                          and ((n1_ = 0 and s.cnt_month >= n2_) or (n1_ = 1 and s.dolg >= n2_))
                                        order by s.name, utils.f_order(s.nd, 6), utils.f_order(s.kw, 7);
elsif var_ = 1 then
                --По фонду
                open prep_refcursor for select s.lsk,
                                               decode(k.psch, 9, 'Закрытые Л/С', 8, 'Старый фонд', 'Открытые Л/С') as psch,
                                               t.name_tr,
                                               t.name_reu,
                                               trim(s.name) as street,
                                               ltrim(s.nd, '0') as nd,
                                               ltrim(s.kw, '0') as kw,
                                               trim(s.name) || ', ' || ltrim(s.nd, '0') || '-' || ltrim(s.kw, '0') as adr,
                                               s.fio,
                                               case when cur_pay_ = 1 then s.cnt_month else s.cnt_month2 end as cnt_month,
                                               case when cur_pay_ = 1 then s.dolg else s.dolg2 end as dolg,
                                               s.period_deb,
                                               s.deb_month,
                                               g.name as deb_org,
                                               s.penya,
                                               s.nachisl,
                                               s.payment,
                                               s.dat,
                                               a.name as st_name
                                        from kart k,
                                             debits_lsk_month s,
                                             s_reu_trest t,
                                             t_org g,
                                             status a
                                        where decode(l_sel_id, 0, 2, 3) = s.var
                                          and decode(l_sel_id, 0, k.fk_tp, l_sel_id) = k.fk_tp-- либо совокупно по помещению, либо по конкретному типу лс
                                          and k.lsk = s.lsk
                                          and s.status = a.id
                                          and s.reu = t.reu
                                          and t.trest = trest_
                                          and s.mg between mg_ and mg1_
                                          and s.fk_deb_org = g.id(+)
                                          and ((cur_pay_ = 1 and s.cnt_month > 0) or (cur_pay_ = 0 and s.cnt_month2 > 0))
                                          and exists(select *
                                                     from kart k
                                                     where k.lsk = s.lsk
                                                       and (kpr1_ is not null and k.kpr >= kpr1_ or kpr1_ is null)
                                                       and (kpr2_ is not null and k.kpr <= kpr2_ or kpr2_ is null))
                                          and ((n1_ = 0 and s.cnt_month >= n2_) or (n1_ = 1 and s.dolg >= n2_))
                                        order by s.name, utils.f_order(s.nd, 6), utils.f_order(s.kw, 7);
elsif var_ = 0 then
                --По городу
                open prep_refcursor for select s.lsk,
                                               decode(k.psch, 9, 'Закрытые Л/С', 8, 'Старый фонд', 'Открытые Л/С') as psch,
                                               t.name_tr,
                                               t.name_reu,
                                               trim(s.name) as street,
                                               ltrim(s.nd, '0') as nd,
                                               ltrim(s.kw, '0') as kw,
                                               trim(s.name) || ', ' || ltrim(s.nd, '0') || '-' || ltrim(s.kw, '0') as adr,
                                               s.fio,
                                               case when cur_pay_ = 1 then s.cnt_month else s.cnt_month2 end as cnt_month,
                                               case when cur_pay_ = 1 then s.dolg else s.dolg2 end as dolg,
                                               s.period_deb,
                                               s.deb_month,
                                               g.name as deb_org,
                                               s.penya,
                                               s.nachisl,
                                               s.payment,
                                               s.dat,
                                               a.name as st_name
                                        from kart k,
                                             debits_lsk_month s,
                                             s_reu_trest t,
                                             t_org g,
                                             status a
                                        where decode(l_sel_id, 0, 2, 3) = s.var
                                          and decode(l_sel_id, 0, k.fk_tp, l_sel_id) = k.fk_tp-- либо совокупно по помещению, либо по конкретному типу лс
                                          and k.lsk = s.lsk
                                          and s.status = a.id
                                          and s.reu = t.reu
                                          and s.mg between mg_ and mg1_
                                          and s.fk_deb_org = g.id(+)
                                          and ((cur_pay_ = 1 and s.cnt_month > 0) or (cur_pay_ = 0 and s.cnt_month2 > 0))
                                          and exists(select *
                                                     from kart k
                                                     where k.lsk = s.lsk
                                                       and (kpr1_ is not null and k.kpr >= kpr1_ or kpr1_ is null)
                                                       and (kpr2_ is not null and k.kpr <= kpr2_ or kpr2_ is null))
                                          and ((n1_ = 0 and s.cnt_month >= n2_) or (n1_ = 1 and s.dolg >= n2_))
                                        order by s.name, utils.f_order(s.nd, 6), utils.f_order(s.kw, 7);
end if;
elsif сd_ = '99' then
    -- список счетчиков, с окончанием срока поверки
    if var_ = 3 then
        --По дому
        open prep_refcursor for
select s.lsk, k.name||', '||NVL(LTRIM(s.nd,0),0)||'-'||NVL(LTRIM(s.kw,0),0) as adr,
       u.nm_for_bot as name_usl, m.npp, m.dt2
from kart s join spul k on s.kul = k.id and s.reu=coalesce(reu_,s.reu) and s.kul=coalesce(kul_,s.kul) and s.nd=coalesce(nd_,s.nd)
            join params p on 1=1
            join meter m on m.fk_klsk_obj=s.k_lsk_id and m.dt2
    between to_date(p.period||'01','YYYYMMDD') and add_months(last_day(to_date(p.period||'01','YYYYMMDD')),2) -- +2 месяца
            join usl u on m.fk_usl=u.usl
order by k.name, utils.f_order(s.nd,6), utils.f_order2(s.nd), utils.f_order(s.kw,7),
         u.npp;
end if;
elsif сd_ = '100' then
    -- список выполненных автоначислений, за период
    if var_ = 3 then
        --По дому
        open prep_refcursor for with a as (select s.lsk,
                                                  k.name || ', ' || nvl(ltrim(s.nd, 0), 0) || '-' ||
                                                  nvl(ltrim(s.kw, 0), 0) as adr,
                                                  u.nm_for_bot as name_usl,
                                                  sum(x.n1) as vol,
                                                  k.name as street,
                                                  utils.f_order(s.nd, 6) as ord_nd,
                                                  utils.f_order2(s.nd) as ord_nd2,
                                                  utils.f_order(s.kw, 7) as ord_kw,
                                                  m.id,
                                                  m.npp
                                           from kart s
                                                    join spul k
                                                         on s.kul = k.id and s.reu=coalesce(reu_,s.reu) and s.kul=coalesce(kul_,s.kul) and s.nd=coalesce(nd_,s.nd)
                                                    join meter m on m.fk_klsk_obj = s.k_lsk_id
                                                    join u_list l on l.cd = 'ins_vol_sch'
                                                    join t_objxpar x on m.k_lsk_id = x.fk_k_lsk and x.tp in (1, 2) and
                                                                        x.mg between mg_ and mg1_ and x.fk_list = l.id
                                                    join usl u on m.fk_usl = u.usl
                                           group by s.lsk, k.name || ', ' || nvl(ltrim(s.nd, 0), 0) || '-' ||
                                                           nvl(ltrim(s.kw, 0), 0), u.nm_for_bot, k.name,
                                                    utils.f_order(s.nd, 6), utils.f_order2(s.nd),
                                                    utils.f_order(s.kw, 7), m.id, m.npp)
                                select a.lsk, a.adr, a.name_usl, a.vol, a.npp
                                from a
                                order by a.street, a.ord_nd, a.ord_nd2, a.ord_kw, a.npp;
end if;

end if;


end rep_stat;

procedure rep_detail(p_cd in varchar2, p_mg in params.period%type, p_lsk in kart.lsk%type,
                         prep_refcursor in out rep_refcursor) is
    begin
        --дочерний датасет в мастер-детали на форме Form_olap
if p_cd = '83' then
            --1-ый датасет для отчета по нормативам, расценкам, поквартирно
            open prep_refcursor for 'select t.lsk, t.mg, u.usl, u.npp, u.nm2, t.cnt, u.ed_izm,
  case when t.limit is not null then to_char(t.limit)
    else to_char(t.val_group2) end as val_group2, t.cena
  from STATISTICS_LSK t, usl u, usl u2 where t.mg=:p_mg and t.lsk=:p_lsk
  /*and t.usl(+)=u.usl and t.usl=u2.fk_usl_chld(+)*/
  and t.usl=u.usl and u.fk_usl_chld=u2.usl(+)
  and u.cd in (''кап.'', ''г.вода'', ''х.вода'', ''х.вода.ОДН'',
   ''г.вода.ОДН'', ''эл.энерг.2'', ''эл.эн.ОДН'', ''найм'', ''канализ'')
--  and t.cnt is not null
--  group by t.lsk, u.usl, u.nm2, u.ed_izm, u.npp
  order by t.lsk, u.npp' using p_mg, p_lsk;
end if;

end;


procedure sqltofile(p_sql in varchar2,
                        p_dir in varchar2,
                        p_header_file in varchar2,
                        p_data_file in varchar2 := null,
                        p_dlmt in varchar2 := ';' --разделитель, по умолчанию - ';'
    ) is
        v_finaltxt varchar2(4000);
v_v_val    varchar2(4000);
v_n_val    number;
v_d_val    date;
v_ret      number;
c          number;
d          number;
col_cnt    integer;
f          boolean;
rec_tab    dbms_sql.desc_tab;
col_num    number;
v_fh       utl_file.file_type;
v_samefile boolean := (nvl(p_data_file, p_header_file) = p_header_file);
    begin
        --выгружает результат SQL в файл
c := dbms_sql.open_cursor;
dbms_sql.parse(c, p_sql, dbms_sql.native);
d := dbms_sql.execute(c);
dbms_sql.describe_columns(c, col_cnt, rec_tab);
for j in 1..col_cnt
            loop
                case rec_tab(j).col_type
                    when 1 then dbms_sql.define_column(c, j, v_v_val, 2000);
when 2 then dbms_sql.define_column(c, j, v_n_val);
when 12 then dbms_sql.define_column(c, j, v_d_val);
else dbms_sql.define_column(c, j, v_v_val, 2000); end case;
end loop;
-- This part outputs the HEADER
v_fh := utl_file.fopen(upper(p_dir), p_header_file, 'w', 32767);
for j in 1..col_cnt
            loop
                v_finaltxt := ltrim(v_finaltxt || p_dlmt || lower(rec_tab(j).col_name), p_dlmt);
end loop;
--  DBMS_OUTPUT.PUT_LINE(v_finaltxt);
utl_file.put_line(v_fh, v_finaltxt);
if not v_samefile then utl_file.fclose(v_fh); end if;
--
-- This part outputs the DATA
if not v_samefile then v_fh := utl_file.fopen(upper(p_dir), p_data_file, 'w', 32767); end if;
loop
            v_ret := dbms_sql.fetch_rows(c);
exit when v_ret = 0;
v_finaltxt := null;
for j in 1..col_cnt
                loop
                    case rec_tab(j).col_type
                        when 1 then dbms_sql.column_value(c, j, v_v_val);
v_finaltxt := ltrim(v_finaltxt || p_dlmt || '' || v_v_val || '', p_dlmt);
when 2 then dbms_sql.column_value(c, j, v_n_val);
v_finaltxt := ltrim(v_finaltxt || p_dlmt || v_n_val, p_dlmt);
when 12 then dbms_sql.column_value(c, j, v_d_val);
v_finaltxt := ltrim(v_finaltxt || p_dlmt ||
                                                         to_char(v_d_val, 'DD/MM/YYYY HH24:MI:SS'), p_dlmt);
else dbms_sql.column_value(c, j, v_v_val);
v_finaltxt := ltrim(v_finaltxt || p_dlmt || '' || v_v_val || '', p_dlmt); end case;
end loop;
utl_file.put_line(v_fh, v_finaltxt);
end loop;
utl_file.fclose(v_fh);
dbms_sql.close_cursor(c);
end;
end stat;
/

