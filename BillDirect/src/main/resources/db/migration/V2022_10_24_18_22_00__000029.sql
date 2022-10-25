insert into spr_params
    (cd, name, cdtp, parn1)
select 'DAY_RESTRICT_METER_VOL', 'День ограничения обмена по счетчикам, 0 - без ограничений', 0, 0
from dual
where not exists
    (select * from spr_params t where t.cd = 'DAY_RESTRICT_METER_VOL');
