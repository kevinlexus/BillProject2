-- удалить, так как будут пересоздаваться уже корректные задания (была ошибка во взаимосвязях)
delete
from exs.task t
where t.fk_act in
      (select s.id
       from bs.list s
       where s.cd in ('GIS_EXP_HOUSE',
                      'GIS_EXP_METERS',
                      'GIS_IMP_ACCS',
                      'GIS_EXP_ACCS',
                      'GIS_IMP_METER_VALS',
                      'GIS_EXP_METER_VALS',
                      'GIS_IMP_PAY_DOCS',
                      'GIS_IMP_CANCEL_NOTIFS',
                      'GIS_IMP_SUP_NOTIFS',
                      'GIS_EXP_PAY_DOCS',
                      'GIS_EXP_BRIEF_SUPPLY_RES_CONTRACT',
                      'GIS_EXP_DEB_SUB_REQUEST',
                      'GIS_EXP_NOTIF_1',
                      'GIS_EXP_NOTIF_16',
                      'GIS_EXP_NOTIF_8',
                      'GIS_EXP_NOTIF_24',
                      'GIS_IMP_DEB_SUB_RESPONSE'
           ));
