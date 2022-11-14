-- удалить ненужный параметр, касается задач:
-- SYSTEM_RPT_MET_EXP_VAL
-- SYSTEM_RPT_CHECK
-- SYSTEM_RPT_HOUSE_EXP
-- SYSTEM_RPT_ORG_EXP
-- SYSTEM_RPT_REF_EXP
-- SYSTEM_RPT_IMP_PD
-- SYSTEM_RPT_IMP_SUP_NOTIF
-- SYSTEM_RPT_IMP_NOTIF_CANCEL
-- SYSTEM_RPT_HOUSE_IMP
-- SYSTEM_RPT_MET_IMP_VAL
-- SYSTEM_RPT_DEB_SUB_EXCHANGE
-- SYSTEM_RPT_EXP_PD
-- SYSTEM_RPT_EXP_NOTIF

-- все они выполняются в 0 0 1 * * ?
delete from exs.taskxpar t where exists (select * from oralv.U_HFPAR s where s.cd='ГИС ЖКХ.Crone' and t.fk_par=s.id);