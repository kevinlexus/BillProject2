insert into reports (id, name, maxlevel, fk_type, expand_row, expand_col, can_detail, cd,
                     show_sel_org, show_sel_oper, sel_many, have_date, fname, ishead, iscnt,
                     issum, fldsum, show_pay, show_paychk, show_deb, frx_fname, isoem, frm_name,
                     show_total_row, show_total_col)
select 100, 'Список выполненных автоначислений', 3, 1, 1, 0, null,
       '100', null, null, null, null, null, null, null, null, null, null, null, null,
       'rep_meter_auto_chrg.fr3', null, null, 1, 1 from dual
where not exists (select * from reports t where t.cd='100');

begin
  logger.ins_period_rep('100', '202301', null, 0);
  logger.ins_period_rep('100', '202302', null, 0);
  logger.ins_period_rep('100', '202303', null, 0);
  logger.ins_period_rep('100', '202304', null, 0);
end;