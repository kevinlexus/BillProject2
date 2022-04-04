-- Add/modify columns
alter table T_ORG add var_deb_pen_pd_gis number default 0;
-- Add comments to the columns
comment on column T_ORG.var_deb_pen_pd_gis
    is 'Вариант заполнения задолженности и пени в ПД (0-обычный, 1-ГисЖкхЭкоТек (Кис.))';
