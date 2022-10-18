alter table k_lsk add dt_gen_deb_pen date default sysdate;

comment on column k_lsk.dt_gen_deb_pen is 'Дата последнего формирования движения и пени по фин.л.с';


