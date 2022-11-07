alter table exs.task add next_block_guid varchar2(36);
comment on column exs.task.next_block_guid is 'идентификатор, используемый для экспорта 2-го и последующих блоков данных';

