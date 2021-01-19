  CREATE TABLE params (
    name  varchar2(100)   primary key,
    value varchar2(4000),
    description varchar2(4000)
  );

  CREATE TABLE logs (
    name        varchar2(1000),
    log_level   varchar2(20),
    start_date  date,
    end_date    date,
    message     varchar2(4000),
    statement   clob
  ) parallel nologging compress;

  create or replace type util.strings is table of varchar2(4000);

  create public synonym s for util.strings;
