CREATE TABLE logs (
  name        varchar2(1000),
  log_level   varchar2(20),
  start_date  date,
  end_date    date,
  message     varchar2(4000),
  statement   clob
) parallel nologging compress
;