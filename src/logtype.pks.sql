CREATE OR REPLACE TYPE util.logtype as object (

  name        varchar2(1000),
  log_level   varchar2(20),
  start_date  date,
  end_date    date, 
  message     varchar2(4000),
  statement   varchar2(32000),   
  -- initialize the log object with the name field
  static function init(name varchar2 default 'anonymous', log_level varchar2 default 'INFO') return logtype,

  member procedure persist,

  member procedure log(
    message   varchar2 default null, 
    statement varchar2 default null, 
    log_type  varchar2 default 'INFO'
  )

);