CREATE OR REPLACE TYPE logtype as object (

  name        varchar2(1000),
  log_level   varchar2(20),
  start_date  date,
  end_date    date, 
  message     varchar2(4000),
  statement   varchar2(32000),   
  
  
  static function init(name varchar2 default 'anonymous') return logtype,
  static function init(package_name varchar2 default 'anonymous', proc_name varchar2 default null )

  member procedure persist,

  member procedure info(message varchar2, statement varchar2 default null),
  member procedure success(message varchar2, statement varchar2 default null),
  member procedure error(message varchar2, statement varchar2 default null),
  
  member procedure log(
    name      varchar2,
    message   varchar2 default null, 
    statement varchar2 default null, 
    log_level varchar2 default 'INFO'
  ),
  

  member procedure print

);