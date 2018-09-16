create or replace TYPE logtype as object (

  name        varchar2(1000),
  log_level   varchar2(20),
  start_date  date,
  end_date    date,
  message     varchar2(4000),
  statement   clob,


  static function init(name varchar2 default 'anonymous') return logtype,
  static function init(package_name varchar2 , proc_name varchar2 )  return logtype,


  member procedure persist,

  member procedure info(message varchar2, statement long default null),
  member procedure success(message varchar2, statement long default null),
  member procedure error(message varchar2, statement long default null),
  member procedure warning(message varchar2, statement long default null),

  member procedure log(
    name      varchar2,
    message   varchar2 default null,
    statement long default null,
    log_level varchar2 default 'INFO'
  ),


  member procedure print

);