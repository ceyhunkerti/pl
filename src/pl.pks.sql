create or replace package util.pl
as
  logger  logtype;
  
  function date_string(pid_date date) return varchar2;
  
  procedure truncate_table(piv_owner varchar2, piv_table in varchar2);

end;