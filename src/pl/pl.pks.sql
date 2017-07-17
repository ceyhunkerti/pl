create or replace package util.pl authid current_user
as
  logger  logtype;
  
  function date_string(pid_date date) return varchar2;
  
  procedure truncate_table(piv_owner varchar2, piv_table varchar2);
  
  procedure drop_table(piv_owner varchar2, piv_table varchar2, pib_ignore_err boolean default true);
  
  function  table_exists(piv_owner varchar2, piv_table varchar2) return boolean;
  
  procedure truncate_partition(piv_owner varchar2, piv_table varchar2, piv_partition varchar2);
  
  procedure drop_partition(piv_owner varchar2, piv_table varchar2, piv_partition varchar2);
  procedure drop_partition(piv_owner varchar2, piv_table varchar2, pid_date date, piv_operator varchar2 default '<' );
  procedure drop_partition_lt (piv_owner varchar2, piv_table varchar2, pid_date date);  
  procedure drop_partition_lte(piv_owner varchar2, piv_table varchar2, pid_date date);  
  procedure drop_partition_gt (piv_owner varchar2, piv_table varchar2, pid_date date);  
  procedure drop_partition_gte(piv_owner varchar2, piv_table varchar2, pid_date date);

  procedure enable_parallel_dml;
  procedure disable_parallel_dml;


  procedure printl(piv_message varchar2);
  procedure print (piv_message varchar2);
  

end;