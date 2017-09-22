create or replace package UTIL.pl authid current_user
as
  logger logtype := logtype.init('anonymous');

  MAIL_HOST varchar2(100) := 'MAIL_HOST';
  MAIL_PORT varchar2(100) := 'MAIL_PORT';
  MAIL_FROM varchar2(100) := 'MAIL_FROM';

  function is_number(piv_str varchar2) return boolean;
  function split(piv_str varchar2, piv_split varchar2 default ',', pin_limit number default null) return dbms_sql.varchar2_table;


  function date_string(pid_date date) return varchar2;
  
  procedure truncate_table(piv_owner varchar2, piv_table varchar2);
  procedure drop_table(piv_owner varchar2, piv_table varchar2, pib_ignore_err boolean default true);
  function table_exists(piv_owner varchar2, piv_table varchar2) return boolean;
  procedure gather_table_stats(piv_owner varchar2, piv_table varchar2, piv_part_name varchar2 default null);
  procedure manage_constraints(piv_owner varchar2, piv_table varchar2, piv_order varchar2 default 'enable');
  procedure enable_constraints(piv_owner varchar2, piv_table varchar2);
  procedure disable_constraints(piv_owner varchar2, piv_table varchar2);
  procedure manage_indexes(piv_owner varchar2, piv_table varchar2, piv_order varchar2 default 'enable');
  procedure enable_indexes(piv_owner varchar2, piv_table varchar2);
  procedure disable_indexes(piv_owner varchar2, piv_table varchar2);

  procedure drop_constraint(piv_owner varchar2, piv_table varchar2, piv_constraint varchar2);
  procedure add_unique_constraint(piv_owner varchar2, piv_table varchar2, piv_col_list varchar2, piv_constraint varchar2);

  -- partition management  
  procedure add_partitions(piv_owner varchar2, piv_table varchar2,pid_date date);
  procedure add_partition (piv_owner varchar2, piv_table varchar2,pid_date date);
  procedure truncate_partition(piv_owner varchar2, piv_table varchar2, piv_partition varchar2);
  procedure truncate_partition(piv_owner varchar2, piv_table varchar2, pin_date number);
  procedure truncate_partitions(piv_owner varchar2, piv_table varchar2, pin_date date, pin_num_part number);
  procedure truncate_partitions(piv_owner varchar2, piv_table varchar2, pin_start_date number, pin_end_date number);
  procedure drop_partition(piv_owner varchar2, piv_table varchar2, piv_partition varchar2);
  procedure drop_partition_lt (piv_owner varchar2, piv_table varchar2, pid_date date);  
  procedure drop_partition_lte(piv_owner varchar2, piv_table varchar2, pid_date date);  
  procedure drop_partition_gt (piv_owner varchar2, piv_table varchar2, pid_date date);  
  procedure drop_partition_gte(piv_owner varchar2, piv_table varchar2, pid_date date);
  procedure drop_partition_btw(piv_owner varchar2, piv_table varchar2, pid_start_date date, pid_end_date date);
  procedure window_partitions(piv_owner varchar2, piv_table varchar2, pid_date date, pin_window_size number);
  procedure exchange_partition(
    piv_owner     varchar2, 
    piv_table_1   varchar2, 
    piv_part_name varchar2,
    piv_table_2   varchar2,
    pib_validate  boolean default false
  );

  -- session management
  procedure enable_parallel_dml;
  procedure disable_parallel_dml;

--  procedure async_exec(piv_sql varchar2);

  procedure print_locks; 


  ------------------------------------------------------------------------------
  -- params
  procedure set_param(i_name varchar2, i_value varchar2);
  function find_param(i_name varchar2) return varchar2;
  function param_exists(i_name varchar2) return boolean;
  ------------------------------------------------------------------------------

  -- mail
  procedure send_mail(
    i_to      varchar2,
    i_subject varchar2,
    i_body    varchar2,
    i_cc      varchar2  default null
    i_from    varchar2  default null
  );

  procedure println(piv_message varchar2);
  procedure printl(piv_message varchar2);
  procedure p(piv_message varchar2);
  procedure print(piv_message varchar2);
  

end;