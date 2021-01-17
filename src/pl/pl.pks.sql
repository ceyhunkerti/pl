CREATE OR REPLACE package UTIL.pl authid current_user
as
  logger logtype := logtype.init('anonymous');


  type varchar2_table is table of varchar2(4000);

  function format(i_base long, i_values strings) return long;

  function make_string(
    i_data varchar2_table,
    i_delimiter varchar2 default ',') return varchar2;

  function make_string(
    i_data dbms_sql.varchar2_table,
    i_delimiter varchar2 default ',') return long;


  procedure exec(i_sql varchar2, i_silent boolean default false);
  procedure exec(i_sql dbms_sql.varchar2_table, i_silent boolean default false);
  procedure exec_silent(i_sql varchar2);

  function index_ddls(i_owner varchar2, i_table varchar2) return dbms_sql.varchar2_table;
  function index_ddls(i_table varchar2)  return dbms_sql.varchar2_table;
  procedure drop_indexes(i_owner varchar2, i_table varchar2, i_silent boolean default true);
  procedure drop_indexes(i_table varchar2, i_silent boolean default true);

  procedure send_mail(
    i_from      varchar2,
    i_to        varchar2,
    i_subject   varchar2,
    i_message   varchar2,
    i_mime_type varchar2 default 'text/plain; charset=iso-8859-9',
    i_async     boolean default true
  );


  procedure sleep(P in number)
    as language java name 'java.lang.Thread.sleep(long)';

  function is_number(i_str varchar2) return boolean;
  function split(i_str varchar2, i_split varchar2 default ',', i_limit number default null) return dbms_sql.varchar2_table;

  function parse_date (i_str varchar2) return date;
  function date_string(i_date date) return varchar2;

  function table_exists(i_owner varchar2, i_table varchar2) return boolean;

  procedure truncate_table(i_table varchar2);
  procedure truncate_table(i_owner varchar2, i_table varchar2);
  procedure drop_table(i_table in varchar2, i_ignore_err boolean default true);
  procedure drop_table(i_owner varchar2, i_table varchar2, i_ignore_err boolean default true);
  procedure gather_table_stats(i_table varchar2, i_part_name varchar2 default null);
  procedure gather_table_stats(i_owner varchar2, i_table varchar2, i_part_name varchar2 default null);
  procedure manage_constraints(i_owner varchar2, i_table varchar2, i_order varchar2 default 'ENABLE', i_validate number := 0);
  procedure enable_constraints(i_owner varchar2, i_table varchar2, i_validate number := 0);
  procedure disable_constraints(i_owner varchar2, i_table varchar2);
  procedure manage_indexes(i_owner varchar2, i_table varchar2, i_order varchar2 default 'ENABLE');
  procedure enable_indexes(i_owner varchar2, i_table varchar2);
  procedure disable_indexes(i_owner varchar2, i_table varchar2);
  procedure unlock_table_stats(i_owner varchar2, i_table varchar2);

  procedure drop_constraint(i_owner varchar2, i_table varchar2, i_constraint varchar2, i_silent boolean := true);
  procedure add_unique_constraint(i_owner varchar2, i_table varchar2, i_col_list varchar2, i_constraint varchar2);

  -- partition management
  procedure add_partitions(i_owner varchar2, i_table varchar2, i_date date := sysdate);
  procedure add_partition (i_owner varchar2, i_table varchar2);
  procedure truncate_partition(i_owner varchar2, i_table varchar2, i_partition varchar2);
  procedure truncate_partition(i_owner varchar2, i_table varchar2, i_date date);
  procedure truncate_partitions(i_owner varchar2, i_table varchar2, i_date date, i_num_part number);
  procedure truncate_partitions(i_owner varchar2, i_table varchar2, i_start_date date, i_end_date date default sysdate );
  procedure drop_partition(i_owner varchar2, i_table varchar2, i_partition varchar2);
  procedure drop_partition_lt (i_owner varchar2, i_table varchar2, i_date date);
  procedure drop_partition_lte(i_owner varchar2, i_table varchar2, i_date date);
  procedure drop_partition_gt (i_owner varchar2, i_table varchar2, i_date date);
  procedure drop_partition_gte(i_owner varchar2, i_table varchar2, i_date date);
  procedure drop_partition_btw(i_owner varchar2, i_table varchar2, i_start_date date, i_end_date date);
  procedure window_partitions(i_owner varchar2, i_table varchar2, i_date date, i_window_size number);
  procedure exchange_partition(
    i_owner     varchar2,
    i_table_1   varchar2,
    i_part_name varchar2,
    i_table_2   varchar2,
    i_validate  boolean default false
  );

  -- session management
  procedure enable_parallel_dml;
  procedure disable_parallel_dml;

  procedure async_exec(i_sql varchar2, i_name varchar2 default 'ASYNC_EXEC');

  procedure print_locks;

  ------------------------------------------------------------------------------
  -- metadata
  function ddl(
    i_name varchar2,
    i_schema varchar2 default null,
    i_dblk varchar2 default null,
    i_type varchar2 default 'TABLE'
  ) return clob;

  ------------------------------------------------------------------------------
  -- data
  procedure imp(i_name varchar2, i_schema varchar2, i_dblk varchar2);

  -- todo
  -- function locks; --pipelined

  ------------------------------------------------------------------------------
  -- validation
  function is_email(i_email varchar2) return boolean;

  procedure println(i_message varchar2);
  procedure printl(i_message varchar2);
  procedure p(i_message varchar2);
  procedure print(i_message varchar2);



end;