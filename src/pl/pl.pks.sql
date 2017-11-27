CREATE OR REPLACE package UTIL.pl authid current_user
as
  logger logtype := logtype.init('anonymous');

  type attachment IS RECORD (
    name varchar2(1000),
    data_type varchar2(100) default 'text/plain',
    content clob default ''
  );

  type attachments is table of attachment;

  procedure send_mail (
    i_from varchar2,
    i_to varchar2,
    i_cc varchar2 default '',
    i_subject varchar2,
    i_body varchar2,
    i_attachments attachments default null
    i_host varchar2,
    i_port number default 25,
    i_username varchar2,
    i_password varchar2,
    i_content_type varchar2 default 'text/plain'
  );


  procedure sleep(i_millis in number) 
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
  procedure gather_table_stats(i_owner varchar2, i_table varchar2, i_part_name varchar2 default null);
  procedure manage_constraints(i_owner varchar2, i_table varchar2, i_order varchar2 default 'ENABLE');
  procedure enable_constraints(i_owner varchar2, i_table varchar2);
  procedure disable_constraints(i_owner varchar2, i_table varchar2);
  procedure manage_indexes(i_owner varchar2, i_table varchar2, i_order varchar2 default 'ENABLE');
  procedure enable_indexes(i_owner varchar2, i_table varchar2);
  procedure disable_indexes(i_owner varchar2, i_table varchar2);

  procedure drop_constraint(i_owner varchar2, i_table varchar2, i_constraint varchar2);
  procedure add_unique_constraint(i_owner varchar2, i_table varchar2, i_col_list varchar2, i_constraint varchar2);

  -- partition management  
  procedure add_partitions(i_owner varchar2, i_table varchar2,i_date date);
  procedure add_partition (i_owner varchar2, i_table varchar2);
  procedure truncate_partition(i_owner varchar2, i_table varchar2, i_partition varchar2);
  procedure truncate_partition(i_owner varchar2, i_table varchar2, i_date date);
  procedure truncate_partitions(i_owner varchar2, i_table varchar2, i_date date, i_num_part number);
  procedure truncate_partitions(i_owner varchar2, i_table varchar2, i_start_date date, i_end_date date);
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