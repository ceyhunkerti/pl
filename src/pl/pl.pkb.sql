create or replace package body pl
as

  ------------------------------------------------------------------------------
  -- License
  ------------------------------------------------------------------------------
  -- BSD 2-Clause License
  -- Copyright (c) 2017, bluecolor, All rights reserved.
  -- Redistribution and use in source and binary forms, with or without modification, are permitted 
  -- provided that the following conditions are met:
  -- 
  -- * Redistributions of source code must retain the above copyright notice, 
  -- this list of conditions and the following disclaimer.
  -- 
  -- * Redistributions in binary form must reproduce the above copyright notice, 
  -- this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
  --
  -- THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, 
  -- INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR 
  -- PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY 
  -- DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, 
  -- PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) 
  -- HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, 
  -- STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, 
  -- EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
  ------------------------------------------------------------------------------

  -- name of this package
  gv_package varchar2(30) := 'PL'; 
  
  -- dynamic task for execute immediate
  gv_sql  long;
  gv_proc varchar2(128);


  -- exceptions
  partition_not_found   exception;
  table_not_partitioned exception;

  pragma exception_init(partition_not_found,   -20170);
  pragma exception_init(table_not_partitioned, -20171);


  function split(piv_str varchar2, piv_split varchar2 default ',') return dbms_sql.varchar2_table
  is
    i number := 0;
    v_str varchar2(4000) := piv_str;
    v_res dbms_sql.varchar2_table;
  begin
    loop
      i := i + 1;
      v_str := ltrim(v_str, piv_split);
      if v_str is not null and instr(v_str,piv_split) > 0 then
        v_res(i) := substr(v_str,1,instr(v_str,piv_split)-1);
        v_str := ltrim(v_str, v_res(i));
      else
        if  length(v_str) > 0 then
          v_res(i) := v_str;
        end if;
        exit;
      end if;
    end loop;

    return v_res;
  end;

  function find_max_partition(piv_owner varchar2, piv_table varchar2) return long
  is
    v_part long;
    v_partition_name varchar2(20);
    v_high_value varchar2(4000);
  begin

    select partition_name, high_value into v_partition_name, v_high_value 
    from
      (
        select 
          partition_name,
          high_value,
          row_number() over(partition by table_owner, table_name order by partition_position desc) rank_id 
        from all_tab_partitions 
        where table_owner = upper(piv_owner) and table_name = upper(piv_table)
      )
    where rank_id = 1;

    return v_partition_name||':'||v_high_value;
  end;  

  
  function find_partiotion_col_type(piv_owner varchar2, piv_table varchar2) return varchar2
  is
    v_col_data_type varchar2(20)  := 'DATE';
  begin
    gv_sql :='
      select 
        c.data_type
      from 
        ALL_TAB_COLS         c,
        ALL_PART_KEY_COLUMNS p
      where
        p.owner       = c.owner             and
        p.column_name = c.column_name       and
        c.table_name = '''||upper(piv_table)||''' and
        p.name = '''||upper(piv_table)||''' and
        p.owner= '''||upper(piv_owner)||'''
    ';
    execute immediate gv_sql into v_col_data_type;
    return v_col_data_type;
  end;



  function is_number(piv_str varchar2) return boolean 
  is
  begin
    return case nvl(length(trim(translate(piv_str, ' +-.0123456789', ' '))),0) when 0 then true else false end;
  end;

  ------------------------------------------------------------------------------
  -- return a string representation of date object.
  -- this method is useful when you use dynamic sql with exec. immediate
  -- and want to use a date object in your dynamic sql string.  
  ------------------------------------------------------------------------------
  function date_string(pid_date date) return varchar2
  is
  begin
    return 'to_date('''||to_char(pid_date, 'ddmmyyyy hh24:mi:ss')|| ''',''ddmmyyyy hh24:mi:ss'')';
  end;

  function escape_sq(piv_string varchar2) return varchar2
  is
  begin
    return replace(piv_string, '''', '''''');
  end;

  ------------------------------------------------------------------------------
  -- truncate table given with schema name, and table name 
  -- eg. pl.truncate_table('UTIL','LOGS')
  ------------------------------------------------------------------------------
  procedure truncate_table(piv_owner varchar2, piv_table in varchar2)
  is
    v_proc varchar2(1000) := gv_package || '.truncate_table';
  begin
    logger := logtype.init(v_proc);
    gv_sql := 'truncate table '|| piv_owner || '.' || piv_table;
    execute immediate gv_sql;
    logger.success(piv_owner || '.' || piv_table|| ' truncated', gv_sql);
  exception 
    when others then
      logger.error(SQLERRM, gv_sql);
      raise;
  end;


  ------------------------------------------------------------------------------
  -- drop table given with schema name, and table name 
  -- eg. pl.truncate_table('UTIL','LOGS') ignores errors if table not found
  ------------------------------------------------------------------------------
  procedure drop_table(piv_owner varchar2, piv_table in varchar2, pib_ignore_err boolean default true)
  is
    v_proc varchar2(1000) := gv_package || '.drop_table';
  begin
    logger := logtype.init(v_proc);
    gv_sql := 'drop table '|| piv_owner || '.' || piv_table;
    execute immediate gv_sql;
    logger.success(piv_owner || '.' || piv_table|| ' dropped', gv_sql);
    
  exception 
    when others then
      logger.error(SQLERRM, gv_sql);
      if pib_ignore_err = false then raise; end if;
  end;


  ------------------------------------------------------------------------------
  -- enable parallel dml for the current session
  ------------------------------------------------------------------------------
  procedure enable_parallel_dml
  is
    v_proc varchar2(1000) := gv_package || '.enable_parallel_dml';
  begin  
    gv_sql := 'alter session enable parallel dml';
    execute immediate gv_sql;
    logger.success( ' enabled parallel dml for current session', gv_sql);
  exception
    when others then
      logger.error(SQLERRM, gv_sql);
      raise;
  end;


  ------------------------------------------------------------------------------
  -- truncates given partition, raises error if partition not found.
  ------------------------------------------------------------------------------
  procedure truncate_partition(piv_owner varchar2, piv_table varchar2, piv_partition varchar2)
  is
    v_proc varchar2(1000) := gv_package || '.truncate_partition';
    v_cnt  number := 0;
  begin
    gv_sql := '
      select count(1)
      from all_tables
      where 
        table_name = upper('''||piv_table||''') and 
        owner = upper('''||piv_owner||''')      and
        partition_name = upper('''||piv_partition||''') 
    ';
    execute immediate gv_sql into v_cnt;

    if v_cnt = 0 then
      raise partition_not_found;
    else 
      gv_sql := 'alter table '|| piv_owner||'.'||piv_table||' truncate partition '||piv_partition;
      execute immediate gv_sql;
      logger.success( ' partition '||piv_partition||' truncated', gv_sql);
    end if;
  
  exception 
    when partition_not_found then
      logger.error(v_proc||' partition '||piv_partition||' not found!', gv_sql);
      raise_application_error (
        -20170,
        v_proc||' partition '||piv_partition||' not found!'
      );
    when others then 
      logger.error(SQLERRM, gv_sql);
      raise;
  end;
  

  ------------------------------------------------------------------------------
  -- drops given partition
  ------------------------------------------------------------------------------
  procedure drop_partition(piv_owner varchar2, piv_table varchar2, piv_partition varchar2)
  is
    v_proc varchar2(1000) := gv_package || '.drop_partition';
    v_cnt  number := 0;
  begin
    gv_sql := '
      select count(1)
      from all_tables
      where 
        table_name = upper('''||piv_table||''') and 
        owner = upper('''||piv_owner||''')      and
        partition_name = upper('''||piv_partition||''') 
    ';
    execute immediate gv_sql into v_cnt;

    if v_cnt = 0 then
      logger.info(' partition '||piv_partition||' not found', gv_sql);
    else 
      gv_sql := 'alter table '|| piv_owner||'.'||piv_table||' drop partition '||piv_partition;
      execute immediate gv_sql;
      logger.success( ' partition '||piv_partition||' dropped', gv_sql);
    end if;
  
  exception 
    when others then 
      logger.error( SQLERRM, gv_sql);
      raise;
  end;
  

  procedure drop_partition_lt(piv_owner varchar2, piv_table varchar2,pid_date date)
  is
  begin
    drop_partition(piv_owner, piv_table, pid_date,'<');
  end;  

  procedure drop_partition_lte(piv_owner varchar2, piv_table varchar2, pid_date date)
  is
  begin
    drop_partition(piv_owner, piv_table, pid_date,'<=');
  end;  

  procedure drop_partition_gt(piv_owner varchar2,piv_table varchar2,pid_date date)
  is
  begin
    drop_partition(piv_owner, piv_table, pid_date,'>');
  end;  

  procedure drop_partition_gte(piv_owner varchar2, piv_table varchar2,pid_date date)
  is
  begin
    drop_partition(piv_owner, piv_table, pid_date,'>=');
  end;  

  ------------------------------------------------------------------------------
  -- drops all partitions that are <= piv_max_date
  ------------------------------------------------------------------------------
  procedure drop_partition(
    piv_owner varchar2,piv_table varchar2, pid_date date, piv_operator varchar2 default '<'
  )
  is
    v_proc          varchar2(20)  := 'drop_partition';
    v_col_name      varchar2(200) := '';
    v_col_data_type varchar2(20)  := 'DATE';
    v_cnt           number;
    
  begin
    logger := logtype.init(v_proc);

    v_col_data_type := find_partiotion_col_type(piv_owner, piv_table);

    for c1 in (
      select 
        t.partition_name, t.high_value 
      from 
        all_tab_partitions t 
      where 
        upper(t.table_owner)= upper(piv_owner) and
        upper(t.table_name) = upper(piv_table)  
    ) loop

      gv_sql := 'select count(1) from dual where 
        ' || c1.high_value || piv_operator || 
        case v_col_data_type 
          when 'DATE' then date_string(trunc(pid_date)) 
          else to_char(pid_date,'yyyymmdd') 
        end; 
      execute immediate gv_sql into v_cnt;

      if v_cnt = 1 then 
        gv_sql := 'alter table '||piv_owner||'.'||piv_table||' drop partition '|| c1.partition_name; 
        execute immediate gv_sql;
        logger.success('op:'||piv_operator, gv_sql);
      end if;

    end loop;
    
  
  exception 
    when others then 
      logger.error(SQLERRM, gv_sql);
      raise;
  end;


  function find_partition_prefix(piv_part_name varchar2) return varchar2
  is
    v_chr char(2);
    v_part_prefix varchar2(10) := '';
  begin
    
    printl('v_part_name: ' || piv_part_name);    
  
    for i in 1 .. length(piv_part_name) loop
      v_chr := substr(piv_part_name,i,1);
      if(is_number(v_chr)) then
        exit;
      else 
        v_part_prefix := v_part_prefix || v_chr;
      end if;
    end loop;
    
    printl('part prefix: ' || v_part_prefix);    

    return trim(v_part_prefix);
  end;

  function to_date(piv_str long) return date
  is
    v_col   varchar2(1000);
    v_date  date;
  begin
    
    v_col := case length(piv_str) 
      when 4 then 'to_date('''||piv_str||''',''yyyy'')'   
      when 6 then 'to_date('''||piv_str||''',''yyyymm'')'
      when 8 then 'to_date('''||piv_str||''',''yyyymmdd'')'
      else piv_str 
    end;

    gv_sql := 'select '||v_col||' from dual';
    execute immediate gv_sql into v_date; 
    return v_date;
  end;

  function find_next_high_value(piv_col_data_type varchar2, piv_range_type char, piv_prev_high_val long) return long
  is
    v_high_date date;
  begin

    v_high_date := to_date(piv_prev_high_val);

    if piv_range_type = 'd' then 
      v_high_date := v_high_date + 1;
    else
      v_high_date := add_months(v_high_date,1);
    end if;  

    if piv_col_data_type = 'DATE' then return date_string(v_high_date); end if;

    return to_char(v_high_date, 
      case piv_range_type 
        when 'd' then 'yyyymmdd' 
        when 'm' then 'yyyymm'
        -- :todo implement others 
      end
    );    

  end;


  function find_partition_range_type(piv_part_name varchar2) return char 
  is
    v_part_prefix varchar2(10) := '';
    v_part_suffix varchar2(10) := '';
    v_range_type  char(1):= 'd';
  begin
    v_part_prefix := find_partition_prefix(piv_part_name);
    v_part_suffix := ltrim(piv_part_name, v_part_prefix);
    v_range_type  := case length(v_part_suffix) when 6 then 'm' else 'd' end;
    return v_range_type;
  end;

  function find_partition_range_type(piv_owner varchar2, piv_table varchar2) return char
  is
    v_part_name varchar2(100);
  begin
    gv_sql := '
      select partition_name from all_tab_partitions 
      where 
        owner = '''||upper(piv_owner) ||''' and
        table_name = '''||upper(piv_owner) ||''' and
        rownum = 1
    ';
    execute immediate gv_sql into v_part_name;
    return find_partition_range_type(v_part_name);   
  end;


  procedure add_partitions(piv_owner varchar2, piv_table varchar2,pid_date date)
  is
    v_part long := find_max_partition(piv_owner, piv_table);
    v_part_name   varchar2(50);
    v_high_value  long;
    v_part_prefix varchar2(10) := '';
    v_range_type  char(1):= 'd';
    v_partiotion_col_type varchar2(20) := find_partiotion_col_type(piv_owner, piv_table);  
    v_max_date    date;
  begin
    
    gv_proc   := 'pl.add_partitions'; 
    logger := logtype.init(gv_proc);
    
    v_part_name   := substr(v_part, 1, instr(v_part, ':')-1);
    v_part_prefix := find_partition_prefix(v_part_name);
    v_high_value  := ltrim(v_part, v_part_name||':');
    v_range_type  := find_partition_range_type(v_part_name); 

    if v_partiotion_col_type = 'DATE' then
      gv_sql := 'select '||v_high_value||' from dual';   
    elsif v_range_type = 'm' then
      gv_sql := 'select to_date('|| to_char(v_high_value) ||',''yyyymm'') from dual';
    elsif v_range_type = 'd' then
      gv_sql := 'select to_date('|| to_char(v_high_value) ||',''yyyymmdd'') from dual';    
    end if;

    execute immediate gv_sql into v_max_date;

    loop 

      v_max_date := case v_range_type
        when 'd' then v_max_date + 1
        when 'm' then add_months(v_max_date,1)
        when 'y' then add_months(v_max_date,12)
      end;
      add_partition(piv_owner, piv_table, v_max_date);

      exit when v_max_date > pid_date; 
    end loop;

--  exception 
--  when others then 
--    pl.logger.error(SQLERRM, gv_sql);
--    raise;
  end;


  procedure add_partition(piv_owner varchar2,piv_table varchar2, pid_date date)
  is
    v_high_value long;
    v_part_name  varchar2(50);
    v_partiotion_col_type varchar2(20);
    v_last_part   long;
    v_part_prefix varchar2(20);
    v_part_suffix varchar2(10);
    v_range_type  char(1):= 'd';
    v_date date;
  begin
  
    gv_proc := gv_package||'.add_partition';
    logger := logtype.init(gv_proc);

    v_partiotion_col_type := find_partiotion_col_type(piv_owner, piv_table);
    v_last_part   := find_max_partition(piv_owner, piv_table);
    v_part_name   := substr(v_last_part,1,instr(v_last_part,':')-1);
    v_high_value  := ltrim(v_last_part, v_part_name||':');
    v_part_prefix := find_partition_prefix(v_part_name);
    v_part_suffix := ltrim(v_part_name, v_part_prefix);
    v_range_type  := case length(v_part_suffix) when 6 then 'm' else 'd' end; 

    v_date := to_date(v_high_value);

    if v_range_type = 'm' then
      v_part_name := v_part_prefix || to_char(v_date, 'yyyymm');
    else 
      v_part_name := v_part_prefix || to_char(v_date, 'yyyymmdd');
    end if;
    
    gv_sql :=  'alter table '||piv_owner||'.'||piv_table||' add partition '|| v_part_name ||
      ' values less than (
          '||find_next_high_value(v_partiotion_col_type , v_range_type, v_high_value)||'
        )';
    
    printl(gv_sql);
    execute immediate gv_sql;
    
    logger.success('partition '||v_part_name ||' added to '||piv_owner||'.'||piv_table, gv_sql);

--  exception 
--  when others then 
--    pl.logger.error(SQLERRM, gv_sql);
--    raise;
  end;

  
  procedure window_partitions(piv_owner varchar2, piv_table varchar2, pid_date date, pin_window_size number)
  is
    v_range_type char(2) := find_partition_range_type(piv_owner, piv_table);
  begin
    gv_proc := 'pl.window_partitions';
    add_partitions(piv_owner,piv_table,pid_date);
    drop_partition_lt(piv_owner,piv_table, pid_date-pin_window_size);
  end;  



  ------------------------------------------------------------------------------
  -- check if table exists
  ------------------------------------------------------------------------------
  function table_exists(piv_owner varchar2, piv_table varchar2) return boolean
  IS
    v_proc varchar2(1000) := gv_package || '.table_exists';
    v_cnt  number := 0;
  begin

    gv_sql := '
      select count(1)
      from all_tables
      where table_name = upper ('''||piv_table||''') and owner = upper ('''||piv_owner||''')
    ';
    execute immediate gv_sql into v_cnt;
    return case v_cnt when 0 then false else true end; 

  exception
    when others then
      logger.error(SQLERRM, gv_sql);
      raise;
  end;

  procedure gather_table_stats(piv_owner varchar2, piv_table varchar2, piv_part_name varchar2 default null) 
  is
  begin
    dbms_stats.gather_table_stats (piv_owner,piv_table,piv_part_name);
  end;


  procedure exchange_partition(
    piv_owner     varchar2, 
    piv_table_1   varchar2, 
    piv_part_name varchar2,
    piv_table_2   varchar2,
    pib_validate  boolean default false
  ) IS
  begin

    gv_proc := gv_package||'.exchange_partition';
    logger := logtype.init(gv_proc);

    gv_sql := 
      'alter table '||piv_owner||'.'||piv_table_1||' exchange partition '|| piv_part_name||'
      with table '||piv_table_2||case pib_validate when false then ' without validation' else '' end;
    
    execute immediate gv_sql;

    logger.success('partition exchange', gv_sql);

  exception 
    when others then 
      pl.logger.error(SQLERRM, gv_sql);
      raise;  
  end;



  ------------------------------------------------------------------------------
  -- disable parallel dml for the current session
  ------------------------------------------------------------------------------
  procedure disable_parallel_dml
  is
    v_proc varchar2(1000) := gv_package || '.enable_parallel_dml';
  begin
      
      gv_sql := 'alter session disable parallel dml';
      execute immediate gv_sql;
      logger.success(' disabled parallel dml for current session', gv_sql);
  exception
    when others then
      logger.error( SQLERRM, gv_sql);
      raise;
  end;

  ------------------------------------------------------------------------------
  -- for those who struggels to remember dbms_output.putline! :) like me
  ------------------------------------------------------------------------------
  procedure printl(piv_message varchar2)
  is
  begin
    dbms_output.put_line(piv_message);
  end;

  procedure println(piv_message varchar2)
  is
  begin
    dbms_output.put_line(piv_message);
  end;

  procedure p(piv_message varchar2)
  is
  begin
    dbms_output.put_line(piv_message);
  end;

  ------------------------------------------------------------------------------
  -- for those who struggels to remember dbmsoutput.put! :) like me
  ------------------------------------------------------------------------------
  procedure print(piv_message varchar2)
  is
  begin
    dbms_output.put(piv_message);
  end;


end;