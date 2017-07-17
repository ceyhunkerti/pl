create or replace package body util.pl
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
  gv_sql long;


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
    pl.logger := logtype.init(v_proc);
    gv_sql := 'truncate table '|| piv_owner || '.' || piv_table;
    execute immediate gv_sql;
    pl.logger.success(piv_owner || '.' || piv_table|| ' truncated', gv_sql);
  exception 
    when others then
      pl.logger.error(SQLERRM, gv_sql);
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
    pl.logger := logtype.init(v_proc);
    gv_sql := 'drop table '|| piv_owner || '.' || piv_table;
    execute immediate gv_sql;
    pl.logger.success(piv_owner || '.' || piv_table|| ' dropped', gv_sql);
    
  exception 
    when others then
      pl.logger.error(SQLERRM, gv_sql);
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
    pl.logger.success(v_proc, ' enabled parallel dml for current session', gv_sql);
  exception
    when others then
      pl.logger.error(v_proc, SQLERRM, gv_sql);
      raise;
  end;


  ------------------------------------------------------------------------------
  -- truncates given partition, raises error if partition not found.
  ------------------------------------------------------------------------------
  procedure truncate_partition(piv_owner varchar2, piv_table varchar2, piv_partition varchar2)
  is
    v_proc varchar2(1000) := gv_package || '.truncate_partition';
    partition_not_found exception;
    pragma exception_init(partition_not_found, -20170);
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
      pl.logger.success(v_proc, ' partition '||piv_partition||' truncated', gv_sql);
    end if;
  
  exception 
    when partition_not_found then
      pl.logger.error(v_proc, v_proc||' partition '||piv_partition||' not found!', gv_sql);
      raise_application_error (
        -20100,
        v_proc||' partition '||piv_partition||' not found!'
      );
    when others then 
      pl.logger.error(v_proc, SQLERRM, gv_sql);
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
      pl.logger.info(v_proc, ' partition '||piv_partition||' not found', gv_sql);
    else 
      gv_sql := 'alter table '|| piv_owner||'.'||piv_table||' drop partition '||piv_partition;
      execute immediate gv_sql;
      pl.logger.success(v_proc, ' partition '||piv_partition||' dropped', gv_sql);
    end if;
  
  exception 
    when others then 
      pl.logger.error(v_proc, SQLERRM, gv_sql);
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
    piv_owner varchar2,piv_table varchar2, pid_date  date, piv_operator varchar2 default '<'
  )
  is
    v_proc          varchar2(20)  := 'drop_partition';
    v_col_name      varchar2(200) := '';
    v_col_data_type varchar2(20)  := 'DATE';
    v_cnt           number;
    
  begin

    pl.logger := util.logtype.init(v_proc);

    gv_sql :='
      select 
        c.column_name,
        c.data_type
      from 
        ALL_TAB_COLS         c,
        ALL_PART_KEY_COLUMNS p
      where
        p.owner       = c.owner             and
        p.column_name = c.column_name       and
        p.name = '''||upper(piv_table)||''' and
        p.owner= '''||upper(piv_owner)||'''
    ';
    execute immediate gv_sql into v_col_name, v_col_data_type;

    for c1 in (
      select t.partition_name, t.high_value from all_tab_partitions t 
      where 
        upper(t.table_owner)= upper(piv_owner) and
        upper(t.table_name) = upper(piv_table)  
    ) loop

      gv_sql := 'select count(1) from dual where 
        ' || c1.high_value || piv_operator || 
        case v_col_data_type 
          when 'DATE' then date_string(pid_date) 
          else to_char(pid_date,'yyyymmdd') 
        end; 
      execute immediate gv_sql into v_cnt;

      if v_cnt = 1 then 
        gv_sql := 'alter table '||piv_owner||'.'||piv_table||' drop partition '|| c1.partition_name; 
        execute immediate gv_sql;
        pl.logger.success(v_proc, 'op:'||piv_operator, gv_sql);
      end if;

    end loop;
    
  
  exception 
    when others then 
      pl.logger.error(v_proc, SQLERRM, gv_sql);
      raise;
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
      pl.logger.error(v_proc, SQLERRM, gv_sql);
      raise;
  end;


  procedure window_partitions(piv_owner varchar2, piv_table varchar2, pid_date date)
  is
  begin
    
    null;
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
      pl.logger.success(v_proc, ' disabled parallel dml for current session', gv_sql);
  exception
    when others then
      pl.logger.error(v_proc, SQLERRM, gv_sql);
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

  ------------------------------------------------------------------------------
  -- for those who struggels to remember dbmsoutput.put! :) like me
  ------------------------------------------------------------------------------
  procedure print(piv_message varchar2)
  is
  begin
    dbms_output.put(piv_message);
  end;


end;