create or replace package body util.pl
as

  ------------------------------------------------------------------------------
  -- License
  ------------------------------------------------------------------------------
  -- BSD 2-Clause License
  -- Copyright (c) 2017, bluecolor All rights reserved.
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
  IS
  begin
    return 'to_date('''||to_char(pid_date, 'ddmmyyyy h24:mi:ss')|| ''',''ddmmyyyy h24:mi:ss'') ';
  end;

  ------------------------------------------------------------------------------
  -- truncate table given with schema name, and table name 
  -- eg. pl.truncate_table('UTIL','LOGS')
  ------------------------------------------------------------------------------
  procedure truncate_table(piv_owner varchar2, piv_table in varchar2)
  is
    v_proc varchar2(1000) := gv_package || 'truncate_table';
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

end;