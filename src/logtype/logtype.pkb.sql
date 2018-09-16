create or replace type body logtype as

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


  ------------------------------------------------------------------------------
  -- initialize a logtype object
  ------------------------------------------------------------------------------
  static function init(name varchar2 default 'anonymous') return logtype
  is
  begin
    return logtype(name, null, sysdate, null, null, null);
  end;


  static function init(package_name varchar2 , proc_name varchar2 )  return logtype
  is
    name varchar2(100) := package_name ||'.'||proc_name;
  begin
    return logtype(name, null, sysdate, null, null, null);
  end;
  ------------------------------------------------------------------------------
  -- store log object on logtable
  ------------------------------------------------------------------------------
  member procedure persist
  is
  begin
    insert into logs(
      name,
      log_level,
      start_date,
      end_date,
      message,
      statement
    )
    values(
      self.name,
      self.log_level,
      self.start_date,
      self.end_date,
      self.message,
      self.statement
    );
  end;

  ------------------------------------------------------------------------------
  -- general log method
  ------------------------------------------------------------------------------
  member procedure log(
    name      varchar2,
    message   varchar2 default null,
    statement long default null,
    log_level varchar2 default 'INFO'
  )
  is
    pragma autonomous_transaction;
    begin
    self.name      := name;
    self.message   := message;
    self.statement := statement;
    self.log_level := log_level;
    self.end_date  := sysdate;
    self.persist;
    commit;
  end;

  ------------------------------------------------------------------------------
  -- log info level messages
  ------------------------------------------------------------------------------
  member procedure info(message varchar2, statement long  default null)
  is
    pragma autonomous_transaction;
  begin
    self.log(self.name, message, statement,'INFO');
  end;


  ------------------------------------------------------------------------------
  -- log success level messages
  ------------------------------------------------------------------------------
  member procedure success(message varchar2, statement long  default null)
  is
    pragma autonomous_transaction;
  begin
    log(self.name, message, statement, 'SUCCESS');
  end;

  ------------------------------------------------------------------------------
  -- log warning level messages
  ------------------------------------------------------------------------------
  member procedure warning(message varchar2, statement long  default null)
  is
    pragma autonomous_transaction;
  begin
    log(self.name, message, statement, 'WARNING');
  end;

  ------------------------------------------------------------------------------
  -- log error level messages
  ------------------------------------------------------------------------------
  member procedure error(message varchar2, statement long default null)
  is
    pragma autonomous_transaction;
  begin
    log(self.name,message, statement,'ERROR');
  end;



  ------------------------------------------------------------------------------
  -- print current log to dbms output
  ------------------------------------------------------------------------------
  member procedure print
  is
  begin
    dbms_output.put_line('-- [Log: '||self.name||']');
    dbms_output.put_line(self.log_level);
    dbms_output.put_line(to_char(self.start_date,'yyyy.mm.dd h24:mi:ss') );
    dbms_output.put_line(to_char(self.end_date,'yyyy.mm.dd h24:mi:ss') );
    dbms_output.put_line(self.message);
    dbms_output.put_line('-- [/Log]');
  end;



end;