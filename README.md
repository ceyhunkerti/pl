<a rel="license" href="http://creativecommons.org/licenses/by/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by/4.0/">Creative Commons Attribution 4.0 International License</a>.

## PL/SQL Commons

  Contains common utility and logging methods.
  A simple proc that uses `pl` looks like the following;

```sql
  -- PL in action !
  --
  PROCEDURE PRC_PROC_NAME(<i_input_var vartype>, <o_output_var vartype> ) IS
  BEGIN
    gv_proc := 'PRC_PROC_NAME'; -- name of the procedure
    -- gv_pkg -- constant name of the package set globally once

    -- initialize logger here
    pl.logger := util.logtype.init(gv_pkg ||'.'||gv_proc);

    --------------------
    -- proc body here --
    --------------------
    -- gv_sql : global variable
    --
    gv_sql := '
      -- sql statement to execute
    ';
    execute immediate gv_sql;

    -- success message
    pl.logger.success(SQL%ROWCOUNT,gv_sql);

    -- !!! commit should be after success message
    commit;

  EXCEPTION
    WHEN OTHERS THEN
      -- error message
      pl.logger.error(SQLCODE || ' : ' ||SQLERRM);
      raise;
  END;
```

You can see the recent logs in `logs` table
```sql
select * from util.logs order by 3 desc;
```

### INSTALLATION

  You can put the objects under any schema you like, but you can create a utility
  schema ,if you do not have already, and put all the objects under that schema.


  * Create a schema named **util** with:
    ```sql
      create user util identified by <password>;
    ```

  * Grant privileges

    ```sql
      GRANT CONNECT, RESOURCE TO UTIL;

      GRANT SELECT ON sys.dba_constraints TO util;

      GRANT SELECT ON sys.dba_indexes TO util;

      GRANT SELECT ON sys.dba_objects TO util;

      GRANT SELECT ON sys.v_$lock TO util;

      GRANT SELECT ON sys.v_$session TO util;

      GRANT SELECT ON sys.v_$locked_object to util;

      GRANT execute on sys.UTL_MAIL to util;
    ```

  * Change the current schema to **util**

    ```sql
      alter session set current_schema = util;
    ```

  * Run the contents of [init.ddl.sql](src/init.ddl.sql)

  * Run the contents of [logtype.pks.sql](src/logtype/logtype.pks.sql) and [logtype.pkb.sql](src/logtype/logtype.pkb.sql) in order.

  * Run the contents of [pl.pks.sql](src/pl/pl.pks.sql) and [pl.pkb.sql](src/pl/pl.pkb.sql) in order.

  * Optionally create a public synonym for pl with;

    ```sql
      create public synonym pl for util.pl;

      grant execute on util.logtype to public;

      grant execute on util.pl to public;
    ```


### API

  * **exec**
  ```sql
  -- execute given statement, raise exception if i_silent is set to false
  --
  -- Args:
  --    [i_sql varchar2]: statement to execute
  --    [i_silent boolean = False]: raise exception if i_silent is set to false, defaults to `False`
  procedure exec(i_sql varchar2, i_silent boolean default false);
  ```

  * **exec_silent**
  ```sql
  -- execute given statement and ignore error
  --
  -- Args:
  --    [i_sql varchar2]: statement to execute
  procedure exec_silent(i_sql varchar2);
  ```

  * **parse_date**
    ```sql
    -- parse given string to date
    --
    -- Args:
    --    [i_str number]: date in string
    function parse_date (i_str varchar2) return date
    ```

  * **sleep**
    ```sql
    -- Sleep given number of milliseconds. !DOES NOT uses dbms_lock
    --
    -- Args:
    --    [i_millis number]: milliseconds
    -- Returns:
    --    date: Returns date value of the given string
    procedure sleep(i_millis in number)
    ```


  * **is_number**
    ```sql
    -- Checks if string is classified as a Number or not.
    --
    -- Args:
    --    [i_str varchar2 = '']: The string to check.
    -- Returns
    --    boolean: Returns true if string is numeric.
    function is_number(i_str varchar2) return boolean
    ```

  * **split**
    ```sql
    -- Splits string by separator.
    --
    -- Args:
    --    [i_str varchar2 = '']: The string to split.
    --    [i_split varchar2 = ',']: The separator pattern to split by.
    --    [i_limit number = null]: The length to truncate results to.
    -- Returns:
    --    varchar2_table: Returns the string segments.
    function split(
      i_str varchar2,
      i_split varchar2 default ',',
      i_limit number default null
    ) return dbms_sql.varchar2_table
    ```

  * **date_string**
    ```sql
    -- Returns a date as string containing to_date function.
    -- Useful when used with 'execute immediate'
    --
    -- Args:
    --    [i_date date]: The date object to convert to string to_char representation.
    -- Returns:
    --   varchar2: the date function string
    --   example return value: `'to_date(''20120101 22:12:00'',''yyyymmdd hh24:mi:ss'')'`
    function date_string(i_date date) return varchar2
    ```

  * **truncate_table**
    ```sql
    -- Truncates the given table
    --
    -- Args:
    --    [i_owner varchar2]: Schema of the table
    --    [i_table varchar2]: Name of the table
    procedure truncate_table(i_owner varchar2, i_table varchar2)
    ```

  * **truncate_table**
    ```sql
    -- Truncates the given table
    --
    -- Args:
    --    [i_table varchar2]: Schema and Name of the table eg. `owner.table_name`
    procedure truncate_table(i_table varchar2)
    ```

  * **drop_table**
    ```sql
    -- Drops the given table
    --
    -- Args:
    --    [i_owner varchar2]: Schema of the table
    --    [i_table varchar2]: Name of the table
    --    [pib_ignore_err boolean = true]: when set to false raises error
    procedure drop_table(i_owner varchar2, i_table varchar2, pib_ignore_err boolean default true);
    ```

  * **drop_table**
    ```sql
    -- Drops the given table
    --
    -- Args:
    --    [i_table varchar2]: Schema and Name of the table eg: `owner.table_name`
    procedure drop_table(i_table varchar2);
    ```

  * **table_exists**
    ```sql
    -- Checks whether the given table exists or not
    --
    -- Args:
    --    [i_owner varchar2]
    --    [i_table varchar2]: Name of the table
    -- Returns:
    --    boolean: True if table exists
    function table_exists(i_owner varchar2, i_table varchar2) return boolean;
    ```

  * **gather_table_stats**
    ```sql
    -- Gather table/partition statistics
    --
    -- Args:
    --    [i_owner varchar2]: Schema of the table
    --    [i_table varchar2]: Name of the table
    --    [i_part_name varchar2 = null]: Name of the partition defaults to `null`
    procedure gather_table_stats(
      i_owner varchar2,
      i_table varchar2,
      i_part_name varchar2 default null)
    ```

  * **manage_constraints**
    ```sql
    -- Enable/Disable constraints for the given table.
    --
    -- Args:
    --    [i_owner varchar2]: Schema of the table
    --    [i_table varchar2]: Name of the table
    --    [i_order varchar2 = 'enable']: DISABLE|ENABLE
    procedure manage_constraints(
      i_owner varchar2,
      i_table varchar2,
      i_order varchar2 default 'enable')
    ```

  * **enable_constraints**
    ```sql
    -- Enable constraints for the given table.s
    --
    -- Args:
    --    [i_owner varchar2]: Schema of the table
    --    [i_table varchar2]: Name of the table
    procedure enable_constraints(i_owner varchar2, i_table varchar2)
    ```

  * **disable_constraints**
    ```sql
    -- Disable constraints for the given table.
    --
    -- Args:
    --    [i_owner varchar2]: Schema of the table
    --    [i_table varchar2]: Name of the table
    procedure disable_constraints(i_owner varchar2, i_table varchar2)
    ```

  * **manage_indexes**
    ```sql
    -- Unusable/Rebuild indexes for the given table.
    --
    -- Args:
    --  [i_owner varchar2]: Schema of the table
    --  [i_table varchar2]: Name of the table
    --  [i_order varchar2 = 'enable']: DISABLE|ENABLE
    --    DISABLE makes the indexes unusable
    --    ENABLE rebuilds the indexes
    procedure manage_indexes(
      i_owner varchar2,
      i_table varchar2,
      i_order varchar2 default 'enable')
    ```

  * **enable_indexes**
    ```sql
    -- Rebuild indexes for the given table.
    --
    -- Args:
    --    [i_owner varchar2]: Schema of the table
    --    [i_table varchar2]: Name of the table

    procedure enable_indexes(i_owner varchar2, i_table varchar2)
    ```

  * **disable_indexes**
    ```sql
    -- Make indexes unusable for the given table.
    --
    -- Args:
    --    [i_owner varchar2]: Schema of the table
    --    [i_table varchar2]: Name of the table
    procedure disable_indexes(i_owner varchar2, i_table varchar2)
    ```

  * **add_partitions**
    ```sql
    -- Adds partitions to the given table up to the date given by the `i_date` parameter.
    --
    -- Args:
    --    [i_owner varchar2: Schema of the table
    --    [i_table varchar2: Name of the table
    --    [i_date date]: the date up to partitions will be added
    procedure add_partitions(i_owner varchar2, i_table varchar2, i_date date)
    ```

  * **add_partition**
    ```sql
    -- Adds a single partition to the given table with the date given by the 'i_date' parameter.
    --
    -- Args:
    --    [i_owner varchar2]: Schema of the table
    --    [i_table varchar2]: Name of the table
    --    [i_date date]: the date partition will be created for
    procedure add_partition (i_owner varchar2, i_table varchar2,i_date date)
    ```

  * **truncate_partition**
    ```sql
    -- Truncates the given partition.
    --
    -- Args:
    --    [i_owner varchar2]: Schema of the table
    --    [i_table varchar2]: Name of the table
    --    [i_partition varchar2]: name of the partition
    procedure truncate_partition(i_owner varchar2, i_table varchar2, i_partition varchar2)
    ```

  * **drop_partition**
    ```sql
    -- Drops the given partition.
    --
    -- Args:
    --    [i_owner varchar2]: Schema of the table
    --    [i_table varchar2]: Name of the table
    --    [i_partition varchar2]: name of the partition
    procedure drop_partition(i_owner varchar2, i_table varchar2, i_partition varchar2)
    ```

  * **drop_partition_lt**
    ```sql
    -- Drops partitions less than the given date.
    --
    -- Args:
    --    [i_owner varchar2]: Schema of the table
    --    [i_table varchar2]: Name of the table
    --    [i_date varchar2]: date boundary
    procedure drop_partition_lt (i_owner varchar2, i_table varchar2, i_date date);
    ```

  * **drop_partition_lte**
    ```sql
    -- Drops partitions less than or equal to the given date.
    --
    -- Args:
    --    [i_owner varchar2]: Schema of the table
    --    [i_table varchar2]: Name of the table
    --    [i_date varchar2]: date boundary
    procedure drop_partition_lte(i_owner varchar2, i_table varchar2, i_date date)
    ```

  * **drop_partition_gt**

    ```sql
    -- Drops partitions greater than the given date.
    --
    -- Args:
    --    [i_owner varchar2): Schema of the table
    --    [i_table varchar2): Name of the table
    --    [i_date varchar2): date boundary
    procedure drop_partition_gt (i_owner varchar2, i_table varchar2, i_date date)
    ```

  * **drop_partition_gte**
    ```sql
    -- Drops partitions greater than or equal to the given date.
    --
    -- Args:
    --    [i_owner varchar2]: Schema of the table
    --    [i_table varchar2]: Name of the table
    --    [i_date varchar2]: date boundary
    procedure drop_partition_gte (i_owner varchar2, i_table varchar2, i_date date)
    ```

  * **window_partitions**
    ```sql
    -- Manages partitions for the given table by fitting the partitions to the given date with i_date parameter
    -- and given number by i_number_size parameter. Basically it adds partitions until i_date and drops partitions
    -- older than i_window_size * (year|month|day)
    --
    -- Args:
    --   [i_owner varchar2]: Schema of the table
    --   [i_table varchar2]: Name of the table
    --   [i_date varchar2]: date boundary
    --   [i_window_size number]: number of partitions to keep
    procedure window_partitions(
      i_owner varchar2,
      i_table varchar2,
      i_date date,
      i_window_size number)
    ```

  * **exchange_partition**
    ```sql
    -- Exchanges partition of table_1 with the table_2
    --
    -- Args:
    --    [i_owner varchar2]: Schema of the table
    --    [i_table varchar2]: Name of the table
    --    [i_part_name varchar2]: partitions to be exchanged
    --    [i_table_2 varchar2]: table to replace partition
    --    [pib_validate boolean =false]: validate partition after exchange
    procedure exchange_partition(
      i_owner     varchar2,
      i_table_1   varchar2,
      i_part_name varchar2,
      i_table_2   varchar2,
      pib_validate  boolean default false
    );
    ```

  * **enable_parallel_dml**
    ```sql
    -- Enable parallel dml for the current session.
    procedure enable_parallel_dml
    ```

  * **disable_parallel_dml**

    ```sql
    -- Disable parallel dml for the current session.
    procedure disable_parallel_dml
    ```


  * **async_exec**
    ```sql
      -- Execute given statement asynchronously.
      --
      -- Args:
      --    [i_sql varchar2]: Statement to execute
      --    [i_name varchar2 = 'ASYNC_EXEC']: Name of the dbms job entry

      procedure async_exec(i_sql varchar2, i_name varchar2 default 'ASYNC_EXEC')
    ```

  * **set_param**
    ```sql
      -- Set parameter on `params` table
      --
      -- Args:
      --    [i_name varchar2]: parameter name
      --    [i_value varchar2]: parameter value
      procedure set_param(i_name varchar2, i_value)
    ```

  * **find_param**
    ```sql
      -- Find given parameter
      --
      -- Args:
      --    [i_name varchar2]: parameter name
      -- Returns
      --    varchar2: Returns parameter value
      procedure find_param(i_name varchar2)
    ```

  * **param_exists**
    ```sql
      -- Check whether given parameter exists.
      -- Args:
      --    [i_name] (varchar2): parameter name
      --    boolean: true if param exists false otherwise
      function param_exists(i_name varchar2) return boolean;
    ```

  * **send_mail**
    ```sql
      -- Send mail to given recipients. Set mail server settings on `params` before
      -- using this method!
      -- Args:
      --  ** Mail options
      procedure send_mail(
        i_to      varchar2,
        i_subject varchar2,
        i_body    varchar2,
        i_cc      varchar2  default null
        i_from    varchar2  default null
      )
    ```

  * **is_email**
    ```sql
      -- Test given string is a valid email address
      --
      -- Args:
      --   [i_email varchar2]: given email address
      -- Returns:
      --   boolean: true if input is a valid email address
      function is_email(i_email varchar2)
    ```

  * **ddl**
    ```sql
      -- Retrieve metadata of the object(s). If only name is given returns all matching objects'' metadata
      --
      -- Args:
      --    [i_name varchar2]: name of the object
      --    [i_schema varchar2]: owner of the object
      --    [i_dblk varchar2]: db-link for remote objects
      --    [i_type varchar2 ='TABLE']: object type
      -- Returns
      --    boolean: true if param exists false otherwise
      function ddl(
        i_name varchar2,
        i_schema varchar2 default null,
        i_dblk varchar2 default null,
        i_type varchar2 default 'TABLE'
      ) return clob;
    ```

  * **print_locks**
    ```sql
    -- Print locked objects.
    procedure print_locks
    ```


  * **println**
    ```sql
    -- Print to dbms out. Shortcut for dbms_output.put_line
    --
    -- Args:
    --    [i_message varchar2]: Message to print
    procedure println(i_message varchar2);
    ```

  * **printl**
    ```sql
    -- Print to dbms out. Shortcut for dbms_output.put_line
    --
    -- Args:
    --    [i_message varchar2]: Message to print
    procedure printl(i_message varchar2);
    ```

  * **p**
    ```sql
    -- Print to dbms out. Shortcut for dbms_output.put_line
    --
    -- Args:
    --    [i_message varchar2]: Message to print
    procedure p(i_message varchar2);
    ```

  * **print**
    ```sql
    -- Print to dbms out. Shortcut for dbms_output.put_line
    --
    -- Args:
    --    [i_message] (varchar2): Message to print
    procedure print(i_message varchar2);
    ```