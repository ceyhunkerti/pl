<a rel="license" href="http://creativecommons.org/licenses/by/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by/4.0/">Creative Commons Attribution 4.0 International License</a>.

## PL/SQL Commons

  Contains common utility and logging methods.


### INSTALLATION

  You can put the objects under any schema you like, but you can create a utility
  schema ,if you do not have already, and put all the objects under that schema.


  * Create a schema named **util** with:
    ```sql
      create user util identified by <password>;

      grant connect, resource to util;
    ```

  * Change the current schema to **util**

    ```sql
      alter session set current_schema = util;
    ```

  * Grant privileges
  
    ```sql
      GRANT SELECT ON dba_constraints TO util;
      
      GRANT SELECT ON dba_indexes TO util;

      GRANT SELECT ON dba_objects TO util;

      GRANT SELECT ON v$locked_object TO util;

    ```

  * Run the contents of [logs.ddl.sql](src/logtype/logs.ddl.sql)

  * Run the contents of [logtype.pks.sql](src/logtype/logtype.pks.sql) and [logtype.pkb.sql](src/logtype/logtype.pkb.sql) in order.

  * Run the contents of [pl.pks.sql](src/pl/pl.pks.sql) and [pl.pkb.sql](src/pl/pl.pkb.sql) in order.

  * Optionally create a public synonym for pl with;

    ```sql
      create public synonym pl for util.pl;  

      grant execute on util.logtype to public;

      grant execute on util.pl to public;

      GRANT SELECT ON v$lock TO util;

      GRANT SELECT ON v$session  TO util;
    ```


### API

  * **sleep**
    ```sql
    procedure sleep(i_millis in number) 
    ```

    ```sql
    Sleep given number of milliseconds. !DOES NOT uses dbms_lock
    
    Arguments: 
       [i_millis] (number): milliseconds
    ```


  * **is_number**
  
    ```sql
    function is_number(i_str varchar2) return boolean
    ```

    ```sql
    Checks if string is classified as a Number or not.
    
    Arguments: 
       [i_str='']    (varchar2): The string to check.
    Returns
       (boolean): Returns true if string is numeric.
    ```


  * **split**

    ```sql
    function split(
      i_str varchar2, 
      i_split varchar2 default ',', 
      i_limit number default null
    ) return dbms_sql.varchar2_table
    ``` 

    ```sql
      Splits string by separator.

      Arguments: 
         [i_str='']    (varchar2): The string to split.
         [i_split=','] (varchar2): The separator pattern to split by.
         [i_limit]     (number): The length to truncate results to.
      Returns
         (varchar2_table): Returns the string segments.
    ```
  

  * **date_string**

    ```sql
    function date_string(i_date date) return varchar2
    ```

    ```sql
    Returns a date as string containing to_date function. 
    Useful when used with 'execute immediate'
    
    Arguments: 
       [i_date] (date): The date object.
    Returns:
       (varchar2): the date function string
       example: 'to_date(''20120101 22:12:00'',''yyyymmdd hh24:mi:ss'')' 
    ```

  * **truncate_table**

    ```sql
    procedure truncate_table(i_owner varchar2, i_table varchar2)
    ```

    ```sql
    Truncates the given table
    
    Arguments: 
       [i_owner] (varchar2): Schema of the table
       [i_table] (varchar2): Name of the table
    ```

  * **drop_table**

    ```
    procedure drop_table(i_owner varchar2, i_table varchar2, pib_ignore_err boolean default true);
    ```

    ```sql
    Drops the given table
    
    Arguments: 
       [i_owner] (varchar2): Schema of the table
       [i_table] (varchar2): Name of the table
       [pib_ignore_err=true] (boolean): when set to false raises error
    ```

  * **table_exists**

    ```sql
    function table_exists(i_owner varchar2, i_table varchar2) return boolean;
    ```

    ```sql
    Checks whether the given table exists or not 
    
    Arguments: 
       [i_owner] (varchar2): Schema of the table
       [i_table] (varchar2): Name of the table
    Returns:
       (boolean): true if table exists
    ```

  * **gather_table_stats**

    ```sql
    procedure gather_table_stats(
      i_owner varchar2, 
      i_table varchar2, 
      i_part_name varchar2 default null) 
    ```

    ```sql
    Gather table/partition statistics 
    
    Arguments: 
       [i_owner] (varchar2): Schema of the table
       [i_table] (varchar2): Name of the table
       [i_part_name] (varchar2): Name of the partition defaults to null
    ```

  * **manage_constraints**

    ```sql
    procedure manage_constraints(
      i_owner varchar2, 
      i_table varchar2, 
      i_order varchar2 default 'enable') 
    ```

    ```sql
    Enable/Disable constraints for the given table. 
    
    Arguments: 
       [i_owner] (varchar2): Schema of the table
       [i_table] (varchar2): Name of the table
       [i_order] (varchar2): DISABLE|ENABLE
    ```  

  * **enable_constraints**

    ```sql
    procedure enable_constraints(
      i_owner varchar2, 
      i_table varchar2) 
    ```

    ```sql
    Enable constraints for the given table. 
    
    Arguments: 
       [i_owner] (varchar2): Schema of the table
       [i_table] (varchar2): Name of the table
    ```

  * **disable_constraints**

    ```sql
    procedure disable_constraints(
      i_owner varchar2, 
      i_table varchar2) 
    ```

    ```sql
    Disable constraints for the given table. 
    
    Arguments: 
       [i_owner] (varchar2): Schema of the table
       [i_table] (varchar2): Name of the table
    ```

  * **manage_indexes**

    ```sql
    procedure manage_indexes(
      i_owner varchar2, 
      i_table varchar2, 
      i_order varchar2 default 'enable') 
    ```

    ```sql
    Unusable/Rebuild indexes for the given table. 
    
    Arguments: 
       [i_owner] (varchar2): Schema of the table
       [i_table] (varchar2): Name of the table
       [i_order] (varchar2): DISABLE|ENABLE
        DISABLE makes the indexes unusable
        ENABLE rebuilds the indexes
    ```  

  * **enable_indexes**

    ```sql
    procedure enable_indexes(
      i_owner varchar2, 
      i_table varchar2) 
    ```

    ```sql
    Rebuild indexes for the given table. 
    
    Arguments: 
       [i_owner] (varchar2): Schema of the table
       [i_table] (varchar2): Name of the table
    ```

  * **disable_indexes**

    ```sql
    procedure disable_indexes(
      i_owner varchar2, 
      i_table varchar2) 
    ```

    ```sql
    Make indexes unusable for the given table. 
    
    Arguments: 
       [i_owner] (varchar2): Schema of the table
       [i_table] (varchar2): Name of the table
    ```

  * **add_partitions**  

    ```sql
    procedure add_partitions(i_owner varchar2, i_table varchar2, i_date date)
    ```

    ```sql
    Adds partitions to the given table up to the date given by the 'i_date' parameter. 
    
    Arguments: 
       [i_owner] (varchar2): Schema of the table
       [i_table] (varchar2): Name of the table
       [i_date] (date): the date up to partitions will be added 
    ```
  
  * **add_partition**

    ```sql
    procedure add_partition (i_owner varchar2, i_table varchar2,i_date date)
    ```

    ```sql
    Adds a single partition to the given table with the date given by the 'i_date' parameter. 
    
    Arguments: 
       [i_owner] (varchar2): Schema of the table
       [i_table] (varchar2): Name of the table
       [i_date] (date): the date partition will be created for 
    ```

  * **truncate_partition**

    ```
    procedure truncate_partition(i_owner varchar2, i_table varchar2, i_partition varchar2)
    ```

    ```sql
    Truncates the given partition.

    Arguments: 
       [i_owner] (varchar2): Schema of the table
       [i_table] (varchar2): Name of the table
       [i_partition] (varchar2): name of the partition 
    ```

  * **drop_partition**

    ```
    procedure drop_partition(i_owner varchar2, i_table varchar2, i_partition varchar2)
    ```

    ```sql
    Drops the given partition.

    Arguments: 
       [i_owner] (varchar2): Schema of the table
       [i_table] (varchar2): Name of the table
       [i_partition] (varchar2): name of the partition 
    ```
  
  * **drop_partition_lt**

    ```sql
    procedure drop_partition_lt (i_owner varchar2, i_table varchar2, i_date date); 
    ```

    ```sql
    Drops partitions less than the given date.

    Arguments: 
       [i_owner] (varchar2): Schema of the table
       [i_table] (varchar2): Name of the table
       [i_date] (varchar2): date boundary 
    ```


  * **drop_partition_lte**
  
    ```sql
    procedure drop_partition_lte(i_owner varchar2, i_table varchar2, i_date date)
    ```

    ```sql
    Drops partitions less than or equal to the given date.

    Arguments: 
       [i_owner] (varchar2): Schema of the table
       [i_table] (varchar2): Name of the table
       [i_date] (varchar2): date boundary 
    ```


  * **drop_partition_gt**

    ```sql
    procedure drop_partition_gt (i_owner varchar2, i_table varchar2, i_date date)
    ```

    ```sql
    Drops partitions greater than the given date.

    Arguments: 
       [i_owner] (varchar2): Schema of the table
       [i_table] (varchar2): Name of the table
       [i_date] (varchar2): date boundary 
    ```

  
  * **drop_partition_gte**

    ```sql
    procedure drop_partition_gte (i_owner varchar2, i_table varchar2, i_date date)
    ```

    ```sql
    Drops partitions greater than or equal to the given date.

    Arguments: 
       [i_owner] (varchar2): Schema of the table
       [i_table] (varchar2): Name of the table
       [i_date] (varchar2): date boundary 
    ```

  * **window_partitions**

    ```sql
    procedure window_partitions(
      i_owner varchar2, 
      i_table varchar2, 
      i_date date, 
      i_window_size number)
    ```

    ```sql
    Manages partitions for the given table by fitting the partitions to the given date with i_date parameter
    and given number by i_number_size parameter. Basically it adds partitions until i_date and drops partitions
    older than i_window_size * (year|month|day)

    Arguments: 
       [i_owner] (varchar2): Schema of the table
       [i_table] (varchar2): Name of the table
       [i_date] (varchar2): date boundary 
       [i_window_size] (number): number of partitions to keep
    ```

  * **exchange_partition**

    ```sql
      procedure exchange_partition(
        i_owner     varchar2, 
        i_table_1   varchar2, 
        i_part_name varchar2,
        i_table_2   varchar2,
        pib_validate  boolean default false
      );
    ```

    ```sql
    Exchanges partition of table_1 with the table_2

    Arguments: 
       [i_owner] (varchar2): Schema of the table
       [i_table] (varchar2): Name of the table
       [i_part_name] (varchar2): partitions to be exchanged 
       [i_table_2] (varchar2): table to replace partition
       [pib_validate=false] (boolean): validate partition after exchange
    ```

  * **enable_parallel_dml**
    
    ```sql
    procedure enable_parallel_dml
    ```

    ```sql
    Enable parallel dml for the current session.     
    ```

  
  * **disable_parallel_dml**
    
    ```sql
    procedure disable_parallel_dml
    ```

    ```sql
    Disable parallel dml for the current session.     
    ```


  * **async_exec**

    ```sql
      procedure async_exec(i_sql varchar2, i_name varchar2 default 'ASYNC_EXEC')
    ```

    ```sql
    Execute given statement asynchronously.

    Arguments: 
       [i_sql] (varchar2): Statement to execute 
       [i_name ='ASYNC_EXEC'] (varchar2): Name of the dbms job entry
    ```

  * **set_param**
    ```sql
      procedure set_param(i_name varchar2, i_value)
    ```

    ```sql
    Set parameter on `params` table

    Arguments: 
      [i_name] (varchar2): parameter name 
      [i_value] (varchar2): parameter value
    ```

  * **find_param**
    ```sql
      procedure find_param(i_name varchar2)
    ```

    ```sql
    Find given parameter

    Arguments: 
      [i_name] (varchar2): parameter name 
    Returns
      (varchar2): Returns parameter value
    ```

  * **param_exists**
    ```sql
      procedure param_exists(i_name varchar2)
    ```

    ```sql
      Check whether given parameter exists.

      Arguments: 
        [i_name] (varchar2): parameter name 
      Returns
        (boolean): true if param exists false otherwise
    ```

  * **send_mail**

    ```sql
      procedure send_mail(
        i_to      varchar2,
        i_subject varchar2,
        i_body    varchar2,
        i_cc      varchar2  default null
        i_from    varchar2  default null
      )
    ```

    ```sql
      Send mail to given recipients. Set mail server settings on `params` before
      using this method!
    ```

  * **print_locks**

    ```sql
    procedure print_locks
    ```

    ```sql
    Print locked objects.
    ```



  * **println**

    ```sql
    procedure println(i_message varchar2);
    ```

    ```sql
    Print to dbms out. Shortcut for dbms_output.put_line

    Arguments: 
       [i_message] (varchar2): Message to print
    ```


  * **printl**

    ```sql
    procedure printl(i_message varchar2);
    ```

    ```sql
    Print to dbms out. Shortcut for dbms_output.put_line

    Arguments: 
       [i_message] (varchar2): Message to print
    ```    

  * **p**

    ```sql
    procedure p(i_message varchar2);
    ```

    ```sql
    Print to dbms out. Shortcut for dbms_output.put_line

    Arguments: 
       [i_message] (varchar2): Message to print
    ```    

  * **print**

    ```sql
    procedure print(i_message varchar2);
    ```

    ```sql
    Print to dbms out. Shortcut for dbms_output.put

    Arguments: 
       [i_message] (varchar2): Message to print
    ```    
