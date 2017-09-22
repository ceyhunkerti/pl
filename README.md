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
    ```

  * Run the contents of [logs.ddl.sql](src/logtype/logs.ddl.sql)

  * Run the contents of [logtype.pks.sql](src/logtype/logtype.pks.sql) and [logtype.pkb.sql](src/logtype/logtype.pkb.sql) in order.

  * Run the contents of [pl.pks.sql](src/pl/pl.pks.sql) and [pl.pkb.sql](src/pl/pl.pkb.sql) in order.

  * Optionally create a public synonym for pl with;

    ```sql
      create public synonym pl for util.pl;  

      grant execute on util.logtype to public;

      grant execute on util.pl to public;
    ```


### API

  * **is_number**
  
    ```sql
    function is_number(piv_str varchar2) return boolean
    ```

    ```sql
    Checks if string is classified as a Number or not.
    
    Arguments: 
       [piv_str='']    (varchar2): The string to check.
    Returns
       (boolean): Returns true if string is numeric.
    ```


  * **split**

    ```sql
    function split(
      piv_str varchar2, 
      piv_split varchar2 default ',', 
      pin_limit number default null
    ) return dbms_sql.varchar2_table
    ``` 

    ```sql
      Splits string by separator.

      Arguments: 
         [piv_str='']    (varchar2): The string to split.
         [piv_split=','] (varchar2): The separator pattern to split by.
         [pin_limit]     (number): The length to truncate results to.
      Returns
         (varchar2_table): Returns the string segments.
    ```
  

  * **date_string**

    ```sql
    function date_string(pid_date date) return varchar2
    ```

    ```sql
    Returns a date as string containing to_date function. 
    Useful when used with 'execute immediate'
    
    Arguments: 
       [pid_date] (date): The date object.
    Returns:
       (varchar2): the date function string
       example: 'to_date(''20120101 22:12:00'',''yyyymmdd hh24:mi:ss'')' 
    ```

  * **truncate_table**

    ```sql
    procedure truncate_table(piv_owner varchar2, piv_table varchar2)
    ```

    ```sql
    Truncates the given table
    
    Arguments: 
       [piv_owner] (varchar2): Schema of the table
       [piv_table] (varchar2): Name of the table
    ```

  * **drop_table**

    ```
    procedure drop_table(piv_owner varchar2, piv_table varchar2, pib_ignore_err boolean default true);
    ```

    ```sql
    Drops the given table
    
    Arguments: 
       [piv_owner] (varchar2): Schema of the table
       [piv_table] (varchar2): Name of the table
       [pib_ignore_err=true] (boolean): when set to false raises error
    ```

  * **table_exists**

    ```sql
    function table_exists(piv_owner varchar2, piv_table varchar2) return boolean;
    ```

    ```sql
    Checks whether the given table exists or not 
    
    Arguments: 
       [piv_owner] (varchar2): Schema of the table
       [piv_table] (varchar2): Name of the table
    Returns:
       (boolean): true if table exists
    ```

  * **gather_table_stats**

    ```sql
    procedure gather_table_stats(
      piv_owner varchar2, 
      piv_table varchar2, 
      piv_part_name varchar2 default null) 
    ```

    ```sql
    Gather table/partition statistics 
    
    Arguments: 
       [piv_owner] (varchar2): Schema of the table
       [piv_table] (varchar2): Name of the table
       [piv_part_name] (varchar2): Name of the partition defaults to null
    ```

  * **manage_constraints**

    ```sql
    procedure manage_constraints(
      piv_owner varchar2, 
      piv_table varchar2, 
      piv_order varchar2 default 'enable') 
    ```

    ```sql
    Enable/Disable constraints for the given table. 
    
    Arguments: 
       [piv_owner] (varchar2): Schema of the table
       [piv_table] (varchar2): Name of the table
       [piv_order] (varchar2): DISABLE|ENABLE
    ```  

  * **enable_constraints**

    ```sql
    procedure enable_constraints(
      piv_owner varchar2, 
      piv_table varchar2) 
    ```

    ```sql
    Enable constraints for the given table. 
    
    Arguments: 
       [piv_owner] (varchar2): Schema of the table
       [piv_table] (varchar2): Name of the table
    ```

  * **disable_constraints**

    ```sql
    procedure disable_constraints(
      piv_owner varchar2, 
      piv_table varchar2) 
    ```

    ```sql
    Disable constraints for the given table. 
    
    Arguments: 
       [piv_owner] (varchar2): Schema of the table
       [piv_table] (varchar2): Name of the table
    ```

  * **manage_indexes**

    ```sql
    procedure manage_indexes(
      piv_owner varchar2, 
      piv_table varchar2, 
      piv_order varchar2 default 'enable') 
    ```

    ```sql
    Unusable/Rebuild indexes for the given table. 
    
    Arguments: 
       [piv_owner] (varchar2): Schema of the table
       [piv_table] (varchar2): Name of the table
       [piv_order] (varchar2): DISABLE|ENABLE
        DISABLE makes the indexes unusable
        ENABLE rebuilds the indexes
    ```  

  * **enable_indexes**

    ```sql
    procedure enable_indexes(
      piv_owner varchar2, 
      piv_table varchar2) 
    ```

    ```sql
    Rebuild indexes for the given table. 
    
    Arguments: 
       [piv_owner] (varchar2): Schema of the table
       [piv_table] (varchar2): Name of the table
    ```

  * **disable_indexes**

    ```sql
    procedure disable_indexes(
      piv_owner varchar2, 
      piv_table varchar2) 
    ```

    ```sql
    Make indexes unusable for the given table. 
    
    Arguments: 
       [piv_owner] (varchar2): Schema of the table
       [piv_table] (varchar2): Name of the table
    ```

  * **add_partitions**  

    ```sql
    procedure add_partitions(piv_owner varchar2, piv_table varchar2, pid_date date)
    ```

    ```sql
    Adds partitions to the given table up to the date given by the 'pid_date' parameter. 
    
    Arguments: 
       [piv_owner] (varchar2): Schema of the table
       [piv_table] (varchar2): Name of the table
       [pid_date] (date): the date up to partitions will be added 
    ```
  
  * **add_partition**

    ```sql
    procedure add_partition (piv_owner varchar2, piv_table varchar2,pid_date date)
    ```

    ```sql
    Adds a single partition to the given table with the date given by the 'pid_date' parameter. 
    
    Arguments: 
       [piv_owner] (varchar2): Schema of the table
       [piv_table] (varchar2): Name of the table
       [pid_date] (date): the date partition will be created for 
    ```

  * **truncate_partition**

    ```
    procedure truncate_partition(piv_owner varchar2, piv_table varchar2, piv_partition varchar2)
    ```

    ```sql
    Truncates the given partition.

    Arguments: 
       [piv_owner] (varchar2): Schema of the table
       [piv_table] (varchar2): Name of the table
       [piv_partition] (varchar2): name of the partition 
    ```

  * **drop_partition**

    ```
    procedure drop_partition(piv_owner varchar2, piv_table varchar2, piv_partition varchar2)
    ```

    ```sql
    Drops the given partition.

    Arguments: 
       [piv_owner] (varchar2): Schema of the table
       [piv_table] (varchar2): Name of the table
       [piv_partition] (varchar2): name of the partition 
    ```
  
  * **drop_partition_lt**

    ```sql
    procedure drop_partition_lt (piv_owner varchar2, piv_table varchar2, pid_date date); 
    ```

    ```sql
    Drops partitions less than the given date.

    Arguments: 
       [piv_owner] (varchar2): Schema of the table
       [piv_table] (varchar2): Name of the table
       [pid_date] (varchar2): date boundary 
    ```


  * **drop_partition_lte**
  
    ```sql
    procedure drop_partition_lte(piv_owner varchar2, piv_table varchar2, pid_date date)
    ```

    ```sql
    Drops partitions less than or equal to the given date.

    Arguments: 
       [piv_owner] (varchar2): Schema of the table
       [piv_table] (varchar2): Name of the table
       [pid_date] (varchar2): date boundary 
    ```


  * **drop_partition_gt**

    ```sql
    procedure drop_partition_gt (piv_owner varchar2, piv_table varchar2, pid_date date)
    ```

    ```sql
    Drops partitions greater than the given date.

    Arguments: 
       [piv_owner] (varchar2): Schema of the table
       [piv_table] (varchar2): Name of the table
       [pid_date] (varchar2): date boundary 
    ```

  
  * **drop_partition_gte**

    ```sql
    procedure drop_partition_gte (piv_owner varchar2, piv_table varchar2, pid_date date)
    ```

    ```sql
    Drops partitions greater than or equal to the given date.

    Arguments: 
       [piv_owner] (varchar2): Schema of the table
       [piv_table] (varchar2): Name of the table
       [pid_date] (varchar2): date boundary 
    ```

  * **window_partitions**

    ```sql
    procedure window_partitions(
      piv_owner varchar2, 
      piv_table varchar2, 
      pid_date date, 
      pin_window_size number)
    ```

    ```sql
    Manages partitions for the given table by fitting the partitions to the given date with pid_date parameter
    and given number by pin_number_size parameter. Basically it adds partitions until pid_date and drops partitions
    older than pin_window_size * (year|month|day)

    Arguments: 
       [piv_owner] (varchar2): Schema of the table
       [piv_table] (varchar2): Name of the table
       [pid_date] (varchar2): date boundary 
       [pin_window_size] (number): number of partitions to keep
    ```

  * **exchange_partition**

    ```sql
      procedure exchange_partition(
        piv_owner     varchar2, 
        piv_table_1   varchar2, 
        piv_part_name varchar2,
        piv_table_2   varchar2,
        pib_validate  boolean default false
      );
    ```

    ```sql
    Exchanges partition of table_1 with the table_2

    Arguments: 
       [piv_owner] (varchar2): Schema of the table
       [piv_table] (varchar2): Name of the table
       [piv_part_name] (varchar2): partitions to be exchanged 
       [piv_table_2] (varchar2): table to replace partition
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
      procedure async_exec(piv_sql varchar2)
    ```

    ```sql
    Execute given statement asynchronously.

    Arguments: 
       [piv_sql] (varchar2): Statement to execute 
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
    procedure println(piv_message varchar2);
    ```

    ```sql
    Print to dbms out. Shortcut for dbms_output.put_line

    Arguments: 
       [piv_message] (varchar2): Message to print
    ```


  * **printl**

    ```sql
    procedure printl(piv_message varchar2);
    ```

    ```sql
    Print to dbms out. Shortcut for dbms_output.put_line

    Arguments: 
       [piv_message] (varchar2): Message to print
    ```    

  * **p**

    ```sql
    procedure p(piv_message varchar2);
    ```

    ```sql
    Print to dbms out. Shortcut for dbms_output.put_line

    Arguments: 
       [piv_message] (varchar2): Message to print
    ```    

  * **print**

    ```sql
    procedure print(piv_message varchar2);
    ```

    ```sql
    Print to dbms out. Shortcut for dbms_output.put

    Arguments: 
       [piv_message] (varchar2): Message to print
    ```    
