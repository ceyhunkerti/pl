<a rel="license" href="http://creativecommons.org/licenses/by/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by/4.0/">Creative Commons Attribution 4.0 International License</a>.

## PL/SQL Commons

  Contains common utility and logging methods.


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

  * **add_partitions**  

    ```sql
    procedure add_partitions(piv_owner varchar2, piv_table varchar2, pid_date date)
    ```

    ```sql
    Adds partitions to the given table up to the date given by the 'pid_date' parameter. 
    
    Arguments: 
       [piv_owner] (varchar2): Schema of the table
       [piv_table] (varchar2): Name of the table
       [pid_date]  (date): the date up to partitions will be added 
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
       [pid_date]  (date): the date partition will be created for 
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
       [piv_partition]  (varchar2): name of the partition 
    ```