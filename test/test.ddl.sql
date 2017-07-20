create table t (
  x date    primary key,
  y number  
)
partition by range(x) (
  partition p20170701 values less than (to_date('20170702','YYYYMMDD')),
  partition p20170702 values less than (to_date('20170703','YYYYMMDD')),
  partition p20170703 values less than (to_date('20170704','YYYYMMDD')),
  partition p_default values less than (maxvalue)
)


CREATE TABLE t1 (id NUMBER, c1 DATE)
PARTITION BY RANGE (c1)
  (PARTITION p20170714 VALUES LESS THAN (TO_DATE('2017-07-15', 'YYYY-MM-DD')),
   PARTITION p20170715 VALUES LESS THAN (TO_DATE('2017-07-16', 'YYYY-MM-DD')),
   PARTITION p20170716 VALUES LESS THAN (TO_DATE('2018-07-17', 'YYYY-MM-DD'))
  );

select * from all_tab_partitions
where table_name = 'T1'


begin
--if util.pl.is_number('1') = true then util.pl.printl(1);else util.pl.printl(2); end if;
  -- util.pl.add_partitions('DDBWH','t1', sysdate+5);
  util.pl.drop_partition_gt('DDBWH','t1', sysdate);  
    
end;

declare
    v_res dbms_sql.varchar2_table := util.pl.split('aaa:bbb:ccc',':');

begin
    for i in v_res.first .. v_res.last loop
        util.pl.printl(v_res(i));
    end loop;
end;
