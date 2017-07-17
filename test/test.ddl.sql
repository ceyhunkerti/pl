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