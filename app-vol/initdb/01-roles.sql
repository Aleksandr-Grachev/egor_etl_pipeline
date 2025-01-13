ALTER SYSTEM SET client_min_messages TO DEBUG1;

drop role if EXISTS testus;
CREATE role testus login PASSWORD '';

drop role if EXISTS bn_airflow;
CREATE role bn_airflow login PASSWORD '';
