# LIBRARY UNTUK DATETIME
from datetime import datetime, timedelta

# LIBRARY UNTUK OPERATOR AIRFLOW
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow import DAG

# DEFAULT ARGS UNTUK PEMILIK PROJECT
default_args = {
    'owner' : 'rohmanpostgres',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=5)
}

# CREATE DAGS
with DAG(
    dag_id = 'postgres_local6',
    default_args = default_args,
    start_date = datetime(2022, 6, 19),
    schedule_interval = '0 0 * * *'
 ) as dags:

# JOB PERTAMA CREATE TABEL 
    cr_task_1 = PostgresOperator(
        task_id = 'create_tabel',
        postgres_conn_id = 'postgres_db',
        sql = """
            CREATE TABLE IF NOT EXISTS KARYAWAN (
                id_karyawan INT,
                nama CHARACTER VARYING,
                email CHARACTER VARYING,
                no_hp CHARACTER VARYING,
                alamat CHARACTER VARYING,
                tgl DATE,
                PRIMARY KEY (id_karyawan)
            )
        """
    )

    cr_task_2 = PostgresOperator(
        task_id = 'create_tabel2',
        postgres_conn_id = 'postgres_db',
        sql =         """
            CREATE TABLE IF NOT EXISTS GAJI (
                id_gaji INT,
                id_karyawan INT,
                gapok FLOAT,
                tunjangan FLOAT,
                PRIMARY KEY (id_gaji)
            )
        """
    )

# JOB KEDUA INSERT TABEL
    ins_task_1 = PostgresOperator(
        task_id = 'insert_table',
        postgres_conn_id = 'postgres_db',
        sql = """
            INSERT INTO KARYAWAN (id_karyawan, nama, email, no_hp, alamat, tgl)
            VALUES ('121702','Mohammad Nurohman','rohman@gmail.com','0888332211','Rawajati Timur','2022-01-23');
            INSERT INTO KARYAWAN (id_karyawan, nama, email, no_hp, alamat, tgl)
            VALUES ('121703','Khalid Ahmad Al Ghozali','khalid@gmail.com','0888332222','Rawajati Timur','2022-01-23');
            INSERT INTO KARYAWAN (id_karyawan, nama, email, no_hp, alamat, tgl)
            VALUES ('121704','Susy Susila Dewi','suzy@gmail.com','0888332233','Rawajati Timur','2022-01-25');
        """
    )

    ins_task_2 = PostgresOperator(
        task_id = 'insert_tabel2',
        postgres_conn_id = 'postgres_db',
        sql = """
            INSERT INTO GAJI (id_gaji, id_karyawan, gapok, tunjangan)
            VALUES ('111','121702','50000000','80000000');
            INSERT INTO GAJI (id_gaji, id_karyawan, gapok, tunjangan)
            VALUES ('113','121703','90000000','100000000');
            INSERT INTO GAJI (id_gaji, id_karyawan, gapok, tunjangan)
            VALUES ('114','121704','80000000','60000000');
        """ 
    )
