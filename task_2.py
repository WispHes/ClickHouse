import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from clickhouse_driver import Client
from datetime import datetime, timedelta
import pendulum, pytz


local_tz = pendulum.timezone("Europe/Moscow",)

email_list = ['your@mail.ru']


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 02, 10, tzinfo=local_tz),
    'email': email_list,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(seconds=60),
}

dag = DAG(
    dag_id='report_working_employees_over_15h_102',
    default_args=default_args,
    schedule_interval='18 */1 * * *',
    description='Первый даг',
    catchup=False,
    max_active_runs=1,
    tags=["100"]
)


def main():

    dbname = 'report'
    dst_table = 'report.working_employees_over_15h_102'

    with open('/opt/airflow/dags/secret/ch_rowan.json') as json_file:
        data = json.load(json_file) 

    client = Client(data['server'][0]['host'],
                    user=data['server'][0]['user'],
                    password=data['server'][0]['password'],
                    port=data['server'][0]['port'],
                    verify=False,
                    database=dbname,
                    settings={"numpy_columns": True, 'use_numpy': True},
                    compression=True)

    sql_temp1 = """create temporary table t1 as
    WITH smena as
    (
        select employee_id
            , office_id
            , dt_in
            , dt_prev
            , is_in_prev
            , date_diff('hour', dt_prev, dt_in) diff_h
            , is_in
            , is_smena_false
        from
        (
            select employee_id, dt dt_in, is_in, 0 is_smena_false, office_id
            from history.turniket
            where dt >= now() - interval 14 day
                and is_in = 1
            union all
            select employee_id, now() + interval 1 day dt_prev, 1 is_in, 1 is_smena_false, 0 office_id
            from history.turniket
            where dt >= now() - interval 14 day
            group by employee_id
        ) t1
        asof left join
        (
            select employee_id, is_in is_in_prev, dt dt_prev
            from history.turniket
            where dt >= now() - interval 14 day
            union all
            select employee_id, 0 is_in, now() - interval 6 month dt_prev
            from history.turniket
            where dt >= now() - interval 14 day
            group by employee_id
        ) t2
        on t1.employee_id = t2.employee_id and dt_in > dt_prev
        where (diff_h > 7 and is_in_prev = 0)
            or (diff_h > 22 and is_in_prev = 1)
    )
    SELECT employee_id, office_id
        , l.dt_in dt_smena_start
        , r.dt_prev dt_smena_end
    FROM smena l
    asof left join
    (
        select employee_id
            , dt_prev
        from smena
    ) r
    on l.employee_id = r.employee_id and l.dt_in < r.dt_prev
    where is_smena_false = 0
    """

    sql_temp2 = """create temporary table t2 as
        select
            employee_id,
            dt_smena_start,
            office_id,
            date_diff('hour', dt_smena_start, (select max(dt) from history.turniket)) diff_h,
            dictGet('dictionary.Department', 'parent_department_id', dept_id) parent_dept_id,
            dictGet('dictionary.Department', 'parent_department_id', parent_dept_id) parent2_dept_id,
            dictGet('dictionary.Employee', 'department_id', employee_id) dept_id
        from t1
        where dt_smena_end = 0
            and diff_h between 15 and 22
        """

    sql_temp3 = """create temporary table t3 as
        select
            employee_id,
            dictGet('dictionary.ProdType','ProdTypePart_id', prodtype_id) ProdTypePart_id,
            prodtype_id,
            count() qty_opers
        from history.calc
        where dt >= now() - interval 5 day
            and employee_id in (select employee_id from t2)
        group by employee_id, ProdTypePart_id, prodtype_id
        """

    sql_temp4 = """create temporary table t4 as
        select
            employee_id,
            round(avg(date_diff('hour', dt_smena_start, dt_smena_end))) avg_smena_duration
        from t1
        where employee_id in (select employee_id from t2)
            and dt_smena_end != 0
        group by employee_id
        """

    sql_temp5 = """create temporary table t5 as
        select
            t2.employee_id employee_id,
            office_id,
            dt_smena_start,
            parent2_dept_id,
            diff_h,
            avg_smena_duration,
            ProdTypePart_id,
            prodtype_id,
            qty_opers
        from t2
        left join t3
        on t2.employee_id = t3.employee_id
        semi join t4
        on t2.employee_id = t4.employee_id    
        """

    insert_query = f"""
        insert into {dst_table}
        select
            employee_id,
            office_id,
            dt_smena_start,
            parent2_dept_id,
            prodtype_id,
            diff_h,
            avg_smena_duration,
            ProdTypePart_id,
            qty_opers,
            now() dt_last_load
        from t5
        """

    client.execute(sql_temp1)
    print('======================================')
    print('Временная таблица-1 создана.')

    client.execute(sql_temp2)
    print('======================================')
    print('Временная таблица-2 создана.')

    client.execute(sql_temp3)
    print('======================================')
    print('Временная таблица-3 создана.')

    client.execute(sql_temp4)
    print('======================================')
    print('Временная таблица-4 создана.')

    client.execute(sql_temp5)
    print('======================================')
    print('Временная таблица-5 создана.')

    client.execute(insert_query)
    print('======================================')
    print(f'Основная таблица {dst_table} заполнена.')

task1 = PythonOperator(
    task_id='report_working_employees_over_15h_102', python_callable=main, dag=dag)
