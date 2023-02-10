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
    'start_date': datetime(2023, 10, 02, tzinfo=local_tz),
    'email': email_list,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(seconds=60),
}

dag = DAG(
    dag_id='report_mp_102',
    default_args=default_args,
    schedule_interval='12 */12 * * *',
    description='Добавление и обновление заказов (mp)',
    catchup=False,
    max_active_runs=1,
    tags=["102"]
)

def main():

    dbname = 'report'
    
    report_table = 'report.t_d_102'
    
    tmp_all_mp = 'tmp.t16_102'
    tmp_all_mx = 'tmp.t16_102v'
    tmp_all_stage = 'tmp.t16_102_stage'

    order_table = 'history.OrderDetails'
    mx_table = 'history.item_mx'
    state_table = 'history.ItemState'
    calc_table = 'history.calc'


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

    
    delete_tmp_all_mp = f"""drop table if exists {tmp_all_mp};"""
        
    create_tmp_all_mp = f"""
        CREATE TABLE {tmp_all_mp}
        (	
            `item_id` UInt64,
            `position_id` UInt64,
            `status_id` UInt64,
            `dt` DateTime,
            `dt_start` DateTime,
            `delivery_dt` Date
        )
        ENGINE = MergeTree
        ORDER BY dt
        SETTINGS index_granularity = 8192
    """

    insert_tmp_all_mp = f"""
        insert into {tmp_all_mp}
        select argMax(item_id, log_id) item_id
            , position_id
            , argMax(status_id, log_id) status_id_last
            , max(dt) dt_last
            , maxIf(dt, status_id = 18) dt_start
            , max(delivery_dt) delivery_dt
        from {order_table}
        where is_marketplace = 1
        group by position_id
        having item_id > 0
            and status_id_last not in (1,8,14)
            and dt_start > 0
    """

    delete_tmp_all_mx = f"""drop table if exists {tmp_all_mx};"""

    create_tmp_all_mx = f"""
        CREATE TABLE {tmp_all_mx}
        (	
            `item_id` UInt64,
            `position_id` UInt64,
            `status_id` UInt64,
            `mx` UInt64,
            `dt` DateTime,
            `dt_start` DateTime,
            `delivery_dt` Date
        )
        ENGINE = MergeTree
        ORDER BY dt
        SETTINGS index_granularity = 8192
    """

    insert_tmp_all_mx = f"""
        insert into {tmp_all_mx}
        select item_id
            , position_id
            , status_id
            , mx_max
            , dt
            , dt_start
            , delivery_dt
        from {tmp_all_mp} l
        left join 
        (
            select toUInt64(item_id) item_id
                , argMax(mx, dt) mx_max
                , max(dt) dt_max 
            from {mx_table} t1
            where item_id in (select item_id from {tmp_all_mp})
                and isdeleted = 0
                and mx != 0
            group by item_id
        ) r
        on l.item_id = r.item_id
    """

    delete_tmp_all_stage = f"""drop table if exists {tmp_all_stage};"""

    create_tmp_all_stage = f"""
        CREATE TABLE {tmp_all_stage}
        (	
            `item_id` UInt64,
            `position_id` UInt64,
            `dt` DateTime,
            `stage` UInt64,
            `mx_max` UInt64,
            `delivery_dt` Date
        )
        ENGINE = MergeTree
        ORDER BY dt
        SETTINGS index_granularity = 8192
    """

    insert_tmp_all_stage_9 = f"""
        insert into {tmp_all_stage}
        select item_id
            , position_id
            , dt_max
            , stage
            , mx
            , delivery_dt
        from
        (
            select toUInt64(item_id) item_id
                , max(dt) dt_max
                , argMax(state_id, dt) state_max
                , 9 stage
            from {state_table}
            where item_id in (select item_id from {tmp_all_mx})
            group by item_id
            having state_max in ('FTC', 'FIS')
        ) l
        semi join {tmp_all_mx} r
        on l.item_id = r.item_id
    """

    delete_tmp_all_stage_9 = f"""
        ALTER TABLE {tmp_all_mx} delete where position_id in (select position_id from {tmp_all_stage})
    """

    insert_tmp_all_stage_8 = f"""
        insert into {tmp_all_stage}
        select item_id
            , position_id
            , dt_max
            , stage
            , mx_max
            , delivery_dt
        from
        (
            select toUInt64(item_id) item_id
                , max(dt) dt_max
                , 8 stage
                , toUInt64(argMax(mx, dt)) mx_max
                , dictGet('dictionary.StoragePlace','mx_type_id', mx_max) mx_type_id
            from {mx_table}
            where item_id in (select item_id from {tmp_all_mx})
                and isdeleted = 0
                and mx !=0
            group by item_id
            HAVING mx_type_id in (1106, 1107)
        )l
        semi join {tmp_all_mx} r
        on l.item_id = r.item_id
    """

    delete_tmp_all_stage_8 = f"""
        ALTER TABLE {tmp_all_mx} delete where position_id in (select position_id from {tmp_all_stage})
    """

    insert_tmp_all_stage_7 = f"""
        insert into {tmp_all_stage}
        select item_id
            , position_id
            , dt
            , 7 stage
            , mx
            , delivery_dt
        from {tmp_all_mx}
        where status_id = 27
    """

    delete_tmp_all_stage_7 = f"""
        ALTER TABLE {tmp_all_mx} delete where position_id in (select position_id from {tmp_all_stage})
    """

    insert_tmp_all_stage_3 = f"""
        insert into {tmp_all_stage}
        select item_id 
            , position_id
            , dt
            , 3 stage
            , mx
            , delivery_dt
        from {tmp_all_mx}
        where status_id = 36
    """

    delete_tmp_all_stage_3 = f"""
        ALTER TABLE {tmp_all_mx} delete where position_id in (select position_id from {tmp_all_stage})
    """

    insert_tmp_all_stage_6 = f"""
        insert into {tmp_all_stage}
        select item_id 
            , position_id
            , dt_max
            , stage
            , mx
            , delivery_dt
        from (
            select toUInt64(item_id) item_id
                , max(dt) dt_max
                , 6 stage
            from {calc_table}
            where item_id in (select item_id from {tmp_all_mx})
                and prodtype_id in (16021, 30001)
            group by item_id
        ) l
        asof join
        {tmp_all_mx} r
        on l.item_id = r.item_id and l.dt_max > r.dt_start
    """

    delete_tmp_all_stage_6 = f"""
        ALTER TABLE {tmp_all_mx} delete where position_id in (select position_id from {tmp_all_stage})
    """

    insert_tmp_all_stage_5 = f"""
        insert into {tmp_all_stage}
        select item_id 
            , position_id
            , dt_max
            , stage
            , mx
            , delivery_dt
        from (
            select toUInt64(item_id) item_id
                , max(dt) dt_max
                , 5 stage
            from {calc_table}
            where item_id in (select item_id from {tmp_all_mx})
                and prodtype_id in (1004, 1204, 1007)
            group by item_id
        ) l
        asof join
        {tmp_all_mx} r
        on l.item_id = r.item_id and l.dt_max > r.dt_start
    """

    delete_tmp_all_stage_5 = f"""
        ALTER TABLE {tmp_all_mx} delete where position_id in (select position_id from {tmp_all_stage})
    """

    insert_tmp_all_stage_4 = f"""
        insert into {tmp_all_stage}
        select item_id 
            , position_id
            , dt
            , 4 stage
            , mx
            , delivery_dt
        from {tmp_all_mx}
        where status_id = 23
    """

    delete_tmp_all_stage_4 = f"""
        ALTER TABLE {tmp_all_mx} delete where position_id in (select position_id from {tmp_all_stage})
    """

    insert_tmp_all_stage_2 = f"""
        insert into {tmp_all_stage}
        select item_id 
            , position_id
            , dt
            , 2 stage
            , mx
            , delivery_dt
        from {tmp_all_mx}
        where status_id = 18
    """

    delete_tmp_all_stage_2 = f"""
        ALTER TABLE {tmp_all_mx} delete where position_id in (select position_id from {tmp_all_stage})    
    """

    insert_tmp_all_stage_1 = f"""
        insert into {tmp_all_stage}
        select item_id 
            , position_id
            , dt
            , 1 stage
            , mx
            , delivery_dt
        from {tmp_all_mx}
        where status_id = 4;
    """

    delete_tmp_all_stage_1 = f"""
        ALTER TABLE {tmp_all_mx} delete where position_id in (select position_id from {tmp_all_stage})
    """
    
    client.execute(delete_tmp_all_mp)
    print('Времянка для хранения заказов из mp удалена')
    
    client.execute(delete_tmp_all_mx)
    print('Времянка для хранения mx заказов удалена')

    client.execute(delete_tmp_all_stage)
    print('Времянка для хранения stage заказов удалена')

    client.execute(create_tmp_all_mp)
    print('Времянка для хранения заказов из mp создана')
    
    client.execute(insert_tmp_all_mp)
    print('Времянка для хранения заказов из mp заполнена')

    client.execute(create_tmp_all_mx)
    print('Времянка для хранения mx заказов создана')
    
    client.execute(insert_tmp_all_mx)
    print('Времянка для хранения mx заказов заполнена')
    
    client.execute(create_tmp_all_stage)
    print('Времянка для хранения stage заказов создана')
    
    try:
        client.execute(insert_tmp_all_stage_9)
        client.execute(delete_tmp_all_stage_9)
        
        client.execute(insert_tmp_all_stage_8)
        client.execute(delete_tmp_all_stage_8)
        
        client.execute(insert_tmp_all_stage_7)
        client.execute(delete_tmp_all_stage_7)
        
        client.execute(insert_tmp_all_stage_3)
        client.execute(delete_tmp_all_stage_3)
        
        client.execute(insert_tmp_all_stage_6)
        client.execute(delete_tmp_all_stage_6)
        
        client.execute(insert_tmp_all_stage_5)
        client.execute(delete_tmp_all_stage_5)
        
        client.execute(insert_tmp_all_stage_4)
        client.execute(delete_tmp_all_stage_4)
        
        client.execute(insert_tmp_all_stage_2)
        client.execute(delete_tmp_all_stage_2)
        
        client.execute(insert_tmp_all_stage_1)
        client.execute(delete_tmp_all_stage_1)
    except Exception:
        print('Произошла ошибка при заполнении времянки для хранения stage')

    delete_report = f"""drop table if exists {report_table};"""

    create_report = f"""
        CREATE TABLE {report_table}
        (
            `item_id` UInt64,
            `position_id` UInt64,
            `mx` UInt64,
            `stage` UInt64,
            `dt_stage` DateTime,
            `delivery_dt` Date
        )
        ENGINE = ReplacingMergeTree
        ORDER BY position_id
        SETTINGS index_granularity = 8192
    """

    insert_report = f"""
        insert into {report_table}
        select item_id
            , position_id
            , mx_max
            , stage
            , dt
            , delivery_dt
        from {tmp_all_stage}
    """

    client.execute(delete_report)
    client.execute(create_report)
    client.execute(insert_report)
    print("Произошло обновление витрины")

    client.execute(delete_tmp_all_mp)
    client.execute(delete_tmp_all_mx)
    client.execute(delete_tmp_all_stage)
    print('Оставшиеся времянки удалены')

task1 = PythonOperator(
    task_id='report_mp_102', python_callable=main, dag=dag)
