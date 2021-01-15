from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
#from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'kubernetes_hello_world2', default_args=default_args, schedule_interval=timedelta(minutes=10))


start = DummyOperator(task_id='start', dag=dag)

passing1 = KubernetesPodOperator(namespace='default',
                          image="python:3.6",
                          cmds=["python","-c"],
                          arguments=["print('hello world')"],
                          labels={"foo": "bar"},
                          name="passing-test1",
                          task_id="passing-task1",
                          get_logs=True,
                          dag=dag
                          )

failing1 = KubernetesPodOperator(namespace='default',
                          image="python:3.6",
                          cmds=["python","-c"],
                          arguments=["print('hello world')"],
                          labels={"foo": "bar"},
                          name="failing-task1",
                          task_id="failing-task1",
                          get_logs=True,
                          dag=dag
                          )

passing2 = KubernetesPodOperator(namespace='default',
                          image="python:3.6",
                          cmds=["python","-c"],
                          arguments=["print('hello world')"],
                          labels={"foo": "bar"},
                          name="passing-test2",
                          task_id="passing-task2",
                          get_logs=True,
                          dag=dag
                          )

passing3 = KubernetesPodOperator(namespace='default',
                          image="python:3.6",
                          cmds=["python","-c"],
                          arguments=["print('hello world')"],
                          labels={"foo": "bar"},
                          name="passing-test3",
                          task_id="passing-task3",
                          get_logs=True,
                          dag=dag
                          )
write_xcom1 = KubernetesPodOperator(
        namespace='default',
        image='alpine',
        cmds=["sh", "-c", "mkdir -p /airflow/xcom/;echo '[1,2,3,4]' > /airflow/xcom/return.json"],
        labels={"foo": "bar"},
        name="write-xcom1",
        do_xcom_push=True,
        task_id="write-xcom1",
        get_logs=True,
        dag=dag
    )

# pod_task_xcom_result = BashOperator(
#         bash_command="echo \"{{ task_instance.xcom_pull('write-xcom')[0] }}\"",
#         task_id="pod_task_xcom_result",
#     )

 
end = DummyOperator(task_id='end', dag=dag)


#start >> passing1 >> [passing2, failing1] >> passing3 >> write_xcom1 >> end

#start >> [passing1, passing2, failing1]
#[passing1, passing2, failing1] >> passing3


start >> passing1 >> passing2 >> passing3 >> write_xcom1 >> failing1 >> end

# passing1.set_upstream(start)
# failing1.set_upstream(passing1)
# passing2.set_upstream(passing1)
# failing1.set_downstream(passing3)
# passing2.set_downstream(passing3)
# #passing3.set_downstream(passing2)
# #passing3.set_downstream(failing1)
# write_xcom1.set_upstream(passing3)
# write_xcom1.set_downstream(end)
# # pod_task_xcom_result.set_upstream(write_xcom)
# # pod_task_xcom_result.set_downstream(end)
