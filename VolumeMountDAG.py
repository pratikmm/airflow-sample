from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.secret import Secret
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.volume_mount import VolumeMount
from airflow.contrib.kubernetes.pod import Port
from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG
from datetime import datetime, timedelta


#secret_file = Secret('volume', '/etc/sql_conn', 'airflow-secrets', 'sql_alchemy_conn')
#secret_env  = Secret('env', 'SQL_CONN', 'airflow-secrets', 'sql_alchemy_conn')
#secret_all_keys  = Secret('env', None, 'airflow-secrets-2')
volume_mount = VolumeMount('task-pv-claim',
                            mount_path='/root/mount_file',
                            sub_path=None,
                            read_only=True)
port = Port('http', 80)
#configmaps = ['test-configmap-1', 'test-configmap-2']

volume_config= {
    'persistentVolumeClaim':
      {
        'claimName': 'task-pv-claim'
      }
    }
volume = Volume(name='task-pv-claim', configs=volume_config)

# affinity = {
#     'nodeAffinity': {
#       'preferredDuringSchedulingIgnoredDuringExecution': [
#         {
#           "weight": 1,
#           "preference": {
#             "matchExpressions": {
#               "key": "disktype",
#               "operator": "In",
#               "values": ["ssd"]
#             }
#           }
#         }
#       ]
#     },
#     "podAffinity": {
#       "requiredDuringSchedulingIgnoredDuringExecution": [
#         {
#           "labelSelector": {
#             "matchExpressions": [
#               {
#                 "key": "security",
#                 "operator": "In",
#                 "values": ["S1"]
#               }
#             ]
#           },
#           "topologyKey": "failure-domain.beta.kubernetes.io/zone"
#         }
#       ]
#     },
#     "podAntiAffinity": {
#       "requiredDuringSchedulingIgnoredDuringExecution": [
#         {
#           "labelSelector": {
#             "matchExpressions": [
#               {
#                 "key": "security",
#                 "operator": "In",
#                 "values": ["S2"]
#               }
#             ]
#           },
#           "topologyKey": "kubernetes.io/hostname"
#         }
#       ]
#     }
# }

# tolerations = [
#     {
#         'key': "key",
#         'operator': 'Equal',
#         'value': 'value'
#      }
# ]

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
    'VolumeMountDAG', default_args=default_args, schedule_interval=timedelta(minutes=10))

start = DummyOperator(task_id='start', dag=dag)

k = KubernetesPodOperator(namespace='default',
                          image="ubuntu:16.04",
                          cmds=["bash", "-cx"],
                          arguments=["echo", "10"],
                          labels={"foo": "bar"},
                          #secrets=[secret_file, secret_env, secret_all_keys],
                          ports=[port],
                          volumes=[volume],
                          volume_mounts=[volume_mount],
                          name="test",
                          task_id="task",
                          #affinity=affinity,
                          is_delete_operator_pod=True,
                          hostnetwork=False,
                          #tolerations=tolerations,
                          #configmaps=configmaps
                          dag=dag
                          )

end = DummyOperator(task_id='end', dag=dag)

start >> k >> end