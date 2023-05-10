
import pendulum
import os
import platform

X_ENV = 'dev'

if platform.system() == 'Windows':
    HOST_HOME = os.getenv('USERPROFILE')
elif platform.system() == 'Linux':
    HOST_HOME = os.getenv('HOME')
else:
    HOST_HOME = None

if HOST_HOME is not None:
    HOST_PROJ = f'{HOST_HOME}/iac-etl/'
    HOST_PROJ_VENV = f'{HOST_PROJ}/venv'
    os.environ.setdefault('HOST_HOME', HOST_HOME)
    os.environ.setdefault('HOST_PROJ', HOST_PROJ)

EXTERNAL_MODULES_PATH = os.path.dirname(os.path.abspath(__file__))

LOCAL_TZ_NAME = "Asia/Vladivostok"
LOCAL_TZ = pendulum.timezone(LOCAL_TZ_NAME)

DOCKER_SOCKET = "unix://var/run/docker.sock"

SQL_TEMPLATES_PATH = r'/opt/airflow/dags/sql/'

os.environ.setdefault('AIRFLOW__CORE__DEFAULT_TIMEZONE', LOCAL_TZ_NAME)
os.environ.setdefault('EXTERNAL_MODULES_PATH', EXTERNAL_MODULES_PATH)
