
import pendulum
import os

X_ENV = 'dev'

LOCAL_TZ_NAME = "Asia/Vladivostok"
EXTERNAL_MODULES_PATH = os.path.dirname(os.path.abspath(__file__))

os.environ.setdefault('AIRFLOW__CORE__DEFAULT_TIMEZONE', LOCAL_TZ_NAME)
os.environ.setdefault('EXTERNAL_MODULES_PATH', EXTERNAL_MODULES_PATH)
LOCAL_TZ = pendulum.timezone(LOCAL_TZ_NAME)

SQL_TEMPLATES_PATH = r'/opt/airflow/dags/sql/'
