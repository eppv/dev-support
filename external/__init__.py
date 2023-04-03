
import pendulum
import os

X_ENV = 'dev'

LOCAL_TZ_NAME = "Asia/Vladivostok"
os.environ.setdefault('AIRFLOW__CORE__DEFAULT_TIMEZONE', LOCAL_TZ_NAME)
LOCAL_TZ = pendulum.timezone(LOCAL_TZ_NAME)

SQL_TEMPLATES_PATH = r'/opt/airflow/dags/sql/'
