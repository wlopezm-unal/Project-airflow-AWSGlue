from datetime import datetime, timedelta

default_args = {
    'depends_on_past': False,
    'email': ['wlopezm@unal.edu.co'],
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}