import os
from dotenv import load_dotenv

load_dotenv()

data_base_source=os.getenv('data_base_source')
user_source=os.getenv('user_source')
password_source=os.getenv('password_source')
host_source=os.getenv('host_source')
data_base_target=os.getenv('data_base_target')
user_target=os.getenv('user_target')
password_target=os.getenv('password_target')
host_target=os.getenv('host_target')
time_run_job=os.getenv('time_run_job')