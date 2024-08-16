import random
import string
import snowflake.connector
import textwrap
import os

account_name = "ekb70116"
user = os.getenv("SNOWFLAKE_USER")
password = os.getenv("SNOWFLAKE_PASSWORD")

# Snowflakeへの接続情報を設定
conn = snowflake.connector.connect(
    user=user, # 各自のユーザー名に変える
    account=account_name,
    password=password,
    role='INTERNSHIP_MEMBER',
    warehouse='TRAINING_WH'
)

conn.cursor().execute(f"")
cur = conn.cursor()

try:
    cur.execute("select current_timestamp()")
    for col in cur:
        print('current_timestamp: {0}'.format(col[0]))
except snowflake.connector.errors.ProgrammingError as e:
    print(e)
finally:
    cur.close()
