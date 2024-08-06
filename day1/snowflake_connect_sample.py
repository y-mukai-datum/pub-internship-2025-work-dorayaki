import random
import string
import snowflake.connector
from cryptography.hazmat.primitives import serialization
import textwrap

account_name = "ekb70116"

# 秘密鍵を読み込む
with open("/home/vscode/.ssh/rsa_key.p8", "rb") as key_file:
    p_key = serialization.load_pem_private_key(
        key_file.read(),
        password=None,
    )

# Snowflakeへの接続情報を設定
conn = snowflake.connector.connect(
    user='<user name>', # 各自のユーザー名に変える
    account=account_name,
    private_key=p_key,
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
