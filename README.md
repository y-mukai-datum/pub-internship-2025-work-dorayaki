# pub-internship-2025-work
FY25 DATUM STUDIOインターンシップ作業用リポジトリ

 # setup
 ## AWSのアカウントキーを発行
 資料を参考にすること

 ## .envの作成
 .envという名前でファイルを作成する。

 ```
# aws
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_DEFAULT_REGION=ap-northeast-1 # このまま

# Snowflake
export SNOWFLAKE_USER='<your name>'
export SNOWFLAKE_PASSWORD='password'
export SNOWFLAKE_AUTHENTICATOR='UsernamePasswordMFA' # このまま
export SNOWFLAKE_CLIENT_REQUEST_MFA_TOKEN=true # このまま
 ```

## 環境変数の読み込み
以下コマンドをターミナルから実行する

```
$ source /workspaces/pub-internship-2025-work/.env
$ echo 'source /workspaces/pub-internship-2025-work/.env' >> ~/.bashrc
```

## 動作確認
AWS

```
$ aws s3 ls
```

Snowflake

```
$ python3 day1/snowflake_connect_sample.py
```
