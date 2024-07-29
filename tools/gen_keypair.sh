#!/bin/bash

PRIVATE_KEY="$HOME/.ssh/rsa_key.p8"
PUBLIC_KEY="$HOME/.ssh/rsa_key.pub"

# 秘密鍵が存在するか確認
if [ ! -f "$PRIVATE_KEY" ]; then
    echo "private key is not exists."

    # .sshディレクトリが存在しない場合は作成
    mkdir -p "$HOME/.ssh"

    # 鍵を生成
    openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out $PRIVATE_KEY -nocrypt
    openssl rsa -in $PRIVATE_KEY -pubout -out $PUBLIC_KEY

    echo "key pair generated."
    echo "public key generated: $HOME/.ssh/rsa_key.pub"
else
    echo "private key already exists."
    echo "public key exists: $HOME/.ssh/rsa_key.pub"
fi
