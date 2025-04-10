#!/bin/bash
set -e

openssl rand -base64 756 >./mongo-keyfile
chmod 600 mongo-keyfile
sudo chown 999:999 mongo-keyfile
