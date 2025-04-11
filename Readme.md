## Starting of:
  Go inside PC1 and run the following commands to create a mongokey-file necessary to have a MongoDB RePLset with authentication enabled:
  ```
    openssl rand -base64 756 >./mongo-keyfile
    chmod 600 mongo-keyfile
    sudo chown 999:999 mongo-keyfile
```
## RUNNING PC1:
  Inside PC1 run:
  ```
    docker-compose build
    docker-compose up -d
  ```
  If you need sudo to run docker check this tutorial:\
  
    https://askubuntu.com/questions/477551/how-can-i-use-docker-without-sudo
