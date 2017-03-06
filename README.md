# CloudBackup


## Install

```
git clone https://github.com/chibiegg/cloudbackup.git
cd cloudbackup
pip install -r requirements.txt
```

## Config


```json:~/.cloudbackup.json
{
  "s3":{
    "token": "BACKET",
    "secret": "SECRET",
    "host": "b.sakurastorage.jp"
  }
}
```


## Pipe to Object Storage

```shell
cat hoge.bin | \
python cloudbackup/commands/__init__.py send --part-size 128 --threads 10 --input stdin --output s3:BACKET/hogehoge
```
