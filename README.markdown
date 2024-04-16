### 初始化

```bash
./setup.sh
```

### 编译

```
./local_build.sh
```

### 运行

```bash
sudo ./target/sbin/nginx -p target -c conf/nginx.conf
```

### 签发证书

```txt
openssl req -x509 -newkey rsa:4096 -keyout localhost.key.pem -out localhost.cert.pem -sha256 -days 365 -nodes
-subj '/CN=localhost'
```

### 测试

```
curl "https://localhost:443" -k
```

```
https://github.com/xiaokai-wang/nginx-stream-upsync-module
```
