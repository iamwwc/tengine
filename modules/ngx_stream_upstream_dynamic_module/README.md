## consul cmd 使用格式

```nginx
stream {
    upstream backend {
        upsync 127.0.0.1:2280/v1/lookup/name?name=sys.ti.cert_watchdog upsync_timeout=6m upsync_interval=500ms upsync_type=consul strong_dependency=off;
        consul 127.0.0.1:2280/v1/lookup/name?name=sys.ti.cert_watchdog update_timeout=5s update_interval=2s;
        hash $remote_addr consistent;
        server 127.0.0.1:12345            max_fails=3 fail_timeout=30s;
        server unix:/tmp/backend3;
        upstream_dump_path ../upstreams/backend.conf;
        include ../upstreams/backend.conf;
    }

    # upstream dns {
    #     consul 127.0.0.1/v1/lookup/name?name=sys.ti.cert_watchdog update_timeout=1m update_interval=200ms;
    #     server 192.168.0.1:53535;
    #     server 1.1.1.1:53;
    # }

    server {
        listen 12345;
        proxy_connect_timeout 1s;
        proxy_timeout 3s;
        proxy_pass backend;
    }
    server {
        listen 11111;
        dynamic_show;
    }

    # server {
    #     listen 127.0.0.1:53 udp reuseport;
    #     proxy_timeout 20s;
    #     proxy_pass dns;
    # }

    server {
        listen [::1]:12345;
        proxy_pass unix:/tmp/stream.socket;
    }
}
```

## 指令
### consul

支持的格式
```nginx
# has port
127.0.0.1:443/v1/lookup/name?name=a.b.c
127.0.0.1:443?name=a.b.c

# default 80 port
127.0.0.1/v1/lookup/name?name=a.b.c
127.0.0.1?name=a.b.c

```
总之，consul默认http，不允许在ip之前加 `http(s)://`

### upstream_dump_path

```
upstream_dump_path /tmp/dynamic/upstreams/backend.conf;
```

upstream_dump_path 的 parent folder都需要手动创建
比如
```
mkdir -p /tmp/dynamic/upstreams
```


### dynamic_show
输出upstream下全部server列表