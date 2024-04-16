#include "ngx_config.h"
#include "ngx_core.h"
#include <ngx_stream.h>

#include "ngx_stream_upstream_dynamic.h"

// TODO
// 1. switch to http parser instead of \r\n\r\n
static ngx_int_t ngx_stream_upstream_dynamic_init_module(ngx_cycle_t *cycle);

static ngx_int_t ngx_stream_upstream_dynamic_init_process(ngx_cycle_t *cycle);

static void ngx_stream_upstream_dynamic_exit_process(ngx_cycle_t *cycle);

static void *ngx_stream_upstream_dynamic_create_main_conf(ngx_conf_t *cf);

static char *ngx_stream_upstream_dynamic_init_main_conf(ngx_conf_t *cf,
                                                        void *conf);

static void *ngx_stream_upstream_dynamic_create_srv_conf(ngx_conf_t *cf);

static char *ngx_stream_upstream_dynamic_set_consul_slot(ngx_conf_t *cf,
                                                         ngx_command_t *cmd,
                                                         void *conf);

static char *ngx_stream_upstream_dynamic_set_dynamic_show(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);

static char *ngx_stream_upstream_dynamic_set_dump_path(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);

static void ngx_stream_upstream_dynamic_show_handler(ngx_stream_session_t *session);

static ngx_int_t ngx_stream_upstream_dynamic_do_dump_path(ngx_stream_upstream_dynamic_srv_conf_t *srv_conf);

static char *
ngx_stream_upstream_dynamic_build_main_conf(ngx_conf_t *cf, ngx_stream_upstream_dynamic_main_conf_t *main_conf,
                                            ngx_stream_upstream_srv_conf_t *upstream_srv_conf, ngx_uint_t i);

static ngx_int_t
ngx_stream_upstream_dynamic_parse_consul_list(ngx_pool_t *pool, ngx_stream_upstream_dynamic_srv_conf_t *, void *data,
                                              ngx_array_t *servers);

static ngx_int_t
ngx_stream_upstream_dynamic_sync_fetch_consul_list(ngx_pool_t *pool, ngx_stream_upstream_dynamic_srv_conf_t *srv_conf,
                                                   u_char **p);

static void ngx_stream_upstream_dynamic_on_update_timer_handler(ngx_event_t *event);

static void ngx_stream_upstream_dynamic_on_timeout_timer_handler(ngx_event_t *event);

static ngx_int_t ngx_stream_upstream_dynamic_add_timer(ngx_stream_upstream_dynamic_srv_conf_t *srv_conf);

static ngx_int_t
ngx_stream_upstream_dynamic_update_upstream_server_list(ngx_stream_upstream_dynamic_srv_conf_t *srv_conf,
                                                        ngx_array_t *servers);

static ngx_array_t *
ngx_stream_upstream_dynamic_consul_like_server_to_upstream_server(ngx_pool_t *pool, ngx_array_t *consul_like_servers);

static void ngx_stream_upstream_dynamic_reset_timer_state(ngx_stream_upstream_dynamic_inner_state_t *state);

static ngx_stream_upstream_dynamic_inner_state_t *
ngx_stream_upstream_dynamic_create_inner_state(ngx_cycle_t *cycle, ngx_stream_upstream_dynamic_srv_conf_t *srv_conf);

static ngx_peer_connection_t *ngx_stream_upstream_dynamic_create_peer_conn(ngx_cycle_t *cycle, ngx_pool_t *pool,
                                                                           ngx_stream_upstream_dynamic_srv_conf_t *srv_conf);

static send_request_t
ngx_stream_upstream_dynamic_build_request(ngx_pool_t *pool, ngx_stream_upstream_dynamic_srv_conf_t *srv_conf);

static char *ngx_stream_upstream_dynamic_ngx_str_to_char_array(ngx_pool_t *pool, ngx_str_t *str);

static ngx_atomic_t *shared_memory_created = NULL;

static ngx_int_t ngx_stream_upstream_dynamic_release_old_peer(ngx_stream_upstream_dynamic_srv_conf_t *srv_conf,
                                                              ngx_stream_upstream_rr_peer_t *old_peer);

static void ngx_stream_upstream_dynamic_clear_all_events(ngx_cycle_t *cycle);

static void ngx_stream_upstream_dynamic_send_out(ngx_stream_session_t *session, vector *v);

static void ngx_stream_upstream_dynamic_print_one_upstream(ngx_stream_upstream_dynamic_srv_conf_t *srv_conf, vector *v);

char *ngx_stream_upstream_dynamic_ngx_str_to_char_array(ngx_pool_t *pool, ngx_str_t *str) {
    char *p = ngx_pcalloc(pool, str->len + 1);
    ngx_memcpy(p, str->data, str->len);
    ngx_memset(p + str->len, '\0', 1);
    return p;
}

static ngx_stream_module_t ngx_stream_upstream_dynamic_ctx = {
        NULL,                                                       /* preconfiguration */
        NULL,                                                       /* postconfiguration */

        ngx_stream_upstream_dynamic_create_main_conf,               /* create main configuration */
        ngx_stream_upstream_dynamic_init_main_conf,                 /* init main configuration */

        ngx_stream_upstream_dynamic_create_srv_conf,                /* create server configuration */
        NULL,                                                       /* merge server configuration */
};

static ngx_command_t ngx_stream_upstream_dynamic_command[] = {
        {
                ngx_string("consul"),
                NGX_STREAM_UPS_CONF | NGX_CONF_1MORE,
                ngx_stream_upstream_dynamic_set_consul_slot,
                NGX_STREAM_SRV_CONF_OFFSET,
                0,
                NULL
        },
        {
                ngx_string("upstream_dump_path"),
                NGX_STREAM_UPS_CONF | NGX_CONF_1MORE,
                ngx_stream_upstream_dynamic_set_dump_path,
                NGX_STREAM_SRV_CONF_OFFSET,
                0,
                NULL
        },
        {
                ngx_string("dynamic_show"),
                NGX_STREAM_SRV_CONF | NGX_CONF_NOARGS,
                ngx_stream_upstream_dynamic_set_dynamic_show,
                NGX_STREAM_SRV_CONF_OFFSET,
                0,
                NULL
        },
        ngx_null_command
};

ngx_module_t ngx_stream_upstream_dynamic_module = {
        NGX_MODULE_V1,
        &
                ngx_stream_upstream_dynamic_ctx,                    /* module context */
        ngx_stream_upstream_dynamic_command,                /* module directives */
        NGX_STREAM_MODULE,                                  /* module type */
        NULL,                                               /* init master */
        ngx_stream_upstream_dynamic_init_module,            /* init module */
        ngx_stream_upstream_dynamic_init_process,           /* init process */
        NULL,                                               /* init thread */
        NULL,                                               /* exit thread */
        ngx_stream_upstream_dynamic_exit_process,           /* exit process */
        NULL,                                               /* exit master */
        NGX_MODULE_V1_PADDING
};

static ngx_int_t create_shared_memory_mutex(ngx_cycle_t *cycle) {
    ngx_shm_t shm;
    u_char *shared;
    ngx_stream_upstream_dynamic_main_conf_t *main_conf;
    ngx_uint_t cache_line_size = 64;
    ngx_uint_t shm_size;
    ngx_uint_t i;
    ngx_stream_upstream_dynamic_srv_conf_t **srv_confs;
    ngx_stream_upstream_dynamic_srv_conf_t *srv_conf;
    ngx_log_t *log;
    u_char name[] = "ngx_dynamic_shared_zone";

    log = cycle->log;
    main_conf = ngx_stream_cycle_get_module_main_conf(cycle, ngx_stream_upstream_dynamic_module);
    shm_size = cache_line_size * main_conf->upstream_numbers;
    shm.log = cycle->log;
    shm.name.data = name;
    shm.name.len = ngx_strlen(name);
    shm.size = shm_size;

    if (shared_memory_created != NULL) {
        shm.addr = (u_char *) shared_memory_created;
        ngx_shm_free(&shm);
    }
    if (ngx_shm_alloc(&shm) != NGX_OK) {
        ngx_log_error(NGX_LOG_ERR, cycle->log, 0, "create shared memory failed");
        return NGX_ERROR;
    }
    shared = shm.addr;

    shared_memory_created = (ngx_atomic_t *) shared;

    srv_confs = main_conf->servers->elts;
    for (i = 0; i < main_conf->upstream_numbers; i++) {
        srv_conf = srv_confs[i];
        if (ngx_shmtx_create(&srv_conf->state->shm_mutex, (ngx_shmtx_sh_t *) shared + cache_line_size * i, name) !=
            NGX_OK) {
            ngx_log_error(NGX_LOG_ERR, log, 0, "create mutex in shared memory failed. actual: %d, expect: %d",
                          main_conf->upstream_numbers, main_conf->servers->nelts);
            return NGX_ERROR;
        }
    }

    return NGX_OK;
}

static ngx_int_t ngx_stream_upstream_dynamic_init_module(ngx_cycle_t *cycle) {
    // 将upstream server list dump到文件
    // 作为 SD crash 的兜底
    // dump 之前需要共享内存锁定文件
    ngx_stream_upstream_dynamic_main_conf_t *main_conf;
    ngx_stream_upstream_dynamic_srv_conf_t *srv_conf;
    ngx_stream_upstream_dynamic_srv_conf_t **srv_confs;
    if (create_shared_memory_mutex(cycle) != NGX_OK) {
        return NGX_OK;
    }
    main_conf = ngx_stream_cycle_get_module_main_conf(cycle, ngx_stream_upstream_dynamic_module);
    ngx_uint_t i = 0;
    srv_confs = main_conf->servers->elts;
    // nginx run as nobody by default, change dump file to 777 to allow to nobody to write
    for (; i < main_conf->upstream_numbers; i++) {
        srv_conf = srv_confs[i];
        ngx_change_file_access(srv_conf->dump_file_path.data, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH | S_IWOTH);
    }
    return NGX_OK;
}

static ngx_int_t
ngx_stream_upstream_dynamic_start_update_list(ngx_pool_t *pool, ngx_stream_upstream_dynamic_srv_conf_t *srv_conf,
                                              u_char *data) {
    ngx_array_t *servers_from_consul = ngx_array_create(pool, 10, sizeof(server_from_consul_t));
    if (ngx_stream_upstream_dynamic_parse_consul_list(pool, srv_conf, data, servers_from_consul) == NGX_ERROR) {
        return NGX_ERROR;
    }
    ngx_array_t *upstream_servers = ngx_stream_upstream_dynamic_consul_like_server_to_upstream_server(pool,
                                                                                                      servers_from_consul);
    if (ngx_stream_upstream_dynamic_update_upstream_server_list(srv_conf, upstream_servers) == NGX_ERROR) {
        return NGX_ERROR;
    }
    if (ngx_shmtx_trylock(&srv_conf->state->shm_mutex)) {
        if (ngx_stream_upstream_dynamic_do_dump_path(srv_conf) == NGX_ERROR) {
            ngx_shmtx_unlock(&srv_conf->state->shm_mutex);
            return NGX_ERROR;
        }
        ngx_shmtx_unlock(&srv_conf->state->shm_mutex);
    }
    return NGX_OK;
}

static ngx_int_t ngx_stream_upstream_dynamic_init_process(ngx_cycle_t *cycle) {
    ngx_stream_upstream_dynamic_main_conf_t *main_conf;
    ngx_stream_upstream_dynamic_srv_conf_t *srv_conf;
    ngx_uint_t i;
    main_conf = ngx_stream_cycle_get_module_main_conf(cycle, ngx_stream_upstream_dynamic_module);
    ngx_stream_upstream_dynamic_srv_conf_t **servers = main_conf->servers->elts;
    // 下面请求consul获取server，并更新peer
    // 数据必须copy到peers，不能直接指针赋值
    // 此函数最后会将pool释放
    ngx_pool_t *pool = ngx_create_pool(NGX_DEFAULT_POOL_SIZE, cycle->log);
    for (i = 0; i < main_conf->servers->nelts; i++) {
        srv_conf = servers[i];
        ngx_stream_upstream_dynamic_add_timer(srv_conf);
        u_char *data = NULL;
        if (ngx_stream_upstream_dynamic_sync_fetch_consul_list(pool, srv_conf, &data) == NGX_ERROR) {
            continue;
        }
        if (data == NULL) {
            ngx_log_error(NGX_LOG_ERR, cycle->log, 0, "upstream %V no consul list found!",
                          &srv_conf->upstream_srv_conf->host);
            continue;
        }
        if (ngx_stream_upstream_dynamic_start_update_list(pool, srv_conf, data) == NGX_ERROR) {
            continue;
        }
    }
    ngx_destroy_pool(pool);
    return NGX_OK;
}

static void ngx_stream_upstream_dynamic_exit_process(ngx_cycle_t *cycle) {
    ngx_stream_upstream_dynamic_clear_all_events(cycle);
}

static void *ngx_stream_upstream_dynamic_create_main_conf(ngx_conf_t *cf) {
    ngx_stream_upstream_dynamic_main_conf_t *main_conf;
    main_conf =
            ngx_pcalloc(cf->pool, sizeof(ngx_stream_upstream_dynamic_main_conf_t));
    ngx_memzero((void *) main_conf, sizeof(*main_conf));
    if (main_conf == NULL) {
        return NULL;
    }
    main_conf->servers = ngx_array_create(cf->cycle->pool, 10, sizeof(ngx_stream_upstream_dynamic_srv_conf_t *));
    return main_conf;
}

static char *
ngx_stream_upstream_dynamic_build_main_conf(ngx_conf_t *cf, ngx_stream_upstream_dynamic_main_conf_t *main_conf,
                                            ngx_stream_upstream_srv_conf_t *upstream_srv_conf, ngx_uint_t index) {
    ngx_stream_upstream_dynamic_inner_state_t *state;

    if (upstream_srv_conf->srv_conf == NULL) {
        return NGX_CONF_OK;
    }
    // get module conf in upstream srv_conf
    ngx_stream_upstream_dynamic_srv_conf_t *maybe_srv_conf = ngx_stream_conf_upstream_srv_conf(upstream_srv_conf,
                                                                                               ngx_stream_upstream_dynamic_module);
    if (maybe_srv_conf->host.data == NGX_CONF_UNSET_PTR && maybe_srv_conf->host.len == NGX_CONF_UNSET_SIZE) {
        // 此upstream下没 ngx_stream_upstream_dynamic_module 的指令，跳过
        return NGX_CONF_OK;
    }
    maybe_srv_conf->upstream_srv_conf = upstream_srv_conf;
    main_conf->upstream_numbers++;
    // 每个 srv_conf 从创建开始始终关联同一个state
    state = ngx_stream_upstream_dynamic_create_inner_state(maybe_srv_conf->cycle, maybe_srv_conf);

    if (state == NULL) {
        return NULL;
    }
    ngx_stream_upstream_dynamic_srv_conf_t **server_conf = ngx_array_push(main_conf->servers);
    *server_conf = maybe_srv_conf;
    return NGX_OK;
}

static char *ngx_stream_upstream_dynamic_init_main_conf(ngx_conf_t *cf,
                                                        void *conf) {
    ngx_uint_t i;
    ngx_stream_upstream_dynamic_main_conf_t *main_conf = conf;
    ngx_stream_upstream_srv_conf_t **upstream_srv_confs;
    ngx_stream_upstream_main_conf_t *upstreams_main_conf;

    // get upstream conf
    // step over it and found all upstream that has consul directive
    upstreams_main_conf = ngx_stream_conf_get_module_main_conf(cf, ngx_stream_upstream_module);
    upstream_srv_confs = upstreams_main_conf->upstreams.elts;
    for (i = 0; i < upstreams_main_conf->upstreams.nelts; i++) {
        if (ngx_stream_upstream_dynamic_build_main_conf(cf, main_conf, upstream_srv_confs[i], i) != NGX_OK) {
            return NGX_CONF_ERROR;
        }
    }
    return NGX_CONF_OK;
}

ngx_peer_connection_t *ngx_stream_upstream_dynamic_create_peer_conn(ngx_cycle_t *cycle, ngx_pool_t *pool,
                                                                    ngx_stream_upstream_dynamic_srv_conf_t *srv_conf) {
    struct sockaddr_in *sin;

    ngx_peer_connection_t *pc = ngx_pcalloc(pool, sizeof(ngx_peer_connection_t));
    sin = ngx_pcalloc(pool, sizeof(struct sockaddr_in));

    pc->data = srv_conf;
    pc->get = ngx_event_get_peer;
    pc->log = cycle->log;
    pc->cached = 0;
    pc->connection = NULL;
    pc->log_error = NGX_ERROR_ERR;
    pc->connection = NULL;

    // 初始化 srv_conf 其他需计算的字段
    sin = ngx_pcalloc(pool, sizeof(struct sockaddr_in));
    sin->sin_family = AF_INET;
    sin->sin_port = htons((in_port_t) srv_conf->port);
    sin->sin_addr.s_addr = ngx_inet_addr(srv_conf->host.data, srv_conf->host.len);
    pc->sockaddr = (struct sockaddr *) sin;
    pc->socklen = sizeof(struct sockaddr_in);

    return pc;
}


static ngx_stream_upstream_dynamic_inner_state_t *
ngx_stream_upstream_dynamic_create_inner_state(ngx_cycle_t *cycle, ngx_stream_upstream_dynamic_srv_conf_t *srv_conf) {
    // pool will be destroy after recv done
    ngx_pool_t *pool;
    ngx_stream_upstream_dynamic_inner_state_t *state = srv_conf->state
                                                       ? srv_conf->state
                                                       : ngx_pcalloc(cycle->pool,
                                                                     sizeof(ngx_stream_upstream_dynamic_inner_state_t));
    if (srv_conf->state != NULL) {
        // reinit
        goto reinit;
    }

    // 首次初始化
    if (state == NULL) {
        ngx_log_error(NGX_LOG_EMERG, cycle->log, 0, "alloc state failed");
        return NULL;
    }

    state->srv_conf = srv_conf;
    state->update_generation = 0;
    srv_conf->state = state;

    ngx_event_t *timeout_timer = &state->request_timeout_timer;
    timeout_timer->log = cycle->log;
    timeout_timer->data = state;
    timeout_timer->handler = ngx_stream_upstream_dynamic_on_timeout_timer_handler;

    ngx_event_t *update_timer = &state->update_timer;
    update_timer->log = cycle->log;
    update_timer->data = state;
    update_timer->handler = ngx_stream_upstream_dynamic_on_update_timer_handler;

    reinit:
    pool = ngx_create_pool(NGX_DEFAULT_POOL_SIZE, cycle->log);
    state->recv_vector = vector_create(pool);
    state->send_vector = vector_create(pool);
    if (state->pool != NULL) {
        ngx_destroy_pool(state->pool);
        state->pool = NULL;
    }
    state->pool = pool;
    return state;
}

static void *ngx_stream_upstream_dynamic_create_srv_conf(ngx_conf_t *cf) {
    ngx_stream_upstream_dynamic_srv_conf_t *srv_conf;
    srv_conf =
            ngx_pcalloc(cf->pool, sizeof(ngx_stream_upstream_dynamic_srv_conf_t));
    if (srv_conf == NULL) {
        return NULL;
    }
    srv_conf->host.data = NGX_CONF_UNSET_PTR;
    srv_conf->host.len = NGX_CONF_UNSET;
    srv_conf->port = NGX_CONF_UNSET;

    srv_conf->path_rest_of.data = NGX_CONF_UNSET_PTR;
    srv_conf->path_rest_of.len = NGX_CONF_UNSET;

    srv_conf->dump_file_path.data = NGX_CONF_UNSET_PTR;
    srv_conf->dump_file_path.len = NGX_CONF_UNSET;

    srv_conf->update_interval = NGX_CONF_UNSET;
    srv_conf->request_timeout = NGX_CONF_UNSET;
    srv_conf->cycle = cf->cycle;

    return srv_conf;
}

static char *ngx_stream_upstream_dynamic_set_consul_slot(ngx_conf_t *cf,
                                                         ngx_command_t *cmd,
                                                         void *conf) {
    ngx_uint_t i;
    ngx_str_t *value, s;
    ngx_stream_upstream_dynamic_srv_conf_t *srv_conf;
    ngx_stream_upstream_srv_conf_t *upstream_srv_conf;
    ngx_url_t url;

    // get server config in current server block
    // cf#ctx point to current upstream block context
    upstream_srv_conf = ngx_stream_conf_get_module_srv_conf(cf, ngx_stream_upstream_module);
    srv_conf = ngx_stream_conf_get_module_srv_conf(cf, ngx_stream_upstream_dynamic_module);
    srv_conf->upstream_srv_conf = upstream_srv_conf;
    value = cf->args->elts;

    ngx_memzero(&url, sizeof(url));

    for (i = 2; i < cf->args->nelts; i++) {
        if (ngx_strncmp(value[i].data, "update_interval=", 16) == 0) {
            // use is_sec==0 to get milliseconds
            s.data = value[i].data + 16;
            s.len = value[i].len - 16;
            srv_conf->update_interval = ngx_parse_time(&s, 0);
            continue;
        }
        if (ngx_strncmp(value[i].data, "update_timeout=", 15) == 0) {
            s.data = value[i].data + 15;
            s.len = value[i].len - 15;
            srv_conf->request_timeout = ngx_parse_time(&s, 0);
            continue;
        }
    }
    // value[1].data should be like 127.0.0.1:443/v1/lookup/name?name=a.b.c
    url.url.data = value[1].data;
    url.url.len = value[1].len;
    url.uri_part = 1;
    if (ngx_parse_url(cf->pool, &url) != NGX_OK) {
        if (url.err) {
            ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "url parse err %s in upstream %V", url.err,
                               &srv_conf->upstream_srv_conf->host);
            return NGX_CONF_ERROR;
        }
    }
    srv_conf->port = url.port;
    srv_conf->path_rest_of = url.uri;
    srv_conf->host = url.host;
    return NGX_CONF_OK;
}

static void ngx_stream_upstream_dynamic_send_out(ngx_stream_session_t *session, vector *v) {
    ngx_int_t n;
    ngx_connection_t *conn = session->connection;
    ngx_buf_t buf;

    buf.start = v->data;
    buf.end = v->data + v->cap;
    buf.pos = buf.start;
    buf.last = v->data + v->w_pos;

    while (buf.pos < buf.last) {
        n = conn->send(conn, buf.pos, buf.last - buf.pos);
        if (n > 0) {
            buf.pos += n;
        } else if (n == 0 || n == NGX_AGAIN) {
            continue;
        } else {
            conn->error = 1;
            break;
        }
    }
}

static void
ngx_stream_upstream_dynamic_print_one_upstream(ngx_stream_upstream_dynamic_srv_conf_t *srv_conf, vector *v) {
    ngx_stream_upstream_srv_conf_t *upstream_srv_conf;
    ngx_stream_upstream_rr_peers_t *peers;
    ngx_stream_upstream_rr_peer_t *peer;

    upstream_srv_conf = srv_conf->upstream_srv_conf;
    peers = upstream_srv_conf->peer.data;

    upstream_srv_conf = srv_conf->upstream_srv_conf;

    vector_sprintf(v, "Upstream name: %V, ", &upstream_srv_conf->host);
    vector_sprintf(v, "Backend count: %d\n", peers->number);
    for (peer = peers->peer; peer != NULL; peer = peer->next) {
        vector_sprintf(v, " server %V\n", &peer->name);
    }
}

static void ngx_stream_upstream_dynamic_show_handler(ngx_stream_session_t *session) {
    ngx_stream_upstream_dynamic_main_conf_t *main_conf;
    ngx_uint_t i;
    ngx_stream_upstream_dynamic_srv_conf_t **srv_confs;
    ngx_stream_upstream_dynamic_srv_conf_t *srv_conf;
    ngx_pool_t *pool = ngx_create_pool(NGX_DEFAULT_POOL_SIZE, ngx_cycle->log);
    main_conf = ngx_stream_cycle_get_module_main_conf(ngx_cycle, ngx_stream_upstream_dynamic_module);
    vector *body_buf = vector_create(pool);
    vector *header_buf = vector_create(pool);
    srv_confs = main_conf->servers->elts;

    for (i = 0; i < main_conf->servers->nelts; i++) {
        srv_conf = srv_confs[i];
        ngx_stream_upstream_dynamic_print_one_upstream(srv_conf, body_buf);
    }
    vector_sprintf(header_buf, "HTTP/1.1 200 OK\r\n");
    vector_sprintf(header_buf, "Server: nginx\r\n");
    vector_sprintf(header_buf, "Content-Type: text/plain\r\n");
    vector_sprintf(header_buf, "Content-Length: %d\r\n", body_buf->w_pos);
    vector_sprintf(header_buf, "Connection: close\r\n\r\n");

    // write out!
    ngx_stream_upstream_dynamic_send_out(session, header_buf);
    ngx_stream_upstream_dynamic_send_out(session, body_buf);

    ngx_stream_finalize_session(session, NGX_OK);

    ngx_destroy_pool(pool);
}


static char *ngx_stream_upstream_dynamic_set_dynamic_show(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
    ngx_stream_core_srv_conf_t *core_srv_conf;
    core_srv_conf = ngx_stream_conf_get_module_srv_conf(cf, ngx_stream_core_module);
    core_srv_conf->handler = ngx_stream_upstream_dynamic_show_handler;
    return NGX_CONF_OK;
}


static char *ngx_stream_upstream_dynamic_set_dump_path(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
    ngx_stream_upstream_dynamic_srv_conf_t *srv_conf = conf;
    ngx_str_t *elts, *path;
    elts = cf->args->elts;
    path = &elts[1];
    srv_conf->dump_file_path.data = ngx_pcalloc(cf->pool, path->len);
    ngx_memcpy(srv_conf->dump_file_path.data, path->data, path->len);
    srv_conf->dump_file_path.len = path->len;
    if (ngx_conf_full_name(cf->cycle, &srv_conf->dump_file_path, 1) != NGX_OK) {
        return NGX_CONF_ERROR;
    }
    return NGX_CONF_OK;
}

static ngx_int_t ngx_stream_upstream_dynamic_do_dump_path(ngx_stream_upstream_dynamic_srv_conf_t *srv_conf) {
    ngx_cycle_t *cycle = srv_conf->cycle;
    ngx_str_t *path = &srv_conf->dump_file_path;
    ngx_open_file_t *file;
    ngx_pool_t *temp_pool = ngx_create_pool(NGX_DEFAULT_POOL_SIZE, cycle->log);
    vector *v = vector_create(temp_pool);
    ngx_stream_upstream_rr_peers_t *peers;
    peers = srv_conf->upstream_srv_conf->peer.data;

    ngx_stream_upstream_rr_peer_t *peer = peers->peer;
    for (; peer != NULL; peer = peer->next) {
        vector_sprintf(v, "server %V", &peer->name);
        vector_sprintf(v, " weight=%d", peer->weight);
        vector_sprintf(v, " max_fails=%d", peer->max_fails);
        vector_sprintf(v, " fail_timeout=%ds", peer->fail_timeout);
        vector_sprintf(v, ";\n");
    }
    file = ngx_conf_open_file(cycle, path);
    char *dump_path = ngx_stream_upstream_dynamic_ngx_str_to_char_array(temp_pool, path);
    file->fd = ngx_open_file(dump_path, NGX_FILE_TRUNCATE, NGX_FILE_WRONLY, NGX_FILE_DEFAULT_ACCESS);
    if (file->fd == NGX_INVALID_FILE) {
        ngx_err_t err = errno;
        ngx_log_error(NGX_LOG_ERR, cycle->log, err, "open dump file failed %V", path);
        goto invalid;
    }
    if (ngx_write_fd(file->fd, v->data, v->w_pos) == NGX_ERROR) {
        ngx_log_error(NGX_LOG_ERR, cycle->log, 0, "dump file %V write failed", path);
        goto invalid;
    }
    ngx_close_file(file->fd);
    ngx_destroy_pool(temp_pool);
    return NGX_OK;
    invalid:
    ngx_close_file(file->fd);
    ngx_destroy_pool(temp_pool);
    return NGX_ERROR;
}

static ngx_int_t
ngx_stream_upstream_dynamic_parse_consul_list(ngx_pool_t *pool, ngx_stream_upstream_dynamic_srv_conf_t *srv_conf,
                                              void *data, ngx_array_t *servers) {
    ngx_cycle_t *cycle = srv_conf->cycle;
    cJSON *json;
    // don't parse http response
    // just get body
    char *index_p = ngx_strstr(data, "\r\n\r\n");
    char *body_p;
    if (index_p == NULL) {
        ngx_log_error(NGX_LOG_ERR, cycle->log, 0, "no body present in http response");
        return NGX_ERROR;
    }
    body_p = index_p + 4;
    json = cJSON_Parse(body_p);
    if (json == NULL) {
        const char *error_ptr = cJSON_GetErrorPtr();
        ngx_log_error(NGX_LOG_ERR, cycle->log, 0, "json parse error from upstream %V:%d. err ptr: %s", &srv_conf->host,
                      srv_conf->port, error_ptr);
        return NGX_ERROR;
    }
    cJSON *server_next;
    for (server_next = json->child; server_next != NULL; server_next = server_next->next) {
        // host
        cJSON *host_key = cJSON_GetObjectItem(server_next, "Host");
        // port
        cJSON *port_key = cJSON_GetObjectItem(server_next, "Port");
        // tags
        cJSON *tags_key = cJSON_GetObjectItem(server_next, "Tags");
        // weight
        cJSON *weight_key = cJSON_GetObjectItem(tags_key, "weight");
        if(host_key == NULL || port_key == NULL || tags_key == NULL || weight_key == NULL) {
            ngx_log_error(NGX_LOG_WARN, cycle->log, 0, "json parse error, no key present in body %s",server_next->string);
            continue;
        }
        server_from_consul_t *server = ngx_array_push(servers);
        // host
        int len = strlen(host_key->valuestring);
        server->host.data = ngx_pcalloc(pool, len);
        ngx_memcpy(server->host.data, host_key->valuestring, len);
        server->host.len = len;
        // port
        server->port = port_key->valueint;
        // weight
        char *v = weight_key->valuestring;
        // default 10
        if (v != NULL) {
            server->weight = ngx_atoi((u_char *) v, ngx_strlen(weight_key->valuestring));
        } else {
            server->weight = 10;
        }
    }
    cJSON_Delete(json);
    return NGX_OK;
};

send_request_t
ngx_stream_upstream_dynamic_build_request(ngx_pool_t *pool, ngx_stream_upstream_dynamic_srv_conf_t *srv_conf) {
    send_request_t req;
    char *format_url = "GET %V HTTP/1.1\r\nHost: %V:%d\r\n\r\n";
    ngx_uint_t len = srv_conf->host.len + sizeof(srv_conf->port) + srv_conf->path_rest_of.len + strlen(format_url);
    req.data = ngx_pcalloc(pool, len);
    ngx_sprintf(req.data, format_url, &srv_conf->path_rest_of, &srv_conf->host, srv_conf->port);
    req.len = ngx_strlen(req.data);
    return req;
}

static ngx_int_t
ngx_stream_upstream_dynamic_sync_fetch_consul_list(ngx_pool_t *pool, ngx_stream_upstream_dynamic_srv_conf_t *srv_conf,
                                                   u_char **data) {
    send_request_t req;
    ngx_int_t socket_fd = NGX_ERROR;
    u_char response_buf[ngx_pagesize];
    struct timeval tv_timeout;
    ngx_cycle_t *cycle = srv_conf->cycle;

    // 1s connect timeout
    tv_timeout.tv_sec = 1;
    tv_timeout.tv_usec = 0;
    if ((socket_fd = socket(AF_INET, SOCK_STREAM, 0)) == NGX_ERROR) {
        ngx_log_error(NGX_LOG_ERR, cycle->log, 0, "create socket failed");
        return NGX_ERROR;
    }
    if (setsockopt(socket_fd, SOL_SOCKET, SO_SNDTIMEO, (void *) &tv_timeout, sizeof(struct timeval)) < 0) {
        ngx_log_error(NGX_LOG_ERR, cycle->log, 0, "socket set SO_SNDTIMEO failed");
        return NGX_ERROR;
    }
    if (setsockopt(socket_fd, SOL_SOCKET, SO_RCVTIMEO, (void *) &tv_timeout, sizeof(struct timeval)) < 0) {
        ngx_log_error(NGX_LOG_ERR, cycle->log, 0, "socket set SO_RCVTIMEO failed");
        return NGX_ERROR;
    }
    vector *v = vector_create(pool);
    struct sockaddr_in sin;
    sin.sin_family = AF_INET;
    sin.sin_port = htons((in_port_t) srv_conf->port);
    sin.sin_addr.s_addr = ngx_inet_addr(srv_conf->host.data, srv_conf->host.len);
    // connect
    if (connect(socket_fd, (struct sockaddr *) &sin, sizeof(sin)) == NGX_ERROR) {
        ngx_err_t err = errno;
        ngx_log_error(NGX_LOG_ERR, cycle->log, err, "connect remote host %V:%d failed", srv_conf->host, srv_conf->port);
        return NGX_ERROR;
    }
    // send
    req = ngx_stream_upstream_dynamic_build_request(pool, srv_conf);
    ngx_uint_t pos = 0;
    while (req.len > pos) {
        int n = 0;
        if ((n = send(socket_fd, (char *) req.data + pos, req.len, 0)) <= 0) {
            ngx_err_t err = errno;
            ngx_log_error(NGX_LOG_WARN, cycle->log, err, "send(sync) return %d", n);
            ngx_log_error(NGX_LOG_ERR, cycle->log, err, "ngx send bytes %n failed", n);
            ngx_destroy_pool(srv_conf->cycle->pool);
            return NGX_ERROR;
        }
        pos += n;
    }
    pos = 0;
    while (1) {
        int n = 0;
        // By default, if application layer protocol is http, after first http response recv successfully
        // tcp connection won't be close, so another recv call will block and -1 will returned when SO_RCVTIMEO expired
        if ((n = recv(socket_fd, response_buf, ngx_pagesize, 0)) <= 0) {
            if (n == 0) {
                // eof
                break;
            } else {
                // maybe http response transfer completed.
                // pass to http parser.
                ngx_err_t err = errno;
                ngx_log_error(NGX_LOG_WARN, cycle->log, err, "recv(sync) return %d", n);
                break;
            }
        }
        vector_push(v, response_buf, ngx_strlen(response_buf));
    }
    vector_put_char(v, '\0');
    *data = v->data;
    return NGX_OK;
}

static ngx_array_t *
ngx_stream_upstream_dynamic_consul_like_server_to_upstream_server(ngx_pool_t *pool, ngx_array_t *consul_like_servers) {
    ngx_array_t *upstream_servers = ngx_pcalloc(pool, sizeof(ngx_stream_upstream_server_t));
    ngx_uint_t len = consul_like_servers->nelts;
    ngx_uint_t i;
    server_from_consul_t *c_like = consul_like_servers->elts;

    u_char b[8];
    ngx_memzero(b, 8);
    upstream_servers = ngx_array_create(pool, len, sizeof(ngx_stream_upstream_server_t));

    for (i = 0; i < len; i++) {
        ngx_stream_upstream_server_t *upstream_server;
        server_from_consul_t *temp_c_like;
        upstream_server = ngx_array_push(upstream_servers);
        temp_c_like = &c_like[i];

        ngx_sprintf(b, "%d", temp_c_like->port);

        // host must be ip
        if (temp_c_like->host.len + 2 + 1 > NGX_SOCKADDRLEN) {
            continue;
        }
        struct sockaddr_in *sin = ngx_pcalloc(pool, sizeof(struct sockaddr_in));
        sin->sin_family = AF_INET;
        sin->sin_port = htons((in_port_t) temp_c_like->port);
        sin->sin_addr.s_addr = ngx_inet_addr(temp_c_like->host.data, temp_c_like->host.len);

        ngx_addr_t *addr = ngx_pcalloc(pool, sizeof(ngx_addr_t));
        addr->sockaddr = (struct sockaddr *) sin;
        addr->socklen = sizeof(*sin);
        ngx_uint_t port_len = strlen((char *) b);
        ngx_uint_t name_len = temp_c_like->host.len + port_len + 1;
        addr->name.data = ngx_pcalloc(pool, name_len);
        u_char *dst_p = ngx_snprintf(addr->name.data, temp_c_like->host.len + 1, "%V:", &temp_c_like->host);
        ngx_snprintf(dst_p, port_len, (const char *) b);

        addr->name.len = name_len;

        upstream_server->weight = temp_c_like->weight;
        upstream_server->max_fails = 2;
        upstream_server->fail_timeout = 10;
        upstream_server->backup = 0;
        upstream_server->down = 0;
        upstream_server->addrs = addr;
        upstream_server->naddrs = 1;
        upstream_server->name = addr->name;
    }
    return upstream_servers;
}

static ngx_int_t
ngx_stream_upstream_dynamic_update_upstream_server_list(ngx_stream_upstream_dynamic_srv_conf_t *srv_conf,
                                                        ngx_array_t *dynamic_servers_arr) {
    ngx_stream_upstream_srv_conf_t *upstream_srv_conf;
    ngx_stream_upstream_server_t *upstream_servers;
    ngx_uint_t i, w = 0, n = 0;
    ngx_stream_upstream_rr_peer_t *peer;
    ngx_stream_upstream_rr_peers_t *peers;
    ngx_stream_upstream_rr_peer_t *old_peer;
    ngx_stream_upstream_dynamic_inner_state_t *state;
    ngx_cycle_t *cycle;

    cycle = srv_conf->cycle;
    upstream_srv_conf = srv_conf->upstream_srv_conf;
    peers = (ngx_stream_upstream_rr_peers_t *) upstream_srv_conf->peer.data;
    old_peer = peers->peer;
    state = srv_conf->state;
    state->update_generation++;
    // avoid uint_t overflow
    state->update_generation %= 1000;

    upstream_servers = dynamic_servers_arr->elts;
    // 更新upstream的peers
    for (i = 0; i < dynamic_servers_arr->nelts; i++) {
        ngx_stream_upstream_server_t *upstream_server = &upstream_servers[i];

        peer = ngx_calloc(sizeof(ngx_stream_upstream_rr_peer_t), cycle->log);
        if (peer == NULL) {
            ngx_log_error(NGX_LOG_ERR, cycle->log, 0, "upstream peer alloc failed");
            return NGX_ERROR;
        }
        void *p = peers->peer;
        peer->sockaddr = ngx_calloc(sizeof(struct sockaddr_in), cycle->log);
        if (peer->sockaddr == NULL) {
            ngx_log_error(NGX_LOG_ERR, cycle->log, 0, "calloc for peer sockaddr failed");
            return NGX_ERROR;
        }
        ngx_memcpy(peer->sockaddr, upstream_server->addrs->sockaddr, sizeof(struct sockaddr_in));

        peer->socklen = sizeof(struct sockaddr_in);
        peer->name.data = ngx_calloc(upstream_server->name.len, cycle->log);
        ngx_memcpy(peer->name.data, upstream_server->name.data, upstream_server->name.len);
        peer->name.len = upstream_server->name.len;
        peer->max_fails = upstream_server->max_fails;
        peer->fail_timeout = upstream_server->fail_timeout;
        peer->down = upstream_server->down;
        peer->weight = upstream_server->weight;
        w += peer->weight;
        peer->effective_weight = upstream_server->weight;
        peer->current_weight = 0;

        peer->conns = 0;

        peers->peer = peer;
        peer->next = p;
        if (i == 0) {
            peer->next = NULL;
        }
    }
    n = dynamic_servers_arr->nelts;
    peers->number = n;
    peers->total_weight = w;
    peers->weighted = (n != w);
    peers->single = (n == 1);
    ngx_stream_upstream_dynamic_release_old_peer(srv_conf, old_peer);
    return NGX_OK;
}

static void ngx_stream_upstream_dynamic_async_recv_handler(ngx_event_t *event) {
    ngx_stream_upstream_dynamic_srv_conf_t *srv_conf;
    ngx_stream_upstream_dynamic_inner_state_t *state;
    ngx_connection_t *conn;
    ngx_cycle_t *cycle;
    u_char tempbuf[ngx_pagesize];
    vector *recv_buf;

    conn = event->data;
    state = conn->data;
    srv_conf = state->srv_conf;
    cycle = srv_conf->cycle;
    recv_buf = state->recv_vector;

    while (conn->read->ready) {
        ngx_int_t n = 0;
        n = conn->recv(conn, tempbuf, ngx_pagesize);
        if (ngx_handle_read_event(conn->read, 0) != NGX_OK) {
            ngx_log_error(NGX_LOG_WARN, cycle->log, 0, "ngx_handle_read_event error");
        }
        if (n > 0) {
            vector_push(recv_buf, tempbuf, ngx_strlen(tempbuf));
            continue;
        } else if (n == 0) {
            // eof
            break;
        } else if (n == NGX_AGAIN) {
            break;
        } else {
            ngx_err_t err = errno;
            ngx_log_error(NGX_LOG_WARN, cycle->log, err, "recv(async) return %d", n);
            conn->error = 1;
            goto finally;
        }
    }
    // release all mems allocated during update upstreams period
    if (ngx_stream_upstream_dynamic_start_update_list(state->pool, srv_conf, recv_buf->data) == NGX_ERROR) {
        ngx_log_error(NGX_LOG_EMERG, cycle->log, 0,
                      "update from consul failed at ngx_stream_upstream_dynamic_start_update_list");
    }
    finally:
    ngx_stream_upstream_dynamic_reset_timer_state(state);
    ngx_destroy_pool(state->pool);
    state->pool = NULL;
    state->recv_vector = NULL;
    state->send_vector = NULL;
    state->peer_conn = NULL;
    return;
}

static void ngx_stream_upstream_dynamic_reset_timer_state(ngx_stream_upstream_dynamic_inner_state_t *state) {
    if (state->request_timeout_timer.timer_set) {
        ngx_del_timer(&state->request_timeout_timer);
    }
    if (state->update_timer.timer_set) {
        ngx_del_timer(&state->update_timer);
    }
    if (state->peer_conn->connection != NULL) {
        ngx_close_connection(state->peer_conn->connection);
    }
    ngx_add_timer(&state->update_timer, state->srv_conf->update_interval);
}

static void ngx_stream_upstream_dynamic_async_send_handler(ngx_event_t *event) {
    ngx_stream_upstream_dynamic_inner_state_t *state;
    ngx_stream_upstream_dynamic_srv_conf_t *srv_conf;
    ngx_connection_t *conn;
    vector *send_buf;
    ngx_uint_t *send_pos;
    ngx_cycle_t *cycle;

    conn = event->data;
    state = conn->data;
    srv_conf = state->srv_conf;
    cycle = srv_conf->cycle;
    send_buf = state->send_vector;
    send_pos = &send_buf->r_pos;

    ngx_uint_t total_len = send_buf->w_pos;

    while (*send_pos < total_len && conn->write->ready) {
        ngx_int_t n = 0;
        n = conn->send(conn, send_buf->data + *send_pos, total_len - *send_pos);
        if (ngx_handle_write_event(conn->write, 0) != NGX_OK) {
            ngx_log_error(NGX_LOG_WARN, cycle->log, 0, "ngx_handle_write_event error");
        }
        if (n > 0) {
            *send_pos += n;
            continue;
        } else if (n == 0 || n == NGX_AGAIN) {
            return;
        } else {
            conn->error = 1;
            ngx_err_t err = errno;
            ngx_log_error(NGX_LOG_WARN, cycle->log, err, "send(async) return %d", n);
            goto invalid;
        }
    }
    return;

    invalid:
    ngx_stream_upstream_dynamic_reset_timer_state(state);
    return;
}

static void ngx_stream_upstream_dynamic_on_update_timer_handler(ngx_event_t *event) {
    ngx_stream_upstream_dynamic_inner_state_t *state;
    ngx_stream_upstream_dynamic_srv_conf_t *srv_conf;
    ngx_int_t res;
    ngx_cycle_t *cycle;
    ngx_log_t *log;
    send_request_t req;
    ngx_pool_t *temp_pool;
    ngx_peer_connection_t *pc;
    state = event->data;
    cycle = state->srv_conf->cycle;
    log = cycle->log;
    srv_conf = state->srv_conf;

    ngx_stream_upstream_dynamic_create_inner_state(cycle, srv_conf);
    temp_pool = ngx_create_pool(NGX_DEFAULT_POOL_SIZE, log);
    pc = ngx_stream_upstream_dynamic_create_peer_conn(cycle, state->pool, srv_conf);

    state->peer_conn = pc;

    ngx_add_timer(&state->request_timeout_timer, srv_conf->request_timeout);
    res = ngx_event_connect_peer(state->peer_conn);
    // ngx_add_timer(&state->update_timer,srv_conf->update_interval);
    if (res == NGX_ERROR || res == NGX_DECLINED) {
        ngx_log_error(NGX_LOG_ERR, log, 0, "async connect to upstream %V failed. waiting for next update",
                      &srv_conf->upstream_srv_conf->host);
        ngx_stream_upstream_dynamic_reset_timer_state(state);
        return;
    }

    req = ngx_stream_upstream_dynamic_build_request(temp_pool, srv_conf);

    vector_push(state->send_vector, req.data, req.len);
    ngx_destroy_pool(temp_pool);

    state->peer_conn->connection->data = state;
    state->peer_conn->connection->sendfile = 0;
    state->peer_conn->connection->log = log;
    state->peer_conn->connection->read->log = log;
    state->peer_conn->connection->write->log = log;
    state->peer_conn->connection->read->handler = ngx_stream_upstream_dynamic_async_recv_handler;
    state->peer_conn->connection->write->handler = ngx_stream_upstream_dynamic_async_send_handler;
}


// 请求consul超时关闭掉之前connection，删除timer
// 设置新timer，重新请求
static void ngx_stream_upstream_dynamic_on_timeout_timer_handler(ngx_event_t *event) {
    ngx_stream_upstream_dynamic_inner_state_t *state;
    state = event->data;
    ngx_stream_upstream_dynamic_reset_timer_state(state);
}

static ngx_int_t ngx_stream_upstream_dynamic_add_timer(ngx_stream_upstream_dynamic_srv_conf_t *srv_conf) {
    ngx_stream_upstream_dynamic_inner_state_t *state;
    state = srv_conf->state;
    ngx_add_timer(&state->update_timer, srv_conf->update_interval);
    return NGX_OK;
}

static ngx_int_t ngx_stream_upstream_dynamic_release_old_peer(ngx_stream_upstream_dynamic_srv_conf_t *srv_conf,
                                                              ngx_stream_upstream_rr_peer_t *old_peer) {
    ngx_stream_upstream_dynamic_inner_state_t *state;
    ngx_stream_upstream_rr_peer_t *next_old_peer;

    next_old_peer = old_peer->next;
    state = srv_conf->state;

    if (state->update_generation > 1) {
        // 我们只释放由我们alloc的peer，不释放启动时的peer
        while (old_peer != NULL) {
            ngx_free(old_peer->sockaddr);
            ngx_free(old_peer->name.data);
            ngx_free(old_peer);
            old_peer = next_old_peer;
            if (next_old_peer != NULL) {
                next_old_peer = next_old_peer->next;
            }
        }
    }
    return NGX_OK;
}

static void ngx_stream_upstream_dynamic_clear_all_events(ngx_cycle_t *cycle) {
    ngx_stream_upstream_dynamic_main_conf_t *main_conf;
    ngx_stream_upstream_dynamic_srv_conf_t *srv_conf;
    ngx_stream_upstream_dynamic_srv_conf_t **srv_confs;
    ngx_uint_t i;
    main_conf = ngx_stream_cycle_get_module_main_conf(cycle, ngx_stream_upstream_dynamic_module);
    srv_confs = main_conf->servers->elts;
    for (i = 0; i < main_conf->servers->nelts; i++) {
        srv_conf = srv_confs[i];
        if (srv_conf->state->request_timeout_timer.timer_set) {
            ngx_del_timer(&srv_conf->state->request_timeout_timer);

        }
        if (srv_conf->state->update_timer.timer_set) {
            ngx_del_timer(&srv_conf->state->update_timer);
        }
    }
}