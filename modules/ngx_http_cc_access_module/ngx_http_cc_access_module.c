#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

/*
预先分配1G shm
1. 启动后从文件 load初始数据，在shm初始化rbtree
2. 启动http server，接口暴露 add，delete
3. 接口wlock修改shm的rbtree
4. worker rlock 读 rbtree
*/
// 1G
#define SHM_DATA_SIZE (1 << 30)
#define API_PATH "/cc-api"

struct threshold_s {
    ngx_str_t key;
    ngx_int_t value;
};

struct protect_rule_s {
    ngx_int_t rule_id;
    ngx_str_t path;
    struct {
        struct {
            ngx_str_t key;
            ngx_str_t value;
        } object;
        // struct threshold_s
        ngx_array_t threshold;
        ngx_int_t timing_window;
    } rate;
    ngx_str_t action;
};

typedef struct protect_rule_s protect_rule_t;

struct domain_protect_s {
    ngx_str_t domain;
    struct {
        ngx_array_t list;
        ngx_int_t timeout;
    } ip_blocklist;
    protect_rule_t protect_rule;
};

typedef struct domain_protect_t domain_protect_t;

typedef struct {
    ngx_rbtree_t rbtree;
    ngx_rbtree_node_t *sentinel;

    /* custom per-tree data here */
} cc_rbtree_t;

typedef ngx_str_node_t cc_rbtree_node_t;

static ngx_str_t SHARED_NAME = ngx_string("ngx-http-cc-filter-shared-pool");

struct ngx_http_cc_access_main_conf_s {
    ngx_str_t iplist_path;
    ngx_cycle_t *cycle;
    ngx_pool_t *pool;
    ngx_hash_t hash;
    // mmap 分配
    ngx_atomic_t *rwlock;
    // 以下全部结构都由shm slab分配
    cc_rbtree_t *cc_rbtree;
    ngx_slab_pool_t *shm_pool;
};

struct ngx_http_cc_access_srv_conf_s {};

#define UPDATE_INTERVAL 5000
typedef struct ngx_http_cc_access_main_conf_s ngx_http_cc_access_main_conf_t;

static char *ngx_http_cc_access_set_server(ngx_conf_t *cf, ngx_command_t *cmd,
                                           void *conf);
static void *ngx_http_cc_access_create_main_conf(ngx_conf_t *cf);
static char *ngx_http_cc_access_init_main_conf(ngx_conf_t *cf, void *conf);
static ngx_int_t ngx_http_cc_access_init_process(ngx_cycle_t *cycle);
static ngx_int_t ngx_http_cc_access_init_module(ngx_cycle_t *cycle);
static ngx_int_t ngx_http_cc_access_http_request_hook(ngx_conf_t *cf);
static ngx_int_t ngx_http_cc_access_postread_handler(ngx_http_request_t *r);
static void ngx_http_cc_access_exit_process(ngx_cycle_t *cycle);
static cc_rbtree_node_t *cc_rbtree_str_lookup(ngx_rbtree_t *rbtree,
                                              ngx_str_t *str);
ngx_int_t ngx_http_cc_access_add(ngx_cycle_t *cycle, char *file_data,
                                 ngx_int_t file_data_size);
ngx_int_t ngx_http_cc_module_delete(ngx_cycle_t *cycle, ngx_pool_t *pool,
                                    char *file_data, ngx_int_t file_data_size);
static ngx_command_t ngx_http_cc_access_command[] = {
    {ngx_string("iplist_path"), NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1,
     ngx_conf_set_str_slot,
     offsetof(ngx_http_cc_access_main_conf_t, iplist_path), 0, NULL},
    {
        ngx_string("cc_filter_server"),
        NGX_HTTP_LOC_CONF | NGX_CONF_NOARGS,
        ngx_http_cc_access_set_server,
        NGX_HTTP_LOC_CONF_OFFSET,
        0,
        NULL,
    },
    ngx_null_command};
static ngx_http_module_t ngx_http_cc_access_ctx = {
    NULL,                                 /* preconfiguration */
    ngx_http_cc_access_http_request_hook, /* postconfiguration */

    ngx_http_cc_access_create_main_conf, /* create main configuration */
    ngx_http_cc_access_init_main_conf,   /* init main configuration */

    NULL, /* create server configuration */
    NULL,
    NULL,
    NULL, /* merge server configuration */
};

ngx_module_t ngx_http_cc_access_module = {
    NGX_MODULE_V1,
    &ngx_http_cc_access_ctx,         /* module context */
    ngx_http_cc_access_command,      /* module directives */
    NGX_HTTP_MODULE,                 /* module type */
    NULL,                            /* init master */
    ngx_http_cc_access_init_module,  /* init module */
    ngx_http_cc_access_init_process, /* init process */
    NULL,                            /* init thread */
    NULL,                            /* exit thread */
    ngx_http_cc_access_exit_process, /* exit process */
    NULL,                            /* exit master */
    NGX_MODULE_V1_PADDING};

ngx_str_t *create_str(ngx_pool_t *pool, void *p, ngx_int_t size) {
    ngx_str_t *str = ngx_pcalloc(pool, sizeof(ngx_str_t));
    if (str == NULL) {
        return NULL;
    }
    str->data = ngx_pcalloc(pool, size);
    if (str->data == NULL) {
        return NULL;
    }
    str->len = size;
    ngx_memcpy(str->data, p, size);
    return str;
}

// 遍历rbtree
static ngx_int_t
ngx_http_cc_access_dump_rbtree(ngx_http_cc_access_main_conf_t *main_conf) {
    ngx_int_t ret = NGX_OK;
    ngx_rwlock_rlock(main_conf->rwlock);

    ngx_rwlock_unlock(main_conf->rwlock);
    return ret;
}

// POST /?action=<add|delete>
static void ngx_http_cc_access_server_posthandler(ngx_http_request_t *r) {
    ngx_int_t ret = NGX_DONE;
    ngx_http_cc_access_main_conf_t *main_conf;
    ngx_chain_t *in, out;
    ngx_cycle_t *cycle;

    ngx_buf_t *out_buf;
    main_conf = ngx_http_get_module_main_conf(r, ngx_http_cc_access_module);
    cycle = main_conf->cycle;
    ngx_pool_t *temp_pool = NULL;
    ngx_buf_t *buf =
        ngx_create_temp_buf(cycle->pool, r->headers_in.content_length_n);

    for (in = r->request_body->bufs; in; in = in->next) {
        ngx_int_t len = ngx_buf_size(in->buf);
        memcpy(buf->last, in->buf->pos, len);
        buf->last += len;
    }

    char *action = "action";
    ngx_str_t act;
    ret = ngx_http_arg(r, (u_char *)action, ngx_strlen(action), &act);
    out_buf = ngx_create_temp_buf(cycle->pool, 1024);
    if (ret) {
        goto done;
    }
    temp_pool = ngx_create_pool(2048, cycle->log);
    if (temp_pool == NULL) {
        goto done;
    }
    if (out_buf == NULL) {
        ret = NGX_HTTP_INTERNAL_SERVER_ERROR;
        goto done;
    }
    ngx_log_debug(NGX_LOG_DEBUG_HTTP, cycle->log, 0,
                  "receive cc http action %V", &act);
    if (memcmp(act.data, "add", act.len) == 0) {
        // add
        ret = ngx_http_cc_access_add(cycle, (char *)buf->start,
                                     buf->end - buf->start);

    } else if (memcmp(act.data, "delete", act.len) == 0) {
        // delete
        ret = ngx_http_cc_module_delete(cycle, temp_pool, (char *)buf->start,
                                        buf->end - buf->start);
    } else if (memcmp(act.data, "dump", act.len) == 0) {
        ret = ngx_http_cc_access_dump_rbtree(main_conf);
    }
done:
    out_buf->last = ngx_sprintf(out_buf->pos, "%O", ret);
    out_buf->last_buf = (r == r->main) ? 1 : 0;
    out_buf->last_in_chain = 1;
    r->headers_out.status = NGX_HTTP_OK;
    r->headers_out.content_length_n = out_buf->last - out_buf->pos;
    ret = ngx_http_send_header(r);
    out.buf = out_buf;
    out.next = NULL;
    ret = ngx_http_output_filter(r, &out);
    ngx_http_finalize_request(r, ret);
    if (temp_pool) {
        ngx_destroy_pool(temp_pool);
    }
}
static ngx_int_t
ngx_http_cc_access_module_server_handler(ngx_http_request_t *r) {
    ngx_http_read_client_request_body(r, ngx_http_cc_access_server_posthandler);
    return NGX_DONE;
}

static char *ngx_http_cc_access_set_server(ngx_conf_t *cf, ngx_command_t *cmd,
                                           void *conf) {
    ngx_http_core_loc_conf_t *core_http_loc_conf;
    core_http_loc_conf =
        ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
    core_http_loc_conf->handler = ngx_http_cc_access_module_server_handler;
    return NGX_CONF_OK;
}

ngx_int_t ngx_http_cc_access_add(ngx_cycle_t *cycle, char *file_data,
                                 ngx_int_t file_data_size) {
    ngx_http_cc_access_main_conf_t *main_conf =
        ngx_http_cycle_get_module_main_conf(cycle, ngx_http_cc_access_module);
    char *last = file_data + file_data_size;
    char *pos = file_data;
    ngx_int_t len = 0;

    for (; pos < last; pos += len + 1) {
        pos = strtok(pos, "\n");
        if (pos == NULL) {
            // invalid file or not found
            break;
        }
        len = ngx_strlen(pos);
        // skip last \0
        ngx_rwlock_wlock(main_conf->rwlock);
        cc_rbtree_node_t *node = ngx_slab_alloc_locked(
            main_conf->shm_pool, sizeof(cc_rbtree_node_t));
        void *str = ngx_slab_alloc_locked(main_conf->shm_pool, len);
        ngx_memcpy(str, pos, len);
        node->str.data = str;
        node->str.len = len;
        u_char *ip = str;
        ngx_int_t ip_len = len;
        ngx_uint_t hash = ngx_hash_key(ip, ip_len);
        node->node.key = hash;
        ngx_log_debug(NGX_LOG_DEBUG_HTTP, cycle->log, 0, "key %V", &node->str);
        ngx_rbtree_insert(&main_conf->cc_rbtree->rbtree, &node->node);
        ngx_rwlock_unlock(main_conf->rwlock);
    }
    return NGX_OK;
}

ngx_str_node_t *ngx_cc_str_rbtree_lookup(ngx_rbtree_t *rbtree, ngx_str_t *val,
                                         ngx_uint_t hash) {
    ngx_int_t rc;
    ngx_str_node_t *n;
    ngx_rbtree_node_t *node, *sentinel;

    node = rbtree->root;
    sentinel = rbtree->sentinel;

    while (node != sentinel) {

        n = (ngx_str_node_t *)node;

        if (hash != node->key) {
            node = (hash < node->key) ? node->left : node->right;
            continue;
        }

        if (val->len != n->str.len) {
            node = (val->len < n->str.len) ? node->left : node->right;
            continue;
        }

        rc = ngx_memcmp(val->data, n->str.data, val->len);

        if (rc < 0) {
            node = node->left;
            continue;
        }

        if (rc > 0) {
            node = node->right;
            continue;
        }

        return n;
    }

    return NULL;
}

static cc_rbtree_node_t *cc_rbtree_str_lookup(ngx_rbtree_t *rbtree,
                                              ngx_str_t *str) {

    u_char *ip = str->data;
    ngx_int_t ip_len = str->len;
    ngx_uint_t hash = ngx_hash_key(ip, ip_len);
    return ngx_cc_str_rbtree_lookup(rbtree, str, hash);
}

ngx_int_t ngx_http_cc_module_delete(ngx_cycle_t *cycle, ngx_pool_t *pool,
                                    char *file_data, ngx_int_t file_data_size) {
    ngx_http_cc_access_main_conf_t *main_conf =
        ngx_http_cycle_get_module_main_conf(cycle, ngx_http_cc_access_module);
    char *last = file_data + file_data_size;
    char *pos = file_data;
    ngx_int_t len = 0;
    ngx_str_t str;
    for (; pos < last; pos += len + 1) {
        pos = strtok(pos, "\n");
        if (pos == NULL) {
            // invalid file or not found
            break;
        }
        len = ngx_strlen(pos);
        // skip last \0
        ngx_rwlock_wlock(main_conf->rwlock);
        str.data = (u_char *)pos;
        str.len = len;
        ngx_log_debug(NGX_LOG_DEBUG_HTTP, cycle->log, 0, "hash key %V", &str);
        cc_rbtree_node_t *node =
            cc_rbtree_str_lookup(&main_conf->cc_rbtree->rbtree, &str);
        if (node != NULL) {
            ngx_rbtree_delete(&main_conf->cc_rbtree->rbtree, &node->node);
            // free ngx_str_t#*data
            ngx_slab_free_locked(main_conf->shm_pool, node->str.data);
            ngx_slab_free_locked(main_conf->shm_pool, node);
        }
        ngx_rwlock_unlock(main_conf->rwlock);
    }
    return NGX_OK;
}

static void *ngx_http_cc_access_create_main_conf(ngx_conf_t *cf) {
    ngx_http_cc_access_main_conf_t *main_conf;
    main_conf =
        ngx_pcalloc(cf->cycle->pool, sizeof(ngx_http_cc_access_main_conf_t));

    return main_conf;
}

static ngx_int_t ngx_http_cc_access_init_zone(ngx_shm_zone_t *shm_zone,
                                              void *data) {
    ngx_http_cc_access_main_conf_t *main_conf;
    ngx_slab_pool_t *shmpool;

    shmpool = (ngx_slab_pool_t *)shm_zone->shm.addr;
    main_conf = shm_zone->data;
    main_conf->shm_pool = shmpool;
    main_conf->cc_rbtree = ngx_slab_alloc(shmpool, sizeof(cc_rbtree_t));
    main_conf->cc_rbtree->sentinel =
        ngx_slab_alloc(shmpool, sizeof(ngx_rbtree_node_t));
    ngx_rbtree_init(&main_conf->cc_rbtree->rbtree,
                    main_conf->cc_rbtree->sentinel,
                    ngx_str_rbtree_insert_value);
    return NGX_OK;
}

static char *ngx_http_cc_access_init_main_conf(ngx_conf_t *cf, void *conf) {

    ngx_http_cc_access_main_conf_t *main_conf = conf;

    ngx_log_error(NGX_LOG_INFO, cf->cycle->log, 0, "iplist_path %V",
                  &main_conf->iplist_path);
    main_conf->cycle = cf->cycle;
    ngx_shm_zone_t *shm_zone;
    shm_zone = ngx_shared_memory_add(cf, &SHARED_NAME, SHM_DATA_SIZE,
                                     &ngx_http_cc_access_module);
    if (shm_zone == NULL) {
        ngx_log_error(NGX_LOG_EMERG, cf->cycle->log, 0, "shm zone error");
        return NGX_CONF_ERROR;
    }
    shm_zone->data = main_conf;
    shm_zone->init = ngx_http_cc_access_init_zone;
    main_conf->rwlock = mmap(NULL, sizeof(ngx_atomic_t), PROT_READ | PROT_WRITE,
                             MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    return NGX_CONF_OK;
}

static ngx_int_t ngx_http_cc_access_init_process(ngx_cycle_t *cycle) {
    ngx_int_t ret = NGX_OK;
    // ngx_http_cc_access_main_conf_t *main_conf =
    //     ngx_http_cycle_get_module_main_conf(cycle,
    //     ngx_http_cc_access_module);
    return ret;
}

static ngx_int_t ngx_http_cc_access_load_from_file(ngx_cycle_t *cycle,
                                                   ngx_pool_t *pool,
                                                   ngx_str_t filename,
                                                   u_char **filedata,
                                                   ngx_uint_t *filesize) {
    ngx_file_t file;
    size_t total_len;
    ssize_t n;
    ngx_int_t ret = NGX_OK;
    if (ngx_get_full_name(pool, (ngx_str_t *)&cycle->conf_prefix, &filename) !=
        NGX_OK) {
        ngx_log_error(NGX_LOG_WARN, cycle->log, ngx_errno,
                      "get file %V full name error", &file.name);
        ret = NGX_ERROR;
        goto out;
    }
    // 文件格式
    // 127.0.0.1
    // 0.0.0.0
    // 0.0.0.1
    // 0.0.0.2
    // 0.0.0.3/8
    file.name = filename;
    file.log = cycle->log;
    file.fd = ngx_openat_file(NGX_AT_FDCWD, file.name.data, NGX_FILE_RDONLY,
                              NGX_FILE_OPEN, NGX_FILE_DEFAULT_ACCESS);
    struct stat sb;
    if (file.fd < 0) {
        ngx_log_error(NGX_LOG_WARN, cycle->log, ngx_errno,
                      "file %V error, try again", &file.name);
        ret = NGX_ERROR;
        goto out;
    }
    if (fstat(file.fd, &sb)) {
        ngx_log_error(NGX_LOG_EMERG, cycle->log, ngx_errno, "file %V error",
                      &file.name);
        ret = NGX_ERROR;
        goto out;
    }
    total_len = sb.st_size;
    *filesize = total_len;
    u_char *_filedata = ngx_pcalloc(pool, total_len + 1);
    n = ngx_read_file(&file, _filedata, total_len, 0);
    if (n < 0) {
        ret = NGX_ERROR;
        goto out;
    }
    *filedata = _filedata;
    *filesize = total_len;
out:
    return ret;
}

static ngx_int_t ngx_http_cc_access_init_module(ngx_cycle_t *cycle) {
    ngx_int_t ret = NGX_OK;
    ngx_pool_t *temp_pool = NULL;
    ngx_http_cc_access_main_conf_t *main_conf;
    ngx_str_t empty_file = ngx_string("");
    u_char *filedata = empty_file.data;
    ngx_uint_t filesize = empty_file.len;
    main_conf =
        ngx_http_cycle_get_module_main_conf(cycle, ngx_http_cc_access_module);
    temp_pool = ngx_create_pool(NGX_CYCLE_POOL_SIZE, cycle->log);
    if (temp_pool == NULL) {
        ret = NGX_ERROR;
        goto out;
    }

    if (main_conf->iplist_path.data != NULL) {
        ret = ngx_http_cc_access_load_from_file(
            cycle, temp_pool, main_conf->iplist_path, &filedata, &filesize);
        if (ret != NGX_OK) {
            goto out;
        }
    }
    ret = ngx_http_cc_access_add(cycle, (char *)filedata, filesize);
out:
    ngx_destroy_pool(temp_pool);
    return ret;
}

ngx_int_t print_current_pwd(ngx_cycle_t *cycle) {
    char *p;
    p = ngx_pnalloc(cycle->pool, NGX_MAX_PATH);
    if (p == NULL) {
        return NGX_ERROR;
    }
    if (ngx_getcwd(p, NGX_MAX_PATH) == 0) {
        ngx_log_stderr(ngx_errno, "[emerg]: " ngx_getcwd_n " failed");
        return NGX_ERROR;
    }
    ngx_log_error(NGX_LOG_INFO, cycle->log, 0, "cwd %s", p);
    return NGX_OK;
}

static void ngx_http_cc_access_exit_process(ngx_cycle_t *cycle) {}

static ngx_int_t ngx_http_cc_access_http_request_hook(ngx_conf_t *cf) {
    ngx_http_core_main_conf_t *http_main_conf;
    ngx_http_handler_pt *h;

    http_main_conf =
        ngx_http_conf_get_module_main_conf(cf, ngx_http_core_module);
    h = ngx_array_push(&http_main_conf->phases[NGX_HTTP_ACCESS_PHASE].handlers);
    if (h == NULL) {
        return NGX_ERROR;
    }
    *h = ngx_http_cc_access_postread_handler;
    return NGX_OK;
}

static ngx_int_t ngx_http_cc_access_postread_handler(ngx_http_request_t *r) {
    ngx_http_cc_access_main_conf_t *main_conf;
    main_conf = ngx_http_get_module_main_conf(r, ngx_http_cc_access_module);
    if (ngx_memcmp(r->uri.data, API_PATH, r->uri.len) == 0) {
        // don't capture api path
        return NGX_OK;
    }

    cc_rbtree_node_t *node;
    ngx_rwlock_rlock(main_conf->rwlock);
    node = cc_rbtree_str_lookup(&main_conf->cc_rbtree->rbtree,
                                &r->connection->addr_text);
    if (node != NULL) {
        ngx_rwlock_unlock(main_conf->rwlock);
        ngx_log_error(NGX_LOG_WARN, main_conf->cycle->log, 0,
                      "request path %V addr %V blocked", &r->uri,
                      &r->connection->addr_text);
        // found
        ngx_http_finalize_request(r, NGX_ERROR);
        return NGX_ERROR;
    }
    ngx_rwlock_unlock(main_conf->rwlock);
    return NGX_OK;
}
