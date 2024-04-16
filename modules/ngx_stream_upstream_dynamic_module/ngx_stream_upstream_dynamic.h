#include "ngx_config.h"
#include "ngx_core.h"
#include <ngx_stream.h>

#include "cJSON.h"
#include <sys/socket.h>

typedef struct {
    char *data;
    ngx_peer_connection_t peer_connection;
} async_recv_state;

typedef struct ngx_stream_upstream_dynamic_request_state_s ngx_stream_upstream_dynamic_inner_state_t;
typedef struct ngx_stream_upstream_dynamic_srv_conf_s ngx_stream_upstream_dynamic_srv_conf_t;
typedef struct {
    u_char *data;
    ngx_uint_t w_pos;
    ngx_uint_t r_pos;
    ngx_uint_t cap;
    ngx_pool_t *pool;
} vector;

struct ngx_stream_upstream_dynamic_request_state_s {
    ngx_peer_connection_t *peer_conn;
    ngx_event_t update_timer;
    ngx_event_t request_timeout_timer;

    ngx_shmtx_t shm_mutex;

    vector *send_vector;
    vector *recv_vector;
    ngx_pool_t *pool;
    ngx_uint_t update_generation;
    ngx_stream_upstream_dynamic_srv_conf_t *srv_conf;
};

struct ngx_stream_upstream_dynamic_srv_conf_s {
    ngx_str_t host;
    ngx_uint_t port;
    ngx_str_t path_rest_of;

    ngx_str_t dump_file_path;

    time_t request_timeout;
    time_t update_interval;

    ngx_cycle_t *cycle;

    ngx_stream_upstream_srv_conf_t *upstream_srv_conf;
    ngx_stream_upstream_dynamic_inner_state_t *state;
};

// from consul response
typedef struct {
    ngx_str_t host;
    u_short port;
    ngx_str_t cluster;
    ngx_str_t env;
    ngx_uint_t weight;
} server_from_consul_t;

typedef struct {
    u_char sockaddr[NGX_SOCKADDRLEN];
} ngx_stream_upstream_dynamic_from_consul_conf_t;

typedef struct {
    ngx_array_t *servers; // ngx_stream_upstream_dynamic_srv_conf_t*
    ngx_uint_t upstream_numbers; /* upstream block counts which have consul directive*/
} ngx_stream_upstream_dynamic_main_conf_t;

typedef struct {
    ngx_int_t fd;
    struct sockaddr_in sin;
} http_client_t;


typedef struct {
    void *data;
    ngx_uint_t len;
} send_request_t;


ngx_uint_t vector_get_next_cap(vector *v) {
    return ((v->cap % ngx_pagesize) + 2) * ngx_pagesize;
}

ngx_int_t vector_realloc(vector *v, ngx_uint_t size) {
    v->data = ngx_palloc(v->pool, size);
    if (v->data == NULL) {
        return NGX_ERROR;
    }
    v->cap = size;
    return NGX_OK;
}

ngx_int_t vector_sprintf(vector *v, const char *fmt, ...) {
    va_list args;
    ngx_int_t n;
    u_char *start = v->data + v->w_pos;
    ngx_int_t left = v->cap - v->w_pos;
    va_start(args, fmt);
    while (1) {
        u_char *end;
        end = ngx_vslprintf(start, start + left, fmt, args);
        n = end - start;
        ngx_uint_t size = vector_get_next_cap(v);
        if (n < left) {
            // 足量写入
            v->w_pos += n;
            break;
        } else {
            // 可能没写完，分配更大空间，重新尝试写入
            vector_realloc(v, size);
        }
    }
    va_end(args);
    return NGX_OK;
}

ngx_int_t vector_push(vector *v, u_char *data, ngx_uint_t len) {
    ngx_uint_t left = v->cap - v->w_pos;
    while (1) {
        if (left >= len) {
            ngx_memcpy(v->data + v->w_pos, data, len);
            v->w_pos += len;
            break;
        } else {
            ngx_uint_t size = vector_get_next_cap(v);
            vector_realloc(v, size);
        }
    }
    return NGX_OK;
}

ngx_int_t vector_put_char(vector *v, u_char ch) {
    return vector_push(v, &ch, 1);
}

vector *vector_create(ngx_pool_t *pool) {
    vector *v = ngx_pcalloc(pool, sizeof(vector));
    v->pool = pool;
    v->cap = ngx_pagesize * 2;
    v->data = ngx_pcalloc(pool, v->cap);
    if (v->data == NULL) return NULL;
    v->w_pos = 0;
    v->r_pos = 0;
    return v;
}


void vector_clear(vector *v) {
    ngx_memzero(v->data, v->cap);
    v->w_pos = 0;
    v->r_pos = 0;
}

vector *vector_create_from_str(ngx_pool_t *pool, ngx_str_t *str) {
    vector *v;
    v = vector_create(pool);
    if (vector_push(v, str->data, str->len) == NGX_ERROR) {
        return NULL;
    }
    return v;
}

