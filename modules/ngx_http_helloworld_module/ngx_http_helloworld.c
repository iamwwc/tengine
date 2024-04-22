#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

struct ngx_http_helloworld_main_conf_s {
    ngx_int_t var_uri_index;
    ngx_cycle_t *cycle;
};

typedef struct ngx_http_helloworld_main_conf_s ngx_http_helloworld_main_conf_t;

struct ngx_http_helloworld_srv_conf_s {};
struct ngx_http_helloworld_loc_conf_s {
    ngx_str_t echo;
};

typedef struct ngx_http_helloworld_srv_conf_s ngx_http_helloworld_srv_conf_t;
typedef struct ngx_http_helloworld_loc_conf_s ngx_http_helloworld_loc_conf_t;
static ngx_int_t ngx_http_helloworld_pre_conf(ngx_conf_t *cf);
static ngx_int_t ngx_http_helloworld_post_conf(ngx_conf_t *cf);
static void *ngx_http_helloworld_create_main_conf(ngx_conf_t *cf);
static char *ngx_http_helloworld_init_main_conf(ngx_conf_t *cf, void *conf);
static void *ngx_http_helloworld_create_srv_conf(ngx_conf_t *cf);
static char *ngx_http_helloworld_merge_srv_conf(ngx_conf_t *cf, void *prev,
                                                void *conf);
static void *ngx_http_helloworld_create_loc_conf(ngx_conf_t *cf);
static char *ngx_http_helloworld_merge_loc_conf(ngx_conf_t *cf, void *prev,
                                                void *conf);

static ngx_int_t ngx_http_helloworld_init_process(ngx_cycle_t *cycle);
static ngx_command_t ngx_http_helloworld_commands[] = {

    {ngx_string("helloworld"),
     NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF |
         NGX_CONF_TAKE1,
     ngx_conf_set_str_slot,
     // 如果 conf 是0，会报 duplicate 错误
     // 因为http总是会将http_conf_ctx的main_conf传入 ngx_conf_set_str_slot
     // 二次call 时 ngx_conf_set_str_slot，检测到field->data有值报错

     //  NGX_HTTP_LOC_CONF_OFFSET
     //  会确保nginx总是从ngx_http_conf_ctx_t找到loc_conf传递
     // typedef struct {
     //     void        **main_conf;
     //     void        **srv_conf;
     //     void        **loc_conf;
     // } ngx_http_conf_ctx_t;
     //
     NGX_HTTP_LOC_CONF_OFFSET, offsetof(ngx_http_helloworld_loc_conf_t, echo),
     NULL},
    ngx_null_command};

static ngx_http_module_t ngx_http_helloworld_module_ctx = {
    ngx_http_helloworld_pre_conf,  /* preconfiguration */
    ngx_http_helloworld_post_conf, /* postconfiguration */

    ngx_http_helloworld_create_main_conf, /* create main configuration */
    ngx_http_helloworld_init_main_conf,   /* init main configuration */

    ngx_http_helloworld_create_srv_conf, /* create server configuration */
    ngx_http_helloworld_merge_srv_conf,  /* merge server configuration */

    ngx_http_helloworld_create_loc_conf, /* create location configuration */
    ngx_http_helloworld_merge_loc_conf   /* merge location configuration */
};

ngx_module_t ngx_http_helloworld_module = {
    NGX_MODULE_V1,
    &ngx_http_helloworld_module_ctx,  /* module context */
    ngx_http_helloworld_commands,     /* module directives */
    NGX_HTTP_MODULE,                  /* module type */
    NULL,                             /* init master */
    NULL,                             /* init module */
    ngx_http_helloworld_init_process, /* init process */
    NULL,                             /* init thread */
    NULL,                             /* exit thread */
    NULL,                             /* exit process */
    NULL,                             /* exit master */
    NGX_MODULE_V1_PADDING};

static ngx_int_t ngx_http_log_filter_log_handler(ngx_http_request_t *r) {
    ngx_http_variable_value_t *v;
    ngx_http_helloworld_main_conf_t *main_conf =
        ngx_http_get_module_main_conf(r, ngx_http_helloworld_module);
    ngx_http_helloworld_loc_conf_t *loc =
        ngx_http_get_module_loc_conf(r, ngx_http_helloworld_module);
    ngx_log_error(NGX_LOG_INFO, r->connection->log, 0,
                  "ngx_http_helloworld handler called!");
    if (loc->echo.len != 0) {
        v = ngx_http_get_indexed_variable(r, main_conf->var_uri_index);
        if (v == NULL || v->not_found) {
            ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                          "variable not found!");
        } else {
            ngx_log_error(NGX_LOG_INFO, main_conf->cycle->log, 0,
                          "diretive helloworld msg %V at %*s", &loc->echo,
                          v->len, v->data);
        }
    }
    return NGX_OK;
}
static ngx_int_t
ngx_http_helloworld_set_helloworld_handler(ngx_cycle_t *cycle) {
    ngx_http_core_main_conf_t *core_main_conf;
    ngx_http_handler_pt *h;
    core_main_conf =
        ngx_http_cycle_get_module_main_conf(cycle, ngx_http_core_module);
    h = ngx_array_push(&core_main_conf->phases[NGX_HTTP_LOG_PHASE].handlers);
    if (h == NULL) {
        return NGX_ERROR;
    }
    *h = ngx_http_log_filter_log_handler;
    return NGX_OK;
}

static ngx_int_t ngx_http_helloworld_pre_conf(ngx_conf_t *cf) {
    ngx_log_error(NGX_LOG_NOTICE, cf->cycle->log, 0,
                  "helloworld pre_conf notice");
    return NGX_OK;
}

static ngx_int_t ngx_http_helloworld_post_conf(ngx_conf_t *cf) {
    ngx_log_error(NGX_LOG_NOTICE, cf->cycle->log, 0,
                  "helloworld post_conf notice");
    ngx_http_helloworld_set_helloworld_handler(cf->cycle);
    return NGX_OK;
}

static char *ngx_http_helloworld_merge_srv_conf(ngx_conf_t *cf, void *prev,
                                                void *conf) {
    ngx_log_error(NGX_LOG_NOTICE, cf->cycle->log, 0,
                  "helloworld merge_srv_conf notice");
    return NGX_CONF_OK;
}

static char *ngx_http_helloworld_merge_loc_conf(ngx_conf_t *cf, void *parent,
                                                void *child) {
    ngx_http_helloworld_loc_conf_t *prev = parent;
    ngx_http_helloworld_loc_conf_t *conf = child;
    ngx_log_error(NGX_LOG_NOTICE, cf->cycle->log, 0,
                  "helloworld merge_loc_conf notice");
    ngx_conf_merge_str_value(conf->echo, prev->echo, "");
    return NGX_CONF_OK;
}
static void *ngx_http_helloworld_create_main_conf(ngx_conf_t *cf) {
    ngx_http_helloworld_main_conf_t *maincf;

    maincf = ngx_pcalloc(cf->pool, sizeof(ngx_http_helloworld_main_conf_t));
    if (maincf == NULL) {
        return NULL;
    }
    // 启动阶段cycle->log level notice，level == 6，不支持debug_http
    // ngx_log_debug0(NGX_LOG_DEBUG_HTTP 的 log_level 在
    // https://github.com/willdeeper/tengine/blob/01468c688fd16ffa7c322c99b0d39ff3119a071d/src/core/ngx_log.c#L590
    // 设置，但此时log真正的后端是 new_log, new_log 还未打开底层的file，此时fd
    // == -1 开始调用 create_xxx_conf，cycle->log
    // fd正常，但不支持debug_http，所以无法输出 create_xxx 阶段结束后，
    // 1. new_log open file, fd正常
    // 2. cycle->log = &cycle->new_log， log正常
    // init_process 阶段ngx_log_debug0(NGX_LOG_DEBUG_HTTP 正常输出
    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, cf->cycle->log, 0,
                   "helloworld create_main_conf event");
    ngx_log_error(NGX_LOG_NOTICE, cf->cycle->log, 0,
                  "helloworld create_main_conf notice");
    return maincf;
}

static char *ngx_http_helloworld_init_main_conf(ngx_conf_t *cf, void *conf) {
    ngx_http_helloworld_main_conf_t *main_conf = conf;
    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, cf->cycle->log, 0,
                   "helloworld init_main_conf");
    ngx_log_error(NGX_LOG_NOTICE, cf->cycle->log, 0,
                  "helloworld init_main_conf notice");
    ngx_str_t key = ngx_string("request_uri");
    main_conf->var_uri_index = ngx_http_get_variable_index(cf, &key);
    if (main_conf->var_uri_index == NGX_ERROR) {
        ngx_log_error(NGX_LOG_EMERG, cf->cycle->log, 0, "bad variable key %V",
                      &key);
        return NGX_CONF_ERROR;
    }
    main_conf->cycle = cf->cycle;
    return NGX_CONF_OK;
}

static void *ngx_http_helloworld_create_srv_conf(ngx_conf_t *cf) {
    ngx_http_helloworld_srv_conf_t *conf;

    conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_helloworld_srv_conf_t));
    if (conf == NULL) {
        return NULL;
    }

    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, cf->cycle->log, 0,
                   "helloworld create_srv_conf");
    ngx_log_error(NGX_LOG_NOTICE, cf->cycle->log, 0,
                  "helloworld create_srv_conf notice");
    return conf;
}

static void *ngx_http_helloworld_create_loc_conf(ngx_conf_t *cf) {
    ngx_http_helloworld_loc_conf_t *conf;

    conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_helloworld_loc_conf_t));
    if (conf == NULL) {
        return NULL;
    }

    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, cf->cycle->log, 0,
                   "helloworld create_loc_conf");
    ngx_log_error(NGX_LOG_NOTICE, cf->cycle->log, 0,
                  "helloworld create_loc_conf notice");
    ngx_str_null(&conf->echo);

    return conf;
}

static ngx_int_t ngx_http_helloworld_init_process(ngx_cycle_t *cycle) {
    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, cycle->log, 0,
                   "helloworld init_process");
    return NGX_OK;
}
