#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

static char *ngx_http_helloworld_interface(ngx_conf_t *cf, ngx_command_t *cmd,
                                           void *conf) {
    return NGX_CONF_OK;
}

static ngx_command_t ngx_http_helloworld_commands[] = {

    {ngx_string("helloworld"), NGX_HTTP_LOC_CONF | NGX_CONF_NOARGS,
     ngx_http_helloworld_interface, 0, 0, NULL},
    ngx_null_command};

static ngx_int_t ngx_http_helloworld_pre_conf(ngx_conf_t *cf) { return NGX_OK; }

static ngx_int_t ngx_http_helloworld_post_conf(ngx_conf_t *cf) {
    return NGX_OK;
}

struct ngx_http_helloworld_main_conf_s {};

typedef struct ngx_http_helloworld_main_conf_s ngx_http_helloworld_main_conf_t;

struct ngx_http_helloworld_srv_conf_s {};
struct ngx_http_helloworld_loc_conf_s {};

typedef struct ngx_http_helloworld_srv_conf_s ngx_http_helloworld_srv_conf_t;
typedef struct ngx_http_helloworld_loc_conf_s ngx_http_helloworld_loc_conf_t;
static void *ngx_http_helloworld_create_main_conf(ngx_conf_t *cf) {
    ngx_http_helloworld_main_conf_t *maincf;

    maincf = ngx_pcalloc(cf->pool, sizeof(ngx_http_helloworld_main_conf_t));
    if (maincf == NULL) {
        return NULL;
    }
    // 启动阶段cycle->log level notice，level == 6，不支持debug_http
    // ngx_log_debug0(NGX_LOG_DEBUG_HTTP 的 log_level 在
    // https://github.com/willdeeper/tengine/blob/01468c688fd16ffa7c322c99b0d39ff3119a071d/src/core/ngx_log.c#L590
    // 设置，但此时log真正的后端是 new_log, new_log 还未打开底层的file，此时fd == -1
    // 开始调用 create_xxx_conf，cycle->log fd正常，但不支持debug_http，所以无法输出
    // create_xxx 阶段结束后， 
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
    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, cf->cycle->log, 0,
                   "helloworld init_main_conf");
    ngx_log_error(NGX_LOG_NOTICE, cf->cycle->log, 0,
                  "helloworld init_main_conf notice");
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
    return conf;
}

static ngx_int_t ngx_http_helloworld_init_process(ngx_cycle_t *cycle) {
    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, cycle->log, 0,
                   "helloworld init_process");
    return NGX_OK;
}

static ngx_http_module_t ngx_http_helloworld_module_ctx = {
    ngx_http_helloworld_pre_conf,  /* preconfiguration */
    ngx_http_helloworld_post_conf, /* postconfiguration */

    ngx_http_helloworld_create_main_conf, /* create main configuration */
    ngx_http_helloworld_init_main_conf,   /* init main configuration */

    ngx_http_helloworld_create_srv_conf, /* create server configuration */
    NULL,                                /* merge server configuration */

    ngx_http_helloworld_create_loc_conf, /* create location configuration */
    NULL                                 /* merge location configuration */
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
