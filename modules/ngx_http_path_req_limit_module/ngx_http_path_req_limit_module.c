#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <stdio.h>

static void *ngx_http_path_req_limit_create_main_conf(ngx_conf_t *cf);
static char *ngx_http_path_req_limit_init_main_conf(ngx_conf_t *cf, void *conf);
static ngx_int_t ngx_http_path_req_limit_init_process(ngx_cycle_t *cycle);
static ngx_int_t ngx_http_path_req_limit_init_module(ngx_cycle_t *cycle);
static void *ngx_http_path_req_limit_create_srv_conf(ngx_conf_t *cf);
struct ngx_http_path_req_limit_main_conf_s {
  ngx_str_t iplist_path;
};

typedef struct ngx_http_path_req_limit_main_conf_s
    ngx_http_path_req_limit_main_conf_t;

struct ngx_http_path_req_limit_srv_conf_s {
  ngx_str_t iplist_path;
};

typedef struct ngx_http_path_req_limit_srv_conf_s
    ngx_http_path_req_limit_srv_conf_t;

static ngx_http_module_t ngx_http_path_req_limit_ctx = {
    NULL, /* preconfiguration */
    NULL, /* postconfiguration */

    ngx_http_path_req_limit_create_main_conf, /* create main configuration */
    ngx_http_path_req_limit_init_main_conf,   /* init main configuration */

    ngx_http_path_req_limit_create_srv_conf, /* create server configuration */
    .create_loc_conf = NULL,
    NULL, /* merge server configuration */
};

static void *ngx_http_path_req_limit_create_main_conf(ngx_conf_t *cf) {
  ngx_http_path_req_limit_main_conf_t *main_conf;
  main_conf =
      ngx_palloc(cf->cycle->pool, sizeof(ngx_http_path_req_limit_main_conf_t));
  return main_conf;
}

static char *ngx_http_path_req_limit_init_main_conf(ngx_conf_t *cf,
                                                    void *conf) {
  return NGX_CONF_OK;
}

static void *ngx_http_path_req_limit_create_srv_conf(ngx_conf_t *cf) {
  ngx_http_path_req_limit_srv_conf_t *srv_conf;
  srv_conf =
      ngx_palloc(cf->cycle->pool, sizeof(ngx_http_path_req_limit_srv_conf_t));
  return srv_conf;
}

static ngx_command_t ngx_http_path_req_limit_command[] = {
    {ngx_string("iplist_path"), NGX_HTTP_MAIN_CONF | NGX_CONF_1MORE,
     ngx_conf_set_str_slot,
     offsetof(ngx_http_path_req_limit_main_conf_t, iplist_path), 0, NULL},
    ngx_null_command};

static ngx_int_t ngx_http_path_req_limit_init_process(ngx_cycle_t *cycle) {
  return NGX_OK;
}

static ngx_int_t ngx_http_path_req_limit_init_module(ngx_cycle_t *cycle) {
  return NGX_OK;
}

static void ngx_http_path_req_limit_exit_process() {}
ngx_module_t ngx_http_path_req_limit_module = {
    NGX_MODULE_V1,
    &ngx_http_path_req_limit_ctx,         /* module context */
    ngx_http_path_req_limit_command,      /* module directives */
    NGX_HTTP_MODULE,                      /* module type */
    NULL,                                 /* init master */
    ngx_http_path_req_limit_init_module,  /* init module */
    ngx_http_path_req_limit_init_process, /* init process */
    NULL,                                 /* init thread */
    NULL,                                 /* exit thread */
    ngx_http_path_req_limit_exit_process, /* exit process */
    NULL,                                 /* exit master */
    NGX_MODULE_V1_PADDING};