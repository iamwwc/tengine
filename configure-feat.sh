#!/bin/bash
# https://tengine.taobao.org/document/ngx_http_ssl_asynchronous_mode.html
# https://tengine.taobao.org/document/tengine_qat_ssl.html
./configure \
	--with-stream \
	--with-debug \
	--with-http_ssl_module \
	--with-openssl-async \
	--with-http_v2_module \
	--add-module=modules/ngx_http_cc_access_module \
    --add-module=modules/ngx_http_path_req_limit_module \
	--add-module=modules/ngx_http_helloworld_module \
	--prefix=$(pwd)/target
	# --add-module=modules/ngx_http_xquic_module \
	# --add-module=modules/nginx-stream-upsync-module \
	# --add-module=modules/ngx_stream_upstream_dynamic_module \f