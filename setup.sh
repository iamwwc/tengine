#!/bin/bash
make clean
rm -rf target/
mkdir -p target/
./configure \
	--with-stream \
	--with-debug \
	--add-module=modules/ngx_http_cc_access_module \
    --add-module=modules/ngx_http_path_req_limit_module \
	--add-module=modules/ngx_http_helloworld_module \
	--prefix=$(pwd)/target
	# --add-module=modules/nginx-stream-upsync-module \
	# --add-module=modules/ngx_stream_upstream_dynamic_module \