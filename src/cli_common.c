/* CLI (command line interface) common methods
 * 
 * Copyright (c) 2020, Redis Labs
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "cli_common.h"
#include <errno.h>
#include <hiredis.h>
#include <sdscompat.h> /* Use hiredis' sds compat header that maps sds calls to their hi_ variants */
#include <sds.h> /* use sds.h from hiredis, so that only one set of sds functions will be present in the binary */
#ifdef USE_OPENSSL
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <hiredis_ssl.h>

#endif



/* Wrapper around redisSecureConnection to avoid hiredis_ssl dependencies if
 * not building with TLS support.
 */
int cliSecureConnection(redisContext *c, cliSSLconfig config, const char **err) {
#ifdef USE_OPENSSL
    static SSL_CTX *ssl_ctx = NULL;

    if (!ssl_ctx) {
        ssl_ctx = SSL_CTX_new(SSLv23_client_method());
        if (!ssl_ctx) {
            *err = "Failed to create SSL_CTX";
            goto error;
        }


        SSL_CTX_set_options(ssl_ctx, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3);
        //        SSL_VERIFY_NONE：完全忽略验证证书的结果。当握手必须完成的话，就选中这个选项。
        //        其实真正有证书的人很少，尤其是在中国，那么如果 SSL 运用于一些免费的服务，
        //        比如 EMAIL 的时候，SERVER 端最好采用这个模式。


//        SSL_VERIFY_PEER：希望验证对方的证书。这个是最一般的模式。
//        对 CLIENT 来说，如果设置了这样的模式，验证SERVER的证书出了任何错误，SSL 握手都告吹。
//        对 SERVER 来说,如果设置了这样的模式，CLIENT 倒不一定要把自己的证书交出去。
//        如果 CLIENT 没有交出证书，SERVER 自己决定下一步怎么做。

        SSL_CTX_set_verify(ssl_ctx, config.skip_cert_verify ? SSL_VERIFY_NONE : SSL_VERIFY_PEER, NULL);
        if (config.cacert || config.cacertdir) {
            // 从指定地址读取证书并验证
            if (!SSL_CTX_load_verify_locations(ssl_ctx, config.cacert, config.cacertdir)) {
                *err = "Invalid CA Certificate File/Directory";
                goto error;
            }
        } else {
            // 从默认地址读取
            if (!SSL_CTX_set_default_verify_paths(ssl_ctx)) {
                *err = "Failed to use default CA paths";
                goto error;
            }
        }
        
        // 为服务端指定SSL连接所用公钥证书的完整证书链
        //参数 m_pServerCtx，服务端SSL会话环境
        //参数 pCertPath，你存放证书链文件的路径
        if (config.cert && !SSL_CTX_use_certificate_chain_file(ssl_ctx, config.cert)) {
            *err = "Invalid client certificate";
            goto error;
        }

        // 为服务端指定SSL连接所用私钥
        //参数 m_pServerCtx，服务端SSL会话环境
        //参数 pKeyPath，你存放对应私钥文件的路径
        //参数 SSL_FILETYPE_PEM，指定你所要加载的私钥文件的文件编码类型为 Base64
        if (config.key && !SSL_CTX_use_PrivateKey_file(ssl_ctx, config.key, SSL_FILETYPE_PEM)) {
            *err = "Invalid private key";
            goto error;
        }

        // 根据SSL/TLS规范,在ClientHello中,客户端会提交一份自己能够支持的加密方法的列表,
        // 由服务端选择一种方法后在ServerHello中通知服务端, 从而完成加密算法的协商.
        // 这些算法按一定优先级排列,如果不作任何指定,将选用DES-CBC3-SHA.用SSL_CTX_set_cipher_list可以指定自己希望用的算法
        if (config.ciphers && !SSL_CTX_set_cipher_list(ssl_ctx, config.ciphers)) {
            *err = "Error while configuring ciphers";
            goto error;
        }

#ifdef TLS1_3_VERSION
        // 设置密码套件
        if (config.ciphersuites && !SSL_CTX_set_ciphersuites(ssl_ctx, config.ciphersuites)) {
            *err = "Error while setting cypher suites";
            goto error;
        }
#endif
    }

    SSL *ssl = SSL_new(ssl_ctx);
    if (!ssl) {
        *err = "Failed to create SSL object";
        return REDIS_ERR;
    }

    // 设置 域名
    if (config.sni && !SSL_set_tlsext_host_name(ssl, config.sni)) {
        *err = "Failed to configure SNI";
        SSL_free(ssl);
        return REDIS_ERR;
    }

    // 初始化ssl
    return redisInitiateSSL(c, ssl);

error:
    SSL_CTX_free(ssl_ctx);
    ssl_ctx = NULL;
    return REDIS_ERR;
#else
    (void) config;
    (void) c;
    (void) err;
    return REDIS_OK;
#endif
}

/* Wrapper around hiredis to allow arbitrary reads and writes.
 *
 * We piggybacks on top of hiredis to achieve transparent TLS support,
 * and use its internal buffers so it can co-exist with commands
 * previously/later issued on the connection.
 *
 * Interface is close to enough to read()/write() so things should mostly
 * work transparently.
 */

/* Write a raw buffer through a redisContext. If we already have something
 * in the buffer (leftovers from hiredis operations) it will be written
 * as well.
 */
ssize_t cliWriteConn(redisContext *c, const char *buf, size_t buf_len)
{
    int done = 0;

    /* Append data to buffer which is *usually* expected to be empty
     * but we don't assume that, and write.
     */
    c->obuf = sdscatlen(c->obuf, buf, buf_len);
    if (redisBufferWrite(c, &done) == REDIS_ERR) {
        if (!(c->flags & REDIS_BLOCK))
            errno = EAGAIN;

        /* On error, we assume nothing was written and we roll back the
         * buffer to its original state.
         */
        if (sdslen(c->obuf) > buf_len)
            sdsrange(c->obuf, 0, -(buf_len+1));
        else
            sdsclear(c->obuf);

        return -1;
    }

    /* If we're done, free up everything. We may have written more than
     * buf_len (if c->obuf was not initially empty) but we don't have to
     * tell.
     */
    if (done) {
        sdsclear(c->obuf);
        return buf_len;
    }

    /* Write was successful but we have some leftovers which we should
     * remove from the buffer.
     *
     * Do we still have data that was there prior to our buf? If so,
     * restore buffer to it's original state and report no new data was
     * writen.
     */
    if (sdslen(c->obuf) > buf_len) {
        sdsrange(c->obuf, 0, -(buf_len+1));
        return 0;
    }

    /* At this point we're sure no prior data is left. We flush the buffer
     * and report how much we've written.
     */
    size_t left = sdslen(c->obuf);
    sdsclear(c->obuf);
    return buf_len - left;
}

/* Wrapper around OpenSSL (libssl and libcrypto) initialisation
 */
int cliSecureInit()
{
#ifdef USE_OPENSSL
    ERR_load_crypto_strings();
    SSL_load_error_strings(); /* 为了更友好的报错，加载错误码的描述字符串 */
    SSL_library_init(); /* 为SSL加载加密和哈希算法 */
#endif
    return REDIS_OK;
}
