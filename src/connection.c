/* ==========================================================================
 * connection.c - connection layer framework
 * --------------------------------------------------------------------------
 * Copyright (C) 2022  zhenwei pi
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to permit
 * persons to whom the Software is furnished to do so, subject to the
 * following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 * OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
 * USE OR OTHER DEALINGS IN THE SOFTWARE.
 * ==========================================================================
 */

#include "server.h"
#include "connection.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dlfcn.h>

static ConnectionType *connTypes[CONN_TYPE_MAX];

int connTypeRegister(ConnectionType *ct) {
    const char *typename = ct->get_type(NULL);
    ConnectionType *tmpct;
    int type;

    /* find an empty slot to store the new connection type */
    for (type = 0; type < CONN_TYPE_MAX; type++) {
        tmpct = connTypes[type];
        if (!tmpct)
            break;

        /* ignore case, we really don't care "tls"/"TLS" */
        if (!strcasecmp(typename, tmpct->get_type(NULL)))
            serverPanic("Connection types %s already registered", typename);

        continue;
    }

    serverLog(LL_VERBOSE, "Connection type %s registered", typename);
    connTypes[type] = ct;

    if (ct->init) {
        ct->init();
    }

    return C_OK;
}

/* Load a connection type shared library, auto run RedisRegisterConnectionType(),
 * return connection type name if loading succeed, otherwise exit process (typically
 * after loading connection type, we need listen to specified address, so this step
 * is fatal error. */
static const char *connTypeLoadExtension(char *path, void **argv, int argc) {
    const char *(*onload_handler)(void **argv, int argc);
    void *handle;
    char *onload = "RedisRegisterConnectionType";
    const char *typename;
    struct stat st;

    if (stat(path, &st) == 0) {
        if (!(st.st_mode & (S_IXUSR  | S_IXGRP | S_IXOTH))) {
            serverLog(LL_WARNING, "Connection extension %s failed to load: It does not have execute permissions.", path);
            exit(1);
        }
    }

    handle = dlopen(path, RTLD_NOW | RTLD_LOCAL);
    if (handle == NULL) {
        serverLog(LL_WARNING, "Failed to load connection extension (%s): %s", path, dlerror());
        exit(1);
    }

    onload_handler = (const char *(*)(void **, int))(unsigned long)dlsym(handle, onload);
    if (onload_handler == NULL) {
        dlclose(handle);
        serverLog(LL_WARNING, "Failed to lookup symbol %s in %s", onload, path);
        exit(1);
    }

    typename = onload_handler(argv, argc);
    if (typename == NULL) {
        serverLog(LL_WARNING, "Failed to load %s in %s", onload, path);
        exit(1);
    }

    serverLog(LL_NOTICE, "Connection type %s loaded from %s", typename, path);
    return typename;
}

int connTypeInitialize() {
    listIter li;
    listNode *ln;

    /* currently socket connection type is necessary  */
    serverAssert(RedisRegisterConnectionTypeSocket() == C_OK);

    /* currently unix socket connection type is necessary  */
    serverAssert(RedisRegisterConnectionTypeUnix() == C_OK);

    /* may fail if without BUILD_TLS=yes */
    RedisRegisterConnectionTypeTLS();

    /* load all connection extensions form server.conn_ext_queue */
    listRewind(server.conn_ext_queue, &li);
    while ((ln = listNext(&li))) {
        struct moduleLoadQueueEntry *entry = ln->value;
        connTypeLoadExtension(entry->path, (void **)entry->argv, entry->argc);
    }

    return C_OK;
}

ConnectionType *connectionByType(const char *typename) {
    ConnectionType *ct;

    for (int type = 0; type < CONN_TYPE_MAX; type++) {
        ct = connTypes[type];
        if (!ct)
            break;

        if (!strcasecmp(typename, ct->get_type(NULL)))
            return ct;
    }

    serverLog(LL_WARNING, "Missing implement of connection type %s", typename);

    return NULL;
}

/* Cache TCP connection type, query it by string once */
ConnectionType *connectionTypeTcp() {
    static ConnectionType *ct_tcp = NULL;

    if (ct_tcp != NULL)
        return ct_tcp;

    ct_tcp = connectionByType(CONN_TYPE_SOCKET);
    serverAssert(ct_tcp != NULL);

    return ct_tcp;
}

/* Cache TLS connection type, query it by string once */
ConnectionType *connectionTypeTls() {
    static ConnectionType *ct_tls = NULL;

    if (ct_tls != NULL)
        return ct_tls;

    ct_tls = connectionByType(CONN_TYPE_TLS);
    return ct_tls;
}

/* Cache Unix connection type, query it by string once */
ConnectionType *connectionTypeUnix() {
    static ConnectionType *ct_unix = NULL;

    if (ct_unix != NULL)
        return ct_unix;

    ct_unix = connectionByType(CONN_TYPE_UNIX);
    return ct_unix;
}

int connectionIndexByType(const char *typename) {
    ConnectionType *ct;

    for (int type = 0; type < CONN_TYPE_MAX; type++) {
        ct = connTypes[type];
        if (!ct)
            break;

        if (!strcasecmp(typename, ct->get_type(NULL)))
            return type;
    }

    return -1;
}

void connTypeCleanupAll() {
    ConnectionType *ct;
    int type;

    for (type = 0; type < CONN_TYPE_MAX; type++) {
        ct = connTypes[type];
        if (!ct)
            break;

        if (ct->cleanup)
            ct->cleanup();
    }
}

/* walk all the connection types until has pending data */
int connTypeHasPendingData(void) {
    ConnectionType *ct;
    int type;
    int ret = 0;

    for (type = 0; type < CONN_TYPE_MAX; type++) {
        ct = connTypes[type];
        if (ct && ct->has_pending_data && (ret = ct->has_pending_data())) {
            return ret;
        }
    }

    return ret;
}

/* walk all the connection types and process pending data for each connection type */
int connTypeProcessPendingData(void) {
    ConnectionType *ct;
    int type;
    int ret = 0;

    for (type = 0; type < CONN_TYPE_MAX; type++) {
        ct = connTypes[type];
        if (ct && ct->process_pending_data) {
            ret += ct->process_pending_data();
        }
    }

    return ret;
}

