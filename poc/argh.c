#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <libcouchbase/couchbase.h>
#include <event.h>
#include <time.h>
#include <sys/types.h>
#include <sys/socket.h>

static int
event_assign(struct event *ev,
             struct event_base *base,
             int fd,
             short events,
             void (*callback)(int, short, void *),
             void *arg)
{
    event_base_set(base, ev);
    ev->ev_callback = callback;
    ev->ev_arg = arg;
    ev->ev_fd = fd;
    ev->ev_events = events;
    ev->ev_res = 0;
    ev->ev_flags = EVLIST_INIT;
    ev->ev_ncalls = 0;
    ev->ev_pncalls = NULL;

    return 0;
}

static struct event *event_new(struct event_base *base,
			       int fd,
			       short events,
			       void (*cb)(int, short, void *),
			       void *arg)
{
    struct event *ev;
    ev = malloc(sizeof(struct event));
    if (ev == NULL) {
        return NULL;
    }
    if (event_assign(ev, base, fd, events, cb, arg) < 0) {
        free(ev);
        return NULL;
    }
    return ev;
}

char *asciiz(const void *data, size_t nbytes) {
  char *z = malloc(nbytes+1);
  memcpy(z, data, nbytes);
  z[nbytes] = 0;
  return z;
}

void *get_callback(libcouchbase_t instance,
		   const void *cookie,
		   libcouchbase_error_t error,
		   const void *key,
		   libcouchbase_size_t nkey,
		   const void *bytes,
		   libcouchbase_size_t nbytes,
		   libcouchbase_uint32_t flags,
		   libcouchbase_cas_t cas) {
  char *k = asciiz(key, nkey);
  char *v = asciiz(bytes, nbytes);

  printf("get_callback for key %s returned value %s\n", k, v);
  free(k);
  free(v);

  return 0;
}

static struct event_base *base = 0;

void timeout_function(libcouchbase_socket_t sock, short which, void *cb_data) {
  printf("timeout callback. data was %s\n", (char *) cb_data);
  event_base_loopbreak(base);
}

int main(int argc, char **argv) {
  base = event_base_new();
  libcouchbase_error_t e = LIBCOUCHBASE_SUCCESS;
  libcouchbase_io_opt_t *cb_base = libcouchbase_create_io_ops(LIBCOUCHBASE_IO_OPS_LIBEVENT, base, &e);

  if (e == LIBCOUCHBASE_NOT_SUPPORTED) {
    printf("libcouchbase does not support libevent on this platform\n");
    exit(0);
  } else if (e == LIBCOUCHBASE_ENOMEM) {
    printf("libcouchbase ran out of memory!\n");
    exit(0);
  }

  libcouchbase_t cb = libcouchbase_create(0, 0, 0, 0, cb_base);
  libcouchbase_set_get_callback(cb, get_callback);

  if (libcouchbase_connect(cb) != LIBCOUCHBASE_SUCCESS) {
    printf("argh\n");
    exit(0);
  }

  printf("konnekt\n");
  libcouchbase_wait(cb);
  printf("store\n");
  libcouchbase_store_by_key(cb, 0, LIBCOUCHBASE_SET, 0, 0, "test", 4, "contents", 8, 0, 0, 0);
  libcouchbase_wait(cb);
  
  int keysize = 4;
  char *key = "test";
  
  printf("timeout\n");
  struct event *timeout = event_new(base, -1, 0, 0, 0);
  struct timeval tmo;
  unsigned int usec = 50;
  event_assign(timeout, base, -1, EV_TIMEOUT | EV_PERSIST, timeout_function, key);
  tmo.tv_sec = usec / 1000000;
  tmo.tv_usec = usec % 1000000;
  event_add(timeout, &tmo);


  printf("get\n");
  libcouchbase_mget_by_key(cb, 0, 0, 0, 1, &key, &keysize, 0);
  printf("get??\n");
  libcouchbase_wait(cb);
  libcouchbase_wait(cb);

  return 0;
}
