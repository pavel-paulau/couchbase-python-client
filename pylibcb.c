#include <Python.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <libcouchbase/couchbase.h>
#include <event.h>
#include <time.h>
#include <sys/types.h>
#include <sys/socket.h>

/*
#ifndef HAVE_LIBEVENT2

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

#endif */

char *asciiz(const void *data, size_t nbytes) {
  char *z = malloc(nbytes+1);
  memcpy(z, data, nbytes);
  z[nbytes] = 0;
  return z;
}

typedef struct t_pylibcb_instance {
  static int callback_counter = 0;
  static int succeeded = 0;
  static int timed_out = 0;
  static char current_key[256];
  static int current_key_nbytes = 0;
  static char returned_value[16384];
  static int returned_value_nbytes = 0;
  static libcouchbase_cas_t returned_cas;
  static struct event_base *base = 0;
} pylibcb_instance;



int operation_key_is_current(void *key, libcouchbase_size_t nbytes) {
  if (nbytes != current_key_nbytes)
    return 0;
  if (memcmp(current_key, key, nbytes))
    return 0;
  return 1;
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

  /* ensure this callback matches up with the one expected by the current library invocation */
  if (!operation_key_is_current(key, nbytes))
    return 0;
  
  /* flag the operation as a success */
  succeeded = 1;
  memcpy(returned_value, bytes, nbytes);
  returned_value_nbytes = nbytes;
  returned_cas = cas;

  return 0;
}

void *set_callback(libcouchbase_t instance,
		   const void *cookie,
		   libcouchbase_storage_t operation,
		   libcouchbase_error_t error,
		   const void *key,
		   libcouchbase_size_t nkey,
		   libcouchbase_cas_t cas) {
  
  if (!operation_key_is_current(key, nbytes))
    return 0;

  /* something here to pass on successes/failures up the chain */
  
  return 0;
}

void *remove_callback(libcouchbase_t instance,
		      const void *cookie,
		      libcouchbase_error_t error,
		      const void *key,
		      libcouchbase_size_t nkey) {

  if (!operation_key_is_current(key, nbytes))
    return 0;

  /* something here to pass on successes/failures up the chain */

  return 0;
}

void timeout_function(libcouchbase_socket_t sock, short which, void *cb_data) {
  int operation_number = *((int *) cb_data);
  if (operation_number != callback_counter)
    return;

  /* mark current operation as timed out and break the event loop */
 
  timed_out = 1;
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
