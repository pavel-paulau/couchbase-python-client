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

static PyObject *Timeout;
static PyObject *OutOfMemory;
static PyObject *ConnectionFailure;
static PyObject *Failure;

typedef struct t_pylibcb_instance {
  int callback_ticket;
  int succeeded;
  int timed_out;
  char returned_value[16384];
  int returned_value_nbytes;
  libcouchbase_cas_t returned_cas;
  struct event_base *base;
  libcouchbase_t cb;
} pylibcb_instance;

static char *pylibcb_instance_desc = "pylibcb_instance";

void pylibcb_instance_dest(void *obj, void *desc) {
  pylibcb_instance *z = (pylibcb_instance *) obj;

  libcouchbase_destroy(z->cb);
  event_base_free(z->base); /* will libcouchbase do this for us? */
  free(z);
}

static pylibcb_instance *context = 0;

int *new_ticket() {
  int *boxed = malloc(sizeof(int) *2);
  boxed[0] = ++context->callback_ticket;
  boxed[1] = 0;
  return boxed;
}

int *punch_ticket(int *t) {
  ++t[1];
  return t;
}

int rip_ticket(int *t) {
  int r = t[0];
  if (!--t[1])
    free(t);
  return r;
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
  if (rip_ticket(cookie) != context->callback_ticket)
    return 0;
  
  /* flag the operation as a success */
  context->succeeded = 1;
  memcpy(context->returned_value, bytes, nbytes);
  context->returned_value_nbytes = nbytes;
  context->returned_cas = cas;

  return 0;
}

void *set_callback(libcouchbase_t instance,
		   const void *cookie,
		   libcouchbase_storage_t operation,
		   libcouchbase_error_t error,
		   const void *key,
		   libcouchbase_size_t nkey,
		   libcouchbase_cas_t cas) {
  if (rip_ticket(cookie) != context->callback_ticket)
    return 0;

  /* something here to pass on successes/failures up the chain */
  
  return 0;
}

void *remove_callback(libcouchbase_t instance,
		      const void *cookie,
		      libcouchbase_error_t error,
		      const void *key,
		      libcouchbase_size_t nkey) {
  if (rip_ticket(cookie) != context->callback_ticket)
    return 0;
  
  /* something here to pass on successes/failures up the chain */

  return 0;
}

void timeout_callback(libcouchbase_socket_t sock, short which, void *cb_data) {
  if (rip_ticket(cb_data) != context->callback_ticket)
    return 0;

  /* mark current operation as timed out and break the event loop */
 
  context->timed_out = 1;
  event_base_loopbreak(context->base);
}

void create_timeout(unsigned int usec, int *ticket) {
  struct event *timeout = event_new(context->base, -1, 0, 0, 0);
  struct timeval tmo;
  event_assign(timeout, context->base, -1, EV_TIMEOUT, timeout_callback, ticket);
  tmo.tv_sec = usec / 1000000;
  tmo.tv_usec = usec % 1000000;
  event_add(timeout, &tmo);
}

static PyObject *open(PyObject *self, PyObject *args) {
  char *host = 0;
  char *user = 0;
  char *passwd = 0;
  char *bucket = 0;

  if (!PyArg_ParseTuple(args, "|ssss", &host, &user, &passwd, &bucket))
    return 0;

  pylibcb_instance *z = malloc(sizeof(pylibcb_instance));
  if (!z) {
    PyErr_SetString(OutOfMemory, "ran out of memory while allocating pylibcb instance");
    return 0;
  }
  z->callback_ticket = 0;

  z->base = event_base_new();
  if (!z) {
    PyErr_SetString(OutOfMemory, "could not create event base for new instance");
    goto free_instance;
  }

  libcouchbase_error_t e = LIBCOUCHBASE_SUCCESS;
  libcouchbase_io_opt_t *cb_base = libcouchbase_create_io_ops(LIBCOUCHBASE_IO_OPS_LIBEVENT, z->base, &e);
  if (e == LIBCOUCHBASE_NOT_SUPPORTED) {
    PyErr_SetString(Failure, "libcouchbase as built on this platform does not support libevent ?");
    goto free_event_base;
  } else if (e == LIBCOUCHBASE_ENOMEM) {
    PyErr_SetString(OutOfMemory, "ran out of memory while setting up libcouchbase event loop");
    goto free_event_base;
  } else if (e == LIBCOUCHBASE_ERROR) {
    PyErr_SetString(Failure, "a failure occurred while setting up libcouchbase event loop (probably couldn't locate libcouchbase_event shared library)");
    goto free_event_base;
    return 0;
  } else if (e != LIBCOUCHBASE_SUCCESS) {
    PyErr_SetString(Failure, "an unhandled error condition occurred when calling libcouchbase_create_io_ops");
    goto free_event_base;
  }
  
  z->cb = libcouchbase_create(host, user, passwd, bucket, cb_base);
  if (!z->cb) {
    /* unsure what to do with libcouchbase_io_ops_t created in last step */
    PyErr_SetString(OutOfMemory, "could not create libcouchbase instance");
    goto free_event_base;
  }

  libcouchbase_set_storage_callback(z->cb, (libcouchbase_storage_callback) set_callback);
  libcouchbase_set_get_callback(z->cb, (libcouchbase_get_callback) get_callback);
  libcouchbase_set_remove_callback(z->cb, (libcouchbase_remove_callback) remove_callback);

  if (libcouchbase_connect(z->cb) != LIBCOUCHBASE_SUCCESS) {
    PyErr_SetString(ConnectionFailure, "libcouchbase_connect");
    goto free_event_base;
  }

  /* establish connection */
  libcouchbase_wait(z->cb);

  return PyCObject_FromVoidPtrAndDesc(z, pylibcb_instance_desc, pylibcb_instance_dest);

 free_event_base:
  free(z->base);
 free_instance:
  free(z);

  return 0;
}

int pyobject_is_pylibcb_instance(PyObject *x) {
  if (!PyCObject_Check(x)
      || memcmp(PyCObject_GetDesc(x), pylibcb_instance_desc, sizeof("pylibcb_instance"))) {
    PyErr_SetString(Failure, "need pylibcb instance as first argument");
    return 0;
  } return 1;
}

static PyObject *set(PyObject *self, PyObject *args) {
  PyObject *cb;
  void *key, *val;
  int nkey, nval;

  if (!PyArg_ParseTuple(args, "Os#s#", &cb, &key, &nkey, &val, &nval))
    return 0;
  if (!pyobject_is_pylibcb_instance(cb))
    return 0;

  context = (pylibcb_instance *) PyCObject_AsVoidPtr(cb);
  libcouchbase_store_by_key(context->cb, punch_ticket(new_ticket()), LIBCOUCHBASE_SET, 0, 0, key, nkey, val, nval, 0, 0, 0);
  libcouchbase_wait(context->cb);

  Py_INCREF(Py_None);
  return Py_None;
}

static PyObject *_remove(PyObject *self, PyObject *args) {
  PyObject *cb;
  void *key;
  int nkey;

  if (!PyArg_ParseTuple(args, "Os#", &cb, &key, &nkey))
    return 0;
  if (!pyobject_is_pylibcb_instance(cb))
    return 0;

  context = (pylibcb_instance *) PyCObject_AsVoidPtr(cb);
  libcouchbase_remove_by_key(context->cb, punch_ticket(new_ticket()), 0, 0, key, nkey, 0);
  libcouchbase_wait(context->cb);

  Py_INCREF(Py_None);
  return Py_None;
}

static PyObject *get(PyObject *self, PyObject *args) {
  PyObject *cb;
  void *key;
  int nkey;
  int usec = 0;

  if (!PyArg_ParseTuple(args, "Os#|i", &cb, &key, &nkey, &usec))
    return 0;
  if (!pyobject_is_pylibcb_instance(cb))
    return 0;
  
  context = (pylibcb_instance *) PyCObject_AsVoidPtr(cb);
  context->succeeded = 0;
  context->timed_out = 0;

  int *ticket = new_ticket();

  if (usec)
    create_timeout(usec, punch_ticket(ticket));
  libcouchbase_mget_by_key(context->cb, punch_ticket(ticket), 0, 0, 1, &key, &nkey, 0);

  while (!context->timed_out && !context->succeeded)
    libcouchbase_wait(context->cb);

  if (context->succeeded)
    return Py_BuildValue("s#", context->returned_value, context->returned_value_nbytes);
  
  if (context->timed_out) 
    PyErr_SetString(Timeout, "timeout in get");
    return 0;

  PyErr_SetString(Failure, "no timeout.. AND no success?");
  return 0;
}

static PyMethodDef PylibcbMethods[] = {
  { "open", open, METH_VARARGS,
    "Open connection to couchbase server" },
  { "get", get, METH_VARARGS,
    "Get a value by key. Optionally specify timeout in usecs" },
  { "set", set, METH_VARARGS,
    "Set a value by key" },
  { "remove", _remove, METH_VARARGS,
    "Remove a value by key" },
  { 0, 0, 0, 0 }
};

PyMODINIT_FUNC initpylibcb() {
  PyObject *m;

  m = Py_InitModule("pylibcb", PylibcbMethods);
  if (!m)
    return;

  Timeout = PyErr_NewException("pylibcb.Timeout", 0, 0);
  Py_INCREF(Timeout);
  PyModule_AddObject(m, "Timeout", Timeout);
  
  OutOfMemory = PyErr_NewException("pylibcb.OutOfMemory", 0, 0);
  Py_INCREF(OutOfMemory);
  PyModule_AddObject(m, "OutOfMemory", OutOfMemory);

  ConnectionFailure = PyErr_NewException("pylibcb.ConnectionFailure", 0, 0);
  Py_INCREF(ConnectionFailure);
  PyModule_AddObject(m, "ConnectionFailure", ConnectionFailure);
  
  Failure = PyErr_NewException("pylibcb.Failure", 0, 0);
  Py_INCREF(Failure);
  PyModule_AddObject(m, "Failure", Failure);   
}

int main(int argc, char **argv) {  
  Py_SetProgramName(argv[0]);
  Py_Initialize();
  initpylibcb();
  return 0;
}
