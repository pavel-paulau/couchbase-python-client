#include <Python.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <libcouchbase/couchbase.h>
#include <event.h>
#include <time.h>
#include <sys/types.h>
#include <sys/socket.h>

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
static PyObject *KeyExists;

#define CB_EXCEPTION(type, msg) { \
  PyErr_SetString(type, msg); \
  context->exception = 1; \
  return 0; \
  }

/* allocators for commonly held objects */

#define SLAB_SIZE 256

typedef struct t_event_list {
  struct event ev;
  struct t_event_list *next;
} event_list;

typedef struct t_event_slab {
  int count;
  int used;
  event_list *slab;
  struct t_event_slab *next;
} event_slab;

event_slab *new_event_slab(struct t_event_slab *next) {
  event_slab *x = calloc(1, sizeof(event_slab) + sizeof(event_list) * SLAB_SIZE);
  if (!x)
    return 0;

  x->slab = (void *) x + sizeof(event_slab);
  x->count = SLAB_SIZE - 1;
  x->next = next;
  return x;
}

void destroy_event_slab(struct t_event_slab *t) {
  event_slab *n;
  while (t) {
    n = t->next;
    free(t);
    t = n;
  }
}

typedef struct t_ticket {  
  int ticket[2];
  struct t_event_list *ev;
  struct t_ticket *next;
} ticket;

typedef struct t_ticket_slab {
  int count;
  int used;
  ticket *slab;
  struct t_ticket_slab *next;
} ticket_slab;

ticket_slab *new_ticket_slab(struct t_ticket_slab *next) {
  ticket_slab *x = calloc(1, sizeof(ticket_slab) + sizeof(ticket) * SLAB_SIZE);
  if (!x)
    return 0;

  x->slab = (void *) x + sizeof(ticket_slab);
  x->count = SLAB_SIZE - 1;
  x->next = next;
  return x;
}

void destroy_ticket_slab(struct t_ticket_slab *t) {
  ticket_slab *n;
  while (t) {
    n = t->next;
    free(t);
    t = n;
  }
}

/* structure holding all state for python client */

typedef struct t_pylibcb_instance {
  int callback_ticket;
  ticket *ticket_pool;
  ticket_slab *ticket_slabs;
  event_list *event_pool;
  event_slab *event_slabs;
  int succeeded;
  int timed_out;
  int exception;
  int internal_exception;
  PyObject *returned_value;
  libcouchbase_error_t result;
  libcouchbase_error_t internal_error;
  libcouchbase_cas_t returned_cas;
  struct event_base *base;
  libcouchbase_t cb;
} pylibcb_instance;

static char *pylibcb_instance_desc = "pylibcb_instance";

void pylibcb_instance_dest(void *obj, void *desc) {
  pylibcb_instance *z = (pylibcb_instance *) obj;
  
  destroy_ticket_slab(z->ticket_slabs);
  destroy_event_slab(z->event_slabs);
  libcouchbase_destroy(z->cb);
  event_base_free(z->base);
  free(z);
}

static pylibcb_instance *context = 0;

event_list *alloc_event() {
  event_list *ev;

  if (context->event_pool) {
    ev = context->event_pool;
    context->event_pool = ev->next;
  } else {
    event_slab *z = context->event_slabs;
    
    if (z->used == z->count) {
      event_slab *n = new_event_slab(z);
      if (!n)
	return 0;
      context->event_slabs = n;
    }

    ev = &z->slab[z->used++];
  }

  ev->next = 0;
  return ev;
}

int *alloc_ticket() {
  ticket *t;

  if (context->ticket_pool) {
    t = context->ticket_pool;
    context->ticket_pool = t->next;
  } else {
    ticket_slab *z = context->ticket_slabs;

    if (z->used == z->count) {
      ticket_slab *n = new_ticket_slab(z);
      if (!n)
	return 0;
      context->ticket_slabs = n;
    }

    t = &z->slab[z->used++];
  }

  t->ticket[0] = ++context->callback_ticket;
  t->ticket[1] = 0;
  t->ev = 0;
  t->next = 0;

  return (int *) t;  
}

int *hand_out_ticket(int *t) {
  ++t[1];
  return t;
}

int rip_ticket(int *t) {
  int r = t[0];
  if (!--t[1]) {
    ticket *_t = (ticket *) t;
    if (_t->ev) {
      event_del(&_t->ev->ev);
      _t->ev->next = context->event_pool;
      context->event_pool = _t->ev;
    }
    _t->next = context->ticket_pool;
    context->ticket_pool = _t;
  } return r;
}

void timeout_callback(libcouchbase_socket_t sock, short which, void *cb_data) {
  if (rip_ticket((int *) cb_data) != context->callback_ticket)
    return;

  /* mark current operation as timed out and break the event loop */
 
  context->timed_out = 1;
  event_base_loopbreak(context->base);
}

int create_timeout(unsigned int usec, int *_ticket) {
  event_list *timeout = alloc_event();
  if (!timeout) {
    PyErr_SetString(OutOfMemory, "failed to allocate timeout event");
    return 0;
  }

  struct timeval tmo;
  event_assign(&timeout->ev, context->base, -1, EV_TIMEOUT, timeout_callback, _ticket);
  tmo.tv_sec = usec / 1000000;
  tmo.tv_usec = usec % 1000000;
  event_add(&timeout->ev, &tmo);
  ((ticket *) _ticket)->ev = timeout;
  return 1;
}

static char *default_error_string = "internal exception";
static PyObject *default_exception;

void *error_callback(libcouchbase_t instance,
		     libcouchbase_error_t error,
		     const char *errinfo) {
  /* just in case libcouchbase_error_handler starts giving us this */
  if (error == LIBCOUCHBASE_SUCCESS)
    return 0;

  PyErr_SetString(default_exception, errinfo ? errinfo : default_error_string);
  context->internal_error = error;
  context->internal_exception = 1;
  event_base_loopbreak(context->base);
  return 0;
}

#define INTERNAL_EXCEPTION_HANDLER(x) { \
  if (context->internal_exception == 1) { \
    x; \
  } \
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

  if (rip_ticket((int *) cookie) != context->callback_ticket)
    return 0;
  context->result = error;

  switch (error) {
  case LIBCOUCHBASE_SUCCESS:
    break;
  case LIBCOUCHBASE_KEY_ENOENT:
    context->succeeded = 1;
    return 0;
  case LIBCOUCHBASE_ENOMEM:
    CB_EXCEPTION(OutOfMemory, "libcouchbase_enomem");
  case LIBCOUCHBASE_EINTERNAL:
    CB_EXCEPTION(Failure, "libcouchbase_einternal");
  default:
    CB_EXCEPTION(Failure, "mystery condition in get callback");
  }
  
  context->returned_value = Py_BuildValue("s#", bytes, nbytes);
  context->returned_cas = cas;
  context->succeeded = 1;

  return 0;
}

void *set_callback(libcouchbase_t instance,
		   const void *cookie,
		   libcouchbase_storage_t operation,
		   libcouchbase_error_t error,
		   const void *key,
		   libcouchbase_size_t nkey,
		   libcouchbase_cas_t cas) {
  if (rip_ticket((int *) cookie) != context->callback_ticket)
    return 0;
  context->result = error;

  switch (error) {
  case LIBCOUCHBASE_SUCCESS:
    break;
  case LIBCOUCHBASE_KEY_EEXISTS:
    CB_EXCEPTION(KeyExists, "key already present");
  default:
    CB_EXCEPTION(Failure, "mystery condition in set callback");
  }

  return 0;
}

void *remove_callback(libcouchbase_t instance,
		      const void *cookie,
		      libcouchbase_error_t error,
		      const void *key,
		      libcouchbase_size_t nkey) {
  if (rip_ticket((int *) cookie) != context->callback_ticket)
    return 0;
  context->result = error;

  switch (error) {
  case LIBCOUCHBASE_SUCCESS:
    break;
  default:
    CB_EXCEPTION(Failure, "mystery condition in set callback");
  }  

  return 0;
}

static PyObject *open(PyObject *self, PyObject *args) {
  char *host = 0;
  char *user = 0;
  char *passwd = 0;
  char *bucket = 0;

  if (!PyArg_ParseTuple(args, "|ssss", &host, &user, &passwd, &bucket))
    return 0;

  if (!strlen(host))
    host = 0;
  if (!strlen(user))
    user = 0;
  if (!strlen(passwd))
    passwd = 0;
  if (!strlen(bucket))
    passwd = 0;

  pylibcb_instance *z = calloc(1, sizeof(pylibcb_instance));
  if (!z) {
    PyErr_SetString(OutOfMemory, "ran out of memory while allocating pylibcb instance");
    return 0;
  }

  z->ticket_slabs = new_ticket_slab(0);
  z->event_slabs = new_event_slab(0);
  if (!z->ticket_slabs || !z->event_slabs) {
    PyErr_SetString(OutOfMemory, "ran out of memory while allocating pylibcb instance");
    goto free_instance;
  }

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

  context = z;
  libcouchbase_set_error_callback(z->cb, (libcouchbase_error_callback) error_callback);
  libcouchbase_set_storage_callback(z->cb, (libcouchbase_storage_callback) set_callback);
  libcouchbase_set_get_callback(z->cb, (libcouchbase_get_callback) get_callback);
  libcouchbase_set_remove_callback(z->cb, (libcouchbase_remove_callback) remove_callback);
  
  default_error_string = "libcouchbase_connect";
  default_exception = ConnectionFailure;

  if (libcouchbase_connect(z->cb) != LIBCOUCHBASE_SUCCESS) {
    goto free_event_base;
  }

  /* establish connection */
  libcouchbase_wait(z->cb);
  if (z->internal_exception) {
    goto free_event_base;
  }

  default_error_string = "internal exception";
  default_exception = Failure;

  return PyCObject_FromVoidPtrAndDesc(z, pylibcb_instance_desc, pylibcb_instance_dest);

 free_event_base:
  free(z->base);
 free_instance:
  if (z->ticket_slabs)
    destroy_ticket_slab(z->ticket_slabs);
  if (z->event_slabs)
    destroy_event_slab(z->event_slabs);

  free(z);
  default_error_string = "internal exception";
  default_exception = Failure;
  return 0;
}

int pyobject_is_pylibcb_instance(PyObject *x) {
  if (!PyCObject_Check(x)
      || memcmp(PyCObject_GetDesc(x), pylibcb_instance_desc, sizeof("pylibcb_instance"))) {
    PyErr_SetString(Failure, "need pylibcb instance as first argument");
    return 0;
  } return 1;
}

void set_context(PyObject *x) {
  context = PyCObject_AsVoidPtr(x);
  context->succeeded = 0;
  context->timed_out = 0;
  context->exception = 0;
  context->internal_exception = 0;
}

int *new_ticket() {
  int *t = alloc_ticket();
  if (!t) {
    PyErr_SetString(OutOfMemory, "ran out of memory for new callback tracking tickets");
    return 0;
  } return t;
}


static PyObject *set(PyObject *self, PyObject *args) {
  PyObject *cb;
  void *key, *val;
  int nkey, nval;
  unsigned long _expiry = 0;
  unsigned long cas = 0;

  if (!PyArg_ParseTuple(args, "Os#s#|kk", &cb, &key, &nkey, &val, &nval, &_expiry, &cas))
    return 0;
  if (!pyobject_is_pylibcb_instance(cb))
    return 0;
  set_context(cb);

  time_t expiry = _expiry;
  int *ticket = new_ticket();
  if (!ticket)
    return 0;

  libcouchbase_store_by_key(context->cb, hand_out_ticket(ticket), LIBCOUCHBASE_SET, 0, 0, 
			    key, nkey, val, nval, 0, expiry, cas);
  libcouchbase_wait(context->cb);
  INTERNAL_EXCEPTION_HANDLER(return 0);

  if (context->exception)
    return 0;

  Py_INCREF(Py_None);
  return Py_None;
}

static PyObject *_remove(PyObject *self, PyObject *args) {
  PyObject *cb;
  void *key;
  int nkey;
  unsigned long cas = 0;

  if (!PyArg_ParseTuple(args, "Os#|k", &cb, &key, &nkey, &cas))
    return 0;
  if (!pyobject_is_pylibcb_instance(cb))
    return 0;
  set_context(cb);

  int *ticket = new_ticket();
  if (!ticket)
    return 0;

  libcouchbase_remove_by_key(context->cb, hand_out_ticket(ticket), 0, 0, key, nkey, cas);
  libcouchbase_wait(context->cb);
  INTERNAL_EXCEPTION_HANDLER(return 0);

  if (context->exception)
    return 0;

  Py_INCREF(Py_None);
  return Py_None;
}

static PyObject *get(PyObject *self, PyObject *args) {
  PyObject *cb;
  const void * const key;
  int _nkey;
  int usec = 0;
  unsigned long _expiry = 0;
  int return_cas = 0;
  
  if (!PyArg_ParseTuple(args, "Os#|iki", &cb, &key, &_nkey, &usec, &_expiry, &return_cas))
    return 0;
  if (!pyobject_is_pylibcb_instance(cb))
    return 0;
  set_context(cb);

  libcouchbase_size_t nkey = _nkey;
  time_t expiry = _expiry;
  int *ticket = new_ticket();
  if (!ticket)
    return 0;

  if (usec)
    if (!create_timeout(usec, hand_out_ticket(ticket))) {
      rip_ticket(ticket);
      return 0;
    }
  libcouchbase_mget_by_key(context->cb, hand_out_ticket(ticket), 0, 0, 1, &key, &nkey, _expiry ? &expiry : 0);

  while (!context->timed_out && !context->succeeded && !context->exception && !context->internal_exception)
    libcouchbase_wait(context->cb);
  INTERNAL_EXCEPTION_HANDLER(return 0);

  if (context->exception)
    return 0;
  
  if (context->timed_out) {
    PyErr_SetString(Timeout, "timeout in get");
    return 0;
  }

  if (!context->succeeded) {
    PyErr_SetString(Failure, "no timeout.. AND no success?");
    return 0;
  }

  if (context->result == LIBCOUCHBASE_KEY_ENOENT) {
    Py_INCREF(Py_None);
    return Py_None;
  }

  if (return_cas)
    return Py_BuildValue("Ok", context->returned_value, (unsigned long) context->returned_cas);
  return context->returned_value;
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

PyMODINIT_FUNC init_pylibcb() {
  PyObject *m;

  m = Py_InitModule("_pylibcb", PylibcbMethods);
  if (!m)
    return;

  Timeout = PyErr_NewException("_pylibcb.Timeout", 0, 0);
  Py_INCREF(Timeout);
  PyModule_AddObject(m, "Timeout", Timeout);
  
  OutOfMemory = PyErr_NewException("_pylibcb.OutOfMemory", 0, 0);
  Py_INCREF(OutOfMemory);
  PyModule_AddObject(m, "OutOfMemory", OutOfMemory);

  ConnectionFailure = PyErr_NewException("_pylibcb.ConnectionFailure", 0, 0);
  Py_INCREF(ConnectionFailure);
  PyModule_AddObject(m, "ConnectionFailure", ConnectionFailure);
  
  Failure = PyErr_NewException("_pylibcb.Failure", 0, 0);
  Py_INCREF(Failure);
  PyModule_AddObject(m, "Failure", Failure);

  KeyExists = PyErr_NewException("_pylibcb.KeyExists", 0, 0);
  Py_INCREF(KeyExists);
  PyModule_AddObject(m, "KeyExists", KeyExists);

  default_exception = Failure;
}

int main(int argc, char **argv) {  
  Py_SetProgramName(argv[0]);
  Py_Initialize();
  init_pylibcb();
  return 0;
}
