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

typedef struct t_ticket {  
  int ticket[2];
  struct t_ticket *next;
} ticket;

typedef struct t_ticket_slab {
  int count;
  int used;
  ticket *slab;
  struct t_ticket_slab *next;
} ticket_slab;

#define SLAB_SIZE 256

ticket_slab *new_ticket_slab(struct t_ticket_slab *next) {
  ticket_slab *x = calloc(1, sizeof(ticket_slab) + sizeof(ticket) * SLAB_SIZE);
  if (!x)
    return 0;

  int i;
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

typedef struct t_buffer {
  size_t size;
  int filled;
  void *contents;
} buffer;

int guarantee_buffer(buffer *b, size_t size) {
  if (b->size < size) {
    size_t i = 1;
    size_t e = (SIZE_MAX >> 1) + 1;

    --size;
    do {
      size |= size >> i;
      i <<= 1;
    } while (i != e);
    ++size;
    
    if (b->contents)
      free(b->contents);
    b->contents = malloc(sizeof(char *) * size);
    if (!b->contents) {
      b->size = 0;
      return 0;
    }

    b->size = size;
  } return 1;    
}

void destroy_buffer(buffer *b) {
  if (b->contents)
    free(b->contents);
}

typedef struct t_pylibcb_instance {
  int callback_ticket;
  ticket *ticket_pool;
  ticket_slab *ticket_slabs;
  int succeeded;
  int timed_out;
  int exception;
  int internal_exception;
  buffer returned_value;
  libcouchbase_error_t result;
  libcouchbase_error_t internal_error;
  char *internal_error_msg;
  libcouchbase_cas_t returned_cas;
  struct event_base *base;
  libcouchbase_t cb;
} pylibcb_instance;

static char *pylibcb_instance_desc = "pylibcb_instance";

void pylibcb_instance_dest(void *obj, void *desc) {
  pylibcb_instance *z = (pylibcb_instance *) obj;
  
  destroy_ticket_slab(z->ticket_slabs);
  destroy_buffer(&z->returned_value);
  libcouchbase_destroy(z->cb);
  event_base_free(z->base);
  free(z);
}

static pylibcb_instance *context = 0;

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
    _t->next = context->ticket_pool;
    context->ticket_pool = _t;
  } return r;
}

void *error_callback(libcouchbase_t instance,
		     libcouchbase_error_t error,
		     const char *errinfo) {
  /* just in case libcouchbase_error_handler starts giving us this */
  if (error == LIBCOUCHBASE_SUCCESS)
    return 0;

  context->internal_error = error;
  context->internal_error_msg = (char *) errinfo;
  context->internal_exception = 1;
  event_base_loopbreak(context->base);
  return 0;
}

#define INTERNAL_EXCEPTION_HANDLER(x) { \
  if (context->internal_exception == 1) { \
    PyErr_SetString(Failure, context->internal_error_msg ? \
		    context->internal_error_msg : "internal exception"); \
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
  case LIBCOUCHBASE_KEY_ENOENT:
    break;
  case LIBCOUCHBASE_ENOMEM:
    CB_EXCEPTION(OutOfMemory, "libcouchbase_enomem");
  case LIBCOUCHBASE_EINTERNAL:
    CB_EXCEPTION(Failure, "libcouchbase_einternal");
  default:
    CB_EXCEPTION(Failure, "mystery condition in get callback");
  }
  
  if (!guarantee_buffer(&context->returned_value, nbytes)) {
    PyErr_SetString(OutOfMemory, "not enough memory for results of get");
    context->exception = 1;
    return 0;
  }

  memcpy(context->returned_value.contents, bytes, nbytes);
  context->returned_value.filled = nbytes;
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

void timeout_callback(libcouchbase_socket_t sock, short which, void *cb_data) {
  if (rip_ticket((int *) cb_data) != context->callback_ticket)
    return;

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

  pylibcb_instance *z = calloc(1, sizeof(pylibcb_instance));
  if (!z) {
    PyErr_SetString(OutOfMemory, "ran out of memory while allocating pylibcb instance");
    return 0;
  }

  z->ticket_slabs = new_ticket_slab(0);
  if (!z->ticket_slabs) {
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

  if (libcouchbase_connect(z->cb) != LIBCOUCHBASE_SUCCESS) {
    PyErr_SetString(ConnectionFailure, z->internal_error_msg ? 
		    z->internal_error_msg : "libcouchbase_connect");
    goto free_event_base;
  }

  /* establish connection */
  libcouchbase_wait(z->cb);
  if (z->internal_exception) {
    PyErr_SetString(ConnectionFailure, z->internal_error_msg ? 
		    z->internal_error_msg : "libcouchbase_connect");
    goto free_event_base;
  }

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
    create_timeout(usec, hand_out_ticket(ticket));
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
    return Py_BuildValue("s#k", context->returned_value.contents, context->returned_value.filled,
			(unsigned long) context->returned_cas);
  return Py_BuildValue("s#", context->returned_value.contents, context->returned_value.filled);
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

  KeyExists = PyErr_NewException("pylibcb.KeyExists", 0, 0);
  Py_INCREF(KeyExists);
  PyModule_AddObject(m, "KeyExists", KeyExists);
}

int main(int argc, char **argv) {  
  Py_SetProgramName(argv[0]);
  Py_Initialize();
  initpylibcb();
  return 0;
}
