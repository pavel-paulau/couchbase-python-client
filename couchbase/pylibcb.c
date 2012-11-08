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
static PyObject *AsyncLimit;

#define ASYNC_GUARD() if (context->async_mode) {		\
    if (context->async_count >= context->async_limit) {		\
      PyErr_SetString(AsyncLimit, "async limit reached");	\
      return 0;							\
    } else if (context->async.count == context->async.size) {	\
      PyErr_SetString(AsyncLimit, "too many results pending");	\
      return 0;							\
    }								\
  }

#define ASYNC_EXIT(ticket) if(context->async_mode) { \
    ++context->async_count;			     \
    return Py_BuildValue("i", ticket[0]);	     \
  }

#define CB_EXCEPTION(type, msg) {		\
    PyErr_SetString(type, msg);			\
    context->exception = 1;			\
    return 0;					\
  }

#define NEW_EXCEPTION(type, msg) {					\
    return PyObject_CallObject(type, Py_BuildValue("(s)", msg));	\
  }

#define INTERNAL_EXCEPTION_HANDLER(x) {		\
    if (context->internal_exception == 1) {	\
      x;					\
    }						\
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

/* async structures */

typedef struct t_async_result {
  int ticket;
  PyObject *value;
} async_result;

typedef struct t_async_results {
  async_result *buffer;
  int size;
  int count;
  int start;
  int end;
} async_results;


int async_size(async_results *x, int size) {
  if (size == x->size)
    return 0;

  async_result *new = malloc(sizeof(async_result) * size);
  if (!new) {
    PyErr_SetString(OutOfMemory, "failed to resize async result buffer");
    return -1;
  }

  int s = x->start, c = x->count, o = 0;
  while (c--) {
    new[o++] = x->buffer[s];
    s = (s + 1) % x->size;
  }
  
  if (x->buffer)
    free(x->buffer);
  x->buffer = new;
  x->size = size;
  x->count = o;
  x->start = 0;
  x->end = o;

  return 0;
}

void async_push(async_results *x, int ticket, PyObject *value) {
  x->count++;
  x->buffer[x->end].ticket = ticket;
  x->buffer[x->end].value = value;
  x->end = (x->end + 1) % x->size;
}

int process_async_results(async_results *x, int (*f)(async_result *)) {
  while (x->count) {
    if (f(&x->buffer[x->start]))
      return -1;
    x->start = (x->start + 1) % x->size;
    x->count--;
  } return 0;
}

PyObject *_async_rval;

int begin_async_results() {
  _async_rval = PyList_New(0);
  if (!_async_rval) {
    PyErr_SetString(Failure, "begin_async_results");
    return -1;
  } return 0;
}

int process_async_result(async_result *x) {
  PyObject *t = PyTuple_Pack(2, PyInt_FromLong(x->ticket), x->value);
  if (!t) {
    PyErr_SetString(Failure, "process_async_result");
    return 0;
  }

  return PyList_Append(_async_rval, t);
}

/* structure holding all state for python client */

typedef struct t_pylibcb_instance {
  int callback_ticket;
  ticket *ticket_pool;
  ticket_slab *ticket_slabs;
  event_list *event_pool;
  event_slab *event_slabs;
  int async_mode;
  async_results async;
  int async_count;
  int async_limit;
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

PyObject *get_async_results() {
  if (begin_async_results())
    return 0;
  if (process_async_results(&context->async, process_async_result))
    return 0;
  return _async_rval;
}

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

#define lcb_code(code, type)	  \
  case code:			  \
  e_type = type;		  \
  e_msg = # code;		  \
  break

#define lcb_fail(code) lcb_code(code, Failure)

PyObject *lcb_error(libcouchbase_error_t err, int context_error) {
  PyObject *e_type = Failure;
  char *e_msg = "unknown error passed to lcb_error";

  switch (err) {
    lcb_fail(LIBCOUCHBASE_SUCCESS);
    lcb_fail(LIBCOUCHBASE_AUTH_CONTINUE);
    lcb_fail(LIBCOUCHBASE_AUTH_ERROR);
    lcb_fail(LIBCOUCHBASE_DELTA_BADVAL);
    lcb_fail(LIBCOUCHBASE_E2BIG);
    lcb_fail(LIBCOUCHBASE_EBUSY);
    lcb_fail(LIBCOUCHBASE_EINTERNAL);
    lcb_fail(LIBCOUCHBASE_EINVAL);
    lcb_code(LIBCOUCHBASE_ENOMEM, OutOfMemory);
    lcb_fail(LIBCOUCHBASE_ERANGE);
    lcb_fail(LIBCOUCHBASE_ERROR);
    lcb_fail(LIBCOUCHBASE_ETMPFAIL);
    lcb_code(LIBCOUCHBASE_KEY_EEXISTS, KeyExists);
    lcb_fail(LIBCOUCHBASE_KEY_ENOENT);
    lcb_fail(LIBCOUCHBASE_LIBEVENT_ERROR);
    lcb_code(LIBCOUCHBASE_NETWORK_ERROR, ConnectionFailure);
    lcb_fail(LIBCOUCHBASE_NOT_MY_VBUCKET);
    lcb_fail(LIBCOUCHBASE_NOT_STORED);
    lcb_fail(LIBCOUCHBASE_NOT_SUPPORTED);
    lcb_fail(LIBCOUCHBASE_UNKNOWN_COMMAND);
    lcb_code(LIBCOUCHBASE_UNKNOWN_HOST, ConnectionFailure);
    lcb_fail(LIBCOUCHBASE_PROTOCOL_ERROR);
    lcb_code(LIBCOUCHBASE_ETIMEDOUT, Timeout);
    lcb_code(LIBCOUCHBASE_CONNECT_ERROR, ConnectionFailure);
  }

  if (context_error)
    CB_EXCEPTION(e_type, e_msg);
  NEW_EXCEPTION(e_type, e_msg);
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
  int t = rip_ticket((int *) cookie);
  if (context->async_mode) {
    PyObject *rval;
    --context->async_count;

    switch (error) {
    case LIBCOUCHBASE_SUCCESS:
      rval = Py_BuildValue("(s#k)", bytes, nbytes, (unsigned long) cas);
      break;

    case LIBCOUCHBASE_KEY_ENOENT:
      Py_INCREF(Py_None);
      rval = Py_None;
      break;
      
    default:
      rval = lcb_error(error, 0);
    }

    async_push(&context->async, t, rval);
    return 0;
  }

  if (t != context->callback_ticket)
    return 0;
  context->result = error;

  switch (error) {
  case LIBCOUCHBASE_SUCCESS:
    break;
  case LIBCOUCHBASE_KEY_ENOENT:
    context->succeeded = 1;
    return 0;
  default:
    return lcb_error(error, 1);
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
  int t = rip_ticket((int *) cookie);
  if (context->async_mode) {
    PyObject *rval;
    --context->async_count;
    
    switch(error) {
    case LIBCOUCHBASE_SUCCESS:
      Py_INCREF(Py_True);
      rval = Py_True;
      break;
    default:
      rval = lcb_error(error, 0);
    }

    async_push(&context->async, t, rval);
    return 0;
  }

  if (t != context->callback_ticket)
    return 0;
  context->result = error;

  switch (error) {
  case LIBCOUCHBASE_SUCCESS:
    break;
  default:
    return lcb_error(error, 1);
  }

  return 0;
}

void *remove_callback(libcouchbase_t instance,
		      const void *cookie,
		      libcouchbase_error_t error,
		      const void *key,
		      libcouchbase_size_t nkey) {
  int t = rip_ticket((int *) cookie);
  if (context->async_mode) {
    PyObject *rval;
    --context->async_count;
    
    switch (error) {
    case LIBCOUCHBASE_SUCCESS:
      Py_INCREF(Py_True);
      rval = Py_True;
      break;
    default:
      rval = lcb_error(error, 0);
    }

    async_push(&context->async, t, rval);
    return 0; 
  }

  if (t != context->callback_ticket)
    return 0;
  context->result = error;

  switch (error) {
  case LIBCOUCHBASE_SUCCESS:
    break;
  default:
    return lcb_error(error, 1);
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

  z->async_limit = 20; /* maximum number of async events that may be queued up on the event loop */

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

static PyObject *get_async_limit(PyObject *self, PyObject *args) {
  PyObject *cb;
  
  if (!PyArg_ParseTuple(args, "O", &cb))
    return 0;

  set_context(cb);

  return PyInt_FromLong(context->async_limit);
}

static PyObject *set_async_limit(PyObject *self, PyObject *args) {
  PyObject *cb;
  int limit;

  if (!PyArg_ParseTuple(args, "Oi", &cb, &limit))
    return 0;

  if (limit < 1) {
    PyErr_SetString(Failure, "limit must be an integer value greater than 0");
    return 0;
  } else if (limit > 16384) {
    PyErr_SetString(Failure, "there is an arbitrary upper limit of 16384 simultaneous requests as that is enough for anyone");
    return 0;
  }

  set_context(cb);

  if (context->async.count * 2 < limit)
    async_size(&context->async, limit * 2);
  context->async_limit = limit;

  Py_RETURN_NONE;
}

static PyObject *get_async_count(PyObject *self, PyObject *args) {
  PyObject *cb;

  if (!PyArg_ParseTuple(args, "O", &cb))
    return 0;

  set_context(cb);
  return PyInt_FromLong(context->async_count);
}

static PyObject *enable_async(PyObject *self, PyObject *args) {
  PyObject *cb;

  if (!PyArg_ParseTuple(args, "O", &cb))
      return 0;
  set_context(cb);

  context->async_mode = 1;
  context->async_count = 0;
  if (context->async.count < context->async_limit * 2)
    async_size(&context->async, context->async_limit * 2);

  Py_RETURN_NONE;
}

static PyObject *disable_async(PyObject *self, PyObject *args) {
  PyObject *cb;

  if (!PyArg_ParseTuple(args, "O", &cb))
      return 0;
  set_context(cb);

  context->async_mode = 0;

  Py_RETURN_NONE;
}

static PyObject *set(PyObject *self, PyObject *args) {
  PyObject *cb;
  void *key, *val;
  int nkey, nval;
  unsigned long _expiry = 0;
  unsigned long cas = 0;

  if (!PyArg_ParseTuple(args, "Os#s#|kk", &cb, &key, &nkey, &val, &nval, &_expiry, &cas))
    return 0;
  set_context(cb);

  ASYNC_GUARD();

  time_t expiry = _expiry;
  int *ticket = new_ticket();
  if (!ticket)
    return 0;

  libcouchbase_store_by_key(context->cb, hand_out_ticket(ticket), LIBCOUCHBASE_SET, 0, 0, 
			    key, nkey, val, nval, 0, expiry, cas);
  ASYNC_EXIT(ticket);

  libcouchbase_wait(context->cb);
  INTERNAL_EXCEPTION_HANDLER(return 0);

  if (context->exception)
    return 0;

  Py_RETURN_NONE;
}

static PyObject *_remove(PyObject *self, PyObject *args) {
  PyObject *cb;
  void *key;
  int nkey;
  unsigned long cas = 0;

  if (!PyArg_ParseTuple(args, "Os#|k", &cb, &key, &nkey, &cas))
    return 0;
  set_context(cb);

  ASYNC_GUARD();

  int *ticket = new_ticket();
  if (!ticket)
    return 0;

  libcouchbase_remove_by_key(context->cb, hand_out_ticket(ticket), 0, 0, key, nkey, cas);
  ASYNC_EXIT(ticket);

  libcouchbase_wait(context->cb);
  INTERNAL_EXCEPTION_HANDLER(return 0);

  if (context->exception)
    return 0;

  Py_RETURN_NONE;
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
  set_context(cb);

  ASYNC_GUARD();

  libcouchbase_size_t nkey = _nkey;
  time_t expiry = _expiry;
  int *ticket = new_ticket();
  if (!ticket)
    return 0;

  if (usec && !context->async_mode)
    if (!create_timeout(usec, hand_out_ticket(ticket))) {
      rip_ticket(ticket);
      return 0;
    }

  libcouchbase_mget_by_key(context->cb, hand_out_ticket(ticket), 0, 0, 1, &key, &nkey, _expiry ? &expiry : 0);
  ASYNC_EXIT(ticket);

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

  if (context->result == LIBCOUCHBASE_KEY_ENOENT)
    Py_RETURN_NONE;

  if (return_cas)
    return Py_BuildValue("Ok", context->returned_value, (unsigned long) context->returned_cas);
  return context->returned_value;
}

static PyObject *async_wait(PyObject *self, PyObject *args) {
  PyObject *cb;
  int usec = 0;

  if (!PyArg_ParseTuple(args, "Oi", &cb, &usec))
    return 0;
  set_context(cb);

  if (!context->async_mode) {
    PyErr_SetString(Failure, "async mode is not enabled");
    return 0;
  }

  if (usec) {
    int *ticket = new_ticket();
    if (!ticket)
      return 0;

    if (!create_timeout(usec, hand_out_ticket(ticket))) {
      rip_ticket(ticket);
      return 0;
    }
  }

  while (!context->timed_out && context->async_count && !context->internal_exception)
    libcouchbase_wait(context->cb);

  INTERNAL_EXCEPTION_HANDLER(return 0);

  PyObject *r = get_async_results();
  if (!r) {
    PyErr_SetString(Failure, "get_async_results");
    return 0;
  } return r;
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
  { "get_async_limit", get_async_limit, METH_VARARGS,
    "Get the limit for the number of requests allowed before one is required to complete" },
  { "set_async_limit", set_async_limit, METH_VARARGS,
    "Set the limit for the number of requests allowed before one is required to complete" },
  { "get_async_count", get_async_count, METH_VARARGS,
    "Get the number of incomplete asynchronous requests waiting" },
  { "enable_async", enable_async, METH_VARARGS,
    "Enable asynchronous behavior" },
  { "disable_async", disable_async, METH_VARARGS,
    "Disable asynchronous behavior" },
  { "async_wait", async_wait, METH_VARARGS,
    "Execute eventloop for a given number of microseconds" },
  { 0, 0, 0, 0 }
};

PyMODINIT_FUNC init_pylibcb() {
  PyObject *m;

  m = Py_InitModule("_pylibcb", PylibcbMethods);
  if (!m)
    return;
  
  struct exception_init {
    char *name;
    PyObject **exception;
  } exceptions[] = {
    { "Timeout", &Timeout },
    { "OutOfMemory", &OutOfMemory },
    { "ConnectionFailure", &ConnectionFailure },
    { "Failure", &Failure },
    { "KeyExists", &KeyExists },
    { "AsyncLimit", &AsyncLimit },
    { 0, 0 }
  };

  int i = 0;
  while (exceptions[i].name) {
    char name[256];
    sprintf(name, "_pylibcb.%s", exceptions[i].name);
    *exceptions[i].exception = PyErr_NewException(name, 0, 0);
    Py_INCREF(*exceptions[i].exception);
    PyModule_AddObject(m, exceptions[i].name, *exceptions[i].exception);
    ++i;
  }

  default_exception = Failure;
}

int main(int argc, char **argv) {  
  Py_SetProgramName(argv[0]);
  Py_Initialize();
  init_pylibcb();
  return 0;
}
