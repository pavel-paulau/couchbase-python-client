#include <Python.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

static int counter = 0;

typedef struct t_opaque {
  char message[256];
  int x;
} opaque;

static char *opaque_desc = "opaque";

static PyObject *NotOpaqueObject;

//void opaque_dest(PyObject *object) {
  //opaque *obj = (opaque *) PyCapsule_GetPointer(object, "opaque");
  //if (!obj)
  //return 0; /* an exception is already set */
//  free(obj);
//}

void opaque_dest(void *desc, void *obj) {
  free(obj);
}

static PyObject *new_opaque(PyObject *self, PyObject *args) {
  char *str;
  if (!PyArg_ParseTuple(args, "s", &str))
    return 0;

  opaque *z = malloc(sizeof(opaque));

  strcpy(z->message, str);
  z->x = counter++;

  //return PyCapsuleNew(z, "opaque", opaque_dest);

  return PyCObject_FromVoidPtrAndDesc(z, opaque_desc, opaque_dest);
}

static PyObject *show_opaque(PyObject *self, PyObject *args) {
  PyObject *obj;
  opaque *op;

  if (!PyArg_ParseTuple(args, "O", &obj))
    return 0;

  //op = (opaque *) PyCapsule_GetPointer(obj, "opaque");
  //if (!op)
  //return 0;
  
  if (!PyCObject_Check(obj)) {
    PyErr_SetString(NotOpaqueObject, "show_opaque requires an opaque object as its sole argument");
    return 0;
  }
  
  op = (opaque *) PyCObject_AsVoidPtr(obj);
  printf("%d:%s\n", op->x, op->message);
  return Py_None;
}

static PyMethodDef OpaqueMethods[] = {
  {"new_opaque", new_opaque, METH_VARARGS,
   "Create an opaque object containing a message."},
  {"show_opaque", show_opaque, METH_VARARGS,
   "Display the contents of an opaque object."},
  {0, 0, 0, 0}
};

PyMODINIT_FUNC initopaque() {
  PyObject *m;

  m = Py_InitModule("opaque", OpaqueMethods);
  if (!m)
    return;

  NotOpaqueObject = PyErr_NewException("opaque.NotOpaqueObject", 0, 0);
  Py_INCREF(NotOpaqueObject);
  PyModule_AddObject(m, "NotOpaqueObject", NotOpaqueObject);
}

int main(int argc, char *argv[]) {
  Py_SetProgramName(argv[0]);
  Py_Initialize();
  initopaque();
}
