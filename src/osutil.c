/*
 * Copyright (c) 2009 rPath, Inc.
 *
 * All rights reserved.
 */

#include <Python.h>

#include <unistd.h>

#include "pycompat.h"


static PyObject *
py_close_fds(PyObject *self, PyObject *args) {
    PyObject *exclude, *tempfd;
    int i, max, closed = 0;
    int excl_index = 0, excl_count, next_excl = -1;

    /* parse and check args */
    if (!PyArg_ParseTuple(args, "O", &exclude)) {
        return NULL;
    }

    if (!PySequence_Check(exclude)) {
        PyErr_SetString(PyExc_TypeError, "first argument must be a sequence");
        return NULL;
    }

    max = getdtablesize();
    excl_count = PySequence_Length(exclude);

    for (i = 0; i < max; i++) {
        /* test whether the fd is excluded */
        if (i > next_excl && excl_index < excl_count) {
            if ((tempfd = PySequence_GetItem(exclude, excl_index)) == NULL) {
                return NULL;
            }
            if (!PYINT_Check(tempfd)) {
                Py_DECREF(tempfd);
                PyErr_SetString(PyExc_TypeError, "sequence items must be integers");
                return NULL;
            }
            next_excl = PYINT_AS_LONG(tempfd);
            excl_index++;
        }
        if (i == next_excl) {
            continue;
        }

        /* close */
        if (close(i) == 0) {
            closed++;
            fprintf(stderr, "closed %d\n", i);
        }
    }

    return PYINT_FromLong(closed);
}


static PyObject *
pysethostname(PyObject *self, PyObject *args) {
    char *hostname;
    int len;

    if (!PyArg_ParseTuple(args, "s#", &hostname, &len)) {
        return NULL;
    }

    if(sethostname(hostname, len)) {
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }

    Py_RETURN_NONE;
}


static PyMethodDef OSMethods[] = {
    { "_close_fds", py_close_fds, METH_VARARGS,
        "Close all file descriptors" },
    { "sethostname", pysethostname, METH_VARARGS,
        "Set the system hostname" },
    { NULL }
};


PYMODULE_DECLARE(osutil, "jobmaster.osutil",
        "linux extras for python",
        OSMethods);

/* vim: set sts=4 sw=4 expandtab : */
