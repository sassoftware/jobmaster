/*
 * Copyright (c) 2009 rPath, Inc.
 *
 * All rights reserved.
 */

#include <Python.h>

#include <sched.h>
#include <signal.h>
#include <linux/sched.h>

#include "pycompat.h"


struct clone_arg {
    PyObject *callback, *args;
};


static int
clone_run(void *arg) {
    struct clone_arg *clone_arg = arg;

    PyObject *rv = PyObject_Call(clone_arg->callback, clone_arg->args, NULL);

    if (rv == NULL) {
        return 15;
    } else if (PYINT_CHECK_EITHER(rv)) {
        return (int)PyLong_AsLong(rv);
    } else if (rv == Py_None) {
        return 0;
    } else {
        return 1;
    }
}


static PyObject *
pyclone(PyObject *self, PyObject *args, PyObject *kwargs) {
    long stack_size = sysconf(_SC_PAGESIZE);
    void *stack = alloca(stack_size) + stack_size;
    PyObject *callback, *newargs;
    struct clone_arg clone_arg;
    int new_uts = 0, new_ipc = 0, new_user = 0, new_pid = 0, new_net = 0;
    char *kwnames[] = {"callback", "args", "new_uts", "new_ipc", "new_user",
        "new_pid", "new_net", NULL};
    int flags = SIGCHLD | CLONE_NEWNS;
    pid_t pid;

    /* parse and check args */
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|iiiii", kwnames,
                &callback, &newargs, &new_uts, &new_ipc, &new_user, &new_pid,
                &new_net))
        return NULL;

    if (!PyCallable_Check(callback)) {
        PyErr_SetString(PyExc_TypeError,
                "first argument must be a callable object");
        return NULL;
    }

    if (!PyTuple_Check(newargs)) {
        PyErr_SetString(PyExc_TypeError, "second argument must be a tuple "
                "of arguments for the callback function");
        return NULL;
    }

    /* set flags from args and clone */
    if (new_uts)
        flags |= CLONE_NEWUTS;
    if (new_ipc)
        flags |= CLONE_NEWIPC;
    if (new_user)
        flags |= CLONE_NEWUSER;
    if (new_pid)
        flags |= CLONE_NEWPID;
    if (new_net)
        flags |= CLONE_NEWNET;

    Py_INCREF(callback);
    Py_INCREF(newargs);
    clone_arg.callback = callback;
    clone_arg.args = newargs;

    pid = clone(clone_run, stack, flags, &clone_arg);
    if (pid < 0) {
        Py_DECREF(callback);
        Py_DECREF(newargs);
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }
    PyOS_AfterFork();
    Py_DECREF(callback);
    Py_DECREF(newargs);

    return PYINT_FromLong(pid);
}


static PyObject *
pyunshare(PyObject *self, PyObject *args, PyObject *kwargs) {
    int new_uts = 0, new_ipc = 0, new_net = 0;
    char *kwnames[] = {"new_uts", "new_ipc", "new_net", NULL};
    int flags = CLONE_NEWNS;

    /* parse and check args */
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|iii", kwnames,
                &new_uts, &new_ipc, &new_net))
        return NULL;

    /* set flags from args and clone */
    if (new_uts)
        flags |= CLONE_NEWUTS;
    if (new_ipc)
        flags |= CLONE_NEWIPC;
    if (new_net)
        flags |= CLONE_NEWNET;

    if (unshare(flags)) {
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }

    Py_RETURN_NONE;
}


static PyMethodDef NSMethods[] = {
    { "clone", (PyCFunction)pyclone, METH_VARARGS | METH_KEYWORDS,
        "invoke the given callback in a new process and namespace" },
    { "unshare", (PyCFunction)pyunshare, METH_VARARGS | METH_KEYWORDS,
        "create new namespaces in the current process" },
    { NULL }
};


PYMODULE_DECLARE(linuxns, "jobmaster.linuxns",
        "linux namespace support for python",
        NSMethods);

/* vim: set sts=4 sw=4 expandtab : */
