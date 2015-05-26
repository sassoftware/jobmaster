/*
 * Copyright (c) SAS Institute Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#include <Python.h>

#include <sched.h>
#include <signal.h>
#include <unistd.h>

#include "pycompat.h"


/* Copied clone flags for old userspace-kernel-headers */
#define CLONE_NEWUTS    0x04000000  /* New utsname group? */
#define CLONE_NEWIPC    0x08000000  /* New ipcs */
#define CLONE_NEWUSER   0x10000000  /* New user namespace */
#define CLONE_NEWPID    0x20000000  /* New pid namespace */
#define CLONE_NEWNET    0x40000000  /* New network namespace */


struct clone_arg {
    PyObject *callback, *args;
};


static int
clone_run(void *arg) {
    struct clone_arg *clone_arg = arg;
    PyOS_AfterFork();

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
    int i;
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

    /* There seems to be some sort of kernel race that returns EEXIST, so
     * retry a few times to avoid an unnecessary crash.
     *
     * This was observed on 2.6.29.6-0.7.smp.gcc4.1.x86_64
     */
    for (i = 0; i < 5; i++) {
        pid = clone(clone_run, stack, flags, &clone_arg);
        if (pid >= 0 || errno != EEXIST)
            break;
    }

    if (pid < 0) {
        Py_DECREF(callback);
        Py_DECREF(newargs);
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }
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
