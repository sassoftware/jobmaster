/*
 * Copyright (c) 2007 rPath, Inc.
 *
 * All rights reserved.
 *
 * LD_PRELOAD utility to skip past some potentially dangerous functions
 * that do not need to be called when running certain programs (authconfig)
 * against a chroot.
 *
 */

#include <sys/types.h>

int setdomainname(const char *name, size_t len) {
    return 0;
}
