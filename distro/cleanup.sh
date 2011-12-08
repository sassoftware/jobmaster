#!/bin/bash
/etc/init.d/jobmaster cleanroots
/usr/sbin/tmpwatch 2160 /srv/rbuilder/jobmaster/anaconda-templates
