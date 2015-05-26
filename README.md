SAS App Engine Jobmaster
========================

Overview
--------

The jobmaster is the daemon responsible for managing image builds within an App
Engine. It connects to the *rMake Message Bus*, and receives jobs from the *MCP
Dispatcher* via that bus. Each job is handled by a child process running in a
Linux cgroup namespace, and the finished product is uploaded back to *mint* via
its REST API. A jobmaster is included by default as part of the App Engine
Appliance, but they can also be run on additional nodes if more capacity is
needed.

Inside the container, *jobslave* is the process responsible for actually
creating the image. Jobmaster sets up the environment, monitors the process,
and proxies API requests back to mint.

Jobs are isolated in a cgroup namespace in order to limit access to the host
system from within the image build. Access to system block and character
devices is blocked, network access is limited to only jobmaster's own API
proxy, and filesystem writes are also blocked. At the start of the job, the
jobmaster estimates how much scratch space will be needed to run the build and
allocates it from a preconfigured LVM volume group. This volume is
quick-formatted and mounted as /tmp inside the container. Also mounted is a
read-only bindmount of the host's root filesystem and a /dev filesystem with
only a small set of permitted devices.

There is also a builtin *template generator* used to post-process and cache
anaconda-templates for ISO builds. It is invoked by the jobslave using the same
proxy API, and the resulting file contents are fetched via a bind mount in the
cgroup namespace.

Debugging
---------

To debug jobmaster or jobslave issues: stop the jobmaster service, add
'debugMode True' to /srv/rbuilder/jobmaster/config, and run "jobmaster -n" as
root. Any breakpoints inside the jobmaster or jobslave will be accessible from
the terminal.
