This directory contains disclosure of BaseOS packages of RHEL/UBI/CentOS.

If you would like to perform a build and install of these packages, please follow the instructions
listed in below sections.

1. Build:
Any commands that need to be executed for the disclosure should be executed on
RHEL derived systems.
You can pull UBI container image from registry.access.redhat.com/ubi7/ubi-minimal:latest ,
or follow RedHat document to get UBI image: https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux_atomic_host/7/html/getting_started_with_containers/using_red_hat_universal_base_images_standard_minimal_and_runtimes#get_ubi_images .
After getting a UBI image, you can use docker/containerd/podman to start a UBI
container.

To build a package, copy PackageName.src.rpm into the UBI container.

The following steps are run inside UBI container.

Install rpmbuild:

yum install -y rpmbuild

Install other necessary build packages like gcc, make.

Build the package:

rpmbuild --rebuild PackageName.src.rpm

You will find PackageName.rpm file in /root/rpmbuild/RPMS/x86_64/ directory.
Use docker cp or similar command in other container runtimes to copy the rpms
from container to outside of container.

2: Install:

rpm -ivh PackageName.rpm

Or:

rpm -Uvh PackageName.rpm

Or:

yum install PackageName.rpm
