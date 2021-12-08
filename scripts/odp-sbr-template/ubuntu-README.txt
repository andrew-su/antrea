This directory contains disclosure of BaseOS packages of Ubuntu.

If you would like to perform a build and install of these packages, please follow the instructions
listed in below sections.

1. Build:
Any commands that need to be executed for the disclosure should be executed on
a "Ubuntu" system.
You can just install the Ubuntu system from image/iso downloaded from https://cloud-images.ubuntu.com/.

Install the dpkg-dev and build-essentials packages. Find the package you want
to build in the "ubuntu" directory. Usually it's PackageName.tar

Extract PackageName.tar:

tar -xf PackageName.tar

You will get PackageName.dsc and some other source tarballs. Read PackageName.dsc
conetent and install all packages mentioned in build dependencies.

Extract the source code:

dpkg-source -x PackageName.dsc

It will extract the source code into a directory with name PackageName. Enter
the newly created directory:

cd PackageName

Build the package:

dpkg-buildpackage -rfakeroot -b

You will find PackageName.deb file in ../ directory.

2: Install:

dpkg -i PackageName.deb

Or:

apt install PackageName.deb
