Snap Packaging for sosreport
============================

Snap packaging for sosreport is done using `snapcraft
<https://snapcraft.io/>`__ tool that facilitates the management of the
build in a reproducible containerized environment.

1. `Snap: Packaging a New Release <#packaging-a-new-release>`__
2. `Snap: Publishing a Snap <#publishing-a-snap>`__
3. `Snap: Rebuilding a Snap for Security Updates
<#rebuilding-a-snap-for-security-updates>`__

Packaging a New Release
-----------------------

First clone the repo and move into the directory

::

   git clone git@github.com:sosreport/sos.git
   cd sos

Now fetch the latest release tags

::

   git fetch --all

Next we need to checkout the tag that we would like to package as a
snap, for example 4.12.0

::

   git checkout 4.12.0

Run the `snapcraft pack` command to build and package the snap.

::

   snapcraft pack --use-lxd

This will create a snap artifact based on the architecture on which it
was built, such as `sosreport_4.12.0_amd64.snap`

At this stage, the snap can be installed locally for testing using the
following command:

::

   sudo snap install --dangerous sosreport_4.12.0_amd64.snap

Publishing a Snap
-----------------

Before we can publish the snap, we must authenticate against the Snap
Store. You can log in using the following command, which will
subsequently prompt you for your credentials.

::

   snapcraft login

You will need to have an Ubuntu One account registered with the snap
store that has the correct permissions.  The login command will prompt
you for your email, password, and two-factor TOTP code for
authentication.

After ensuring that the snap works as expected, it can be published to
the Snap Store for wider distribution. This can be done using the
`snapcraft upload` command. Typically, snaps are first released to the
`latest/candidate` channel for testing before being promoted to the
`stable` channel.

::

   snapcraft upload sosreport_4.12.0_amd64.snap --release=latest/candidate

The snap should be built and uploaded on both amd64 and arm64
architectures to ensure feature parity.

Rebuilding a Snap for Security Updates
--------------------------------------

The process of packaging and publishing a snap is done when new released
are available and when Ubuntu Security Notices (USNs) are issued which
indicates that one of the dependencies against which the current snap
release was built against has a security vulnerability that needs to be
addressed. As snaps are statically linked, this is resolved with a
no-change rebuild of the snap. Thus, the same steps shown above are
applicable.
