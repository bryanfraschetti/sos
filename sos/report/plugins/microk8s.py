# Copyright (C) 2023 Canonical Ltd.,
#                    David Negreira <david.negreira@canonical.com>

# This file is part of the sos project: https://github.com/sosreport/sos
#
# This copyrighted material is made available to anyone wishing to use,
# modify, copy, or redistribute it subject to the terms and conditions of
# version 2 of the GNU General Public License.
#
# See the LICENSE file in the source distribution for further information.

import re
from sos.report.plugins import Plugin, UbuntuPlugin


class Microk8s(Plugin, UbuntuPlugin):
    """The Microk8s plugin collects the current status of the microk8s
    snap on a Ubuntu machine.

    It will collect logs from journald related to the snap.microk8s
    units as well as run microk8s commands to retrieve the configuration,
    status, version and loaded plugins.
    """

    short_desc = 'The lightweight Kubernetes'
    plugin_name = "microk8s"
    profiles = ('container',)

    packages = ('microk8s',)

    microk8s_cmd = "microk8s"

    dqlite_bin = "/snap/microk8s/current/bin/dqlite"
    microk8s_data_dir = "/var/snap/microk8s/current/var/kubernetes/backend"
    cert = f"{microk8s_data_dir}/cluster.crt"
    key = f"{microk8s_data_dir}/cluster.key"
    servers = f"{microk8s_data_dir}/cluster.yaml"
    dqlite_cmd = f"{dqlite_bin} -c {cert} -k {key} -s file://{servers} k8s"

    try:
        with open(servers, 'r', encoding='utf-8') as cluster_definition:
            cluster = cluster_definition.read()
            nodes = re.findall(r'Address:\s*(\d+\.\d+\.\d+\.\d+:\d+)', cluster)

    except Exception:
        pass

    def setup(self):
        self.add_journal(units="snap.microk8s.*")

        microk8s_subcmds = [
            'addons repo list',
            'config',
            'ctr plugins ls',
            'ctr plugins ls -d',
            'status',
            'version'
        ]

        self.add_cmd_output([
            f"{self.microk8s_cmd} {subcmd}" for subcmd in microk8s_subcmds
        ])

        dqlite_subcmds = [
            f"\".describe {node}\" -f json" for node in self.nodes
        ]

        dqlite_subcmds.extend([
            "\".cluster\"",
            "\".cluster\" -f json",
            "\".leader\""
        ])

        self.add_copy_spec([
            "/var/snap/microk8s/current/credentials/client.config",
            "/var/snap/microk8s/current/var/kubernetes/backend/info.yaml",
            "/var/snap/microk8s/current/var/kubernetes/backend/cluster.yaml",
            "/var/snap/microk8s/current/var/kubernetes/backend/failure-domain",
        ])

        for subcmd in dqlite_subcmds:
            self.add_cmd_output(
                f"{self.dqlite_cmd} {subcmd}",
                suggest_filename=f"dqlite_{subcmd}"
            )

        self.add_cmd_output([
            f"ls -al {self.microk8s_data_dir}",
        ])

    def postproc(self):
        rsub = r'(certificate-authority-data:|token:)\s.*'
        self.do_cmd_output_sub(self.microk8s_cmd, rsub, r'\1 "**********"')

        protect_keys = [
            "certificate-authority-data",
            "client-certificate-data",
            "client-key-data",
            "token",
        ]

        key_regex = fr'(^\s*({"|".join(protect_keys)})\s*:\s*)(.*)'

        self.do_path_regex_sub(
            "/var/snap/microk8s/current/credentials/client.config",
            key_regex, r"\1*********"
        )

# vim: set et ts=4 sw=4
