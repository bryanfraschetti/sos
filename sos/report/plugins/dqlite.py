# Copyright (C) 2023 Canonical Ltd.,
#                    Bryan Fraschetti <bryan.fraschetti@canonical.com>

# This file is part of the sos project: https://github.com/sosreport/sos
#
# This copyrighted material is made available to anyone wishing to use,
# modify, copy, or redistribute it subject to the terms and conditions of
# version 2 of the GNU General Public License.
#
# See the LICENSE file in the source distribution for further information.

import json
import re
from sos.report.plugins import Plugin, IndependentPlugin, SoSPredicate


class dqlite(Plugin, IndependentPlugin):
    """Dqlite (distributed SQLite) extends SQLite across a cluster of machines,
    with automatic failover and high-availability to keep your application
    running. It uses C-Raft, an optimised Raft implementation in C, to gain
    high-performance transactional consensus and fault tolerance while
    preserving SQlite’s outstanding efficiency and tiny footprint.
    """

    short_desc = 'Distributed embedded sqlite database library'
    plugin_name = "dqlite"
    profiles = ('storage', 'cluster',)

    packages = ('microk8s', 'microceph', 'microovn', 'microcloud', 'lxd',)

    def generate_sql_cmd(self, cfg, query_entry):
        db = query_entry.get("db", "local")
        sql_cmd = (
            f"{cfg.get('sql_cmd')} {db}" if cfg.get("pkg") == "lxd"
            else cfg.get('sql_cmd')
        )
        query = query_entry.get("query")

        return f"{sql_cmd} {json.dumps(query)}"

    def generate_socket_cmd(self, cfg, query_entry):
        db = query_entry.get("db", "local")
        sock = cfg.get("socket")
        endpoint = cfg.get("socket_endpoint")
        header = "Content-Type: application/json"
        curl_cmd = f"""
            curl -s --unix-socket {sock} \
                -X POST {endpoint} \
                -H \"{header}\" \
                -d
            """

        query = query_entry.get("query")
        query_json = json.dumps(
                {"query": query, "database": db} if cfg.get("pkg") == "lxd"
                else {"query": query}
            )
        
        return f"{curl_cmd} '{query_json}'"

    def run_batch_dqlite_queries(self, cfg):
        pkg = cfg.get("pkg")
        predicate = cfg.get("predicate")
        queries = cfg.get("queries")

        for query_entry in queries:
            table_name = query_entry.get("table")

            sql_cmd = self.generate_sql_cmd(cfg, query_entry)
            self.add_cmd_output(
                sql_cmd,
                suggest_filename=f"{pkg}_sql_{table_name}",
                subdir=pkg,
                pred=predicate
            )

            socket_cmd = self.generate_socket_cmd(cfg, query_entry)
            self.add_cmd_output(
                socket_cmd,
                suggest_filename=f"{pkg}_curl_{table_name}",
                subdir=pkg,
                pred=predicate,
            )

    def base_collection(self, cfg):
        pkg = cfg.get("pkg")
        db_path = cfg.get("db_path")

        # Check for inconsistent dqlite db intervals
        self.add_dir_listing(
            db_path,
            suggest_filename=f"ls_{pkg}_dqlite_dir",
            subdir=pkg,
        )

        # All dqlite consumers except lxd have info.yaml and cluster.yaml
        self.add_copy_spec(
            [
                f"{db_path}/info.yaml",
                f"{db_path}/cluster.yaml",
                f"{db_path}/../daemon.yaml",  # Not expected for microk8s
            ]
        )

        # Determine queries to run based on installed package
        if pkg == "microk8s":
            # At this time, microk8s is rather divergent w.r.t querying
            return

        cfg["queries"].extend([{
            "query": "SELECT * FROM sqlite_master WHERE type=\"table\";",
            "table": "schema",  # Table name will be used in filename
        },])

        if pkg not in ("microcloud",):
            cfg["queries"].extend([{
                "query": (
                    "SELECT * FROM config WHERE NOT ( "
                    "key LIKE \"%keyring%\" OR "
                    "key LIKE \"%ca_cert%\" OR "
                    "key LIKE \"%ca_key%\" );"
                ),
                "table": "config",
            },])

        if pkg in ("microceph", "microovn",):
            cfg["queries"].extend([{
                "query": "SELECT * FROM services;",
                "table": "services",
            },])

        self.run_batch_dqlite_queries(cfg)

    def microceph_collection(self, cfg):
        cfg["queries"].extend([
            {
                "query": "SELECT * FROM disks;",
                "table": "disks",
            },
            {
                "query": "SELECT * FROM client_config;",
                "table": "client_config",
            },
            {
                "query": "SELECT * FROM remote;",
                "table": "remote",
            },
        ])

    def microovn_collection(self, cfg):
        """ Currently empty, as nothing currently necessitates unique microovn
        dqlite collection. A no-op, present for future extension
        """

    def microcloud_collection(self, cfg):
        """ Currently empty, as nothing currently necessitates unique
        microcloud dqlite collection. A no-op, present for future extension
        """

    def microk8s_collection(self, cfg):
        pkg = cfg.get("pkg")
        db_path = cfg.get("db_path")
        predicate = cfg.get("predicate")

        self.add_copy_spec([
            "/var/snap/microk8s/current/credentials/client.config",
            f"{db_path}/failure-domain",
        ])

        dqlite_bin = "/snap/microk8s/current/bin/dqlite"
        cert = f"{db_path}/cluster.crt"
        key = f"{db_path}/cluster.key"
        servers = f"{db_path}/cluster.yaml"
        dqlite_cmd = f"{dqlite_bin} -c {cert} -k {key} -s file://{servers} k8s"

        queries = [
            "\".cluster\"",
            "\".cluster\" -f json",
            "\".leader\"",
        ]

        suggested_names = [
            f"{pkg}_dqlite_{query}" for query in queries
        ]

        try:
            with open(servers, 'r', encoding='utf-8') as cluster_definition:
                cluster = cluster_definition.read()
                nodes = re.findall(
                    r'Address:\s*(\d+\.\d+\.\d+\.\d+:\d+)', cluster
                )

                for node in nodes:
                    queries.append(
                        f"\".describe {node}\" -f json"
                    )
                    suggested_names.append(
                        f"{pkg}_dqlite_.describe_{node}"
                    )
        except Exception as e:
            self.add_alert(f"Failed to parse {servers}: {e}")

        for query, suggested_name in zip(queries, suggested_names):
            self.add_cmd_output(
                f"{dqlite_cmd} {query}",
                suggest_filename=suggested_name,
                subdir=pkg,
                pred=predicate,
            )

    def lxd_collection(self, cfg):
        cfg["queries"].extend([
            {
                "query": "SELECT * FROM nodes;",
                "table": "nodes",
                "db": "global",
            },
            {
                "query": "SELECT * FROM nodes_roles;",
                "table": "nodes_roles",
                "db": "global",
            },
            {
                "query": "SELECT * FROM raft_nodes;",
                "table": "raft_nodes",
                "db": "local",
            },
        ])

    def generate_microcluster_pkg_map(self, pkg, collection):
        sql_cmd = f"{pkg} sql" if pkg == "microcloud" else f"{pkg} cluster sql"

        return {
            "pkg": pkg,
            "db_path": f"/var/snap/{pkg}/common/state/database",
            "socket": f"/var/snap/{pkg}/common/state/control.socket",
            "socket_endpoint": f"{pkg}/core/internal/sql",
            "sql_cmd": sql_cmd,
            "collection": collection,
            "predicate": None,
            "queries": [
                {
                    "query": (
                        "SELECT id, name, expiry_date "
                        "FROM core_token_records;"
                    ),
                    "table": "token_records",
                },
                {
                    "query": (
                        "SELECT id, name, address, schema_internal, "
                        "schema_external, heartbeat, role, api_extensions "
                        "FROM core_cluster_members;"
                    ),
                    "table": "core_cluster_members",
                },
            ]
        }

    def setup(self):
        lxd_pred = (
            SoSPredicate(
                self,
                services=['snap.lxd.daemon'],
                required={'services': 'all'}
            ) if self.is_snap_installed("lxd") else
            SoSPredicate(
                self,
                services=['lxd'],
                required={'services': 'all'}
            )
        )

        packages = {
            "microceph": self.generate_microcluster_pkg_map(
                "microceph",
                self.microceph_collection
            ),
            "microovn": self.generate_microcluster_pkg_map(
                "microovn",
                self.microovn_collection
            ),
            "microcloud": self.generate_microcluster_pkg_map(
                "microcloud",
                self.microcloud_collection
            ),
            "microk8s": {
                "pkg": "microk8s",
                "db_path": "/var/snap/microk8s/current/var/kubernetes/backend",
                "socket": "/var/snap/microk8s/current/var/kubernetes/backend/"
                "kine.sock:12379",
                "sql_cmd": None,
                "socket_endpoint": "microk8s/core/internal/sql",
                "collection": self.microk8s_collection,
                "predicate": None,
                "queries": [],
            },
            "lxd": {
                "pkg": "lxd",
                "db_path": "/var/snap/lxd/common/lxd/database/global",
                "socket": "/var/snap/lxd/common/lxd/unix.socket",
                "sql_cmd": "lxd sql",
                "socket_endpoint": "lxd/internal/sql",
                "collection": self.lxd_collection,
                "predicate": lxd_pred,
                "queries": [],
            }
        }

        for pkg, config in packages.items():
            if self.is_installed(pkg):
                config.get("collection")(config)
                self.base_collection(config)

    def postproc(self):
        # Remove microk8s certificate data from config file
        protect_keys = [
            "certificate-authority-data",
            "client-certificate-data",
            "client-key-data",
        ]

        regexp = fr"^\s*(#?\s*({'|'.join(protect_keys)}):\s*)(\S.*)"

        self.do_file_sub(
            "/var/snap/microk8s/current/credentials/client.config",
            regexp,
            r"\1 ******",
        )

# vim: set et ts=4 sw=4 :
