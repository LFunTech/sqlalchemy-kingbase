#!/usr/bin/env python
# -*-coding:utf-8-*-
"""
Author : Min
date   : 2019/12/12
"""
import re

from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2


class PGDialect_kingbase(PGDialect_psycopg2):
    name = "kingbase"

    # driver = "kingbase"

    def _get_server_version_info(self, connection):
        return (9, 3)
        v = connection.execute("select version()").scalar()
        m = re.match(
            r".*(?:PostgreSQL|EnterpriseDB|Kingbase) "
            r"(\d+)\.?(\d+)?(?:\.(\d+))?(?:\.\d+)?(?:devel|beta)?",
            v,
        )
        if not m:
            raise AssertionError(
                "Could not determine version from string '%s'" % v
            )
        return tuple([int(x) for x in m.group(1, 2, 3) if x is not None])


dialect = PGDialect_kingbase
