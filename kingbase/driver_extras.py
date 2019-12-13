#!/usr/bin/env python
# -*-coding:utf-8-*-
"""
Author : Min
date   : 2019/12/13
"""

from psycopg2 import extras
from psycopg2.extras import _solve_conn_curs
from psycopg2 import extensions as _ext


class HstoreAdapter(extras.HstoreAdapter):

    @classmethod
    def get_oids(self, conn_or_curs):
        """Return the lists of OID of the hstore and hstore[] types.
        """
        conn, curs = _solve_conn_curs(conn_or_curs)

        # Store the transaction status of the connection to revert it after use
        conn_status = conn.status

        # column typarray not available before PG 8.3
        typarray = conn.info.server_version >= 80300 and "typarray" or "NULL"

        rv0, rv1 = [], []

        # get the oid for the hstore
        curs.execute("""\
    SELECT t.oid, %s
    FROM sys_type t JOIN sys_namespace ns
        ON typnamespace = ns.oid
    WHERE typname = 'hstore';
    """ % typarray)
        for oids in curs:
            rv0.append(oids[0])
            rv1.append(oids[1])

        # revert the status of the connection as before the command
        if (conn_status != _ext.STATUS_IN_TRANSACTION
                and not conn.autocommit):
            conn.rollback()

        return tuple(rv0), tuple(rv1)
