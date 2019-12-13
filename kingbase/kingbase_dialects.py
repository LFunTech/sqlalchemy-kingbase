#!/usr/bin/env python
# -*-coding:utf-8-*-
"""
Author : Min
date   : 2019/12/12
"""
import re
from collections import defaultdict

from sqlalchemy import exc
from sqlalchemy import sql
from sqlalchemy import util
from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
from sqlalchemy.engine import reflection
from sqlalchemy.sql import sqltypes

from kingbase.driver_extras import HstoreAdapter


class PGDialect_kingbase(PGDialect_psycopg2):
    name = "kingbase"

    @util.memoized_instancemethod
    def _hstore_oids(self, conn):
        if self.psycopg2_version >= self.FEATURE_VERSION_MAP["hstore_adapter"]:
            # extras = self._psycopg2_extras()
            oids = HstoreAdapter.get_oids(conn)
            if oids is not None and oids[0]:
                return oids[0:2]
        return None

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

    @reflection.cache
    def get_table_oid(self, connection, table_name, schema=None, **kw):
        """Fetch the oid for schema.table_name.

        Several reflection methods require the table oid.  The idea for using
        this method is that it can be fetched one time and cached for
        subsequent calls.

        """
        table_oid = None
        if schema is not None:
            schema_where_clause = "n.nspname = :schema"
        else:
            schema_where_clause = "sys_catalog.sys_table_is_visible(c.oid)"
        query = (
                """
                SELECT c.oid
                FROM sys_catalog.sys_class c
                LEFT JOIN sys_catalog.sys_namespace n ON n.oid = c.relnamespace
                WHERE (%s)
                AND c.relname = :table_name AND c.relkind in
                ('r', 'v', 'm', 'f', 'p')
            """
                % schema_where_clause
        )
        # Since we're binding to unicode, table_name and schema_name must be
        # unicode.
        table_name = util.text_type(table_name)
        if schema is not None:
            schema = util.text_type(schema)
        s = sql.text(query).bindparams(table_name=sqltypes.Unicode)
        s = s.columns(oid=sqltypes.Integer)
        if schema:
            s = s.bindparams(sql.bindparam("schema", type_=sqltypes.Unicode))
        c = connection.execute(s, table_name=table_name, schema=schema)
        table_oid = c.scalar()
        if table_oid is None:
            raise exc.NoSuchTableError(table_name)
        return table_oid

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        result = connection.execute(
            sql.text(
                "SELECT nspname FROM sys_namespace "
                "WHERE nspname NOT LIKE 'sys_%' "
                "ORDER BY nspname"
            ).columns(nspname=sqltypes.Unicode)
        )
        return [name for name, in result]

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        result = connection.execute(
            sql.text(
                "SELECT c.relname FROM sys_class c "
                "JOIN sys_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = :schema AND c.relkind in ('r', 'p')"
            ).columns(relname=sqltypes.Unicode),
            schema=schema if schema is not None else self.default_schema_name,
        )
        return [name for name, in result]

    @reflection.cache
    def _get_foreign_table_names(self, connection, schema=None, **kw):
        result = connection.execute(
            sql.text(
                "SELECT c.relname FROM sys_class c "
                "JOIN sys_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = :schema AND c.relkind = 'f'"
            ).columns(relname=sqltypes.Unicode),
            schema=schema if schema is not None else self.default_schema_name,
        )
        return [name for name, in result]

    @reflection.cache
    def get_view_names(
            self, connection, schema=None, include=("plain", "materialized"), **kw
    ):

        include_kind = {"plain": "v", "materialized": "m"}
        try:
            kinds = [include_kind[i] for i in util.to_list(include)]
        except KeyError:
            raise ValueError(
                "include %r unknown, needs to be a sequence containing "
                "one or both of 'plain' and 'materialized'" % (include,)
            )
        if not kinds:
            raise ValueError(
                "empty include, needs to be a sequence containing "
                "one or both of 'plain' and 'materialized'"
            )

        result = connection.execute(
            sql.text(
                "SELECT c.relname FROM sys_class c "
                "JOIN sys_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = :schema AND c.relkind IN (%s)"
                % (", ".join("'%s'" % elem for elem in kinds))
            ).columns(relname=sqltypes.Unicode),
            schema=schema if schema is not None else self.default_schema_name,
        )
        return [name for name, in result]

    @reflection.cache
    def get_view_definition(self, connection, view_name, schema=None, **kw):
        view_def = connection.scalar(
            sql.text(
                "SELECT sys_get_viewdef(c.oid) view_def FROM sys_class c "
                "JOIN sys_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = :schema AND c.relname = :view_name "
                "AND c.relkind IN ('v', 'm')"
            ).columns(view_def=sqltypes.Unicode),
            schema=schema if schema is not None else self.default_schema_name,
            view_name=view_name,
        )
        return view_def

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):

        table_oid = self.get_table_oid(
            connection, table_name, schema, info_cache=kw.get("info_cache")
        )
        SQL_COLS = """
            SELECT a.attname,
              sys_catalog.format_type(a.atttypid, a.atttypmod),
              (SELECT sys_catalog.sys_get_expr(d.adbin, d.adrelid)
                FROM sys_catalog.sys_attrdef d
               WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum
               AND a.atthasdef)
              AS DEFAULT,
              a.attnotnull, a.attnum, a.attrelid as table_oid,
              pgd.description as comment
            FROM sys_catalog.sys_attribute a
            LEFT JOIN sys_catalog.sys_description pgd ON (
                pgd.objoid = a.attrelid AND pgd.objsubid = a.attnum)
            WHERE a.attrelid = :table_oid
            AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum
        """
        s = (
            sql.text(SQL_COLS)
                .bindparams(sql.bindparam("table_oid", type_=sqltypes.Integer))
                .columns(attname=sqltypes.Unicode, default=sqltypes.Unicode)
        )
        c = connection.execute(s, table_oid=table_oid)
        rows = c.fetchall()

        # dictionary with (name, ) if default search path or (schema, name)
        # as keys
        domains = self._load_domains(connection)

        # dictionary with (name, ) if default search path or (schema, name)
        # as keys
        enums = dict(
            ((rec["name"],), rec)
            if rec["visible"]
            else ((rec["schema"], rec["name"]), rec)
            for rec in self._load_enums(connection, schema="*")
        )

        # format columns
        columns = []

        for (
                name,
                format_type,
                default_,
                notnull,
                attnum,
                table_oid,
                comment,
        ) in rows:
            column_info = self._get_column_info(
                name,
                format_type.lower(),
                default_,
                notnull,
                domains,
                enums,
                schema,
                comment,
            )
            columns.append(column_info)
        return columns

    def do_recover_twophase(self, connection):
        resultset = connection.execute(
            sql.text("SELECT gid FROM sys_prepared_xacts")
        )
        return [row[0] for row in resultset]

    def _get_default_schema_name(self, connection):
        return connection.scalar("select current_schema()")

    def has_schema(self, connection, schema):
        query = (
            "select nspname from sys_namespace " "where lower(nspname)=:schema"
        )
        cursor = connection.execute(
            sql.text(query).bindparams(
                sql.bindparam(
                    "schema",
                    util.text_type(schema.lower()),
                    type_=sqltypes.Unicode,
                )
            )
        )

        return bool(cursor.first())

    def has_table(self, connection, table_name, schema=None):
        # seems like case gets folded in sys_classsqlalchemy.
        if schema is None:
            cursor = connection.execute(
                sql.text(
                    "select relname from sys_class c join sys_namespace n on "
                    "n.oid=c.relnamespace where "
                    "sys_catalog.sys_table_is_visible(c.oid) "
                    "and relname=:name"
                ).bindparams(
                    sql.bindparam(
                        "name",
                        util.text_type(table_name),
                        type_=sqltypes.Unicode,
                    )
                )
            )
        else:
            cursor = connection.execute(
                sql.text(
                    "select relname from sys_class c join sys_namespace n on "
                    "n.oid=c.relnamespace where n.nspname=:schema and "
                    "relname=:name"
                ).bindparams(
                    sql.bindparam(
                        "name",
                        util.text_type(table_name),
                        type_=sqltypes.Unicode,
                    ),
                    sql.bindparam(
                        "schema",
                        util.text_type(schema),
                        type_=sqltypes.Unicode,
                    ),
                )
            )
        return bool(cursor.first())

    def has_sequence(self, connection, sequence_name, schema=None):
        if schema is None:
            cursor = connection.execute(
                sql.text(
                    "SELECT relname FROM sys_class c join sys_namespace n on "
                    "n.oid=c.relnamespace where relkind='S' and "
                    "n.nspname=current_schema() "
                    "and relname=:name"
                ).bindparams(
                    sql.bindparam(
                        "name",
                        util.text_type(sequence_name),
                        type_=sqltypes.Unicode,
                    )
                )
            )
        else:
            cursor = connection.execute(
                sql.text(
                    "SELECT relname FROM sys_class c join sys_namespace n on "
                    "n.oid=c.relnamespace where relkind='S' and "
                    "n.nspname=:schema and relname=:name"
                ).bindparams(
                    sql.bindparam(
                        "name",
                        util.text_type(sequence_name),
                        type_=sqltypes.Unicode,
                    ),
                    sql.bindparam(
                        "schema",
                        util.text_type(schema),
                        type_=sqltypes.Unicode,
                    ),
                )
            )

        return bool(cursor.first())

    def has_type(self, connection, type_name, schema=None):
        if schema is not None:
            query = """
            SELECT EXISTS (
                SELECT * FROM sys_catalog.sys_type t, sys_catalog.sys_namespace n
                WHERE t.typnamespace = n.oid
                AND t.typname = :typname
                AND n.nspname = :nspname
                )
                """
            query = sql.text(query)
        else:
            query = """
            SELECT EXISTS (
                SELECT * FROM sys_catalog.sys_type t
                WHERE t.typname = :typname
                AND sys_type_is_visible(t.oid)
                )
                """
            query = sql.text(query)
        query = query.bindparams(
            sql.bindparam(
                "typname", util.text_type(type_name), type_=sqltypes.Unicode
            )
        )
        if schema is not None:
            query = query.bindparams(
                sql.bindparam(
                    "nspname", util.text_type(schema), type_=sqltypes.Unicode
                )
            )
        cursor = connection.execute(query)
        return bool(cursor.scalar())

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        table_oid = self.get_table_oid(
            connection, table_name, schema, info_cache=kw.get("info_cache")
        )

        if self.server_version_info < (8, 4):
            PK_SQL = """
                SELECT a.attname
                FROM
                    sys_class t
                    join sys_index ix on t.oid = ix.indrelid
                    join sys_attribute a
                        on t.oid=a.attrelid AND %s
                 WHERE
                  t.oid = :table_oid and ix.indisprimary = 't'
                ORDER BY a.attnum
            """ % self._sys_index_any(
                "a.attnum", "ix.indkey"
            )

        else:
            # unnest() and generate_subscripts() both introduced in
            # version 8.4
            PK_SQL = """
                SELECT a.attname
                FROM sys_attribute a JOIN (
                    SELECT unnest(ix.indkey) attnum,
                           generate_subscripts(ix.indkey, 1) ord
                    FROM sys_index ix
                    WHERE ix.indrelid = :table_oid AND ix.indisprimary
                    ) k ON a.attnum=k.attnum
                WHERE a.attrelid = :table_oid
                ORDER BY k.ord
            """
        t = sql.text(PK_SQL).columns(attname=sqltypes.Unicode)
        c = connection.execute(t, table_oid=table_oid)
        cols = [r[0] for r in c.fetchall()]

        PK_CONS_SQL = """
        SELECT conname
           FROM  sys_catalog.sys_constraint r
           WHERE r.conrelid = :table_oid AND r.contype = 'p'
           ORDER BY 1
        """
        t = sql.text(PK_CONS_SQL).columns(conname=sqltypes.Unicode)
        c = connection.execute(t, table_oid=table_oid)
        name = c.scalar()

        return {"constrained_columns": cols, "name": name}

    @reflection.cache
    def get_foreign_keys(
            self,
            connection,
            table_name,
            schema=None,
            postgresql_ignore_search_path=False,
            **kw
    ):
        preparer = self.identifier_preparer
        table_oid = self.get_table_oid(
            connection, table_name, schema, info_cache=kw.get("info_cache")
        )

        FK_SQL = """
          SELECT r.conname,
                sys_catalog.sys_get_constraintdef(r.oid, true) as condef,
                n.nspname as conschema
          FROM  sys_catalog.sys_constraint r,
                sys_namespace n,
                sys_class c

          WHERE r.conrelid = :table AND
                r.contype = 'f' AND
                c.oid = confrelid AND
                n.oid = c.relnamespace
          ORDER BY 1
        """
        # http://www.postgresql.org/docs/9.0/static/sql-createtable.html
        FK_REGEX = re.compile(
            r"FOREIGN KEY \((.*?)\) REFERENCES (?:(.*?)\.)?(.*?)\((.*?)\)"
            r"[\s]?(MATCH (FULL|PARTIAL|SIMPLE)+)?"
            r"[\s]?(ON UPDATE "
            r"(CASCADE|RESTRICT|NO ACTION|SET NULL|SET DEFAULT)+)?"
            r"[\s]?(ON DELETE "
            r"(CASCADE|RESTRICT|NO ACTION|SET NULL|SET DEFAULT)+)?"
            r"[\s]?(DEFERRABLE|NOT DEFERRABLE)?"
            r"[\s]?(INITIALLY (DEFERRED|IMMEDIATE)+)?"
        )

        t = sql.text(FK_SQL).columns(
            conname=sqltypes.Unicode, condef=sqltypes.Unicode
        )
        c = connection.execute(t, table=table_oid)
        fkeys = []
        for conname, condef, conschema in c.fetchall():
            m = re.search(FK_REGEX, condef).groups()

            (
                constrained_columns,
                referred_schema,
                referred_table,
                referred_columns,
                _,
                match,
                _,
                onupdate,
                _,
                ondelete,
                deferrable,
                _,
                initially,
            ) = m

            if deferrable is not None:
                deferrable = True if deferrable == "DEFERRABLE" else False
            constrained_columns = [
                preparer._unquote_identifier(x)
                for x in re.split(r"\s*,\s*", constrained_columns)
            ]

            if postgresql_ignore_search_path:
                # when ignoring search path, we use the actual schema
                # provided it isn't the "default" schema
                if conschema != self.default_schema_name:
                    referred_schema = conschema
                else:
                    referred_schema = schema
            elif referred_schema:
                # referred_schema is the schema that we regexp'ed from
                # sys_get_constraintdef().  If the schema is in the search
                # path, sys_get_constraintdef() will give us None.
                referred_schema = preparer._unquote_identifier(referred_schema)
            elif schema is not None and schema == conschema:
                # If the actual schema matches the schema of the table
                # we're reflecting, then we will use that.
                referred_schema = schema

            referred_table = preparer._unquote_identifier(referred_table)
            referred_columns = [
                preparer._unquote_identifier(x)
                for x in re.split(r"\s*,\s", referred_columns)
            ]
            fkey_d = {
                "name": conname,
                "constrained_columns": constrained_columns,
                "referred_schema": referred_schema,
                "referred_table": referred_table,
                "referred_columns": referred_columns,
                "options": {
                    "onupdate": onupdate,
                    "ondelete": ondelete,
                    "deferrable": deferrable,
                    "initially": initially,
                    "match": match,
                },
            }
            fkeys.append(fkey_d)
        return fkeys

    @reflection.cache
    def get_indexes(self, connection, table_name, schema, **kw):
        table_oid = self.get_table_oid(
            connection, table_name, schema, info_cache=kw.get("info_cache")
        )

        # cast indkey as varchar since it's an int2vector,
        # returned as a list by some drivers such as pypostgresql

        if self.server_version_info < (8, 5):
            IDX_SQL = """
              SELECT
                  i.relname as relname,
                  ix.indisunique, ix.indexprs, ix.indpred,
                  a.attname, a.attnum, NULL, ix.indkey%s,
                  %s, am.amname
              FROM
                  sys_class t
                        join sys_index ix on t.oid = ix.indrelid
                        join sys_class i on i.oid = ix.indexrelid
                        left outer join
                            sys_attribute a
                            on t.oid = a.attrelid and %s
                        left outer join
                            sys_am am
                            on i.relam = am.oid
              WHERE
                  t.relkind IN ('r', 'v', 'f', 'm')
                  and t.oid = :table_oid
                  and ix.indisprimary = 'f'
              ORDER BY
                  t.relname,
                  i.relname
            """ % (
                # version 8.3 here was based on observing the
                # cast does not work in PG 8.2.4, does work in 8.3.0.
                # nothing in PG changelogs regarding this.
                "::varchar" if self.server_version_info >= (8, 3) else "",
                "i.reloptions"
                if self.server_version_info >= (8, 2)
                else "NULL",
                self._sys_index_any("a.attnum", "ix.indkey"),
            )
        else:
            IDX_SQL = """
              SELECT
                  i.relname as relname,
                  ix.indisunique, ix.indexprs, ix.indpred,
                  a.attname, a.attnum, c.conrelid, ix.indkey::varchar,
                  i.reloptions, am.amname
              FROM
                  sys_class t
                        join sys_index ix on t.oid = ix.indrelid
                        join sys_class i on i.oid = ix.indexrelid
                        left outer join
                            sys_attribute a
                            on t.oid = a.attrelid and a.attnum = ANY(ix.indkey)
                        left outer join
                            sys_constraint c
                            on (ix.indrelid = c.conrelid and
                                ix.indexrelid = c.conindid and
                                c.contype in ('p', 'u', 'x'))
                        left outer join
                            sys_am am
                            on i.relam = am.oid
              WHERE
                  t.relkind IN ('r', 'v', 'f', 'm')
                  and t.oid = :table_oid
                  and ix.indisprimary = 'f'
              ORDER BY
                  t.relname,
                  i.relname
            """

        t = sql.text(IDX_SQL).columns(
            relname=sqltypes.Unicode, attname=sqltypes.Unicode
        )
        c = connection.execute(t, table_oid=table_oid)

        indexes = defaultdict(lambda: defaultdict(dict))

        sv_idx_name = None
        for row in c.fetchall():
            (
                idx_name,
                unique,
                expr,
                prd,
                col,
                col_num,
                conrelid,
                idx_key,
                options,
                amname,
            ) = row

            if expr:
                if idx_name != sv_idx_name:
                    util.warn(
                        "Skipped unsupported reflection of "
                        "expression-based index %s" % idx_name
                    )
                sv_idx_name = idx_name
                continue

            if prd and not idx_name == sv_idx_name:
                util.warn(
                    "Predicate of partial index %s ignored during reflection"
                    % idx_name
                )
                sv_idx_name = idx_name

            has_idx = idx_name in indexes
            index = indexes[idx_name]
            if col is not None:
                index["cols"][col_num] = col
            if not has_idx:
                index["key"] = [int(k.strip()) for k in idx_key.split()]
                index["unique"] = unique
                if conrelid is not None:
                    index["duplicates_constraint"] = idx_name
                if options:
                    index["options"] = dict(
                        [option.split("=") for option in options]
                    )

                # it *might* be nice to include that this is 'btree' in the
                # reflection info.  But we don't want an Index object
                # to have a ``postgresql_using`` in it that is just the
                # default, so for the moment leaving this out.
                if amname and amname != "btree":
                    index["amname"] = amname

        result = []
        for name, idx in indexes.items():
            entry = {
                "name": name,
                "unique": idx["unique"],
                "column_names": [idx["cols"][i] for i in idx["key"]],
            }
            if "duplicates_constraint" in idx:
                entry["duplicates_constraint"] = idx["duplicates_constraint"]
            if "options" in idx:
                entry.setdefault("dialect_options", {})[
                    "postgresql_with"
                ] = idx["options"]
            if "amname" in idx:
                entry.setdefault("dialect_options", {})[
                    "postgresql_using"
                ] = idx["amname"]
            result.append(entry)
        return result

    @reflection.cache
    def get_unique_constraints(
            self, connection, table_name, schema=None, **kw
    ):
        table_oid = self.get_table_oid(
            connection, table_name, schema, info_cache=kw.get("info_cache")
        )

        UNIQUE_SQL = """
            SELECT
                cons.conname as name,
                cons.conkey as key,
                a.attnum as col_num,
                a.attname as col_name
            FROM
                sys_catalog.sys_constraint cons
                join sys_attribute a
                  on cons.conrelid = a.attrelid AND
                    a.attnum = ANY(cons.conkey)
            WHERE
                cons.conrelid = :table_oid AND
                cons.contype = 'u'
        """

        t = sql.text(UNIQUE_SQL).columns(col_name=sqltypes.Unicode)
        c = connection.execute(t, table_oid=table_oid)

        uniques = defaultdict(lambda: defaultdict(dict))
        for row in c.fetchall():
            uc = uniques[row.name]
            uc["key"] = row.key
            uc["cols"][row.col_num] = row.col_name

        return [
            {"name": name, "column_names": [uc["cols"][i] for i in uc["key"]]}
            for name, uc in uniques.items()
        ]

    @reflection.cache
    def get_table_comment(self, connection, table_name, schema=None, **kw):
        table_oid = self.get_table_oid(
            connection, table_name, schema, info_cache=kw.get("info_cache")
        )

        COMMENT_SQL = """
            SELECT
                pgd.description as table_comment
            FROM
                sys_catalog.sys_description pgd
            WHERE
                pgd.objsubid = 0 AND
                pgd.objoid = :table_oid
        """

        c = connection.execute(sql.text(COMMENT_SQL), table_oid=table_oid)
        return {"text": c.scalar()}

    @reflection.cache
    def get_check_constraints(self, connection, table_name, schema=None, **kw):
        table_oid = self.get_table_oid(
            connection, table_name, schema, info_cache=kw.get("info_cache")
        )

        CHECK_SQL = """
            SELECT
                cons.conname as name,
                sys_get_constraintdef(cons.oid) as src
            FROM
                sys_catalog.sys_constraint cons
            WHERE
                cons.conrelid = :table_oid AND
                cons.contype = 'c'
        """

        c = connection.execute(sql.text(CHECK_SQL), table_oid=table_oid)

        # samples:
        # "CHECK (((a > 1) AND (a < 5)))"
        # "CHECK (((a = 1) OR ((a > 2) AND (a < 5))))"
        def match_cons(src):
            m = re.match(r"^CHECK *\(\((.+)\)\)$", src)
            if not m:
                util.warn("Could not parse CHECK constraint text: %r" % src)
                return ""
            return m.group(1)

        return [
            {"name": name, "sqltext": match_cons(src)}
            for name, src in c.fetchall()
        ]

    def _load_enums(self, connection, schema=None):
        schema = schema or self.default_schema_name
        if not self.supports_native_enum:
            return {}

        # Load data types for enums:
        SQL_ENUMS = """
            SELECT t.typname as "name",
               -- no enum defaults in 8.4 at least
               -- t.typdefault as "default",
               sys_catalog.sys_type_is_visible(t.oid) as "visible",
               n.nspname as "schema",
               e.enumlabel as "label"
            FROM sys_catalog.sys_type t
                 LEFT JOIN sys_catalog.sys_namespace n ON n.oid = t.typnamespace
                 LEFT JOIN sys_catalog.sys_enum e ON t.oid = e.enumtypid
            WHERE t.typtype = 'e'
        """

        if schema != "*":
            SQL_ENUMS += "AND n.nspname = :schema "

        # e.oid gives us label order within an enum
        SQL_ENUMS += 'ORDER BY "schema", "name", e.oid'

        s = sql.text(SQL_ENUMS).columns(
            attname=sqltypes.Unicode, label=sqltypes.Unicode
        )

        if schema != "*":
            s = s.bindparams(schema=schema)

        c = connection.execute(s)

        enums = []
        enum_by_name = {}
        for enum in c.fetchall():
            key = (enum["schema"], enum["name"])
            if key in enum_by_name:
                enum_by_name[key]["labels"].append(enum["label"])
            else:
                enum_by_name[key] = enum_rec = {
                    "name": enum["name"],
                    "schema": enum["schema"],
                    "visible": enum["visible"],
                    "labels": [enum["label"]],
                }
                enums.append(enum_rec)
        return enums

    def _load_domains(self, connection):
        # Load data types for domains:
        SQL_DOMAINS = """
            SELECT t.typname as "name",
               sys_catalog.format_type(t.typbasetype, t.typtypmod) as "attype",
               not t.typnotnull as "nullable",
               t.typdefault as "default",
               sys_catalog.sys_type_is_visible(t.oid) as "visible",
               n.nspname as "schema"
            FROM sys_catalog.sys_type t
               LEFT JOIN sys_catalog.sys_namespace n ON n.oid = t.typnamespace
            WHERE t.typtype = 'd'
        """

        s = sql.text(SQL_DOMAINS).columns(attname=sqltypes.Unicode)
        c = connection.execute(s)

        domains = {}
        for domain in c.fetchall():
            # strip (30) from character varying(30)
            attype = re.search(r"([^\(]+)", domain["attype"]).group(1)
            # 'visible' just means whether or not the domain is in a
            # schema that's on the search path -- or not overridden by
            # a schema with higher precedence. If it's not visible,
            # it will be prefixed with the schema-name when it's used.
            if domain["visible"]:
                key = (domain["name"],)
            else:
                key = (domain["schema"], domain["name"])

            domains[key] = {
                "attype": attype,
                "nullable": domain["nullable"],
                "default": domain["default"],
            }

        return domains


dialect = PGDialect_kingbase
