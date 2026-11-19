#!/usr/bin/env python

# test_pool.py - unit test for psycopg2.pool
#
# Copyright (C) 2026 The Psycopg Team
#
# psycopg2 is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# psycopg2 is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
# License for more details.

import threading
import unittest

import psycopg2
import psycopg2.extensions as ext
from psycopg2 import pool

from .testutils import ConnectingTestCase
from . import testconfig


class PoolTestCase(ConnectingTestCase):
    def setUp(self):
        ConnectingTestCase.setUp(self)
        self._pools = []
        curs = self.conn.cursor()
        curs.execute("drop schema if exists pool_s1, pool_s2 cascade")
        curs.execute("create schema pool_s1")
        curs.execute("create schema pool_s2")
        curs.execute("create table pool_s1.widget(name text)")
        curs.execute("create table pool_s2.widget(name text)")
        curs.execute("insert into pool_s1.widget values ('from-schema-1')")
        curs.execute("insert into pool_s2.widget values ('from-schema-2')")
        self.conn.commit()

    def tearDown(self):
        for p in self._pools:
            if not p.closed:
                p.closeall()
        curs = self.conn.cursor()
        curs.execute("drop schema if exists pool_s1, pool_s2 cascade")
        self.conn.commit()
        ConnectingTestCase.tearDown(self)

    def _make_pool(self, cls=pool.SimpleConnectionPool, minconn=1,
                   maxconn=4, **kwargs):
        p = cls(minconn, maxconn, testconfig.dsn, **kwargs)
        self._pools.append(p)
        return p

    def _widget_value(self, conn):
        curs = conn.cursor()
        curs.execute("select name from widget")
        return curs.fetchone()[0]

    def test_reused_connection_resets_search_path(self):
        """A search_path set by one user must not leak to the next user."""
        p = self._make_pool()

        conn = p.getconn()
        conn.cursor().execute("set search_path to pool_s1")
        self.assertEqual(self._widget_value(conn), "from-schema-1")
        p.putconn(conn)

        reused = p.getconn()
        self.assertIs(reused, conn)
        curs = reused.cursor()
        curs.execute("show search_path")
        self.assertEqual(curs.fetchone()[0], '"$user", public')
        # the stale search_path is gone: pool_s1 is no longer visible
        curs.execute(
            "select count(*) from information_schema.tables "
            "where table_schema = 'pool_s1' and table_name = 'widget'")
        self.assertEqual(curs.fetchone()[0], 1)
        p.putconn(reused)

    def test_same_table_name_two_schemas_after_switch(self):
        """Regression test: same table name must resolve per-schema.

        A connection reused after switching schema must read the table of
        the currently selected schema, not data left over by the previous
        transaction's search_path.
        """
        p = self._make_pool()

        conn = p.getconn()
        conn.cursor().execute("set search_path to pool_s1")
        self.assertEqual(self._widget_value(conn), "from-schema-1")
        p.putconn(conn)

        conn = p.getconn()
        conn.cursor().execute("set search_path to pool_s2")
        self.assertEqual(self._widget_value(conn), "from-schema-2")
        p.putconn(conn)

        # and again back to schema 1 on the same physical connection
        conn = p.getconn()
        conn.cursor().execute("set search_path to pool_s1")
        self.assertEqual(self._widget_value(conn), "from-schema-1")
        p.putconn(conn)

    def test_session_gucs_and_temp_tables_are_discarded(self):
        p = self._make_pool()
        conn = p.getconn()
        curs = conn.cursor()
        curs.execute("set application_name to 'stale-app'")
        curs.execute("create temp table pool_temp(x int)")
        curs.execute("insert into pool_temp values (42)")
        p.putconn(conn)

        reused = p.getconn()
        curs = reused.cursor()
        curs.execute("show application_name")
        self.assertNotEqual(curs.fetchone()[0], "stale-app")
        curs.execute(
            "select count(*) from pg_class where relname = 'pool_temp'")
        self.assertEqual(curs.fetchone()[0], 0)
        p.putconn(reused)

    def test_open_transaction_rolled_back_on_return(self):
        p = self._make_pool()
        conn = p.getconn()
        curs = conn.cursor()
        curs.execute("create table pool_keep(id int)")
        curs.execute("insert into pool_keep values (1)")
        self.assertEqual(
            conn.info.transaction_status, ext.TRANSACTION_STATUS_INTRANS)
        p.putconn(conn)

        self.assertEqual(
            conn.info.transaction_status, ext.TRANSACTION_STATUS_IDLE)
        curs = conn.cursor()
        curs.execute(
            "select count(*) from information_schema.tables "
            "where table_name = 'pool_keep'")
        self.assertEqual(curs.fetchone()[0], 0)

    def test_aborted_transaction_recovered_on_return(self):
        p = self._make_pool()
        conn = p.getconn()
        curs = conn.cursor()
        curs.execute("set search_path to pool_s1")
        with self.assertRaises(psycopg2.Error):
            curs.execute("select * from no_such_table_pool_xyz")
        self.assertEqual(
            conn.info.transaction_status, ext.TRANSACTION_STATUS_INERROR)
        p.putconn(conn)

        self.assertFalse(conn.closed)
        self.assertEqual(
            conn.info.transaction_status, ext.TRANSACTION_STATUS_IDLE)
        curs = conn.cursor()
        curs.execute("show search_path")
        self.assertEqual(curs.fetchone()[0], '"$user", public')

    def test_reset_failure_discards_connection(self):
        p = self._make_pool()
        conn = p.getconn()
        original = p._reset_statement
        p._reset_statement = "select * from no_such_reset_function_xyz"
        with self.assertRaises(psycopg2.Error):
            p.putconn(conn)
        p._reset_statement = original
        self.assertTrue(conn.closed)
        # the broken connection never went back into the pool
        self.assertNotIn(conn, p._pool)
        # the pool can still serve a healthy connection
        other = p.getconn()
        self.assertFalse(other.closed)
        other.cursor().execute("select 1")
        p.putconn(other)

    def test_reset_can_be_disabled(self):
        p = self._make_pool(reset=None)
        self.assertIsNone(p._reset_statement)
        conn = p.getconn()
        conn.autocommit = True
        conn.cursor().execute("set search_path to pool_s1")
        p.putconn(conn)
        # reset=None keeps rollback-only behaviour: the session GUC set in
        # its own (committed) transaction survives the return
        reused = p.getconn()
        curs = reused.cursor()
        curs.execute("show search_path")
        self.assertEqual(curs.fetchone()[0], "pool_s1")
        p.putconn(reused)

    def test_default_reset_is_discard_all(self):
        p = self._make_pool()
        self.assertEqual(p._reset_statement, "DISCARD ALL")

    def test_custom_reset_statement(self):
        p = self._make_pool(reset="RESET ALL")
        conn = p.getconn()
        conn.cursor().execute("set search_path to pool_s1")
        p.putconn(conn)
        reused = p.getconn()
        curs = reused.cursor()
        curs.execute("show search_path")
        self.assertEqual(curs.fetchone()[0], '"$user", public')
        p.putconn(reused)

    def test_invalid_reset_argument(self):
        with self.assertRaises(TypeError):
            self._make_pool(reset=123)

    def test_putconns_batches_resets(self):
        p = self._make_pool(minconn=3)
        conns = [p.getconn() for _ in range(3)]
        for i, conn in enumerate(conns):
            conn.cursor().execute(
                "set search_path to pool_s%d" % (1 if i % 2 == 0 else 2))

        reset_calls = []
        original_reset = p._reset

        def spy_reset(conn):
            reset_calls.append(conn)
            return original_reset(conn)

        p._reset = spy_reset
        p.putconns(conns)

        # every connection was reset exactly once and all are back
        self.assertEqual(sorted(id(c) for c in reset_calls),
                         sorted(id(c) for c in conns))
        self.assertEqual(len(p._pool), 3)
        self.assertEqual(p._used, {})

        for conn in conns:
            curs = conn.cursor()
            curs.execute("show search_path")
            self.assertEqual(curs.fetchone()[0], '"$user", public')

    def test_putconns_merges_resets_into_one_phase(self):
        p = self._make_pool(minconn=3)
        conns = [p.getconn() for _ in range(3)]

        events = []
        original_reset = p._reset

        def spy_reset(conn):
            events.append(("reset", id(conn)))
            return original_reset(conn)

        class TrackingList(list):
            def append(self, item):
                events.append(("publish", id(item)))
                super().append(item)

        p._reset = spy_reset
        p._pool = TrackingList(p._pool)
        p.putconns(conns)

        # every reset must complete before any connection is published:
        # the resets are merged into a single phase instead of alternating
        # reset/publish for each connection
        last_reset = max(i for i, e in enumerate(events)
                         if e[0] == "reset")
        first_publish = min(i for i, e in enumerate(events)
                            if e[0] == "publish")
        self.assertLess(last_reset, first_publish)
        self.assertEqual(
            sorted(e[0] for e in events),
            ["publish", "publish", "publish", "reset", "reset", "reset"])

    def test_putconns_runs_one_reset_per_connection(self):
        p = self._make_pool(minconn=2)
        conns = [p.getconn() for _ in range(2)]
        reset_conns = []
        original_reset = p._reset

        def spy_reset(conn):
            reset_conns.append(id(conn))
            return original_reset(conn)

        p._reset = spy_reset
        p.putconns(conns)
        self.assertEqual(sorted(reset_conns),
                         sorted(id(c) for c in conns))

    def test_putconns_equivalent_to_putconn(self):
        p = self._make_pool(minconn=4)
        single = [p.getconn() for _ in range(2)]
        for conn in single:
            conn.cursor().execute("set search_path to pool_s1")
            p.putconn(conn)

        batched = [p.getconn() for _ in range(2)]
        for conn in batched:
            conn.cursor().execute("set search_path to pool_s2")
        p.putconns(batched)

        self.assertEqual(len(p._pool), 4)
        for conn in p._pool:
            curs = conn.cursor()
            curs.execute("show search_path")
            self.assertEqual(curs.fetchone()[0], '"$user", public')

    def test_putconns_unkeyed_connection_raises(self):
        p = self._make_pool()
        conn = p.getconn()
        foreign = psycopg2.connect(testconfig.dsn)
        self._conns.append(foreign)
        with self.assertRaises(pool.PoolError):
            p.putconns([conn, foreign])
        # the valid connection is still checked out
        self.assertIn(conn, p._used.values())
        p.putconn(conn)

    def test_putconns_validates_whole_batch_first(self):
        p = self._make_pool(minconn=2)
        good = p.getconn()
        other = p.getconn()
        foreign = psycopg2.connect(testconfig.dsn)
        self._conns.append(foreign)
        with self.assertRaises(pool.PoolError):
            p.putconns([good, foreign])
        # nothing was returned or reset: both connections stay checked out
        self.assertEqual(len(p._used), 2)
        self.assertIn(good, p._used.values())
        self.assertIn(other, p._used.values())
        p.putconns([good, other])

    def test_putconns_duplicate_connection_raises(self):
        p = self._make_pool()
        conn = p.getconn()
        with self.assertRaises(pool.PoolError):
            p.putconns([conn, conn])
        self.assertIn(conn, p._used.values())
        p.putconn(conn)

    def test_putconns_close(self):
        p = self._make_pool()
        conns = [p.getconn() for _ in range(2)]
        p.putconns(conns, close=True)
        self.assertEqual(p._used, {})
        for conn in conns:
            self.assertTrue(conn.closed)

    def test_threaded_pool_reset_under_concurrency(self):
        p = self._make_pool(cls=pool.ThreadedConnectionPool)
        errors = []

        def worker(schema, expected):
            try:
                for _ in range(20):
                    conn = p.getconn()
                    try:
                        curs = conn.cursor()
                        curs.execute("set search_path to " + schema)
                        curs.execute("select name from widget")
                        if curs.fetchone()[0] != expected:
                            errors.append(
                                "schema %s read wrong data" % schema)
                    finally:
                        p.putconn(conn)
            except Exception as exc:
                errors.append(repr(exc))

        threads = [
            threading.Thread(target=worker, args=("pool_s1", "from-schema-1")),
            threading.Thread(target=worker, args=("pool_s2", "from-schema-2")),
            threading.Thread(target=worker, args=("pool_s1", "from-schema-1")),
            threading.Thread(target=worker, args=("pool_s2", "from-schema-2")),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])

    def test_lost_connection_on_return_is_discarded(self):
        p = self._make_pool()
        conn = p.getconn()
        conn.close()
        p.putconn(conn)
        self.assertNotIn(conn, p._pool)
        self.assertEqual(p._used, {})


def test_suite():
    return unittest.TestLoader().loadTestsFromName(__name__)


if __name__ == "__main__":
    unittest.main()
