"""Connection pooling for psycopg2

This module implements thread-safe (and not) connection pools.
"""
# psycopg/pool.py - pooling code for psycopg
#
# Copyright (C) 2003-2019 Federico Di Gregorio  <fog@debian.org>
# Copyright (C) 2020-2021 The Psycopg Team
#
# psycopg2 is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# In addition, as a special exception, the copyright holders give
# permission to link this program with the OpenSSL library (or with
# modified versions of OpenSSL that use the same license as OpenSSL),
# and distribute linked combinations including the two.
#
# You must obey the GNU Lesser General Public License in all respects for
# all of the code used other than OpenSSL.
#
# psycopg2 is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
# License for more details.

import psycopg2
from psycopg2 import extensions as _ext


class PoolError(psycopg2.Error):
    pass


#: Statement run on a connection when it is returned to the pool, unless the
#: pool is created with a different *reset* value.
_DEFAULT_RESET = "DISCARD ALL"


class AbstractConnectionPool:
    """Generic key-based pooling code."""

    def __init__(self, minconn, maxconn, *args, reset=_DEFAULT_RESET,
                 **kwargs):
        """Initialize the connection pool.

        New 'minconn' connections are created immediately calling 'connfunc'
        with given parameters. The connection pool will support a maximum of
        about 'maxconn' connections.

        *reset* is the SQL statement executed on every connection returned
        to the pool, to clear the session-level state (such as
        ``search_path``, temporary tables, prepared statements or session
        GUCs) left by the previous user.  It defaults to :sql:`DISCARD ALL`;
        pass a custom string to use another statement or `!None` to only
        roll back the current transaction without resetting the session.
        """
        self.minconn = int(minconn)
        self.maxconn = int(maxconn)
        self.closed = False

        self._args = args
        self._kwargs = kwargs
        if reset is not None and not isinstance(reset, str):
            raise TypeError("reset must be a SQL string or None")
        self._reset_statement = reset

        self._pool = []
        self._used = {}
        self._rused = {}    # id(conn) -> key map
        self._keys = 0

        for i in range(self.minconn):
            self._connect()

    def _connect(self, key=None):
        """Create a new connection and assign it to 'key' if not None."""
        conn = psycopg2.connect(*self._args, **self._kwargs)
        if key is not None:
            self._used[key] = conn
            self._rused[id(conn)] = key
        else:
            self._pool.append(conn)
        return conn

    def _getkey(self):
        """Return a new unique key."""
        self._keys += 1
        return self._keys

    def _getconn(self, key=None):
        """Get a free connection and assign it to 'key' if not None."""
        if self.closed:
            raise PoolError("connection pool is closed")
        if key is None:
            key = self._getkey()

        if key in self._used:
            return self._used[key]

        if self._pool:
            self._used[key] = conn = self._pool.pop()
            self._rused[id(conn)] = key
            return conn
        else:
            if len(self._used) == self.maxconn:
                raise PoolError("connection pool exhausted")
            return self._connect(key)

    def _reset(self, conn):
        """Reset the session state of *conn* before it goes back in the pool.

        An open or aborted transaction is rolled back first, then the reset
        statement is run in autocommit mode: :sql:`DISCARD ALL` and friends
        cannot run inside a transaction block.  If the reset fails the
        connection is closed, because the pool must never hand out a
        connection in an unknown session state.
        """
        status = conn.info.transaction_status
        if status == _ext.TRANSACTION_STATUS_UNKNOWN:
            # server connection lost: nothing to reset, caller discards it
            return
        if status != _ext.TRANSACTION_STATUS_IDLE:
            conn.rollback()

        if not self._reset_statement:
            # session reset disabled: only the rollback above is performed
            return

        # DISCARD ALL cannot run inside a transaction block: switch
        # temporarily to autocommit, then restore the mode the connection
        # had when it was returned.
        autocommit = conn.autocommit
        conn.autocommit = True
        try:
            conn.cursor().execute(self._reset_statement)
        except Exception:
            # the session state is now unknown: do not put the connection
            # back in the pool
            try:
                conn.close()
            finally:
                raise
        finally:
            if not conn.closed:
                conn.autocommit = autocommit

    def _putconn(self, conn, key=None, close=False):
        """Put away a connection."""
        if self.closed:
            raise PoolError("connection pool is closed")

        if key is None:
            key = self._rused.get(id(conn))
            if key is None:
                raise PoolError("trying to put unkeyed connection")

        if len(self._pool) < self.minconn and not close:
            # Return the connection into a consistent state before putting
            # it back into the pool
            if not conn.closed:
                status = conn.info.transaction_status
                if status == _ext.TRANSACTION_STATUS_UNKNOWN:
                    # server connection lost
                    conn.close()
                else:
                    # roll back an open/aborted transaction and clear the
                    # session-level state (search_path, temp tables, ...)
                    self._reset(conn)
                if not conn.closed:
                    self._pool.append(conn)
            # If the connection is closed, we just discard it.
        else:
            conn.close()

        # here we check for the presence of key because it can happen that a
        # thread tries to put back a connection after a call to close
        if not self.closed or key in self._used:
            del self._used[key]
            del self._rused[id(conn)]

    def _putconns(self, conns, close=False):
        """Put away several connections in a single batched operation.

        Equivalent to calling `_putconn()` on each connection, but all the
        reset statements are issued together in one reset phase before any
        connection is published back into the pool, instead of alternating
        reset and pool bookkeeping for every single connection.
        """
        if self.closed:
            raise PoolError("connection pool is closed")

        # Validate every connection up front, before touching any of them:
        # a bad batch must fail atomically instead of leaving half of the
        # connections already returned.
        entries = []
        seen = set()
        for conn in conns:
            key = self._rused.get(id(conn))
            if key is None:
                raise PoolError("trying to put unkeyed connection")
            if id(conn) in seen:
                raise PoolError("connection passed more than once")
            seen.add(id(conn))
            entries.append((conn, key))

        # First pass: reset every reusable connection in a single batch so
        # that all the reset statements are merged into one phase.
        keep = max(self.minconn - len(self._pool), 0)
        reusable = []
        for conn, key in entries:
            if close or keep == 0:
                conn.close()
                continue
            keep -= 1
            if conn.closed:
                continue
            if conn.info.transaction_status == _ext.TRANSACTION_STATUS_UNKNOWN:
                conn.close()
                continue
            try:
                self._reset(conn)
            except Exception:
                # _reset() already closed the connection: keep processing
                # the rest of the batch instead of leaking the entries
                pass
            if not conn.closed:
                reusable.append((conn, key))

        # Second pass: publish the reset connections back into the pool.
        for conn, key in reusable:
            self._pool.append(conn)

        for conn, key in entries:
            # here we check for the presence of key because it can happen
            # that a thread tries to put back a connection after close
            if not self.closed or key in self._used:
                del self._used[key]
                del self._rused[id(conn)]

    def _closeall(self):
        """Close all connections.

        Note that this can lead to some code fail badly when trying to use
        an already closed connection. If you call .closeall() make sure
        your code can deal with it.
        """
        if self.closed:
            raise PoolError("connection pool is closed")
        for conn in self._pool + list(self._used.values()):
            try:
                conn.close()
            except Exception:
                pass
        self.closed = True


class SimpleConnectionPool(AbstractConnectionPool):
    """A connection pool that can't be shared across different threads."""

    getconn = AbstractConnectionPool._getconn
    putconn = AbstractConnectionPool._putconn
    putconns = AbstractConnectionPool._putconns
    closeall = AbstractConnectionPool._closeall


class ThreadedConnectionPool(AbstractConnectionPool):
    """A connection pool that works with the threading module."""

    def __init__(self, minconn, maxconn, *args, **kwargs):
        """Initialize the threading lock."""
        import threading
        AbstractConnectionPool.__init__(
            self, minconn, maxconn, *args, **kwargs)
        self._lock = threading.Lock()

    def getconn(self, key=None):
        """Get a free connection and assign it to 'key' if not None."""
        self._lock.acquire()
        try:
            return self._getconn(key)
        finally:
            self._lock.release()

    def putconn(self, conn=None, key=None, close=False):
        """Put away an unused connection."""
        self._lock.acquire()
        try:
            self._putconn(conn, key, close)
        finally:
            self._lock.release()

    def putconns(self, conns, close=False):
        """Put away several unused connections in one batched operation."""
        self._lock.acquire()
        try:
            self._putconns(conns, close)
        finally:
            self._lock.release()

    def closeall(self):
        """Close all connections (even the one currently in use.)"""
        self._lock.acquire()
        try:
            self._closeall()
        finally:
            self._lock.release()
