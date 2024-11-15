
import json
import datetime
import psycopg2

from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

from typing import Optional


def preprocessing(v):
    if isinstance(v, str):
        return "'" + v + "'"
    elif isinstance(v, datetime.datetime):
        return "'" + v.isoformat(sep=" ") + "'"
    elif isinstance(v, dict):
        return "'" + json.dumps(v) + "'"
    elif isinstance(v, bytes):
        return str(psycopg2.Binary(v))
    elif v is None:
        return "NULL"
    else:
        return str(v)


def conditions_to_str(conditions: list):
    if conditions is None or len(conditions) == 0:
        return ""
    elif len(conditions) == 1:
        return " WHERE " + str(conditions[0])
    else:
        return " WHERE " + " AND ".join(list(map(str, conditions)))


class Condition:

    def __init__(self, col: str, ct: str, value):
        self.col = col
        self.ct = ct
        self.value = value

    def __str__(self):
        if self.ct in ["=", "<=", ">=", "<", ">", "!=", "NOT"]:
            return f"{self.col} {self.ct} {preprocessing(self.value)}"
        elif self.ct == "IN":
            if len(self.value) > 1:
                return f"{self.col} IN ({','.join([preprocessing(v) for v in self.value])})"
            else:
                return f"{self.col} = {preprocessing(self.value[0])}"
        else:
            raise ValueError


class DataServiceError(Exception):

    def __init__(self, action: str, query: str = "", error_name: str = ""):

        self.action = action
        self.query = query
        self.error_name = error_name

    @property
    def msg(self):
        return f"Unable to execute the query: '{self.query}' ({self.error_name})"

    def __str__(self):
        return self.msg


class DataService:

    def __init__(self, endpoint: str, user: str, password: str, port: int = 5432, db: str = "postgres"):

        self.endpoint = endpoint
        self.user = user
        self.password = password
        self.port = port
        self.db = db

    @classmethod
    def from_dict(cls, d: dict):
        return cls(**d)

    @property
    def connector(self):
        conn_str = "host={0} dbname={1} user={2} password={3} port={4}".format(
            self.endpoint, self.db, self.user, self.password, self.port)
        return psycopg2.connect(conn_str)

    def pool(self, min_connection: int, max_connection: int):
        return ThreadedConnectionPool(minconn=min_connection, maxconn=max_connection,
                                      user=self.user, password=self.password, host=self.endpoint,
                                      database=self.db, port=self.port)

    def execute_query(self, query: str, fetch: str = None):
        connector = self.connector
        cursor = connector.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query)
        if fetch is None:
            resp = None
        elif fetch == "all":
            resp = cursor.fetchall()
        elif fetch == "one":
            resp = cursor.fetchone()
        else:
            raise ValueError
        connector.commit()
        connector.close()
        return resp

    @classmethod
    def execute_query_w_connector(cls, connector, query: str, fetch: str = None):
        cursor = connector.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query)
        if fetch is None:
            resp = None
        elif fetch == "all":
            resp = cursor.fetchall()
        elif fetch == "one":
            resp = cursor.fetchone()
        else:
            raise ValueError
        cursor.close()
        return resp

    def len(self, table: str):
        r = f"SELECT count (*) from {table}"
        connector = self.connector
        cursor = connector.cursor()
        cursor.execute(r)
        resp = cursor.fetchone()
        connector.close()
        return resp[0]

    def insert(self, table: str, objs: list,
               on_conflict_do_nothing: bool = False,
               on_conflict_do: str = None,
               returning: str = None,
               connector=None):
        if len(objs) == 0:
            return None
        connector = self.connector if connector is None else connector
        cursor = connector.cursor(cursor_factory=RealDictCursor)
        objs_ = [obj.to_dict() for obj in objs]
        columns = objs_[0].keys()
        r_v = ["(" + ",".join(list(map(preprocessing, obj.values()))) + ")" for obj in objs_]
        r_v = ",".join(r_v)
        r = f"INSERT INTO {table} AS t ({','.join(columns)}) VALUES {r_v}"
        if on_conflict_do_nothing:
            r += " ON CONFLICT DO NOTHING"
        elif on_conflict_do is not None:
            r += on_conflict_do
        if returning is not None:
            r += returning
        try:
            cursor.execute(r)
            connector.commit()
            if returning is not None:
                resp = cursor.fetchall()
            else:
                resp = None
            connector.close()
        except Exception as e:
            connector.rollback()
            connector.close()
            raise DataServiceError(action="insert", query="", error_name=e.__class__.__name__)
        return resp

    def delete(self, table: str, conditions: list):
        connector = self.connector
        cursor = connector.cursor()
        r = f"DELETE FROM {table}"
        r += conditions_to_str(conditions)
        try:
            cursor.execute(r)
            connector.commit()
            connector.close()
        except Exception as e:
            connector.rollback()
            connector.close()
            raise DataServiceError(action="delete", query=r, error_name=e.__class__.__name__)

    def fetch(self, table: str, columns: list = None, conditions: Optional[list] = None,
              orderby: str = None, order: str = "asc", limit: int = None, distinct: bool = False):
        connector = self.connector
        cursor = connector.cursor(cursor_factory=RealDictCursor)
        columns = ",".join(columns) if columns is not None else "*"
        r = f"SELECT DISTINCT {columns} FROM {table}" if distinct else f"SELECT {columns} FROM {table}"
        if conditions is not None:
            r += conditions_to_str(conditions)
        if orderby is not None:
            r += f" ORDER BY {orderby} {order}"
        if limit is not None:
            r += f" LIMIT {limit}"
        try:
            cursor.execute(r)
            resp = cursor.fetchall()
            connector.close()
            return resp
        except Exception as e:
            connector.close()
            raise DataServiceError(action="fetch", query=r, error_name=e.__class__.__name__)
