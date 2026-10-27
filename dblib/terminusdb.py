"""TerminusDB backend (microbenchmark only).

TerminusDB is a document/graph database with git-like branches and no SQL
interface: data goes through a JSON document API and WOQL queries, and
every write request is one commit. This backend therefore runs no SQL. It
recognises the statement shapes the microbenchmark issues and sends each
one as the matching native call:

==========================================  ================================
statement                                   TerminusDB call
==========================================  ================================
SELECT ... WHERE <whole primary key> = ...  GET one document by id
INSERT INTO t (...) VALUES (...)            POST documents
UPDATE t SET <every other column> WHERE     PUT (replace) one document
<whole primary key> = ...
DELETE FROM t WHERE <whole primary key>     DELETE one document by id
SELECT with any other conjunctive filter    one WOQL query (triples + filter)
UPDATE/DELETE with any other filter         one WOQL query (one commit)
==========================================  ================================

Anything else (joins, aggregates other than COUNT(*), ORDER BY, expressions
in SET, DDL, transactions) raises NotSupportedError, which is why the
macrobenchmark does not run here.

A table is a document class whose properties are the columns; a row is a
document with the id ``<table>/<pk1>_<pk2>...``. The loader reads the same
SQL dumps the other backends load (CREATE TABLE, ALTER TABLE ... PRIMARY
KEY, COPY ... FROM stdin, INSERT) and writes them as a schema and
documents. The column list, SQL types and primary key of each table are
kept in the class's ``@metadata`` so that any process can rebuild them.

Verbs
-----
branch, delete, log, diff, reset and rebase are single native calls.

commit    Every write is already a commit, so commit() has nothing to
          write. It reads the branch head and returns it as the commit id
          ("simulated"). The first write since the previous commit() is
          remembered (write responses carry the commit they made), so that
          revert can undo everything a commit() covers.
merge     TerminusDB has no merge. It is composed from the source head, the
          common ancestor (remembered per branch pair, found from the two
          logs the first time) and one ``apply`` of the diff between them
          as a single squash commit. ``apply`` refuses a conflicting diff
          (HTTP 409, nothing applied); the documents it names are then
          resolved here: "ours" applies the source's other documents,
          "theirs" all of them. A callable policy is treated as "ours".
revert    ``apply`` of the inverse diff, from the commit back to the parent
          of the first write it covers (one log request finds the parent).
rebase    Native. 12.0.7 answers some rebases with a bare HTTP 500 and
          leaves the branch untouched; the request is repeated (up to 4
          attempts) and counted in OBSERVED["rebase_retries"].
connect   The API is stateless: switching branch changes the URL of the
          next request and costs nothing.

Configuration (environment)
---------------------------
TERMINUSDB_URL          server (default http://127.0.0.1:6363)
TERMINUSDB_USER         default admin
TERMINUSDB_PASSWORD     default root
TERMINUSDB_TEAM         organisation that owns the database (default admin)
TERMINUSDB_STORAGE_DIR  host path of the server's storage directory, for
                        storage measurements (whole server)
TERMINUSDB_CONTAINER    alternatively, the Docker container to measure in
TERMINUSDB_LOAD_BATCH   documents per request while loading (default 5000)
TERMINUSDB_REUSE_LOAD   1: keep a loaded database between runs. The loaded
                        state is pinned by a branch named after the dump;
                        the next run with the same dump deletes the other
                        branches and resets main to it instead of loading
                        again (a 600k-row dump takes about 10 minutes)
TERMINUSDB_TIMEOUT_SEC  give up on a request after this long (default: wait)
"""

import datetime
import decimal
import os
import re
import subprocess
import threading
import uuid
from typing import Any, Optional
from urllib.parse import quote

import requests

try:
    import sqlglot
    from sqlglot import exp
except ImportError:   # optional dependency: pip install '.[terminusdb]'
    sqlglot = exp = None

import dblib.result_collector as rc
from dblib.db_api import DBToolSuite, Ref

MAIN_BRANCH = "main"

TERMINUSDB_URL = os.environ.get("TERMINUSDB_URL", "http://127.0.0.1:6363").rstrip("/")
TERMINUSDB_USER = os.environ.get("TERMINUSDB_USER", "admin")
TERMINUSDB_PASSWORD = os.environ.get("TERMINUSDB_PASSWORD", "root")
TERMINUSDB_TEAM = os.environ.get("TERMINUSDB_TEAM", "admin")
TERMINUSDB_STORAGE_DIR = os.environ.get("TERMINUSDB_STORAGE_DIR", "")
TERMINUSDB_CONTAINER = os.environ.get("TERMINUSDB_CONTAINER", "")
LOAD_BATCH = int(os.environ.get("TERMINUSDB_LOAD_BATCH", "5000"))
REUSE_LOAD = os.environ.get("TERMINUSDB_REUSE_LOAD", "") not in ("", "0")
TIMEOUT_SEC = float(os.environ.get("TERMINUSDB_TIMEOUT_SEC", "0")) or None

AUTHOR = "db-fork-bench"
REBASE_ATTEMPTS = 4


class TerminusDBError(RuntimeError):
    """The server refused a request."""

    def __init__(self, status: int, kind: str, message: str, body: Any = None):
        self.status = status
        self.kind = kind
        self.body = body
        super().__init__(f"{kind or status}: {message}")


class NotSupportedError(RuntimeError):
    """A statement with no TerminusDB equivalent in this backend."""


# ----------------------------------------------------------------------------
# HTTP
# ----------------------------------------------------------------------------


class _Client:
    """One HTTP session to the server."""

    def __init__(self):
        self.http = requests.Session()
        self.http.auth = (TERMINUSDB_USER, TERMINUSDB_PASSWORD)
        # One TCP connection per request. On a kept-alive connection the
        # server's response body waits for a delayed ACK, which adds about
        # 50 ms to every request (measured on 12.0.7: 56 ms against 5 ms for
        # a point read).
        self.http.headers["Connection"] = "close"
        self.api = f"{TERMINUSDB_URL}/api"
        # Commit id in the last response's TerminusDB-Data-Version header
        # (the branch head a write produced), or "".
        self.version = ""

    def call(self, method: str, path: str, *, params=None, body=None, headers=None):
        r = self.http.request(method, f"{self.api}/{path}", params=params, json=body,
                              headers=headers, timeout=TIMEOUT_SEC)
        text = r.text
        self.version = r.headers.get("TerminusDB-Data-Version", "").partition(":")[2]
        # 12.0 answers a missing document with HTTP 200 and a CGI-style
        # "Status: 404" body.
        if r.status_code < 300 and not text.startswith("Status: "):
            return r.json() if text.strip() else None
        status = r.status_code
        if text.startswith("Status: "):
            status = int(text[8:11])
            text = text[text.find("{"):] if "{" in text else text
        try:
            data = requests.models.complexjson.loads(text)
        except ValueError:
            data = {}
        error = data.get("api:error") or {}
        kind = (error.get("@type") if isinstance(error, dict) else "") or data.get("@type", "")
        raise TerminusDBError(status, kind, data.get("api:message") or text[:300], data)

    def documents(self, path: str, ids: list) -> list:
        """The documents among ``ids`` that exist at ``path``. The id list
        goes in the body of a POST that the server treats as a GET."""
        return self.call("POST", f"document/{path}", body={"ids": ids, "as_list": True},
                         headers={"X-HTTP-Method-Override": "GET"}) or []

    def close(self) -> None:
        self.http.close()


def _db_path(db_name: str) -> str:
    return f"{TERMINUSDB_TEAM}/{db_name}"


def _ref_path(db_name: str, ref: Ref) -> str:
    """Resource path of a branch head or of a commit."""
    if ref.commit:
        return f"{_db_path(db_name)}/local/commit/{ref.commit}"
    return f"{_db_path(db_name)}/local/branch/{ref.branch}"


# ----------------------------------------------------------------------------
# Catalog: tables as document classes
# ----------------------------------------------------------------------------

_XSD = (
    (("int", "integer", "smallint", "bigint", "int2", "int4", "int8", "serial",
      "bigserial", "tinyint"), "xsd:integer"),
    (("decimal", "numeric"), "xsd:decimal"),
    (("real", "float", "float4", "float8", "double"), "xsd:double"),
    (("timestamp", "timestamptz", "datetime"), "xsd:dateTime"),
    (("date",), "xsd:date"),
    (("bool", "boolean"), "xsd:boolean"),
)


def _xsd_type(sql_type: str) -> str:
    base = re.split(r"[\s(]", sql_type.strip().lower(), maxsplit=1)[0]
    for names, xsd in _XSD:
        if base in names:
            return xsd
    return "xsd:string"


def _encode(xsd: str, value):
    """A Python or dump value as the JSON value of an ``xsd`` property."""
    if value is None:
        return None
    if xsd == "xsd:integer":
        return int(value)
    if xsd in ("xsd:decimal", "xsd:double"):
        return float(value)
    if xsd == "xsd:boolean":
        if isinstance(value, str):
            return value.lower() in ("t", "true", "1")
        return bool(value)
    if xsd == "xsd:dateTime":
        if isinstance(value, datetime.datetime):
            return value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        text = str(value).strip().replace(" ", "T", 1)
        if len(text) == 10:
            text += "T00:00:00"
        text = re.sub(r"[+-]\d\d(:?\d\d)?$", "", text)
        return text if text.endswith("Z") else text + "Z"
    if xsd == "xsd:date":
        if isinstance(value, (datetime.date, datetime.datetime)):
            return value.strftime("%Y-%m-%d")
        return str(value)[:10]
    if isinstance(value, (decimal.Decimal, datetime.date)):
        return str(value)
    return str(value)


class _Table:
    def __init__(self, name: str, columns: list, pk: list):
        self.name = name
        self.columns = [(c, t) for c, t in columns]   # (column, SQL type)
        self.pk = list(pk)
        self.names = [c for c, _ in self.columns]
        self.xsd = {c: _xsd_type(t) for c, t in self.columns}

    def ddl(self) -> str:
        """CREATE TABLE text in the format get_table_schema() returns."""
        lines = [f"  {c} {t}" for c, t in self.columns]
        return "CREATE TABLE {} (\n{}\n);".format(self.name, ",\n".join(lines))

    def schema(self) -> dict:
        doc = {
            "@type": "Class",
            "@id": self.name,
            "@metadata": {"bench": {"columns": [list(c) for c in self.columns], "pk": self.pk}},
        }
        for column in self.names:
            doc[column] = {"@type": "Optional", "@class": self.xsd[column]}
        return doc

    def doc_id(self, values: dict) -> str:
        """Document id of the row whose primary key is in ``values``."""
        if not self.pk:
            return f"{self.name}/r{uuid.uuid4().hex}"
        key = "_".join(quote(str(_encode(self.xsd[c], values[c])), safe="") for c in self.pk)
        return f"{self.name}/{key}"

    def to_doc(self, values: dict, doc_id: str = None) -> dict:
        doc = {"@type": self.name, "@id": doc_id or self.doc_id(values)}
        for column, value in values.items():
            if column not in self.xsd:
                raise NotSupportedError(f"{self.name} has no column {column}")
            if value is not None:
                doc[column] = _encode(self.xsd[column], value)
        return doc

    def to_row(self, doc: dict, columns: list) -> tuple:
        return tuple(doc.get(c) for c in columns)


class _Catalog:
    """The tables of one database. Read by util.db_helpers in place of the
    information_schema queries it sends to SQL backends."""

    def __init__(self, tables: dict):
        self.tables_by_name = tables

    def table(self, name: str) -> _Table:
        name = name.split(".")[-1].strip('"').lower()
        if name not in self.tables_by_name:
            raise NotSupportedError(f"unknown table {name}")
        return self.tables_by_name[name]

    def tables(self) -> list:
        return sorted(self.tables_by_name)

    def columns(self, table: str) -> list:
        return list(self.table(table).names)

    def pk_columns(self, table: str) -> list:
        return list(self.table(table).pk)


_CATALOGS: dict = {}
_CATALOG_LOCK = threading.Lock()


def _catalog(client: _Client, db_name: str) -> _Catalog:
    with _CATALOG_LOCK:
        if db_name not in _CATALOGS:
            docs = client.call(
                "GET", f"document/{_ref_path(db_name, Ref(MAIN_BRANCH))}",
                params={"graph_type": "schema", "as_list": "true"})
            tables = {}
            for doc in docs or []:
                meta = (doc.get("@metadata") or {}).get("bench")
                if doc.get("@type") == "Class" and meta:
                    tables[doc["@id"]] = _Table(doc["@id"], meta["columns"], meta["pk"])
            _CATALOGS[db_name] = _Catalog(tables)
        return _CATALOGS[db_name]


# ----------------------------------------------------------------------------
# Loading a SQL dump
# ----------------------------------------------------------------------------

_CREATE_RE = re.compile(r"CREATE TABLE\s+(?:IF NOT EXISTS\s+)?([\w.\"]+)\s*\((.*)\)", re.I | re.S)
_PK_RE = re.compile(r"ALTER TABLE\s+(?:ONLY\s+)?([\w.\"]+).*?PRIMARY KEY\s*\(([^)]*)\)", re.I | re.S)
_COPY_RE = re.compile(r"COPY\s+([\w.\"]+)\s*\(([^)]*)\)\s+FROM\s+stdin", re.I)
_COLUMN_RE = re.compile(r"^\"?(\w+)\"?\s+(.+)$", re.S)
_COPY_ESCAPES = {"\\t": "\t", "\\n": "\n", "\\r": "\r", "\\\\": "\\"}


def _bare(name: str) -> str:
    return name.strip().split(".")[-1].strip('"').lower()


def _split_columns(body: str) -> list:
    """Top-level comma-separated parts of a CREATE TABLE body."""
    parts, depth, start = [], 0, 0
    for i, ch in enumerate(body):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append(body[start:i])
            start = i + 1
    parts.append(body[start:])
    return [p.strip() for p in parts if p.strip()]


def _parse_create(statement: str) -> Optional[_Table]:
    m = _CREATE_RE.search(statement)
    if not m:
        return None
    columns, pk = [], []
    for part in _split_columns(m.group(2)):
        low = part.lower()
        if low.startswith("primary key"):
            pk = [_bare(c) for c in part[part.index("(") + 1:part.rindex(")")].split(",")]
            continue
        if low.startswith(("foreign key", "constraint", "unique", "check")):
            continue
        cm = _COLUMN_RE.match(part)
        if not cm:
            continue
        name, rest = cm.group(1).lower(), cm.group(2)
        if re.search(r"\bprimary key\b", rest, re.I):
            pk = [name]
        sql_type = re.split(r"\s+(?:not null|null|default|primary key|references|check|unique)\b",
                            rest, maxsplit=1, flags=re.I)[0].strip()
        columns.append((name, sql_type))
    return _Table(_bare(m.group(1)), columns, pk)


def _statements(path: str):
    """("sql", text) for each statement of a dump and ("copy", (table,
    columns, line iterator)) for each COPY block. The COPY lines must be
    consumed before asking for the next item."""
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        buf = []
        for line in f:
            stripped = line.strip()
            if not buf and (not stripped or stripped.startswith("--") or stripped.startswith("\\")):
                continue
            buf.append(line)
            if not stripped.endswith(";"):
                continue
            text = "".join(buf)
            buf = []
            copy = _COPY_RE.match(text.strip())
            if not copy:
                yield "sql", text
                continue

            def rows():
                for data in f:
                    if data.startswith("\\."):
                        return
                    yield data.rstrip("\n")

            it = rows()
            yield "copy", (_bare(copy.group(1)), [_bare(c) for c in copy.group(2).split(",")], it)
            for _ in it:   # skip what the caller left unread
                pass


def _copy_value(text: str):
    if text == "\\N":
        return None
    if "\\" in text:
        return re.sub(r"\\[tnr\\]", lambda m: _COPY_ESCAPES[m.group(0)], text)
    return text


def _insert_rows(statement: str):
    """(table, columns, rows) of a plain INSERT ... VALUES statement."""
    _require_sqlglot()
    tree = sqlglot.parse_one(statement, read="postgres")
    if not isinstance(tree, exp.Insert) or not isinstance(tree.expression, exp.Values):
        return None
    target = tree.this
    table = target.this if isinstance(target, exp.Schema) else target
    columns = [c.name.lower() for c in target.expressions] if isinstance(target, exp.Schema) else None
    rows = []
    for row in tree.expression.expressions:
        values = []
        for cell in row.expressions:
            if isinstance(cell, exp.Null):
                values.append(None)
            elif isinstance(cell, exp.Neg):
                values.append("-" + cell.this.name)
            else:
                values.append(cell.name)
        rows.append(values)
    return _bare(table.name), columns, rows


def load_dump(client: _Client, db_name: str, path: str) -> dict:
    """Load a SQL dump as a schema plus documents. Returns rows per table."""
    tables: dict = {}
    for kind, item in _statements(path):
        if kind != "sql":
            continue
        table = _parse_create(item)
        if table:
            tables[table.name] = table
            continue
        pk = _PK_RE.search(item)
        if pk and _bare(pk.group(1)) in tables:
            old = tables[_bare(pk.group(1))]
            tables[old.name] = _Table(old.name, old.columns, [_bare(c) for c in pk.group(2).split(",")])
    if not tables:
        raise ValueError(f"no CREATE TABLE found in {path}")

    target = f"document/{_ref_path(db_name, Ref(MAIN_BRANCH))}"
    client.call("POST", target, params={"graph_type": "schema", "author": AUTHOR, "message": "schema"},
                body=[t.schema() for t in tables.values()])
    with _CATALOG_LOCK:
        _CATALOGS[db_name] = _Catalog(tables)

    counts = {name: 0 for name in tables}
    loader = _Loader(client, db_name)
    for kind, item in _statements(path):
        if kind == "copy":
            name, columns, lines = item
            if name not in tables:
                continue
            for line in lines:
                counts[name] += 1
                loader.add(tables[name], columns, [_copy_value(v) for v in line.split("\t")])
        elif item.lstrip()[:6].upper() == "INSERT":
            parsed = _insert_rows(item)
            if parsed and parsed[0] in tables:
                name, columns, rows = parsed
                for values in rows:
                    counts[name] += 1
                    loader.add(tables[name], columns or tables[name].names, values)
    loader.flush()
    return counts


class _Loader:
    """Writes rows as documents, LOAD_BATCH per request (one commit each)."""

    def __init__(self, client: _Client, db_name: str):
        self.client = client
        self.target = f"document/{_ref_path(db_name, Ref(MAIN_BRANCH))}"
        self.batch: list = []

    def add(self, table: _Table, columns: list, values) -> None:
        row = dict(zip(columns, values))
        # A table without a primary key gets a random document id.
        self.batch.append(table.to_doc(row))
        if len(self.batch) >= LOAD_BATCH:
            self.flush()

    def flush(self) -> None:
        if self.batch:
            self.client.call("POST", self.target, params={"author": AUTHOR, "message": "load"},
                             body=self.batch)
            self.batch = []


def insert_rows(db_name: str, table: str, columns: list, rows) -> int:
    """Bulk-load ``rows`` into ``table`` on the main branch (seeding).
    Returns the number of rows written."""
    client = _Client()
    try:
        target = _catalog(client, db_name).table(table)
        loader, count = _Loader(client, db_name), 0
        for values in rows:
            loader.add(target, columns, values)
            count += 1
        loader.flush()
        return count
    finally:
        client.close()

# ----------------------------------------------------------------------------
# WOQL (JSON-LD) building blocks
# ----------------------------------------------------------------------------


def _var(name: str, kind: str = "Value") -> dict:
    return {"@type": kind, "variable": name}


def _node(name: str, kind: str = "NodeValue") -> dict:
    return {"@type": kind, "node": name}


def _lit(xsd: str, value) -> dict:
    return {"@type": "Value", "data": {"@type": xsd, "@value": _encode(xsd, value)}}


def _triple(subject: dict, column: str, obj: dict, kind: str = "Triple") -> dict:
    return {"@type": kind, "subject": subject, "predicate": _node(column), "object": obj}


def _and(parts: list) -> dict:
    return parts[0] if len(parts) == 1 else {"@type": "And", "and": parts}


def _or(parts: list) -> dict:
    return parts[0] if len(parts) == 1 else {"@type": "Or", "or": parts}


def _not(query: dict) -> dict:
    return {"@type": "Not", "query": query}


def _compare(op: str, left: dict, right: dict) -> dict:
    if op == "=":
        return {"@type": "Equals", "left": left, "right": right}
    if op == ">":
        return {"@type": "Greater", "left": left, "right": right}
    if op == "<":
        return {"@type": "Less", "left": left, "right": right}
    if op == ">=":
        return _not(_compare("<", left, right))
    if op == "<=":
        return _not(_compare(">", left, right))
    if op == "<>":
        return _not(_compare("=", left, right))
    raise NotSupportedError(f"comparison {op}")


def _tuple_compare(op: str, lefts: list, rights: list) -> dict:
    """Row-value comparison ``(a, b, ...) op (x, y, ...)``, lexicographic."""
    if op == "=":
        return _and([_compare("=", l, r) for l, r in zip(lefts, rights)])
    if len(lefts) == 1:
        return _compare(op, lefts[0], rights[0])
    strict = op[0]   # ">" or "<"
    return _or([
        _compare(strict, lefts[0], rights[0]),
        _and([_compare("=", lefts[0], rights[0]), _tuple_compare(op, lefts[1:], rights[1:])]),
    ])


def _plain(value):
    """A WOQL binding as a Python value."""
    if isinstance(value, dict) and "@value" in value:
        return value["@value"]
    return value


# ----------------------------------------------------------------------------
# Statement shapes
# ----------------------------------------------------------------------------

_NAMED_RE = re.compile(r"%\((\w+)\)s")
_COMPARISONS = {} if exp is None else {
    exp.EQ: "=", exp.GT: ">", exp.GTE: ">=", exp.LT: "<", exp.LTE: "<=", exp.NEQ: "<>"}
_FLIPPED = {"=": "=", "<>": "<>", ">": "<", "<": ">", ">=": "<=", "<=": ">="}
_IGNORED = ("SET ", "BEGIN", "START TRANSACTION", "COMMIT", "ROLLBACK")


class _Plan:
    """One parsed statement. Values are ("param", key) or ("lit", value)."""

    def __init__(self, kind: str, table: str):
        self.kind = kind             # select | insert | update | delete
        self.table = table
        self.columns: list = []      # select: output columns; insert: columns
        self.star = False
        self.count = False
        self.limit: Optional[int] = None
        self.where: list = []        # (columns, op, values)
        self.sets: list = []         # (column, value)
        self.rows: list = []         # insert: list of value lists


def _unwrap(node):
    while isinstance(node, exp.Paren):
        node = node.this
    return node


def _value(node) -> tuple:
    node = _unwrap(node)
    if isinstance(node, exp.Placeholder):
        return ("param", node.name)
    if isinstance(node, exp.Null):
        return ("lit", None)
    if isinstance(node, exp.Boolean):
        return ("lit", node.this)
    if isinstance(node, exp.Literal):
        return ("lit", node.this if node.is_string else node.to_py())
    if isinstance(node, exp.Neg) and isinstance(node.this, exp.Literal):
        return ("lit", -node.this.to_py())
    raise NotSupportedError(f"expression {node.sql()} has no document-API equivalent")


def _where(node, out: list) -> None:
    node = _unwrap(node)
    if isinstance(node, exp.And):
        _where(node.this, out)
        _where(node.expression, out)
        return
    op = _COMPARISONS.get(type(node))
    if op is None:
        raise NotSupportedError(f"filter {node.sql()} is not a comparison")
    left, right = _unwrap(node.this), _unwrap(node.expression)
    if not isinstance(left, (exp.Column, exp.Tuple)):
        left, right, op = right, left, _FLIPPED[op]
    lefts = left.expressions if isinstance(left, exp.Tuple) else [left]
    rights = right.expressions if isinstance(right, exp.Tuple) else [right]
    if len(lefts) != len(rights) or not all(isinstance(_unwrap(c), exp.Column) for c in lefts):
        raise NotSupportedError(f"filter {node.sql()} does not compare columns with values")
    if len(lefts) > 1 and op == "<>":
        raise NotSupportedError("row-value <> comparison")
    out.append(([_unwrap(c).name.lower() for c in lefts], op, [_value(v) for v in rights]))


def _require_sqlglot() -> None:
    if sqlglot is None:
        raise RuntimeError("sqlglot is not installed (see README: TerminusDB backend)")


def _parse(sql: str) -> _Plan:
    _require_sqlglot()
    counter = iter(range(10 ** 6))
    text = _NAMED_RE.sub(r":\1", sql)
    text = re.sub(r"%s", lambda m: f":_p{next(counter)}", text)
    try:
        tree = sqlglot.parse_one(text, read="postgres")
    except Exception as e:
        raise NotSupportedError(f"cannot parse statement: {e}")

    if isinstance(tree, exp.Select):
        source = tree.args.get("from") or tree.args.get("from_")
        if (source is None or not isinstance(source.this, exp.Table) or tree.args.get("joins")
                or tree.args.get("group") or tree.args.get("order") or tree.args.get("having")):
            raise NotSupportedError("only single-table SELECT with a WHERE filter maps to TerminusDB")
        plan = _Plan("select", source.this.name.lower())
        for item in tree.expressions:
            item = _unwrap(item)
            if isinstance(item, exp.Star):
                plan.star = True
            elif isinstance(item, exp.Count) and isinstance(_unwrap(item.this), exp.Star):
                plan.count = True
            elif isinstance(item, exp.Column):
                plan.columns.append(item.name.lower())
            else:
                raise NotSupportedError(f"select item {item.sql()}")
        if tree.args.get("limit") is not None:
            plan.limit = int(tree.args["limit"].expression.name)
    elif isinstance(tree, exp.Insert):
        target = tree.this
        if not isinstance(target, exp.Schema) or not isinstance(tree.expression, exp.Values):
            raise NotSupportedError("INSERT needs a column list and VALUES")
        plan = _Plan("insert", target.this.name.lower())
        plan.columns = [c.name.lower() for c in target.expressions]
        plan.rows = [[_value(v) for v in row.expressions] for row in tree.expression.expressions]
    elif isinstance(tree, exp.Update):
        plan = _Plan("update", tree.this.name.lower())
        for assignment in tree.expressions:
            plan.sets.append((assignment.this.name.lower(), _value(assignment.expression)))
    elif isinstance(tree, exp.Delete):
        plan = _Plan("delete", tree.this.name.lower())
    else:
        raise NotSupportedError(
            f"TerminusDB has no SQL: {type(tree).__name__} statements have no mapping here")
    if tree.args.get("where") is not None:
        _where(tree.args["where"].this, plan.where)
    return plan


_PLANS: dict = {}


def _plan(sql: str) -> _Plan:
    plan = _PLANS.get(sql)
    if plan is None:
        plan = _PLANS[sql] = _parse(sql)
    return plan


class _Cursor:
    """DB-API cursor over the document API and WOQL."""

    def __init__(self, conn: "_Connection"):
        self.conn = conn
        self.description = None
        self.rowcount = -1
        self._rows: list = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def close(self) -> None:
        self._rows = []

    def fetchall(self) -> list:
        rows, self._rows = self._rows, []
        return rows

    def fetchone(self):
        return self._rows.pop(0) if self._rows else None

    # -- helpers ------------------------------------------------------------

    def _bind(self, plan_value: tuple, params):
        kind, key = plan_value
        if kind == "lit":
            return key
        if isinstance(params, dict):
            return params[key]
        if key.startswith("_p") and params is not None:
            return params[int(key[2:])]
        raise ValueError(f"no value for parameter {key}")

    def _key(self, table: _Table, plan: _Plan, params) -> Optional[dict]:
        """Primary-key values if the filter is exactly ``pk = values``."""
        if not table.pk:
            return None
        found = {}
        for columns, op, values in plan.where:
            if op != "=":
                return None
            for column, value in zip(columns, values):
                found[column] = self._bind(value, params)
        return found if set(found) == set(table.pk) else None

    def _filter(self, table: _Table, plan: _Plan, params, doc: dict) -> list:
        """WOQL conjuncts that bind ``doc`` to the rows the filter selects.
        An equality becomes a triple with the value as its object (an index
        lookup); other comparisons bind the column and compare."""
        parts = [_triple(doc, "rdf:type", _node(f"@schema:{table.name}", "Value"))]
        bound, compares = [], []
        for columns, op, values in plan.where:
            for column in columns:
                if column not in table.xsd:
                    raise NotSupportedError(f"{table.name} has no column {column}")
            rights = [_lit(table.xsd[c], self._bind(v, params)) for c, v in zip(columns, values)]
            if op == "=":
                parts.extend(_triple(doc, c, r) for c, r in zip(columns, rights))
                continue
            for column in columns:
                if column not in bound:
                    bound.append(column)
                    parts.append(_triple(doc, column, _var(f"c_{column}")))
            compares.append(_tuple_compare(op, [_var(f"c_{c}") for c in columns], rights))
        return parts + compares

    def _woql(self, query: dict, write: bool = False) -> list:
        return self.conn.woql(query, write)

    def _document(self, method: str, **kwargs):
        return self.conn.document(method, **kwargs)

    # -- execution ----------------------------------------------------------

    def execute(self, sql: str, params=None) -> None:
        self.description, self.rowcount, self._rows = None, -1, []
        if sql.lstrip().upper().startswith(_IGNORED):
            if not sql.lstrip().upper().startswith("SET "):
                raise NotSupportedError("TerminusDB commits every request; there are no transactions")
            return
        plan = _plan(sql)
        table = self.conn.catalog.table(plan.table)
        getattr(self, f"_{plan.kind}")(plan, table, params)

    def _select(self, plan: _Plan, table: _Table, params) -> None:
        columns = table.names if plan.star else plan.columns
        key = None if plan.count else self._key(table, plan, params)
        if key is not None:
            try:
                docs = [self._document("GET", params={"id": table.doc_id(key)})]
            except TerminusDBError as e:
                if e.status != 404:
                    raise
                docs = []
            self._rows = [table.to_row(d, columns) for d in docs]
        else:
            doc = _var("Doc", "NodeValue")
            parts = self._filter(table, plan, params, doc)
            if plan.count:
                query = {"@type": "Count", "count": _var("N", "DataValue"), "query": _and(parts)}
                self._rows = [(int(_plain(self._woql(query)[0]["N"])),)]
                columns = ["count"]
            elif plan.star:
                parts.append({"@type": "ReadDocument", "identifier": doc, "document": _var("Row")})
                query = {"@type": "Select", "variables": ["Row"], "query": _and(parts)}
                if plan.limit is not None:
                    query = {"@type": "Limit", "limit": plan.limit, "query": query}
                self._rows = [table.to_row(b["Row"], columns) for b in self._woql(query)]
            else:
                for column in columns:
                    if column not in table.xsd:
                        raise NotSupportedError(f"{table.name} has no column {column}")
                    parts.append({"@type": "Optional", "query": _triple(doc, column, _var(f"o_{column}"))})
                query = {"@type": "Select", "variables": [f"o_{c}" for c in columns], "query": _and(parts)}
                if plan.limit is not None:
                    query = {"@type": "Limit", "limit": plan.limit, "query": query}
                self._rows = [tuple(_plain(b.get(f"o_{c}")) for c in columns) for b in self._woql(query)]
        self.description = [(c, None, None, None, None, None, None) for c in columns]
        self.rowcount = len(self._rows)

    def _insert(self, plan: _Plan, table: _Table, params) -> None:
        docs = [table.to_doc({c: self._bind(v, params) for c, v in zip(plan.columns, row)})
                for row in plan.rows]
        self._document("POST", body=docs if len(docs) > 1 else docs[0])
        self.rowcount = len(docs)

    def _update(self, plan: _Plan, table: _Table, params) -> None:
        sets = {c: self._bind(v, params) for c, v in plan.sets}
        if any(c in table.pk for c in sets):
            raise NotSupportedError("updating a primary-key column changes the document id")
        key = self._key(table, plan, params)
        if key is not None and set(sets) | set(table.pk) == set(table.names):
            # The whole row is given: replace the document.
            try:
                self._document("PUT", body=table.to_doc({**key, **sets}))
                self.rowcount = 1
            except TerminusDBError as e:
                if e.status != 404:
                    raise
                self.rowcount = 0
            return
        if key is not None:
            doc, parts = _node(table.doc_id(key)), []
        else:
            doc = _var("Doc", "NodeValue")
            parts = self._filter(table, plan, params, doc)
        for i, (column, value) in enumerate(sets.items()):
            old = _var(f"old_{i}")
            parts.append({"@type": "Optional", "query": _and([
                _triple(doc, column, old), _triple(doc, column, old, "DeleteTriple")])})
            if value is not None:
                parts.append(_triple(doc, column, _lit(table.xsd[column], value), "AddTriple"))
        self.rowcount = len(self._woql(_and(parts), write=True))

    def _delete(self, plan: _Plan, table: _Table, params) -> None:
        key = self._key(table, plan, params)
        if key is not None:
            try:
                self._document("DELETE", params={"id": table.doc_id(key)})
                self.rowcount = 1
            except TerminusDBError as e:
                if e.status != 404:
                    raise
                self.rowcount = 0
            return
        doc = _var("Doc", "NodeValue")
        parts = self._filter(table, plan, params, doc)
        parts.append({"@type": "DeleteDocument", "identifier": doc})
        self.rowcount = len(self._woql(_and(parts), write=True))


class _Connection:
    """What exec() and util.db_helpers see as the DB-API connection."""

    def __init__(self, client: _Client, db_name: str, ref: Ref, lineage: "_Lineage"):
        self.client = client
        self.db_name = db_name
        self.ref = ref
        self.path = _ref_path(db_name, ref)
        self.lineage = lineage
        self.closed = False

    @property
    def catalog(self) -> _Catalog:
        return _catalog(self.client, self.db_name)

    def point_at(self, ref: Ref) -> None:
        self.ref = ref
        self.path = _ref_path(self.db_name, ref)

    def writable(self) -> None:
        if self.ref.commit:
            raise RuntimeError(f"{self.ref} is a commit; commits are read-only")

    def wrote(self) -> None:
        """A write on the current branch succeeded: note the commit it
        made if it is the first since the branch's last commit()."""
        if self.client.version:
            with self.lineage.lock:
                self.lineage.first_write.setdefault(self.ref.branch, self.client.version)

    def woql(self, query: dict, write: bool = False) -> list:
        """Run one WOQL query on the current ref and return its bindings.
        A query that changes data needs ``write`` and makes one commit."""
        body = {"query": query}
        if write:
            self.writable()
            body["commit_info"] = {"author": AUTHOR, "message": "woql"}
        result = self.client.call("POST", f"woql/{self.path}", body=body)
        if write:
            self.wrote()
        return result.get("bindings", [])

    def document(self, method: str, **kwargs):
        """One document-API request (GET, POST, PUT or DELETE) on the
        current ref."""
        if method != "GET":
            self.writable()
            kwargs.setdefault("params", {}).update({"author": AUTHOR, "message": method.lower()})
        result = self.client.call(method, f"document/{self.path}", **kwargs)
        if method != "GET":
            self.wrote()
        return result

    def get_row(self, table: str, key: dict) -> Optional[dict]:
        """The document of the row with primary key ``key``, or None."""
        try:
            return self.document("GET", params={"id": self.catalog.table(table).doc_id(key)})
        except TerminusDBError as e:
            if e.status != 404:
                raise
            return None

    def insert_row(self, table: str, row: dict) -> None:
        self.document("POST", body=self.catalog.table(table).to_doc(row))

    def delete_row(self, table: str, key: dict) -> None:
        self.document("DELETE", params={"id": self.catalog.table(table).doc_id(key)})

    def cursor(self) -> _Cursor:
        return _Cursor(self)

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        self.closed = True


# ----------------------------------------------------------------------------
# Tool suite
# ----------------------------------------------------------------------------


class _Lineage:
    """What the server does not record, for one database, process-wide."""

    def __init__(self):
        self.lock = threading.Lock()
        # branch -> the commit its first write since the last commit() made
        self.first_write: dict = {}
        # commit() id -> the first write it covers; revert undoes from the
        # commit() id back to that write's parent
        self.marks: dict = {}
        self.bases: dict = {}       # frozenset({a, b}) -> their merge base


_LINEAGE: dict = {}
_LINEAGE_LOCK = threading.Lock()


def _lineage(db_name: str) -> _Lineage:
    with _LINEAGE_LOCK:
        return _LINEAGE.setdefault(db_name, _Lineage())


class TerminusDBToolSuite(DBToolSuite):
    BACKEND_NAME = "terminusdb"
    STORAGE_SCOPE = "server"
    SUPPORTS_COMMIT_REFS = True
    SUPPORTS_MULTI_REF_EXEC = False

    # Counters of server behaviour worked around, process-wide.
    OBSERVED: dict = {}

    IMPLEMENTATION = {"commit": "simulated", "merge": "composed", "revert": "composed"}
    IMPLEMENTATION_NOTES = {
        "commit": "every write request is already a commit; commit() reads the branch head",
        "merge": "source head + common ancestor (from the logs the first time) + one apply "
                 "of their diff as a squash commit; conflicting documents resolved client-side",
        "revert": "parent lookup + apply of the inverse diff, from the commit back to before "
                  "the first write it covers",
        "rebase": "native; retried when the server answers a bare 500 (see OBSERVED)",
        "connect": "stateless HTTP API: a branch switch changes the request URL only",
    }

    @classmethod
    def get_default_connection_uri(cls) -> str:
        return TERMINUSDB_URL

    @classmethod
    def init_for_bench(cls, collector: rc.ResultCollector, db_name: str,
                       default_branch_name: str, measure_storage: bool = False):
        return cls(collector, db_name, default_branch_name or MAIN_BRANCH, measure_storage)

    def __init__(self, collector: rc.ResultCollector, db_name: str,
                 default_branch_name: str = MAIN_BRANCH, measure_storage: bool = False):
        self.client = _Client()
        self.db_name = db_name
        self.default_branch = default_branch_name or MAIN_BRANCH
        self.lineage = _lineage(db_name)
        super().__init__(
            _Connection(self.client, db_name, Ref(self.default_branch), self.lineage),
            collector, measure_storage)
        self._current_ref = Ref(self.default_branch)

    # -- helpers ------------------------------------------------------------

    @classmethod
    def _observe(cls, counter: str) -> None:
        with _LINEAGE_LOCK:
            cls.OBSERVED[counter] = cls.OBSERVED.get(counter, 0) + 1

    def _path(self, ref: Ref) -> str:
        return _ref_path(self.db_name, ref)

    def _log(self, ref: Ref, count: int) -> list:
        return self.client.call("GET", f"log/{self._path(ref)}", params={"count": count}) or []

    def _head(self, ref: Ref) -> str:
        if ref.commit:
            return ref.commit
        return self._log(ref, 1)[0]["identifier"]

    def _apply(self, branch: str, before: str, after: str, message: str) -> None:
        self.client.call("POST", f"apply/{self._path(Ref(branch))}", body={
            "before_commit": before, "after_commit": after, "type": "squash",
            "commit_info": {"author": AUTHOR, "message": message},
        })

    def _common_ancestor(self, a: Ref, b: Ref) -> tuple:
        """(newest commit in both histories, head of ``b``), reading the two
        logs in growing pages: a log request costs about 1.5 ms per commit
        returned, and branches forked recently share a commit near the top."""
        count = 8
        while True:
            log_a = [c["identifier"] for c in self._log(a, count)]
            seen = set(log_a)
            log_b = [c["identifier"] for c in self._log(b, count)]
            for commit in log_b:
                if commit in seen:
                    return commit, log_b[0]
            if len(log_a) < count and len(log_b) < count:
                raise RuntimeError(f"{a} and {b} share no commit")
            count *= 4

    # -- hooks --------------------------------------------------------------

    def _storage_bytes(self) -> int:
        if TERMINUSDB_STORAGE_DIR:
            out = subprocess.run(["du", "-sb", TERMINUSDB_STORAGE_DIR], capture_output=True, text=True)
        elif TERMINUSDB_CONTAINER:
            out = subprocess.run(
                ["docker", "exec", TERMINUSDB_CONTAINER, "du", "-sb", "/app/terminusdb/storage"],
                capture_output=True, text=True)
        else:
            return 0
        return int(out.stdout.split()[0]) if out.returncode == 0 and out.stdout else 0

    def _gc_impl(self) -> None:
        self.client.call("POST", f"optimize/{self._path(Ref(self.default_branch))}")

    def _connect_impl(self, ref: Ref) -> None:
        self.conn.point_at(ref)

    def _branch_impl(self, name: str, from_ref: Ref) -> None:
        self.client.call("POST", f"branch/{self._path(Ref(name))}",
                         body={"origin": self._path(from_ref)})

    def _commit_impl(self, ref: Ref, message: str) -> str:
        head = self._head(ref.head())
        with self.lineage.lock:
            first = self.lineage.first_write.pop(ref.branch, None)
            if first:
                self.lineage.marks[head] = first
        return head

    def _log_impl(self, ref: Ref, limit: int) -> list:
        return [
            {"hash": c["identifier"], "author": c.get("author", ""),
             "message": c.get("message", ""), "date": c.get("timestamp")}
            for c in self._log(ref, limit)
        ]

    def _diff_impl(self, ref_a: Ref, ref_b: Ref) -> dict:
        patches = self.client.call("POST", f"diff/{_db_path(self.db_name)}", body={
            "before_data_version": ref_a.commit or ref_a.branch,
            "after_data_version": ref_b.commit or ref_b.branch,
        }) or []
        per: dict = {}
        for patch in patches:
            if "@insert" in patch:
                doc_id, field = patch["@insert"]["@id"], "rows_added"
            elif "@delete" in patch:
                doc_id, field = patch["@delete"]["@id"], "rows_deleted"
            else:
                doc_id, field = patch["@id"], "rows_modified"
            table = doc_id.split("/", 1)[0]
            entry = per.setdefault(table, {"table_name": table, "rows_added": 0,
                                           "rows_deleted": 0, "rows_modified": 0})
            entry[field] += 1
        tables = [per[t] for t in sorted(per)]
        return {"tables": tables,
                "rows_added": sum(t["rows_added"] for t in tables),
                "rows_deleted": sum(t["rows_deleted"] for t in tables),
                "rows_modified": sum(t["rows_modified"] for t in tables)}

    def _merge_impl(self, into: Ref, source: Ref, message: str, on_conflict="ours") -> dict:
        pair = frozenset((into.branch, source.branch))
        with self.lineage.lock:
            base = self.lineage.bases.get(pair)
        if base is None:
            base, head = self._common_ancestor(into.head(), source)
        else:
            head = self._head(source)
        info = {"fast_forward": False, "conflicts": 0, "base": base, "source": head}
        if base != head:
            try:
                self._apply(into.branch, base, head, message or f"merge {source}")
            except TerminusDBError as e:
                if e.status != 409:
                    raise
                witnesses = {w.get("@id") for w in (e.body or {}).get("api:witnesses", [])}
                info["conflicts"] = len(witnesses)
                self._merge_resolved(into, base, head, set() if on_conflict == "theirs" else witnesses,
                                     message or f"merge {source}")
        with self.lineage.lock:
            self.lineage.bases[pair] = head
        return info

    def _merge_resolved(self, into: Ref, base: str, head: str, keep_ours: set, message: str) -> None:
        """Write the source's version of every document it changed since
        ``base``, except the ones in ``keep_ours``."""
        patches = self.client.call("POST", f"diff/{_db_path(self.db_name)}", body={
            "before_data_version": base, "after_data_version": head}) or []
        upserts, deletes = [], []
        for patch in patches:
            if "@delete" in patch:
                doc_id = patch["@delete"]["@id"]
                if doc_id not in keep_ours:
                    deletes.append(doc_id)
            else:
                doc_id = patch["@insert"]["@id"] if "@insert" in patch else patch["@id"]
                if doc_id not in keep_ours:
                    upserts.append(doc_id)
        target = self._path(into.head())
        commit = {"author": AUTHOR, "message": message}
        if upserts:
            docs = self.client.documents(self._path(Ref(into.branch, head)), upserts)
            self.client.call("PUT", f"document/{target}", params={**commit, "create": "true"}, body=docs)
        if deletes:
            ids = [d["@id"] for d in self.client.documents(target, deletes)]
            if ids:
                self.client.call("DELETE", f"document/{target}", params=commit, body=ids)

    def _rebase_impl(self, ref: Ref, onto: Ref, on_conflict="ours") -> dict:
        body = {"rebase_from": self._path(onto), "author": AUTHOR}
        for attempt in range(REBASE_ATTEMPTS):
            try:
                result = self.client.call("POST", f"rebase/{self._path(ref.head())}", body=body)
                break
            except TerminusDBError as e:
                # 12.0.7 fails a rebase now and then with a bare 500 and
                # leaves the branch untouched; the same request then works.
                if e.status != 500 or attempt == REBASE_ATTEMPTS - 1:
                    raise
                self._observe("rebase_retries")
        forwarded = result.get("api:forwarded_commits") or []
        with self.lineage.lock:
            self.lineage.first_write.pop(ref.branch, None)
            if forwarded:
                self.lineage.bases[frozenset((ref.branch, onto.branch))] = forwarded[-1]
        return {"replayed": len(result.get("api:rebase_report") or []), "forwarded": len(forwarded)}

    def _revert_impl(self, ref: Ref, commit: str) -> None:
        with self.lineage.lock:
            first = self.lineage.marks.get(commit, commit)
        history = self._log(Ref(ref.branch, first), 2)
        if len(history) < 2:
            raise ValueError(f"commit {commit} has no parent to revert to")
        self._apply(ref.branch, commit, history[1]["identifier"], f"revert {commit}")

    def _reset_impl(self, ref: Ref, to: str) -> None:
        self.client.call("POST", f"reset/{self._path(ref.head())}", body={
            "commit_descriptor": self._path(Ref(ref.branch, to))})
        with self.lineage.lock:
            self.lineage.first_write.pop(ref.branch, None)

    def _delete_impl(self, ref: Ref) -> None:
        self.client.call("DELETE", f"branch/{self._path(ref)}")
        with self.lineage.lock:
            self.lineage.first_write.pop(ref.branch, None)
            for pair in [p for p in self.lineage.bases if ref.branch in p]:
                del self.lineage.bases[pair]

    def list_branches(self) -> list:
        result = self.client.call("GET", f"db/{_db_path(self.db_name)}", params={"branches": "true"})
        return list(result.get("branches", []))

    def get_table_schema(self, table_name: str) -> str:
        return self.conn.catalog.table(table_name).ddl()

    def close_connection(self) -> None:
        super().close_connection()
        self.client.close()


# ----------------------------------------------------------------------------
# Database lifecycle
# ----------------------------------------------------------------------------


def server_up() -> bool:
    try:
        _Client().call("GET", "info")
        return True
    except Exception:
        return False


def _forget(db_name: str) -> None:
    with _CATALOG_LOCK:
        _CATALOGS.pop(db_name, None)
    with _LINEAGE_LOCK:
        _LINEAGE.pop(db_name, None)


def drop_database(db_name: str, force: bool = False) -> None:
    """Delete ``db_name``. With TERMINUSDB_REUSE_LOAD the database is kept
    for the next run unless ``force``."""
    _forget(db_name)
    if REUSE_LOAD and not force:
        return
    client = _Client()
    try:
        client.call("DELETE", f"db/{_db_path(db_name)}")
    except TerminusDBError as e:
        if e.status not in (400, 404):
            raise
    finally:
        client.close()


def _loaded_branch(sql_dump_path: str) -> str:
    """Name of the branch that pins the state right after loading a dump."""
    stat = os.stat(sql_dump_path)
    name = re.sub(r"\W", "_", os.path.basename(sql_dump_path))
    return f"loaded_{name}_{stat.st_size}_{int(stat.st_mtime)}"


def _restore_loaded(client: _Client, db_name: str, pin: str) -> bool:
    """Put ``db_name`` back to its just-loaded state if it has the ``pin``
    branch: drop every other branch and reset main to the pin."""
    try:
        branches = client.call("GET", f"db/{_db_path(db_name)}", params={"branches": "true"})["branches"]
    except TerminusDBError:
        return False
    if pin not in branches:
        return False
    for branch in branches:
        if branch not in (pin, MAIN_BRANCH):
            client.call("DELETE", f"branch/{_ref_path(db_name, Ref(branch))}")
    head = client.call("GET", f"log/{_ref_path(db_name, Ref(pin))}", params={"count": 1})[0]["identifier"]
    client.call("POST", f"reset/{_ref_path(db_name, Ref(MAIN_BRANCH))}",
                body={"commit_descriptor": _ref_path(db_name, Ref(MAIN_BRANCH, head))})
    return True


def setup_database(db_name: str, sql_dump_path: Optional[str] = None) -> None:
    """Create ``db_name`` empty, then load ``sql_dump_path`` if given."""
    _forget(db_name)
    client = _Client()
    try:
        pin = _loaded_branch(sql_dump_path) if REUSE_LOAD and sql_dump_path else None
        if pin and _restore_loaded(client, db_name, pin):
            print(f"TerminusDB: reusing the loaded database {db_name}")
            return
        drop_database(db_name, force=True)
        client.call("POST", f"db/{_db_path(db_name)}",
                    body={"label": db_name, "comment": "db-fork benchmark"})
        if sql_dump_path:
            counts = load_dump(client, db_name, sql_dump_path)
            print(f"TerminusDB: loaded {sum(counts.values())} rows into {len(counts)} tables")
            if pin:
                client.call("POST", f"branch/{_ref_path(db_name, Ref(pin))}",
                            body={"origin": _ref_path(db_name, Ref(MAIN_BRANCH))})
    finally:
        client.close()
