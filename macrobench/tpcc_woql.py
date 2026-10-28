"""The transactions and CH queries of macrobench/tpcc.py, hand-written for
TerminusDB.

TerminusDB has no SQL, so the macrobenchmark's workload cannot run on it.
This module is the same workload written against TerminusDB's own
interfaces: every ``db.sql(...)`` of tpcc.py has one counterpart here, in
the same order, issued as one request. A statement that addresses one row
by its whole primary key is a document-API call; everything else is one
WOQL query. Each request is timed and recorded as the SQL statement it
stands for, so its rows line up with the statement rows the other backends
record for tpcc.py.

How the SQL maps
----------------
* ``col = col + x`` reads the old triple, computes with ``Eval``, casts the
  result back to the property's type (``Eval`` yields xsd:decimal) and
  swaps the triple, all in one query and one commit.
* ``CASE WHEN`` is an ``Or`` of the two guarded branches.
* ``MIN``/``MAX`` are ``OrderBy`` + ``Limit 1``; ``SUM``/``AVG``/``COUNT``
  are ``GroupBy`` into a list followed by ``Sum``/``Length``. A query with
  two aggregates groups twice, once per aggregated column: a grouped list
  holds one value per row and WOQL has no way to sum one component of it.
* A join is a shared variable between triple patterns. The tables are the
  relational schema as is (one flat document per row, foreign keys as
  plain values), so joins are value joins: there are no link properties
  for TerminusDB to follow.
* ``IN (subquery)`` is a join under ``Distinct``.
* WOQL runs a conjunction in the order it is written and has no ordered
  index: each query starts from its most selective equality (an index
  lookup on a property value), and a small integer range is written as an
  ``Or`` of its values instead of a scan with two comparisons.

Every write request is a commit and there are no multi-request
transactions, so a "transaction" here is a sequence of commits that
another client can interleave with, where the SQL backends run the same
statements in autocommit mode.
"""

import time

from dblib.db_api import OpStatus, Ref, _SessionBase
from dblib.systems.terminusdb.adapter import (
    _and, _compare, _lit, _node, _not, _or, _plain, _triple, _var,
)
from macrobench.datagen.ch import CHScale, DISTRICTS_PER_WAREHOUSE
from macrobench.tpcc import _now

# --------------------------------------------------------------------------
# Session and WOQL building blocks
# --------------------------------------------------------------------------


class NativeSession(_SessionBase):
    """Runs native requests on one ref and records each as the SQL
    statement it replaces."""

    def __init__(self, suite, ref, label: str = ""):
        ref = Ref.parse(ref)
        suite.exec([], refs=[ref], timed=False)   # point the connection at ref
        super().__init__(suite, suite.conn, ref, [ref],
                         suite.result_collector.next_exec_id(), True, label, 0)
        self.catalog = suite.conn.catalog

    def run(self, sql: str, request, rows=lambda value: 1 if value is not None else 0):
        """Time ``request()`` and record it under ``sql``."""
        value, error, status = None, "", OpStatus.OK
        start_wall, start = time.time(), time.perf_counter()
        try:
            value = request()
            return value
        except Exception as e:
            status, error = OpStatus.FAILED, f"{type(e).__name__}: {e}"
            raise
        finally:
            latency = time.perf_counter() - start
            touched = rows(value) if status == OpStatus.OK else 0
            self._record(sql, None, status, latency, start_wall, time.time(), value, error, touched)

    # -- requests -----------------------------------------------------------

    def get(self, sql: str, table: str, **key):
        return self.run(sql, lambda: self.conn.get_row(table, key))

    def insert(self, sql: str, table: str, **row):
        return self.run(sql, lambda: self.conn.insert_row(table, row) or 1)

    def delete(self, sql: str, table: str, **key):
        return self.run(sql, lambda: self.conn.delete_row(table, key) or 1)

    def read(self, sql: str, query: dict) -> list:
        return self.run(sql, lambda: self.conn.woql(query), len)

    def write(self, sql: str, query: dict) -> list:
        return self.run(sql, lambda: self.conn.woql(query, write=True), len)

    # -- typed pieces -------------------------------------------------------

    def lit(self, table: str, column: str, value) -> dict:
        return _lit(self.catalog.table(table).xsd[column], value)

    def node(self, table: str, **key) -> dict:
        """The document of one row, addressed by primary key."""
        return _node(self.catalog.table(table).doc_id(key))

    def match(self, doc: dict, table: str, **equals) -> list:
        """Triples that bind ``doc`` to the ``table`` rows whose columns
        equal the given values. WOQL evaluates a conjunction in the order
        written and does not reorder it, so callers name the most
        selective column first and the class check comes last."""
        parts = [_triple(doc, c, self.lit(table, c, v)) for c, v in equals.items()]
        parts.append(_triple(doc, "rdf:type", _node(f"@schema:{table}", "Value")))
        return parts

    def add(self, doc: dict, table: str, column: str, amount, tag: str) -> list:
        """``column = column + amount``."""
        xsd = self.catalog.table(table).xsd[column]
        old, raw, new = f"old_{tag}", f"raw_{tag}", f"new_{tag}"
        return [
            _triple(doc, column, _var(old)),
            _eval(_plus(_arith(old), _number(amount)), raw),
            _cast(raw, xsd, new),
            _triple(doc, column, _var(old), "DeleteTriple"),
            _triple(doc, column, _var(new), "AddTriple"),
        ]

    def assign(self, doc: dict, table: str, column: str, value, tag: str) -> list:
        """``column = value`` (a NULL value only removes the old one)."""
        old = f"old_{tag}"
        parts = [_optional(_and([_triple(doc, column, _var(old)),
                                 _triple(doc, column, _var(old), "DeleteTriple")]))]
        if value is not None:
            parts.append(_triple(doc, column, self.lit(table, column, value), "AddTriple"))
        return parts


def _doc(name: str) -> dict:
    return _var(name, "NodeValue")


def _arith(name: str) -> dict:
    return {"@type": "ArithmeticValue", "variable": name}


def _number(value) -> dict:
    xsd = "xsd:integer" if isinstance(value, int) else "xsd:decimal"
    return {"@type": "ArithmeticValue", "data": {"@type": xsd, "@value": value}}


def _plus(left: dict, right: dict) -> dict:
    return {"@type": "Plus", "left": left, "right": right}


def _minus(left: dict, right: dict) -> dict:
    return {"@type": "Minus", "left": left, "right": right}


def _divide(left: dict, right: dict) -> dict:
    return {"@type": "Divide", "left": left, "right": right}


def _eval(expression: dict, result: str) -> dict:
    return {"@type": "Eval", "expression": expression, "result": _arith(result)}


def _cast(source: str, xsd: str, result: str) -> dict:
    return {"@type": "Typecast", "value": _var(source), "type": _node(xsd), "result": _var(result)}


def _optional(query: dict) -> dict:
    return {"@type": "Optional", "query": query}


def _select(variables: list, query: dict) -> dict:
    return {"@type": "Select", "variables": variables, "query": query}


def _limit(n: int, query: dict) -> dict:
    return {"@type": "Limit", "limit": n, "query": query}


def _order_by(ordering: list, query: dict) -> dict:
    """``ordering`` is a list of (variable, "asc" | "desc")."""
    return {"@type": "OrderBy", "query": query, "ordering": [
        {"@type": "OrderTemplate", "variable": v, "order": o} for v, o in ordering]}


def _group_by(keys: list, value: str, grouped: str, query: dict) -> dict:
    """Collect ``value`` into the list ``grouped``, one list per distinct
    binding of ``keys`` (one list overall when ``keys`` is empty)."""
    return {"@type": "GroupBy", "group_by": keys, "template": _var(value),
            "grouped": _var(grouped), "query": query}


def _sum(items: str, result: str) -> dict:
    return {"@type": "Sum", "list": _var(items, "DataValue"), "result": _var(result, "DataValue")}


def _length(items: str, result: str) -> dict:
    return {"@type": "Length", "list": _var(items, "DataValue"), "length": _var(result, "DataValue")}


def _count(result: str, query: dict) -> dict:
    return {"@type": "Count", "count": _var(result, "DataValue"), "query": query}


def _distinct(variables: list, query: dict) -> dict:
    return {"@type": "Distinct", "variables": variables, "query": query}


def _values(bindings: list, *names) -> list:
    """Bindings as tuples of plain values, in ``names`` order."""
    return [tuple(_plain(b.get(n)) for n in names) for b in bindings]


# --------------------------------------------------------------------------
# Transactions (same signatures and return values as macrobench/tpcc.py)
# --------------------------------------------------------------------------


def new_order(db: NativeSession, rng, scale: CHScale, w=None, d=None, c=None, n_items=None):
    w = w or rng.randint(1, scale.warehouses)
    d = d or rng.randint(1, DISTRICTS_PER_WAREHOUSE)
    c = c or rng.randint(1, scale.customers_per_district)
    n_items = n_items or rng.randint(5, 15)
    district = db.node("district", d_w_id=w, d_id=d)
    db.write("UPDATE district SET d_next_o_id = d_next_o_id + 1 WHERE d_w_id = %s AND d_id = %s",
             _and(db.add(district, "district", "d_next_o_id", 1, "n")))
    row = db.get("SELECT d_next_o_id, d_tax FROM district WHERE d_w_id = %s AND d_id = %s",
                 "district", d_w_id=w, d_id=d)
    o_id = int(row["d_next_o_id"]) - 1
    entry = _now()
    db.insert("INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, "
              "o_ol_cnt, o_all_local) VALUES (%s, %s, %s, %s, %s, NULL, %s, 1)",
              "orders", o_id=o_id, o_d_id=d, o_w_id=w, o_c_id=c, o_entry_d=entry,
              o_carrier_id=None, o_ol_cnt=n_items, o_all_local=1)
    db.insert("INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES (%s, %s, %s)",
              "new_order", no_o_id=o_id, no_d_id=d, no_w_id=w)
    total = 0.0
    for n in range(1, n_items + 1):
        i_id = rng.randint(1, scale.items)
        qty = rng.randint(1, 10)
        item = db.get("SELECT i_price FROM item WHERE i_id = %s", "item", i_id=i_id)
        price = float(item["i_price"]) if item else 1.0
        stock = db.node("stock", s_w_id=w, s_i_id=i_id)
        db.write(
            "UPDATE stock SET s_quantity = CASE WHEN s_quantity - %s >= 10 "
            "THEN s_quantity - %s ELSE s_quantity - %s + 91 END, "
            "s_ytd = s_ytd + %s, s_order_cnt = s_order_cnt + 1 "
            "WHERE s_w_id = %s AND s_i_id = %s",
            _and([
                _triple(stock, "s_quantity", _var("old_q")),
                _eval(_minus(_arith("old_q"), _number(qty)), "left"),
                _or([
                    _and([_not(_compare("<", _var("left"), _lit("xsd:integer", 10))),
                          _eval(_arith("left"), "raw_q")]),
                    _and([_compare("<", _var("left"), _lit("xsd:integer", 10)),
                          _eval(_plus(_arith("left"), _number(91)), "raw_q")]),
                ]),
                _cast("raw_q", "xsd:integer", "new_q"),
                _triple(stock, "s_quantity", _var("old_q"), "DeleteTriple"),
                _triple(stock, "s_quantity", _var("new_q"), "AddTriple"),
                *db.add(stock, "stock", "s_ytd", qty, "y"),
                *db.add(stock, "stock", "s_order_cnt", 1, "c"),
            ]))
        amount = round(qty * price, 2)
        total += amount
        db.insert("INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, "
                  "ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info) "
                  "VALUES (%s, %s, %s, %s, %s, %s, NULL, %s, %s, %s)",
                  "order_line", ol_o_id=o_id, ol_d_id=d, ol_w_id=w, ol_number=n, ol_i_id=i_id,
                  ol_supply_w_id=w, ol_delivery_d=None, ol_quantity=qty, ol_amount=amount,
                  ol_dist_info="x" * 24)
    return {"o_id": o_id, "w": w, "d": d, "total": round(total, 2)}


def payment(db: NativeSession, rng, scale: CHScale, w=None, d=None, c=None, amount=None,
            touch_c_data=False):
    w = w or rng.randint(1, scale.warehouses)
    d = d or rng.randint(1, DISTRICTS_PER_WAREHOUSE)
    c = c or rng.randint(1, scale.customers_per_district)
    amount = amount or round(rng.uniform(1, 5000), 2)
    db.write("UPDATE warehouse SET w_ytd = w_ytd + %s WHERE w_id = %s",
             _and(db.add(db.node("warehouse", w_id=w), "warehouse", "w_ytd", amount, "w")))
    db.write("UPDATE district SET d_ytd = d_ytd + %s WHERE d_w_id = %s AND d_id = %s",
             _and(db.add(db.node("district", d_w_id=w, d_id=d), "district", "d_ytd", amount, "d")))
    customer = db.node("customer", c_w_id=w, c_d_id=d, c_id=c)
    parts = [
        *db.add(customer, "customer", "c_balance", -amount, "b"),
        *db.add(customer, "customer", "c_ytd_payment", amount, "y"),
        *db.add(customer, "customer", "c_payment_cnt", 1, "p"),
    ]
    if touch_c_data:
        parts += db.assign(customer, "customer", "c_data", f"payment {amount} at {_now()}", "t")
        db.write("UPDATE customer SET c_balance = c_balance - %s, "
                 "c_ytd_payment = c_ytd_payment + %s, c_payment_cnt = c_payment_cnt + 1, "
                 "c_data = %s WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s", _and(parts))
    else:
        db.write("UPDATE customer SET c_balance = c_balance - %s, "
                 "c_ytd_payment = c_ytd_payment + %s, c_payment_cnt = c_payment_cnt + 1 "
                 "WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s", _and(parts))
    db.insert("INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, "
              "h_amount, h_data) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
              "history", h_c_id=c, h_c_d_id=d, h_c_w_id=w, h_d_id=d, h_w_id=w, h_date=_now(),
              h_amount=amount, h_data="payment")
    return {"w": w, "d": d, "c": c, "amount": amount}


def delivery(db: NativeSession, rng, scale: CHScale, w=None, carrier=None):
    w = w or rng.randint(1, scale.warehouses)
    carrier = carrier or rng.randint(1, 10)
    delivered = 0
    for d in range(1, DISTRICTS_PER_WAREHOUSE + 1):
        pending = _doc("Pending")
        found = db.read(
            "SELECT MIN(no_o_id) FROM new_order WHERE no_w_id = %s AND no_d_id = %s",
            _limit(1, _order_by([("O", "asc")], _select(["O"], _and([
                *db.match(pending, "new_order", no_d_id=d, no_w_id=w),
                _triple(pending, "no_o_id", _var("O")),
            ])))))
        if not found:
            continue
        o_id = int(_plain(found[0]["O"]))
        db.delete("DELETE FROM new_order WHERE no_w_id = %s AND no_d_id = %s AND no_o_id = %s",
                  "new_order", no_w_id=w, no_d_id=d, no_o_id=o_id)
        order = db.node("orders", o_w_id=w, o_d_id=d, o_id=o_id)
        db.write("UPDATE orders SET o_carrier_id = %s WHERE o_w_id = %s AND o_d_id = %s AND o_id = %s",
                 _and(db.assign(order, "orders", "o_carrier_id", carrier, "c")))
        line = _doc("Line")
        db.write("UPDATE order_line SET ol_delivery_d = %s "
                 "WHERE ol_w_id = %s AND ol_d_id = %s AND ol_o_id = %s",
                 _and([*db.match(line, "order_line", ol_o_id=o_id, ol_d_id=d, ol_w_id=w),
                       *db.assign(line, "order_line", "ol_delivery_d", _now(), "d")]))
        row = db.get("SELECT o_c_id FROM orders WHERE o_w_id = %s AND o_d_id = %s AND o_id = %s",
                     "orders", o_w_id=w, o_d_id=d, o_id=o_id)
        c = row["o_c_id"]
        summed = db.read(
            "SELECT SUM(ol_amount) FROM order_line "
            "WHERE ol_w_id = %s AND ol_d_id = %s AND ol_o_id = %s",
            _select(["Total"], _and([
                _group_by([], "Amount", "Amounts", _and([
                    *db.match(line, "order_line", ol_o_id=o_id, ol_d_id=d, ol_w_id=w),
                    _triple(line, "ol_amount", _var("Amount")),
                ])),
                _sum("Amounts", "Total"),
            ])))
        total = (_plain(summed[0]["Total"]) if summed else 0) or 0
        customer = db.node("customer", c_w_id=w, c_d_id=d, c_id=c)
        db.write("UPDATE customer SET c_balance = c_balance + %s, c_delivery_cnt = c_delivery_cnt + 1 "
                 "WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s",
                 _and([*db.add(customer, "customer", "c_balance", total, "b"),
                       *db.add(customer, "customer", "c_delivery_cnt", 1, "n")]))
        delivered += 1
    return {"w": w, "delivered": delivered}


def order_status(db: NativeSession, rng, scale: CHScale, w=None, d=None, c=None):
    w = w or rng.randint(1, scale.warehouses)
    d = d or rng.randint(1, DISTRICTS_PER_WAREHOUSE)
    c = c or rng.randint(1, scale.customers_per_district)
    db.get("SELECT c_first, c_middle, c_last, c_balance FROM customer "
           "WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s",
           "customer", c_w_id=w, c_d_id=d, c_id=c)
    order = _doc("Order")
    found = db.read(
        "SELECT MAX(o_id) FROM orders WHERE o_w_id = %s AND o_d_id = %s AND o_c_id = %s",
        _limit(1, _order_by([("O", "desc")], _select(["O"], _and([
            *db.match(order, "orders", o_c_id=c, o_d_id=d, o_w_id=w),
            _triple(order, "o_id", _var("O")),
        ])))))
    o_id = int(_plain(found[0]["O"])) if found else None
    if o_id is not None:
        line = _doc("Line")
        columns = ["ol_i_id", "ol_supply_w_id", "ol_quantity", "ol_amount", "ol_delivery_d"]
        db.read("SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d "
                "FROM order_line WHERE ol_w_id = %s AND ol_d_id = %s AND ol_o_id = %s",
                _select(columns, _and([
                    *db.match(line, "order_line", ol_o_id=o_id, ol_d_id=d, ol_w_id=w),
                    *[_triple(line, col, _var(col)) for col in columns[:-1]],
                    _optional(_triple(line, "ol_delivery_d", _var("ol_delivery_d"))),
                ])))
    return {"w": w, "d": d, "c": c, "o_id": o_id}


def stock_level(db: NativeSession, rng, scale: CHScale, w=None, d=None, threshold=None):
    w = w or rng.randint(1, scale.warehouses)
    d = d or rng.randint(1, DISTRICTS_PER_WAREHOUSE)
    threshold = threshold or rng.randint(10, 20)
    row = db.get("SELECT d_next_o_id FROM district WHERE d_w_id = %s AND d_id = %s",
                 "district", d_w_id=w, d_id=d)
    nxt = int(row["d_next_o_id"])
    line, stock = _doc("Line"), _doc("Stock")
    counted = db.read(
        "SELECT COUNT(*) FROM stock s WHERE s.s_w_id = %s AND s.s_quantity < %s "
        "AND s.s_i_id IN (SELECT ol_i_id FROM order_line "
        "WHERE ol_w_id = %s AND ol_d_id = %s AND ol_o_id >= %s AND ol_o_id < %s)",
        _count("N", _distinct(["Item"], _and([
            _or([_triple(line, "ol_o_id", _lit("xsd:integer", o)) for o in range(nxt - 20, nxt)]),
            *db.match(line, "order_line", ol_d_id=d, ol_w_id=w),
            _triple(line, "ol_i_id", _var("Item")),
            _triple(stock, "s_i_id", _var("Item")),
            *db.match(stock, "stock", s_w_id=w),
            _triple(stock, "s_quantity", _var("Quantity")),
            _compare("<", _var("Quantity"), _lit("xsd:integer", threshold)),
        ]))))
    return {"w": w, "d": d, "low_stock": int(_plain(counted[0]["N"])) if counted else 0}


TRANSACTIONS = {
    "new_order": new_order,
    "payment": payment,
    "delivery": delivery,
    "order_status": order_status,
    "stock_level": stock_level,
}


# --------------------------------------------------------------------------
# CH analytical queries
# --------------------------------------------------------------------------


def _delivered_after(db: NativeSession, line: dict, since) -> list:
    return [_triple(line, "rdf:type", _node("@schema:order_line", "Value")),
            _triple(line, "ol_delivery_d", _var("Delivered")),
            _compare(">", _var("Delivered"), db.lit("order_line", "ol_delivery_d", since))]


def ch_q1(db: NativeSession, since):
    line = _doc("Line")

    def per_number(column: str, value: str, grouped: str) -> dict:
        return _group_by(["Number"], value, grouped, _and([
            *_delivered_after(db, line, since),
            _triple(line, "ol_number", _var("Number")),
            _triple(line, column, _var(value)),
        ]))

    out = ["Number", "SumQuantity", "SumAmount", "AvgQuantity", "AvgAmount", "Count"]
    rows = db.read(
        "SELECT ol_number, SUM(ol_quantity), SUM(ol_amount), AVG(ol_quantity), "
        "AVG(ol_amount), COUNT(*) FROM order_line WHERE ol_delivery_d > %s "
        "GROUP BY ol_number ORDER BY ol_number",
        _order_by([("Number", "asc")], _select(out, _and([
            per_number("ol_quantity", "Quantity", "Quantities"),
            per_number("ol_amount", "Amount", "Amounts"),
            _sum("Quantities", "SumQuantity"),
            _sum("Amounts", "SumAmount"),
            _length("Quantities", "Count"),
            _eval(_divide(_arith("SumQuantity"), _arith("Count")), "AvgQuantity"),
            _eval(_divide(_arith("SumAmount"), _arith("Count")), "AvgAmount"),
        ]))))
    return _values(rows, *out)


def ch_q3(db: NativeSession, since):
    customer, pending, order, line = _doc("Customer"), _doc("Pending"), _doc("Order"), _doc("Line")
    out = ["O", "W", "D", "Revenue", "Entry"]
    rows = db.read(
        "SELECT ol.ol_o_id, ol.ol_w_id, ol.ol_d_id, SUM(ol.ol_amount) AS revenue, o.o_entry_d "
        "FROM customer c JOIN new_order no ON no.no_w_id = c.c_w_id AND no.no_d_id = c.c_d_id "
        "JOIN orders o ON o.o_w_id = no.no_w_id AND o.o_d_id = no.no_d_id AND o.o_id = no.no_o_id "
        "AND o.o_c_id = c.c_id "
        "JOIN order_line ol ON ol.ol_w_id = o.o_w_id AND ol.ol_d_id = o.o_d_id AND ol.ol_o_id = o.o_id "
        "WHERE c.c_state >= 'A' AND o.o_entry_d > %s "
        "GROUP BY ol.ol_o_id, ol.ol_w_id, ol.ol_d_id, o.o_entry_d "
        "ORDER BY revenue DESC, o.o_entry_d LIMIT 100",
        _limit(100, _order_by([("Revenue", "desc"), ("Entry", "asc")], _select(out, _and([
            _group_by(["O", "W", "D", "Entry"], "Amount", "Amounts", _and([
                _triple(pending, "rdf:type", _node("@schema:new_order", "Value")),
                _triple(pending, "no_o_id", _var("O")),
                _triple(pending, "no_w_id", _var("W")),
                _triple(pending, "no_d_id", _var("D")),
                _triple(order, "o_id", _var("O")),
                _triple(order, "o_d_id", _var("D")),
                _triple(order, "o_w_id", _var("W")),
                _triple(order, "rdf:type", _node("@schema:orders", "Value")),
                _triple(order, "o_entry_d", _var("Entry")),
                _compare(">", _var("Entry"), db.lit("orders", "o_entry_d", since)),
                _triple(order, "o_c_id", _var("C")),
                _triple(customer, "c_id", _var("C")),
                _triple(customer, "c_d_id", _var("D")),
                _triple(customer, "c_w_id", _var("W")),
                _triple(customer, "rdf:type", _node("@schema:customer", "Value")),
                _triple(customer, "c_state", _var("State")),
                _compare(">=", _var("State"), db.lit("customer", "c_state", "A")),
                _triple(line, "ol_o_id", _var("O")),
                _triple(line, "ol_d_id", _var("D")),
                _triple(line, "ol_w_id", _var("W")),
                _triple(line, "rdf:type", _node("@schema:order_line", "Value")),
                _triple(line, "ol_amount", _var("Amount")),
            ])),
            _sum("Amounts", "Revenue"),
        ])))))
    return _values(rows, *out)


def ch_q6(db: NativeSession, since, until):
    line = _doc("Line")
    rows = db.read(
        "SELECT SUM(ol_amount) FROM order_line WHERE ol_delivery_d >= %s "
        "AND ol_delivery_d < %s AND ol_quantity BETWEEN 1 AND 100000",
        _select(["Total"], _and([
            _group_by([], "Amount", "Amounts", _and([
                _triple(line, "rdf:type", _node("@schema:order_line", "Value")),
                _triple(line, "ol_delivery_d", _var("Delivered")),
                _compare(">=", _var("Delivered"), db.lit("order_line", "ol_delivery_d", since)),
                _compare("<", _var("Delivered"), db.lit("order_line", "ol_delivery_d", until)),
                _triple(line, "ol_quantity", _var("Quantity")),
                _compare(">=", _var("Quantity"), _lit("xsd:integer", 1)),
                _compare("<=", _var("Quantity"), _lit("xsd:integer", 100000)),
                _triple(line, "ol_amount", _var("Amount")),
            ])),
            _sum("Amounts", "Total"),
        ])))
    return _values(rows, "Total")


def ch_q12(db: NativeSession, since):
    order, line = _doc("Order"), _doc("Line")
    one, two = _lit("xsd:integer", 1), _lit("xsd:integer", 2)
    zero_flag, one_flag = _lit("xsd:integer", 0), _lit("xsd:integer", 1)

    def per_count(flag: str, grouped: str) -> dict:
        # One (High, Low) pair per joined row; an order without a carrier
        # counts for neither, as the SQL CASE does for NULL.
        return _group_by(["LineCount"], flag, grouped, _and([
            *_delivered_after(db, line, since),
            _triple(line, "ol_o_id", _var("O")),
            _triple(line, "ol_d_id", _var("D")),
            _triple(line, "ol_w_id", _var("W")),
            _triple(order, "o_id", _var("O")),
            _triple(order, "o_d_id", _var("D")),
            _triple(order, "o_w_id", _var("W")),
            _triple(order, "rdf:type", _node("@schema:orders", "Value")),
            _triple(order, "o_entry_d", _var("Entry")),
            _compare("<=", _var("Entry"), _var("Delivered")),
            _triple(order, "o_ol_cnt", _var("LineCount")),
            _or([
                _and([_triple(order, "o_carrier_id", _var("Carrier")),
                      _or([_compare("=", _var("Carrier"), one), _compare("=", _var("Carrier"), two)]),
                      _compare("=", _var("High"), one_flag), _compare("=", _var("Low"), zero_flag)]),
                _and([_triple(order, "o_carrier_id", _var("Carrier")),
                      _compare("<>", _var("Carrier"), one), _compare("<>", _var("Carrier"), two),
                      _compare("=", _var("High"), zero_flag), _compare("=", _var("Low"), one_flag)]),
                _and([_not(_triple(order, "o_carrier_id", _var("Carrier"))),
                      _compare("=", _var("High"), zero_flag), _compare("=", _var("Low"), zero_flag)]),
            ]),
        ]))

    out = ["LineCount", "HighLine", "LowLine"]
    rows = db.read(
        "SELECT o.o_ol_cnt, "
        "SUM(CASE WHEN o.o_carrier_id = 1 OR o.o_carrier_id = 2 THEN 1 ELSE 0 END) AS high_line, "
        "SUM(CASE WHEN o.o_carrier_id <> 1 AND o.o_carrier_id <> 2 THEN 1 ELSE 0 END) AS low_line "
        "FROM orders o JOIN order_line ol ON ol.ol_w_id = o.o_w_id AND ol.ol_d_id = o.o_d_id "
        "AND ol.ol_o_id = o.o_id WHERE o.o_entry_d <= ol.ol_delivery_d AND ol.ol_delivery_d > %s "
        "GROUP BY o.o_ol_cnt ORDER BY o.o_ol_cnt",
        _order_by([("LineCount", "asc")], _select(out, _and([
            per_count("High", "Highs"),
            per_count("Low", "Lows"),
            _sum("Highs", "HighLine"),
            _sum("Lows", "LowLine"),
        ]))))
    return _values(rows, *out)


def ch_q18(db: NativeSession, since):
    customer, order, line = _doc("Customer"), _doc("Order"), _doc("Line")
    out = ["Last", "C", "O", "Entry", "LineCount", "Total"]
    rows = db.read(
        "SELECT c.c_last, c.c_id, o.o_id, o.o_entry_d, o.o_ol_cnt, SUM(ol.ol_amount) AS total "
        "FROM customer c JOIN orders o ON o.o_w_id = c.c_w_id AND o.o_d_id = c.c_d_id AND o.o_c_id = c.c_id "
        "JOIN order_line ol ON ol.ol_w_id = o.o_w_id AND ol.ol_d_id = o.o_d_id AND ol.ol_o_id = o.o_id "
        "WHERE o.o_entry_d > %s "
        "GROUP BY c.c_last, c.c_id, o.o_id, o.o_entry_d, o.o_ol_cnt "
        "HAVING SUM(ol.ol_amount) > 200 ORDER BY total DESC, o.o_entry_d LIMIT 100",
        _limit(100, _order_by([("Total", "desc"), ("Entry", "asc")], _select(out, _and([
            _group_by(["Last", "C", "O", "Entry", "LineCount"], "Amount", "Amounts", _and([
                _triple(order, "rdf:type", _node("@schema:orders", "Value")),
                _triple(order, "o_entry_d", _var("Entry")),
                _compare(">", _var("Entry"), db.lit("orders", "o_entry_d", since)),
                _triple(order, "o_id", _var("O")),
                _triple(order, "o_d_id", _var("D")),
                _triple(order, "o_w_id", _var("W")),
                _triple(order, "o_c_id", _var("C")),
                _triple(order, "o_ol_cnt", _var("LineCount")),
                _triple(customer, "c_id", _var("C")),
                _triple(customer, "c_d_id", _var("D")),
                _triple(customer, "c_w_id", _var("W")),
                _triple(customer, "rdf:type", _node("@schema:customer", "Value")),
                _triple(customer, "c_last", _var("Last")),
                _triple(line, "ol_o_id", _var("O")),
                _triple(line, "ol_d_id", _var("D")),
                _triple(line, "ol_w_id", _var("W")),
                _triple(line, "rdf:type", _node("@schema:order_line", "Value")),
                _triple(line, "ol_amount", _var("Amount")),
            ])),
            _sum("Amounts", "Total"),
            _compare(">", _var("Total"), _lit("xsd:decimal", 200)),
        ])))))
    return _values(rows, *out)


QUERIES = {"q1": ch_q1, "q3": ch_q3, "q6": ch_q6, "q12": ch_q12, "q18": ch_q18}
