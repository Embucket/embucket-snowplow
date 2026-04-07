#!/usr/bin/env python3
"""Run all 22 TPC-H queries against Embucket Lambda and report results.

Usage:
    python scripts/tpch/run_tpch_queries.py <lambda_function_arn>

Assumes TPC-H tables have been loaded into demo.tpch schema
via load_tpch_data.py.
"""
import boto3
import json
import sys
import time
import uuid

# All 22 standard TPC-H queries against demo.tpch.* tables
TPCH_QUERIES = [
    (
        "Q1",
        """
        SELECT
            l_returnflag,
            l_linestatus,
            SUM(l_quantity) AS sum_qty,
            SUM(l_extendedprice) AS sum_base_price,
            SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
            SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
            AVG(l_quantity) AS avg_qty,
            AVG(l_extendedprice) AS avg_price,
            AVG(l_discount) AS avg_disc,
            COUNT(*) AS count_order
        FROM demo.tpch.lineitem
        WHERE l_shipdate <= DATEADD(day, -116, '1998-12-01')
        GROUP BY l_returnflag, l_linestatus
        ORDER BY l_returnflag, l_linestatus
        """,
    ),
    (
        "Q2",
        """
        SELECT
            s_acctbal, s_name, n_name, p_partkey, p_mfgr,
            s_address, s_phone, s_comment
        FROM
            demo.tpch.part, demo.tpch.supplier, demo.tpch.partsupp,
            demo.tpch.nation, demo.tpch.region
        WHERE
            p_partkey = ps_partkey AND s_suppkey = ps_suppkey
            AND p_size = 15 AND p_type LIKE '%BRASS'
            AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
            AND r_name = 'EUROPE'
            AND ps_supplycost = (
                SELECT MIN(ps_supplycost)
                FROM demo.tpch.partsupp, demo.tpch.supplier,
                     demo.tpch.nation, demo.tpch.region
                WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey
                    AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                    AND r_name = 'EUROPE'
            )
        ORDER BY s_acctbal DESC, n_name, s_name, p_partkey
        LIMIT 100
        """,
    ),
    (
        "Q3",
        """
        SELECT
            l_orderkey,
            SUM(l_extendedprice * (1 - l_discount)) AS revenue,
            o_orderdate, o_shippriority
        FROM demo.tpch.customer, demo.tpch.orders, demo.tpch.lineitem
        WHERE
            c_mktsegment = 'BUILDING'
            AND c_custkey = o_custkey AND l_orderkey = o_orderkey
            AND o_orderdate < DATE '1995-03-15'
            AND l_shipdate > DATE '1995-03-15'
        GROUP BY l_orderkey, o_orderdate, o_shippriority
        ORDER BY revenue DESC, o_orderdate
        LIMIT 10
        """,
    ),
    (
        "Q4",
        """
        SELECT o_orderpriority, COUNT(*) AS order_count
        FROM demo.tpch.orders
        WHERE
            o_orderdate >= DATE '1993-07-01'
            AND o_orderdate < DATEADD(month, 3, '1993-07-01')
            AND EXISTS (
                SELECT * FROM demo.tpch.lineitem
                WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate
            )
        GROUP BY o_orderpriority
        ORDER BY o_orderpriority
        """,
    ),
    (
        "Q5",
        """
        SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue
        FROM
            demo.tpch.customer, demo.tpch.orders, demo.tpch.lineitem,
            demo.tpch.supplier, demo.tpch.nation, demo.tpch.region
        WHERE
            c_custkey = o_custkey AND l_orderkey = o_orderkey
            AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
            AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
            AND r_name = 'ASIA'
            AND o_orderdate >= DATE '1994-01-01'
            AND o_orderdate < DATEADD(year, 1, '1994-01-01')
        GROUP BY n_name
        ORDER BY revenue DESC
        """,
    ),
    (
        "Q6",
        """
        SELECT SUM(l_extendedprice * l_discount) AS revenue
        FROM demo.tpch.lineitem
        WHERE
            l_shipdate >= DATE '1994-01-01'
            AND l_shipdate < DATEADD(year, 1, '1994-01-01')
            AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
            AND l_quantity < 24
        """,
    ),
    (
        "Q7",
        """
        SELECT supp_nation, cust_nation, l_year, SUM(volume) AS revenue
        FROM (
            SELECT
                n1.n_name AS supp_nation, n2.n_name AS cust_nation,
                EXTRACT(year FROM l_shipdate) AS l_year,
                l_extendedprice * (1 - l_discount) AS volume
            FROM
                demo.tpch.supplier, demo.tpch.lineitem, demo.tpch.orders,
                demo.tpch.customer, demo.tpch.nation n1, demo.tpch.nation n2
            WHERE
                s_suppkey = l_suppkey AND o_orderkey = l_orderkey
                AND c_custkey = o_custkey
                AND s_nationkey = n1.n_nationkey AND c_nationkey = n2.n_nationkey
                AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
                    OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
                AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
        ) AS shipping
        GROUP BY supp_nation, cust_nation, l_year
        ORDER BY supp_nation, cust_nation, l_year
        """,
    ),
    (
        "Q8",
        """
        SELECT o_year,
            SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END)
                / SUM(volume) AS mkt_share
        FROM (
            SELECT
                EXTRACT(year FROM o_orderdate) AS o_year,
                l_extendedprice * (1 - l_discount) AS volume,
                n2.n_name AS nation
            FROM
                demo.tpch.part, demo.tpch.supplier, demo.tpch.lineitem,
                demo.tpch.orders, demo.tpch.customer,
                demo.tpch.nation n1, demo.tpch.nation n2, demo.tpch.region
            WHERE
                p_partkey = l_partkey AND s_suppkey = l_suppkey
                AND l_orderkey = o_orderkey AND o_custkey = c_custkey
                AND c_nationkey = n1.n_nationkey AND n1.n_regionkey = r_regionkey
                AND r_name = 'AMERICA' AND s_nationkey = n2.n_nationkey
                AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
                AND p_type = 'ECONOMY ANODIZED STEEL'
        ) AS all_nations
        GROUP BY o_year
        ORDER BY o_year
        """,
    ),
    (
        "Q9",
        """
        SELECT nation, o_year, SUM(amount) AS sum_profit
        FROM (
            SELECT
                n_name AS nation,
                EXTRACT(year FROM o_orderdate) AS o_year,
                l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
            FROM
                demo.tpch.part, demo.tpch.supplier, demo.tpch.lineitem,
                demo.tpch.partsupp, demo.tpch.orders, demo.tpch.nation
            WHERE
                s_suppkey = l_suppkey AND ps_suppkey = l_suppkey
                AND ps_partkey = l_partkey AND p_partkey = l_partkey
                AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey
                AND p_name LIKE '%green%'
        ) AS profit
        GROUP BY nation, o_year
        ORDER BY nation, o_year DESC
        """,
    ),
    (
        "Q10",
        """
        SELECT
            c_custkey, c_name,
            SUM(l_extendedprice * (1 - l_discount)) AS revenue,
            c_acctbal, n_name, c_address, c_phone, c_comment
        FROM
            demo.tpch.customer, demo.tpch.orders,
            demo.tpch.lineitem, demo.tpch.nation
        WHERE
            c_custkey = o_custkey AND l_orderkey = o_orderkey
            AND o_orderdate >= DATE '1993-10-01'
            AND o_orderdate < DATEADD(month, 3, '1993-10-01')
            AND l_returnflag = 'R' AND c_nationkey = n_nationkey
        GROUP BY c_custkey, c_name, c_acctbal, n_name, c_address, c_phone, c_comment
        ORDER BY revenue DESC
        LIMIT 20
        """,
    ),
    (
        "Q11",
        """
        SELECT ps_partkey, SUM(ps_supplycost * ps_availqty) AS value
        FROM demo.tpch.partsupp, demo.tpch.supplier, demo.tpch.nation
        WHERE
            ps_suppkey = s_suppkey AND s_nationkey = n_nationkey
            AND n_name = 'GERMANY'
        GROUP BY ps_partkey
        HAVING SUM(ps_supplycost * ps_availqty) > (
            SELECT SUM(ps_supplycost * ps_availqty) * 0.0001
            FROM demo.tpch.partsupp, demo.tpch.supplier, demo.tpch.nation
            WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey
                AND n_name = 'GERMANY'
        )
        ORDER BY value DESC
        """,
    ),
    (
        "Q12",
        """
        SELECT
            l_shipmode,
            SUM(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH'
                THEN 1 ELSE 0 END) AS high_line_count,
            SUM(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH'
                THEN 1 ELSE 0 END) AS low_line_count
        FROM demo.tpch.orders, demo.tpch.lineitem
        WHERE
            o_orderkey = l_orderkey
            AND l_shipmode IN ('MAIL', 'SHIP')
            AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate
            AND l_receiptdate >= DATE '1994-01-01'
            AND l_receiptdate < DATEADD(year, 1, '1994-01-01')
        GROUP BY l_shipmode
        ORDER BY l_shipmode
        """,
    ),
    (
        "Q13",
        """
        SELECT c_count, COUNT(*) AS custdist
        FROM (
            SELECT c_custkey, COUNT(o_orderkey) AS c_count
            FROM demo.tpch.customer LEFT OUTER JOIN demo.tpch.orders
                ON c_custkey = o_custkey
                AND o_comment NOT LIKE '%special%requests%'
            GROUP BY c_custkey
        ) AS c_orders
        GROUP BY c_count
        ORDER BY custdist DESC, c_count DESC
        """,
    ),
    (
        "Q14",
        """
        SELECT
            100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%'
                THEN l_extendedprice * (1 - l_discount) ELSE 0 END)
                / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
        FROM demo.tpch.lineitem, demo.tpch.part
        WHERE
            l_partkey = p_partkey
            AND l_shipdate >= DATE '1995-09-01'
            AND l_shipdate < DATEADD(month, 1, '1995-09-01')
        """,
    ),
    (
        "Q15",
        """
        WITH revenue AS (
            SELECT
                l_suppkey AS supplier_no,
                SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
            FROM demo.tpch.lineitem
            WHERE l_shipdate >= DATE '1996-01-01'
                AND l_shipdate < DATEADD(month, 3, '1996-01-01')
            GROUP BY l_suppkey
        ),
        max_revenue AS (
            SELECT MAX(total_revenue) AS max_total_revenue FROM revenue
        )
        SELECT s_suppkey, s_name, s_address, s_phone, r.total_revenue
        FROM revenue r
            JOIN demo.tpch.supplier s ON s.s_suppkey = r.supplier_no
            JOIN max_revenue m ON r.total_revenue = m.max_total_revenue
        ORDER BY s_suppkey
        """,
    ),
    (
        "Q16",
        """
        SELECT p_brand, p_type, p_size, COUNT(DISTINCT ps_suppkey) AS supplier_cnt
        FROM demo.tpch.part, demo.tpch.partsupp
        WHERE
            p_partkey = ps_partkey AND p_brand <> 'Brand#45'
            AND p_type NOT LIKE 'MEDIUM POLISHED%'
            AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
            AND ps_suppkey NOT IN (
                SELECT s_suppkey FROM demo.tpch.supplier
                WHERE s_comment LIKE '%Customer%Complaints%'
            )
        GROUP BY p_brand, p_type, p_size
        ORDER BY supplier_cnt DESC, p_brand, p_type, p_size
        """,
    ),
    (
        "Q17",
        """
        SELECT SUM(l_extendedprice) / 7.0 AS avg_yearly
        FROM demo.tpch.lineitem, demo.tpch.part
        WHERE
            p_partkey = l_partkey AND p_brand = 'Brand#23' AND p_container = 'MED BOX'
            AND l_quantity < (
                SELECT 0.2 * AVG(l_quantity) FROM demo.tpch.lineitem
                WHERE l_partkey = p_partkey
            )
        """,
    ),
    (
        "Q18",
        """
        WITH order_totals AS (
            SELECT l_orderkey, SUM(l_quantity) AS sum_qty
            FROM demo.tpch.lineitem
            GROUP BY l_orderkey
            HAVING SUM(l_quantity) > 300
        )
        SELECT c.c_name, c.c_custkey, o.o_orderkey, o.o_orderdate, o.o_totalprice, ot.sum_qty
        FROM order_totals ot
        JOIN demo.tpch.orders o ON o.o_orderkey = ot.l_orderkey
        JOIN demo.tpch.customer c ON c.c_custkey = o.o_custkey
        ORDER BY o.o_totalprice DESC, o.o_orderdate
        LIMIT 100
        """,
    ),
    (
        "Q19",
        """
        SELECT SUM(l_extendedprice * (1 - l_discount)) AS revenue
        FROM demo.tpch.lineitem, demo.tpch.part
        WHERE
            (p_partkey = l_partkey AND p_brand = 'Brand#12'
                AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                AND l_quantity >= 1 AND l_quantity <= 11
                AND p_size BETWEEN 1 AND 5
                AND l_shipmode IN ('AIR', 'AIR REG')
                AND l_shipinstruct = 'DELIVER IN PERSON')
            OR (p_partkey = l_partkey AND p_brand = 'Brand#23'
                AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                AND l_quantity >= 10 AND l_quantity <= 20
                AND p_size BETWEEN 1 AND 10
                AND l_shipmode IN ('AIR', 'AIR REG')
                AND l_shipinstruct = 'DELIVER IN PERSON')
            OR (p_partkey = l_partkey AND p_brand = 'Brand#34'
                AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                AND l_quantity >= 20 AND l_quantity <= 30
                AND p_size BETWEEN 1 AND 15
                AND l_shipmode IN ('AIR', 'AIR REG')
                AND l_shipinstruct = 'DELIVER IN PERSON')
        """,
    ),
    (
        "Q20",
        """
        SELECT s_name, s_address
        FROM demo.tpch.supplier, demo.tpch.nation
        WHERE
            s_suppkey IN (
                SELECT ps_suppkey FROM demo.tpch.partsupp
                WHERE ps_partkey IN (
                    SELECT p_partkey FROM demo.tpch.part WHERE p_name LIKE 'forest%'
                )
                AND ps_availqty > (
                    SELECT 0.5 * SUM(l_quantity)
                    FROM demo.tpch.lineitem
                    WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey
                        AND l_shipdate >= DATE '1994-01-01'
                        AND l_shipdate < DATEADD(year, 1, '1994-01-01')
                )
            )
            AND s_nationkey = n_nationkey AND n_name = 'CANADA'
        ORDER BY s_name
        """,
    ),
    (
        "Q21",
        """
        SELECT s_name, COUNT(*) AS numwait
        FROM
            demo.tpch.supplier, demo.tpch.lineitem l1,
            demo.tpch.orders, demo.tpch.nation
        WHERE
            s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey
            AND o_orderstatus = 'F'
            AND l1.l_receiptdate > l1.l_commitdate
            AND EXISTS (
                SELECT * FROM demo.tpch.lineitem l2
                WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey
            )
            AND NOT EXISTS (
                SELECT * FROM demo.tpch.lineitem l3
                WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey <> l1.l_suppkey
                    AND l3.l_receiptdate > l3.l_commitdate
            )
            AND s_nationkey = n_nationkey AND n_name = 'SAUDI ARABIA'
        GROUP BY s_name
        ORDER BY numwait DESC, s_name
        """,
    ),
    (
        "Q22",
        """
        SELECT cntrycode, COUNT(*) AS numcust, SUM(c_acctbal) AS totacctbal
        FROM (
            SELECT SUBSTRING(c_phone, 1, 2) AS cntrycode, c_acctbal
            FROM demo.tpch.customer
            WHERE
                SUBSTRING(c_phone, 1, 2) IN ('13','31','23','29','30','18','17')
                AND c_acctbal > (
                    SELECT AVG(c_acctbal) FROM demo.tpch.customer
                    WHERE c_acctbal > 0.00
                        AND SUBSTRING(c_phone, 1, 2) IN ('13','31','23','29','30','18','17')
                )
                AND NOT EXISTS (
                    SELECT * FROM demo.tpch.orders WHERE o_custkey = c_custkey
                )
        ) AS custsale
        GROUP BY cntrycode
        ORDER BY cntrycode
        """,
    ),
]


def invoke(client, fn, payload):
    resp = client.invoke(FunctionName=fn, Payload=json.dumps(payload))
    raw = resp["Payload"].read()
    if resp.get("FunctionError"):
        raise RuntimeError(f"Lambda error: {raw.decode()[:300]}")
    return json.loads(raw) if raw else {}


def login(client, fn):
    result = invoke(client, fn, {
        "version": "2.0",
        "rawPath": "/session/v1/login-request",
        "rawQueryString": "",
        "requestContext": {
            "http": {"method": "POST", "path": "/session/v1/login-request", "sourceIp": "127.0.0.1"},
            "accountId": "anonymous", "apiId": "anonymous",
        },
        "headers": {"content-type": "application/json", "host": "localhost", "x-forwarded-for": "127.0.0.1"},
        "body": json.dumps({"data": {
            "LOGIN_NAME": "demo_user", "PASSWORD": "demo_password_2026",
            "CLIENT_APP_ID": "tpch-runner", "CLIENT_APP_VERSION": "1.0",
            "ACCOUNT_NAME": "embucket", "CLIENT_ENVIRONMENT": {}, "SESSION_PARAMETERS": {},
        }}),
        "isBase64Encoded": False,
    })
    return json.loads(result["body"])["data"]["token"]


def run_sql(client, fn, token, sql):
    result = invoke(client, fn, {
        "version": "2.0",
        "rawPath": "/queries/v1/query-request",
        "rawQueryString": f"requestId={uuid.uuid4()}",
        "requestContext": {
            "http": {"method": "POST", "path": "/queries/v1/query-request", "sourceIp": "127.0.0.1"},
            "accountId": "anonymous", "apiId": "anonymous",
        },
        "headers": {
            "content-type": "application/json",
            "host": "localhost",
            "x-forwarded-for": "127.0.0.1",
            "authorization": f'Snowflake Token="{token}"',
        },
        "body": json.dumps({"sqlText": sql}),
        "isBase64Encoded": False,
    })
    body = json.loads(result.get("body", "{}"))
    if not body.get("success"):
        raise RuntimeError(f"SQL failed: {body.get('message', 'unknown error')}")
    return body


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/tpch/run_tpch_queries.py <lambda_function_arn_or_name>")
        sys.exit(1)

    fn = sys.argv[1]
    region = "us-east-2"
    if ":lambda:" in fn:
        region = fn.split(":")[3]

    client = boto3.client("lambda", region_name=region)

    print("Logging in...")
    token = login(client, fn)

    print(f"\n{'Query':<8} {'Status':<10} {'Time (s)':>10} {'Rows':>8}")
    print("-" * 40)

    results = []
    total_time = 0.0
    passed = 0
    failed = 0

    for query_id, sql in TPCH_QUERIES:
        t0 = time.time()
        try:
            body = run_sql(client, fn, token, sql)
            elapsed = time.time() - t0
            data = body.get("data", {})
            if isinstance(data, dict):
                row_count = len(data.get("rowset", data.get("rows", [])))
            else:
                row_count = len(data)
            status = "OK"
            passed += 1
        except Exception as e:
            elapsed = time.time() - t0
            row_count = 0
            status = "FAIL"
            failed += 1
            results.append((query_id, status, elapsed, row_count, str(e)[:80]))
            print(f"{query_id:<8} {status:<10} {elapsed:>10.2f} {row_count:>8}")
            continue

        total_time += elapsed
        results.append((query_id, status, elapsed, row_count, ""))
        print(f"{query_id:<8} {status:<10} {elapsed:>10.2f} {row_count:>8}")

    print("-" * 40)
    print(f"{'Total':<8} {'':10} {total_time:>10.2f}")
    print(f"\nPassed: {passed}/22, Failed: {failed}/22")

    if failed > 0:
        print("\nFailed queries:")
        for qid, status, _, _, err in results:
            if status == "FAIL":
                print(f"  {qid}: {err}")


if __name__ == "__main__":
    main()
