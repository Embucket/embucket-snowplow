-- TPC-H DDL for all 8 tables in demo.tpch schema

CREATE TABLE IF NOT EXISTS demo.tpch.customer (
  c_custkey BIGINT,
  c_name VARCHAR(25),
  c_address VARCHAR(40),
  c_nationkey INT,
  c_phone VARCHAR(15),
  c_acctbal DOUBLE,
  c_mktsegment VARCHAR(10),
  c_comment VARCHAR(117)
);

CREATE TABLE IF NOT EXISTS demo.tpch.orders (
  o_orderkey BIGINT,
  o_custkey BIGINT,
  o_orderstatus VARCHAR(1),
  o_totalprice DOUBLE,
  o_orderdate DATE,
  o_orderpriority VARCHAR(15),
  o_clerk VARCHAR(15),
  o_shippriority INT,
  o_comment VARCHAR(79)
);

CREATE TABLE IF NOT EXISTS demo.tpch.lineitem (
  l_orderkey BIGINT,
  l_partkey BIGINT,
  l_suppkey BIGINT,
  l_linenumber INT,
  l_quantity DOUBLE,
  l_extendedprice DOUBLE,
  l_discount DOUBLE,
  l_tax DOUBLE,
  l_returnflag VARCHAR(1),
  l_linestatus VARCHAR(1),
  l_shipdate DATE,
  l_commitdate DATE,
  l_receiptdate DATE,
  l_shipinstruct VARCHAR(25),
  l_shipmode VARCHAR(10),
  l_comment VARCHAR(44)
);

CREATE TABLE IF NOT EXISTS demo.tpch.part (
  p_partkey BIGINT,
  p_name VARCHAR(55),
  p_mfgr VARCHAR(25),
  p_brand VARCHAR(10),
  p_type VARCHAR(25),
  p_size INT,
  p_container VARCHAR(10),
  p_retailprice DOUBLE,
  p_comment VARCHAR(23)
);

CREATE TABLE IF NOT EXISTS demo.tpch.supplier (
  s_suppkey BIGINT,
  s_name VARCHAR(25),
  s_address VARCHAR(40),
  s_nationkey INT,
  s_phone VARCHAR(15),
  s_acctbal DOUBLE,
  s_comment VARCHAR(101)
);

CREATE TABLE IF NOT EXISTS demo.tpch.partsupp (
  ps_partkey BIGINT,
  ps_suppkey BIGINT,
  ps_availqty INT,
  ps_supplycost DOUBLE,
  ps_comment VARCHAR(199)
);

CREATE TABLE IF NOT EXISTS demo.tpch.nation (
  n_nationkey INT,
  n_name VARCHAR(25),
  n_regionkey INT,
  n_comment VARCHAR(152)
);

CREATE TABLE IF NOT EXISTS demo.tpch.region (
  r_regionkey INT,
  r_name VARCHAR(25),
  r_comment VARCHAR(152)
);
