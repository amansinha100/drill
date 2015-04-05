/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.exec.physical.impl.join;

import static org.junit.Assert.assertEquals;

import org.apache.drill.PlanTestBase;
import org.apache.drill.common.util.TestTools;
import org.junit.Test;

public class TestNestedLoopJoin extends PlanTestBase {
  private static String nlpattern = "NestedLoopJoin";
  static final String WORKING_PATH = TestTools.getWorkingPath();
  static final String TEST_RES_PATH = WORKING_PATH + "/src/test/resources";

  @Test
  public void testNlJoinExists_1() throws Exception {
    String query = "select r_regionkey from cp.`tpch/region.parquet` "
        + " where exists (select n_regionkey from cp.`tpch/nation.parquet` "
        + " where n_nationkey < 10)";
    testPlanMatchingPatterns(query, new String[]{nlpattern}, new String[]{});
  }

  @Test
  public void testNlJoinNotIn_1() throws Exception {
    String query = "select r_regionkey from cp.`tpch/region.parquet` "
        + " where r_regionkey not in (select n_regionkey from cp.`tpch/nation.parquet` "
        + "                            where n_nationkey < 10)";
    testPlanMatchingPatterns(query, new String[]{nlpattern}, new String[]{});
  }

  @Test
  public void testNlJoinInequality_1() throws Exception {
    String query = "select r_regionkey from cp.`tpch/region.parquet` "
        + " where r_regionkey > (select min(n_regionkey) from cp.`tpch/nation.parquet` "
        + "                        where n_nationkey < 10)";
    testPlanMatchingPatterns(query, new String[]{nlpattern}, new String[]{});
  }

  @Test
  public void testNlJoinAggrs_1() throws Exception {
    String query = "select total1, total2 from "
       + "(select sum(l_quantity) as total1 from cp.`tpch/lineitem.parquet` where l_suppkey between 100 and 200), "
       + "(select sum(l_quantity) as total2 from cp.`tpch/lineitem.parquet` where l_suppkey between 200 and 300)  ";
    testPlanMatchingPatterns(query, new String[]{nlpattern}, new String[]{});
  }

  @Test // equality join and scalar right input, hj and mj disabled
  public void testNlJoinEqualityScalar_1() throws Exception {
    String query = "select r_regionkey from cp.`tpch/region.parquet` "
        + " where r_regionkey = (select min(n_regionkey) from cp.`tpch/nation.parquet` "
        + "                        where n_nationkey < 10)";
    test("alter session set `planner.enable_hashjoin` = false");
    test("alter session set `planner.enable_mergejoin` = false");
    testPlanMatchingPatterns(query, new String[]{nlpattern}, new String[]{});
  }

  @Test // equality join and scalar right input, hj and mj disabled, enforce exchanges
  public void testNlJoinEqualityScalar_2() throws Exception {
    String query = "select r_regionkey from cp.`tpch/region.parquet` "
        + " where r_regionkey = (select min(n_regionkey) from cp.`tpch/nation.parquet` "
        + "                        where n_nationkey < 10)";
    test("alter session set `planner.slice_target` = 1");
    test("alter session set `planner.enable_hashjoin` = false");
    test("alter session set `planner.enable_mergejoin` = false");
    testPlanMatchingPatterns(query, new String[]{nlpattern, "BroadcastExchange"}, new String[]{});
  }

  @Test // equality join and non-scalar right input, hj and mj disabled
  public void testNlJoinEqualityNonScalar_1() throws Exception {
    String query = "select r.r_regionkey from cp.`tpch/region.parquet` r inner join cp.`tpch/nation.parquet` n"
        + " on r.r_regionkey = n.n_regionkey where n.n_nationkey < 10";
    test("alter session set `planner.enable_hashjoin` = false");
    test("alter session set `planner.enable_mergejoin` = false");
    test("alter session set `planner.enable_nljoin_for_scalar_only` = false");
    testPlanMatchingPatterns(query, new String[]{nlpattern}, new String[]{});
  }

  @Test // equality join and non-scalar right input, hj and mj disabled, enforce exchanges
  public void testNlJoinEqualityNonScalar_2() throws Exception {
    // String query = "select r.r_regionkey from cp.`tpch/region.parquet` r inner join cp.`tpch/nation.parquet` n"
    //  + " on r.r_regionkey = n.n_regionkey where n.n_nationkey < 10";
    String query = String.format("select n.n_nationkey from cp.`tpch/nation.parquet` n, "
        + " dfs_test.`%s/multilevel/parquet` o "
        + " where n.n_regionkey = o.o_orderkey and o.o_custkey < 5", TEST_RES_PATH);
    test("alter session set `planner.slice_target` = 1");
    test("alter session set `planner.enable_hashjoin` = false");
    test("alter session set `planner.enable_mergejoin` = false");
    test("alter session set `planner.enable_nljoin_for_scalar_only` = false");
    testPlanMatchingPatterns(query, new String[]{nlpattern, "BroadcastExchange"}, new String[]{});
  }

}
