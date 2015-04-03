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
import org.junit.Test;

public class TestNestedLoopJoin extends PlanTestBase {
  private static String nlpattern = "NestedLoopJoin";

  @Test
  public void testNljoinExists_1() throws Exception {
    String query = String.format("select r_regionkey from cp.`tpch/region.parquet` where exists (select n_regionkey from cp.`tpch/nation.parquet` where n_nationkey < 10)");
    testPlanMatchingPatterns(query, new String[]{nlpattern}, new String[]{});
  }

  @Test
  public void testNljoinNotIn_1() throws Exception {
    String query = String.format("select r_regionkey from cp.`tpch/region.parquet` where r_regionkey not in (select n_regionkey from cp.`tpch/nation.parquet` where n_nationkey < 10)");
    // testPlanMatchingPatterns(query, new String[]{nlpattern}, new String[]{});
    testSql(query);
  }

  @Test
  public void testNljoinInequality_1() throws Exception {
    String query = String.format("select r_regionkey from cp.`tpch/region.parquet` where r_regionkey > (select min(n_regionkey) from cp.`tpch/nation.parquet` where n_nationkey < 10)");
    testPlanMatchingPatterns(query, new String[]{nlpattern}, new String[]{});
  }

}
