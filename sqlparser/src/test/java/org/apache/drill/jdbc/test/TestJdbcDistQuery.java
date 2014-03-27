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
package org.apache.drill.jdbc.test;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import mockit.NonStrictExpectations;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.FunctionRegistry;
import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.sql.DrillSqlWorker;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.hive.HiveTestDataGenerator;
import org.apache.drill.jdbc.Driver;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Stopwatch;

public class TestJdbcDistQuery {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestJdbcDistQuery.class);

  
  // Set a timeout unless we're debugging.
  @Rule public TestRule TIMEOUT = TestTools.getTimeoutRule(200000000);

  private static final String WORKING_PATH;
  static{
    Driver.load();
    WORKING_PATH = Paths.get("").toAbsolutePath().toString();
    
  }
  
  @BeforeClass
  public static void generateHive() throws Exception{
    new HiveTestDataGenerator().generateTestData();
  }
  
/*
  @Test 
  public void testSimpleQuerySingleFile() throws Exception{
    testQuery(String.format("select R_REGIONKEY from dfs.`/Users/asinha/parquet/regions1/`"));    
  }
  
  @Test 
  public void testSimpleQueryMultiFile() throws Exception{
    testQuery(String.format("select R_REGIONKEY from dfs.`/Users/asinha/parquet/regions2/`"));    
  }
  
  @Test 
  public void testAggSingleFile() throws Exception{
    testQuery(String.format("select R_REGIONKEY from dfs.`/Users/asinha/parquet/regions1/` group by R_REGIONKEY"));    
  }
  
  @Test
  public void testAggMultiFile() throws Exception{
    testQuery("select R_REGIONKEY from dfs.`/Users/asinha/parquet/regions2/` group by R_REGIONKEY");    
  }
 
  @Test
  public void testAggOrderByDiffGKeyMultiFile() throws Exception{    
    testQuery("select R_REGIONKEY, SUM(cast(R_REGIONKEY AS int)) As S from dfs.`/Users/asinha/parquet/regions2/` group by R_REGIONKEY ORDER BY S");    
  }
 
  @Test
  public void testAggOrderBySameGKeyMultiFile() throws Exception{
    testQuery("select R_REGIONKEY, SUM(cast(R_REGIONKEY AS int)) As S from dfs.`/Users/asinha/parquet/regions2/` group by R_REGIONKEY ORDER BY R_REGIONKEY");   
  }
   
  @Test
  @Ignore
  public void testJoinSingleFile() throws Exception{
    testQuery("select T1.R_REGIONKEY from dfs.`/Users/asinha/parquet/regions1/` as T1 join dfs.`/Users/asinha/parquet/nations1/` as T2 on T1.R_REGIONKEY = T2.N_REGIONKEY");    
  }

  @Test
  @Ignore
  public void testJoinMultiFile() throws Exception{
    testQuery("select T1.R_REGIONKEY from dfs.`/Users/asinha/parquet/regions2/` as T1 join dfs.`/Users/asinha/parquet/nations2/` as T2 on T1.R_REGIONKEY = T2.N_REGIONKEY");     
  }
  
  @Test
  public void testSortSingleFile() throws Exception{
    testQuery("select R_REGIONKEY from dfs.`/Users/asinha/parquet/regions1/` order by R_REGIONKEY");   
  }

  @Test
  public void testSortMultiFile() throws Exception{
    testQuery("select R_REGIONKEY from dfs.`/Users/asinha/parquet/regions2/` order by R_REGIONKEY");   
  }
*/
  @Test
  public void testAggGroupByTwoKeys() throws Exception {
    testQuery("select N_REGIONKEY, N_NAME, MIN(cast(N_NATIONKEY AS int)) As S from dfs.`/Users/asinha/parquet/nations2/` group by N_REGIONKEY, N_NAME");
  }
  
  private void testQuery(String sql) throws Exception{
    boolean success = false;
    try (Connection c = DriverManager.getConnection("jdbc:drill:zk=local", null);) {
      for (int x = 0; x < 1; x++) {
        Stopwatch watch = new Stopwatch().start();
        Statement s = c.createStatement();
        ResultSet r = s.executeQuery(sql);
        boolean first = true;
        while (r.next()) {
          ResultSetMetaData md = r.getMetaData();
          if (first == true) {
            for (int i = 1; i <= md.getColumnCount(); i++) {
              System.out.print(md.getColumnName(i));
              System.out.print('\t');
            }
            System.out.println();
            first = false;
          }

          for (int i = 1; i <= md.getColumnCount(); i++) {
            System.out.print(r.getObject(i));
            System.out.print('\t');
          }
          System.out.println();
        }

        System.out.println(String.format("Query completed in %d millis.", watch.elapsed(TimeUnit.MILLISECONDS)));
      }

      System.out.println("\n\n\n");
      success = true;
    }finally{
      if(!success) Thread.sleep(2000);
    }
    
    
  }
}
