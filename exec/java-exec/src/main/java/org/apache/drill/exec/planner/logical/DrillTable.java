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
package org.apache.drill.exec.planner.logical;

import java.io.IOException;

import net.hydromatic.optiq.Schema.TableType;
import net.hydromatic.optiq.Statistic;
import net.hydromatic.optiq.Statistics;
import net.hydromatic.optiq.Table;

import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.store.StoragePlugin;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptTable;


public abstract class DrillTable implements Table{
  
  private final String storageEngineName;  
  public final StoragePluginConfig storageEngineConfig;
  private Object selection;
  private StoragePlugin plugin;
  
  /** Creates a DrillTable. */
  public DrillTable(String storageEngineName, StoragePlugin plugin, Object selection) {
    this.selection = selection;
    
    this.storageEngineConfig = plugin.getConfig();
    this.storageEngineName = storageEngineName;
  }

  public StoragePluginConfig getStorageEngineConfig(){
    return storageEngineConfig;
  }
  
  public StoragePlugin getPlugin(){
    return plugin;
  }
    
  public Object getSelection() {
    return selection;
  }
  
  public String getStorageEngineName() {
    return storageEngineName;
  }

  @Override
  public Statistic getStatistic() {
    return Statistics.UNKNOWN;
  }

  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable table) {
    return new DrillScanRel(context.getCluster(),
        context.getCluster().traitSetOf(DrillRel.DRILL_LOGICAL),
        table);
  }

  @Override
  public TableType getJdbcTableType() {
    return TableType.TABLE;
  }

  
  
}