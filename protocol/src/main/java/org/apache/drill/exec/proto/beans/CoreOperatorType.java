/*
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
// Generated by http://code.google.com/p/protostuff/ ... DO NOT EDIT!
// Generated from protobuf

package org.apache.drill.exec.proto.beans;

public enum CoreOperatorType implements com.dyuproject.protostuff.EnumLite<CoreOperatorType>
{
    SINGLE_SENDER(0),
    BROADCAST_SENDER(1),
    FILTER(2),
    HASH_AGGREGATE(3),
    HASH_JOIN(4),
    MERGE_JOIN(5),
    HASH_PARTITION_SENDER(6),
    LIMIT(7),
    MERGING_RECEIVER(8),
    ORDERED_PARTITION_SENDER(9),
    PROJECT(10),
    UNORDERED_RECEIVER(11),
    RANGE_PARTITION_SENDER(12),
    SCREEN(13),
    SELECTION_VECTOR_REMOVER(14),
    STREAMING_AGGREGATE(15),
    TOP_N_SORT(16),
    EXTERNAL_SORT(17),
    TRACE(18),
    UNION(19),
    OLD_SORT(20),
    PARQUET_ROW_GROUP_SCAN(21),
    HIVE_SUB_SCAN(22),
    SYSTEM_TABLE_SCAN(23),
    MOCK_SUB_SCAN(24),
    PARQUET_WRITER(25),
    DIRECT_SUB_SCAN(26),
    TEXT_WRITER(27),
    TEXT_SUB_SCAN(28),
    JSON_SUB_SCAN(29),
    INFO_SCHEMA_SUB_SCAN(30),
    COMPLEX_TO_JSON(31),
    PRODUCER_CONSUMER(32),
    HBASE_SUB_SCAN(33),
    WINDOW(34),
    NESTED_LOOP_JOIN(35),
    AVRO_SUB_SCAN(36),
    PCAP_SUB_SCAN(37),
    KAFKA_SUB_SCAN(38),
    KUDU_SUB_SCAN(39),
    FLATTEN(40),
    LATERAL_JOIN(41),
    UNNEST(42),
    HIVE_DRILL_NATIVE_PARQUET_ROW_GROUP_SCAN(43),
    JDBC_SCAN(44),
    REGEX_SUB_SCAN(45),
    MAPRDB_SUB_SCAN(46),
    MONGO_SUB_SCAN(47),
    KUDU_WRITER(48),
    OPEN_TSDB_SUB_SCAN(49),
    JSON_WRITER(50),
    HTPPD_LOG_SUB_SCAN(51),
    IMAGE_SUB_SCAN(52),
    SEQUENCE_SUB_SCAN(53),
    PARTITION_LIMIT(54),
    PCAPNG_SUB_SCAN(55),
    RUNTIME_FILTER(56),
    ROWKEY_JOIN(57),
    STATISTICS_AGGREGATE(58),
    UNPIVOT_MAPS(59);
    
    public final int number;
    
    private CoreOperatorType (int number)
    {
        this.number = number;
    }
    
    public int getNumber()
    {
        return number;
    }
    
    public static CoreOperatorType valueOf(int number)
    {
        switch(number) 
        {
            case 0: return SINGLE_SENDER;
            case 1: return BROADCAST_SENDER;
            case 2: return FILTER;
            case 3: return HASH_AGGREGATE;
            case 4: return HASH_JOIN;
            case 5: return MERGE_JOIN;
            case 6: return HASH_PARTITION_SENDER;
            case 7: return LIMIT;
            case 8: return MERGING_RECEIVER;
            case 9: return ORDERED_PARTITION_SENDER;
            case 10: return PROJECT;
            case 11: return UNORDERED_RECEIVER;
            case 12: return RANGE_PARTITION_SENDER;
            case 13: return SCREEN;
            case 14: return SELECTION_VECTOR_REMOVER;
            case 15: return STREAMING_AGGREGATE;
            case 16: return TOP_N_SORT;
            case 17: return EXTERNAL_SORT;
            case 18: return TRACE;
            case 19: return UNION;
            case 20: return OLD_SORT;
            case 21: return PARQUET_ROW_GROUP_SCAN;
            case 22: return HIVE_SUB_SCAN;
            case 23: return SYSTEM_TABLE_SCAN;
            case 24: return MOCK_SUB_SCAN;
            case 25: return PARQUET_WRITER;
            case 26: return DIRECT_SUB_SCAN;
            case 27: return TEXT_WRITER;
            case 28: return TEXT_SUB_SCAN;
            case 29: return JSON_SUB_SCAN;
            case 30: return INFO_SCHEMA_SUB_SCAN;
            case 31: return COMPLEX_TO_JSON;
            case 32: return PRODUCER_CONSUMER;
            case 33: return HBASE_SUB_SCAN;
            case 34: return WINDOW;
            case 35: return NESTED_LOOP_JOIN;
            case 36: return AVRO_SUB_SCAN;
            case 37: return PCAP_SUB_SCAN;
            case 38: return KAFKA_SUB_SCAN;
            case 39: return KUDU_SUB_SCAN;
            case 40: return FLATTEN;
            case 41: return LATERAL_JOIN;
            case 42: return UNNEST;
            case 43: return HIVE_DRILL_NATIVE_PARQUET_ROW_GROUP_SCAN;
            case 44: return JDBC_SCAN;
            case 45: return REGEX_SUB_SCAN;
            case 46: return MAPRDB_SUB_SCAN;
            case 47: return MONGO_SUB_SCAN;
            case 48: return KUDU_WRITER;
            case 49: return OPEN_TSDB_SUB_SCAN;
            case 50: return JSON_WRITER;
            case 51: return HTPPD_LOG_SUB_SCAN;
            case 52: return IMAGE_SUB_SCAN;
            case 53: return SEQUENCE_SUB_SCAN;
            case 54: return PARTITION_LIMIT;
            case 55: return PCAPNG_SUB_SCAN;
            case 56: return RUNTIME_FILTER;
            case 57: return ROWKEY_JOIN;
            case 58: return STATISTICS_AGGREGATE;
            case 59: return UNPIVOT_MAPS;
            default: return null;
        }
    }
}
