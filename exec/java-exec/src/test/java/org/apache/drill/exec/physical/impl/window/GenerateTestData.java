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
package org.apache.drill.exec.physical.impl.window;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class GenerateTestData {
  private static final int SUB_MUL = 1;
  private static final int BATCH_SIZE = 20;

  private static class Partition {
    Partition previous;
    final int length;
    final int[] subs;

    public Partition(int length, int[] subs) {
      this.length = length;
      this.subs = subs;
    }

    /**
     * @return total number of rows since first partition, this partition included
     */
    public int cumulLength() {
      int prevLength = previous != null ? previous.cumulLength() : 0;
      return length + prevLength;
    }

    public boolean isPartOf(int rowNumber) {
      int prevLength = previous != null ? previous.cumulLength() : 0;
      return rowNumber >= prevLength && rowNumber < cumulLength();
    }

    public int getSubSize(int sub) {
      if (sub != subs[subs.length-1]) {
        return sub * SUB_MUL;
      } else {
        //last sub has enough rows to reach partition length
        int size = length;
        for (int i = 0; i < subs.length-1; i++) {
          size -= subs[i] * SUB_MUL;
        }
        return size;
      }
    }

    /**
     * @return sub id of the sub that contains rowNumber
     */
    public int getSubId(int rowNumber) {
      assert isPartOf(rowNumber) : "row "+rowNumber+" isn't part of this partition";

      int prevLength = previous != null ? previous.cumulLength() : 0;
      rowNumber -= prevLength; // row num from start of this partition

      for (int s : subs) {
        if (rowNumber < subRunningCount(s)) {
          return s;
        }
      }

      throw new RuntimeException("should never happen!");
    }

    /**
     * @return running count of rows from first row of the partition to current sub, this sub included
     */
    public int subRunningCount(int sub) {
      int count = 0;
      for (int s : subs) {
        count += getSubSize(s);
        if (s == sub) {
          break;
        }
      }
      return count;
    }

    /**
     * @return running sum of salaries from first row of the partition to current sub, this sub included
     */
    public int subRunningSum(int sub) {
      int sum = 0;
      for (int s : subs) {
        sum += (s+10) * getSubSize(s);
        if (s == sub) {
          break;
        }
      }
      return sum;
    }

    /**
     * @return sum of salaries for all rows of the partition
     */
    public int totalSalary() {
      return subRunningSum(subs[subs.length-1]);
    }

  }

  public static void main(String[] args) throws FileNotFoundException {
    int fileId = 0;
    final String path = "/Users/hakim/MapR/data/windowData/b3.p2";

    final File pathFolder = new File(path);
    if (!pathFolder.exists()) {
      pathFolder.mkdir();
    }

    PrintStream resultStream = new PrintStream(path+".tsv");
    PrintStream resultOrderStream = new PrintStream(path+".subs.tsv");
    PrintStream dataStream = new PrintStream(path + "/" + fileId + ".data.json");

// partition 0, sub 0: count = 100, sum 1000            100
// partition 0, sub 1: count = 300, sum 3200            300
// partition 0, sub 2: count = 600, sum 6800            600
// partition 0, sub 3: count = 1000, sum 12000         1000
// partition 0, sub 4: count = 1904, sum 24656      p0 1904
// partition 1, sub 4: count = 500, sum 7000           2404
// partition 1, sub 5: count = 1100, sum 16000         3004
// partition 1, sub 6: count = 1800, sum 27200         3704
// partition 1, sub 9: count = 2800, sum 46200         4704 <<
// partition 1, sub 10: count = 3900, sum 68200        5804
// partition 1, sub 14: count = 5400, sum 104200       7304
// partition 1, sub 19: count = 7400, sum 162200       9304 <<
// partition 1, sub 20: count = 10384, sum 251720  p1 12288 <<

    //TODO write the partitions for b3.p2
    Partition[] partitions = {
      new Partition(5, new int[]{1, 2, 3}), // [1, 3, 5
      new Partition(55, new int[]{4, 5, 7, 8, 9, 10, 11, 12}), // 9, 14, 7:20][7:1 , 9, 18, 10:20][10:8, 19, 12:20]
    };

    for (Partition p : partitions) {
      dataStream.printf("// partition rows %d, subs %s%n", p.length, Arrays.toString(p.subs));
    }

    // set previous partitions
    for (int i = 1; i < partitions.length; i++) {
      partitions[i].previous = partitions[i-1];
    }

    int total = partitions[partitions.length-1].cumulLength(); // total number of rows

    List<Integer> emp_ids = new ArrayList<>(total);
    for (int i = 0; i < total; i++) {
      emp_ids.add(i);
    }

    Collections.shuffle(emp_ids);

    int emp_idx = 0;
    for (int id : emp_ids) {
      int p = 0;
      while (!partitions[p].isPartOf(id)) { // emp x is @ row x-1
        p++;
      }

      int sub = partitions[p].getSubId(id);
      int salary = 10 + sub;

      dataStream.printf("{ \"employee_id\":%d, \"position_id\":%d, \"sub\":%d, \"salary\":%d }%n", id, p + 1, sub, salary);
      emp_idx++;
      if ((emp_idx % BATCH_SIZE)==0 && emp_idx < total) {
        System.out.printf("total: %d, emp_idx: %d, fileID: %d%n", total, emp_idx, fileId);
        dataStream.close();
        fileId++;
        dataStream = new PrintStream(path + "/" + fileId + ".data.json");
      }
    }

    dataStream.close();

    for (int p = 0, idx = 0; p < partitions.length; p++) {
      for (int i = 0; i < partitions[p].length; i++, idx++) {
        int sub = partitions[p].getSubId(idx);
        resultOrderStream.printf("%d\t%d%n", partitions[p].subRunningCount(sub), partitions[p].subRunningSum(sub));
        resultStream.printf("%d\t%d%n", partitions[p].length, partitions[p].totalSalary());
      }
    }

    resultStream.close();
    resultOrderStream.close();
  }
}