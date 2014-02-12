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
package org.apache.drill.exec.physical.impl.common;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.compile.sig.GeneratorMapping;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.ClassGenerator.HoldingContainer;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.HoldingContainerExpression;
import org.apache.drill.exec.expr.ValueVectorWriteExpression;
import org.apache.drill.exec.expr.fn.impl.BitFunctions;
import org.apache.drill.exec.expr.fn.impl.ComparatorFunctions;
import org.apache.drill.exec.expr.fn.impl.HashFunctions;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.filter.ReturnValueExpression;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.vector.*;

import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashCode;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JFieldRef;
import com.sun.codemodel.JVar;
import com.sun.codemodel.JConditional;


public class ChainedHashTable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ChainedHashTable.class);

  private static final GeneratorMapping KEY_MATCH = 
	  GeneratorMapping.create("setupInterior" /* setup method */, "isKeyMatchInternal" /* eval method */, 
                            null /* reset */, null /* cleanup */);

  private static final GeneratorMapping GET_HASH = 
	  GeneratorMapping.create("doSetup" /* setup method */, "getHash" /* eval method */, 
                            null /* reset */, null /* cleanup */);
  private static final GeneratorMapping SET_VALUE = 
	  GeneratorMapping.create("setupInterior" /* setup method */, "setValue" /* eval method */, 
                            null /* reset */, null /* cleanup */);
  
  private final MappingSet KeyMatchIncomingMapping = new MappingSet("incomingRowIdx", null, "incoming", null, KEY_MATCH, KEY_MATCH);
  private final MappingSet KeyMatchHtableMapping = new MappingSet("htRowIdx", null, "htContainer", null, KEY_MATCH, KEY_MATCH);
  private final MappingSet GetHashIncomingMapping = new MappingSet("incomingRowIdx", null, "incoming", null, GET_HASH, GET_HASH);
  private final MappingSet SetValueMapping = new MappingSet("incomingRowIdx" /* read index */, "htRowIdx" /* write index */, "incoming" /* read container */, "htContainer" /* write container */, SET_VALUE, SET_VALUE);

  private HashTableConfig htConfig;
  private final FragmentContext context;
  private final RecordBatch incoming;

  public ChainedHashTable(HashTableConfig htConfig, 
                          FragmentContext context,
                          RecordBatch incoming)  {

    this.htConfig = htConfig;
    this.context = context;
    this.incoming = incoming;
  }

  public HashTable createAndSetupHashTable () throws ClassTransformationException, IOException, SchemaChangeException {
    CodeGenerator<HashTable> top = CodeGenerator.get(HashTable.TEMPLATE_DEFINITION, context.getFunctionRegistry());
    ClassGenerator<HashTable> cg = top.getRoot();
    ClassGenerator<HashTable> cgInner = cg.getInnerGenerator("BatchHolder");

    LogicalExpression[] keyExprs = new LogicalExpression[htConfig.getKeyExprs().length];
    // TypedFieldId[] keyFieldIds = new TypedFieldId[htConfig.getKeyExprs().length];
    
    ErrorCollector collector = new ErrorCollectorImpl();
    int i = 0;
    for (NamedExpression ne : htConfig.getKeyExprs()) { 
      final LogicalExpression expr = ExpressionTreeMaterializer.materialize(ne.getExpr(), incoming, collector, context.getFunctionRegistry());
      if(collector.hasErrors()) throw new SchemaChangeException("Failure while materializing expression. " + collector.toErrorString());
      if (expr == null) continue;
      keyExprs[i] = expr;
      i++;
    }

    // generate code for isKeyMatch(), setValue(), getHash()
    setupIsKeyMatchInternal(cgInner, keyExprs);
    setupSetValue(cgInner, keyExprs);

    // use top level code generator since getHash() is defined in the scope of the outer class
    setupGetHash(cg, keyExprs);

    HashTable ht = context.getImplementationClass(top); 
    ht.setup(htConfig, context, incoming, keyExprs);

    return ht;
  }

  private void setupIsKeyMatchInternal(ClassGenerator<HashTable> cg, LogicalExpression[] keyExprs) throws SchemaChangeException {
    cg.setMappingSet(KeyMatchIncomingMapping);

    for (LogicalExpression expr : keyExprs) { 
      cg.setMappingSet(KeyMatchIncomingMapping);
      HoldingContainer left = cg.addExpr(expr, false);
        
      cg.setMappingSet(KeyMatchHtableMapping);
      HoldingContainer right = cg.addExpr(expr, false);

      // cg.setMappingSet(mainMapping);
      
      // next we wrap the two comparison sides and add the expression block for the comparison.
      FunctionCall f = 
        new FunctionCall(ComparatorFunctions.COMPARE_TO,
                         ImmutableList.of((LogicalExpression) new HoldingContainerExpression(left),
                                          (LogicalExpression) new HoldingContainerExpression(right)),
                         ExpressionPosition.UNKNOWN);
      HoldingContainer out = cg.addExpr(f, false);

      // check if two values are not equal (comparator result != 0)
      JConditional jc = cg.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)));
        
      jc._then()._return(JExpr.FALSE);
    }

    // All key expressions compared equal, so return TRUE
    cg.getEvalBlock()._return(JExpr.TRUE);
  }

  private void setupSetValue(ClassGenerator<HashTable> cg, LogicalExpression[] keyExprs) throws SchemaChangeException {
    cg.setMappingSet(SetValueMapping);

    int i = 0;
    for (LogicalExpression expr : keyExprs) {
      ValueVectorWriteExpression vvwExpr = new ValueVectorWriteExpression(new TypedFieldId(expr.getMajorType(), i++), expr, true);

      HoldingContainer hc = cg.addExpr(vvwExpr, false); // this will write to the htContainer at htRowIdx
      cg.getEvalBlock()._if(hc.getValue().eq(JExpr.lit(0)))._then()._return(JExpr.FALSE);      
    }

    cg.getEvalBlock()._return(JExpr.TRUE);

  }

  private void setupGetHash(ClassGenerator<HashTable> cg, LogicalExpression[] keyExprs) throws SchemaChangeException {

    cg.setMappingSet(GetHashIncomingMapping);
     
    HoldingContainer combinedHashValue = null;

    for (int i = 0; i < keyExprs.length; i++) {
      LogicalExpression expr = keyExprs[i];
      
      cg.setMappingSet(GetHashIncomingMapping);
      HoldingContainer input = cg.addExpr(expr, false);

      // compute the hash(expr)
      FunctionCall hashfunc = 
        new FunctionCall(HashFunctions.HASH,
                         ImmutableList.of((LogicalExpression) new HoldingContainerExpression(input)),
                         ExpressionPosition.UNKNOWN);

      HoldingContainer hashValue = cg.addExpr(hashfunc, false);

      if (i == 0) {
        combinedHashValue = hashValue; // first expression..just use the hash value 
      } 
      else {

        // compute the combined hash value using XOR
        FunctionCall xorfunc = new FunctionCall
      		  (BitFunctions.XOR, ImmutableList.of(
      				  (LogicalExpression) new HoldingContainerExpression(hashValue),
      				  (LogicalExpression) new HoldingContainerExpression(combinedHashValue)),
      				  ExpressionPosition.UNKNOWN);
        combinedHashValue = cg.addExpr(xorfunc, false);
      }
    }

    cg.getEvalBlock()._return(combinedHashValue.getValue()) ; 
    
  }

}

