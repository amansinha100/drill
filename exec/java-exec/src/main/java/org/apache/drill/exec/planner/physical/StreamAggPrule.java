package org.apache.drill.exec.planner.physical;

import java.util.List;
import java.util.logging.Logger;

import net.hydromatic.optiq.util.BitSets;

import org.apache.drill.exec.planner.logical.DrillAggregateRel;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelCollationImpl;
import org.eigenbase.rel.RelFieldCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.trace.EigenbaseTrace;

import com.google.common.collect.Lists;

public class StreamAggPrule extends RelOptRule {
  public static final RelOptRule INSTANCE = new StreamAggPrule();
  protected static final Logger tracer = EigenbaseTrace.getPlannerTracer();

  private StreamAggPrule() {
    super(RelOptHelper.some(DrillAggregateRel.class, RelOptHelper.any(DrillRel.class)), "DrillAggregateRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final DrillAggregateRel aggregate = (DrillAggregateRel) call.rel(0);
    final RelNode input = call.rel(1);
    RelCollation collation = getCollation(aggregate);
    final RelTraitSet traits = call.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(collation);
    final RelNode convertedInput = convert(input, traits);
    final RelOptCluster cluster = aggregate.getCluster();

    try {
      
      
      StreamAggPrel newAgg = new StreamAggPrel(aggregate.getCluster(), traits, convertedInput, aggregate.getGroupSet(),
          aggregate.getAggCallList());
      
      call.transformTo(newAgg);
    } catch (InvalidRelException e) {
      tracer.warning(e.toString());
    }
  }
  
  
  private RelCollation getCollation(DrillAggregateRel rel){
    
    List<RelFieldCollation> fields = Lists.newArrayList();
    for (int group : BitSets.toIter(rel.getGroupSet())) {
      fields.add(new RelFieldCollation(group));
    }
    return RelCollationImpl.of(fields);
  }

}
