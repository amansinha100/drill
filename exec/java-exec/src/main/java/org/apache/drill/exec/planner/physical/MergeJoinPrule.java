package org.apache.drill.exec.planner.physical;

import java.util.List;
import java.util.logging.Logger;

import org.apache.drill.exec.planner.logical.DrillJoinRel;
import org.apache.drill.exec.planner.logical.DrillJoinRule;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait.DistributionField;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.JoinRel;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelCollationImpl;
import org.eigenbase.rel.RelFieldCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.trace.EigenbaseTrace;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class MergeJoinPrule extends RelOptRule {
  public static final RelOptRule INSTANCE = new MergeJoinPrule();
  protected static final Logger tracer = EigenbaseTrace.getPlannerTracer();

  private MergeJoinPrule() {
    super(
        RelOptHelper.some(DrillJoinRel.class, RelOptHelper.any(RelNode.class), RelOptHelper.any(RelNode.class)),
        "Prel.MergeJoinPrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final DrillJoinRel join = (DrillJoinRel) call.rel(0);
    final RelNode left = call.rel(1);
    final RelNode right = call.rel(2);

    RelCollation collationLeft = getCollation(join.getLeftKeys());
    RelCollation collationRight = getCollation(join.getRightKeys());

    // Create transform request for MergeJoin plan with both children HASH distributed
    DrillDistributionTrait hashLeftPartition = new DrillDistributionTrait(DrillDistributionTrait.DistributionType.HASH_DISTRIBUTED, ImmutableList.copyOf(getDistributionField(join.getLeftKeys())));
    DrillDistributionTrait hashRightPartition = new DrillDistributionTrait(DrillDistributionTrait.DistributionType.HASH_DISTRIBUTED, ImmutableList.copyOf(getDistributionField(join.getRightKeys())));
    RelTraitSet traitsLeft = left.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(collationLeft).plus(hashLeftPartition);   
    RelTraitSet traitsRight = right.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(collationRight).plus(hashRightPartition);
        
    createTransformRequest(call, join, left, right, traitsLeft, traitsRight);

    // Create transform request for MergeJoin plan with left child ANY distributed and right child BROADCAST distributed
    DrillDistributionTrait distAnyLeft = new DrillDistributionTrait(DrillDistributionTrait.DistributionType.ANY);
    DrillDistributionTrait distBroadcastRight = new DrillDistributionTrait(DrillDistributionTrait.DistributionType.BROADCAST_DISTRIBUTED);
    traitsLeft = left.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(collationLeft).plus(distAnyLeft);
    traitsRight = right.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(collationRight).plus(distBroadcastRight);
    
    createTransformRequest(call, join, left, right, traitsLeft, traitsRight);
  }

  private void createTransformRequest(RelOptRuleCall call, DrillJoinRel join, 
                                      RelNode left, RelNode right, 
                                      RelTraitSet traitsLeft, RelTraitSet traitsRight) {
    
    final RelNode convertedLeft = convert(left, traitsLeft);
    final RelNode convertedRight = convert(right, traitsRight);
    
    try {          
      MergeJoinPrel newJoin = new MergeJoinPrel(join.getCluster(), traitsLeft, convertedLeft, convertedRight, join.getCondition(),
                                                join.getJoinType());
      call.transformTo(newJoin);
    } catch (InvalidRelException e) {
      tracer.warning(e.toString());
    }        
  }
      
  private RelCollation getCollation(List<Integer> keys){    
    List<RelFieldCollation> fields = Lists.newArrayList();
    for (int key : keys) {
      fields.add(new RelFieldCollation(key));
    }
    return RelCollationImpl.of(fields);
  }

  private List<DistributionField> getDistributionField(List<Integer> keys) {
    List<DistributionField> distFields = Lists.newArrayList();

    for (int key : keys) {
      distFields.add(new DistributionField(key));
    }
     
    return distFields;
  }

}
