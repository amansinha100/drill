package org.apache.drill.exec.planner.physical;

import java.io.IOException;
import java.util.List;

import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SingleRel;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;

public class UnionExchangePrel extends SingleRel implements Prel {

  public UnionExchangePrel(RelOptCluster cluster, RelTraitSet traitSet, RelNode input) {
    super(cluster, traitSet, input);
    assert input.getConvention() == Prel.DRILL_PHYSICAL;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    //return super.computeSelfCost(planner).multiplyBy(.1);
    return planner.getCostFactory().makeCost(10, 10, 10);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new UnionExchangePrel(getCluster(), traitSet, sole(inputs));
  }
  
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    throw new IOException(this.getClass().getSimpleName() + " not supported yet!");
  }
  
}
