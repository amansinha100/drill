package org.apache.drill.exec.planner.physical;

import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillScreenRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelTraitSet;

public class DrillInsertExchUnderScreenRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new DrillInsertExchUnderScreenRule();

  private DrillInsertExchUnderScreenRule() {
    super(RelOptHelper.some(DrillScreenRel.class, DrillRel.DRILL_LOGICAL, RelOptHelper.any(DrillRel.class)), "DrillInsertExchUnderScrenRule");     
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    
    final DrillScreenRel screen = (DrillScreenRel) call.rel(0);
    final RelNode input = call.rel(1);
    final RelTraitSet traits = input.getTraitSet().plus(Prel.DRILL_PHYSICAL);
    final RelNode convertedInput = convert(input, traits);
  
    final RelNode unionExch = new DrillUnionExchangePrel(convertedInput.getCluster(), convertedInput.getTraitSet().plus(Prel.DRILL_PHYSICAL), convertedInput);
    
    final RelNode newScreen = new ScreenPrel(screen.getCluster(), traits, unionExch);
    call.transformTo(newScreen);
    
  }

}