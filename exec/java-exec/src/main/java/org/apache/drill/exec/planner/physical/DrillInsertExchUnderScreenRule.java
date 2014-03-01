package org.apache.drill.exec.planner.physical;

import org.apache.drill.exec.planner.common.BaseScreenRel;
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
    super(RelOptHelper.some(BaseScreenRel.class, RelOptHelper.any(RelNode.class)), "DrillInsertExchUnderScrenRule");     
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    
    final BaseScreenRel screen = (BaseScreenRel) call.rel(0);
    final RelNode input = call.rel(1);
    
    //Requires child under "screen" is NOT simplex mode. Otherwise, this rule should not be fired.
    if (input.getTraitSet().getTrait(DrillMuxModeDef.INSTANCE).equals(DrillMuxMode.SIMPLEX))
      return;
    
    final RelTraitSet traits = input.getTraitSet().plus(Prel.DRILL_PHYSICAL);
    final RelNode convertedInput = convert(input, traits);
  
    final RelNode unionExch = new UnionExchangePrel(convertedInput.getCluster(), convertedInput.getTraitSet().replace(DrillMuxMode.EXCHANGE_MULTIPLEX), convertedInput);
    
    final RelNode newScreen = new ScreenPrel(screen.getCluster(), traits.replace(DrillMuxMode.EXCHANGE_SIMPLEX), unionExch);
    
    call.transformTo(newScreen);
    
  }

}