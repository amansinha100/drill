package org.apache.drill.exec.planner.physical;

import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelCollationImpl;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitDef;

public class DrillMuxModeDef extends RelTraitDef<DrillMuxMode>{
  public static final DrillMuxModeDef INSTANCE = new DrillMuxModeDef();
  
  private DrillMuxModeDef() {
    super();
  }
  
  public boolean canConvert(
      RelOptPlanner planner, DrillMuxMode fromTrait, DrillMuxMode toTrait) {
    return false;
  }  

  public Class<DrillMuxMode> getTraitClass(){
    return DrillMuxMode.class;
  }
  
  public DrillMuxMode getDefault() {
    return DrillMuxMode.DEFAULT;
  }

  public String getSimpleName() {
    return "DrillMuxMode";
  }

}
