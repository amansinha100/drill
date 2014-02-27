package org.apache.drill.exec.planner.physical;

import org.eigenbase.relopt.RelTrait;
import org.eigenbase.relopt.RelTraitDef;

/**
 * 
 * Drill operator's multiple mode:
 *
 */
public class DrillMuxMode implements RelTrait {
  public static enum MuxMode {SIMPLEX, EXCHANGE_MULTIPLEX, SCAN_MULTIPLEX};
  
  public static DrillMuxMode DEFAULT = new DrillMuxMode(MuxMode.SIMPLEX);
  
  private MuxMode mode;
  
  public DrillMuxMode(MuxMode mode) {
    this.mode = mode;
  }
  
  public boolean subsumes(RelTrait trait) {
    return this == trait;
  }

  public RelTraitDef getTraitDef() {
    return DrillMuxModeDef.INSTANCE;
  }
}
