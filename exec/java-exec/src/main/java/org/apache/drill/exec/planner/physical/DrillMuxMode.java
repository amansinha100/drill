package org.apache.drill.exec.planner.physical;

import org.eigenbase.rel.RelCollationImpl;
import org.eigenbase.relopt.RelTrait;
import org.eigenbase.relopt.RelTraitDef;

/**
 * 
 * Drill operator's multiple mode:
 *
 */
public class DrillMuxMode implements RelTrait {
  public static enum MuxMode {SIMPLEX, SCAN_MULTIPLEX, EXCHANGE_SIMPLEX, EXCHANGE_MULTIPLEX};
  
  public static DrillMuxMode DEFAULT = new DrillMuxMode(MuxMode.SIMPLEX);
  
  public static DrillMuxMode SIMPLEX = new DrillMuxMode(MuxMode.SIMPLEX);
  public static DrillMuxMode SCAN_MULTIPLEX = new DrillMuxMode(MuxMode.SCAN_MULTIPLEX);
  public static DrillMuxMode EXCHANGE_MULTIPLEX = new DrillMuxMode(MuxMode.EXCHANGE_MULTIPLEX);
  public static DrillMuxMode EXCHANGE_SIMPLEX = new DrillMuxMode(MuxMode.EXCHANGE_SIMPLEX);
  
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
  
  public int hashCode() {
    return mode.hashCode();
  }
  
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof DrillMuxMode) {
      DrillMuxMode that = (DrillMuxMode) obj;
      return this.mode ==that.mode;
    }
    return false;
  }
  
  @Override
  public String toString() {
    return this.mode.toString();
  }
  
}
