package org.apache.drill.exec.planner.physical;

import java.util.Collections;

import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelCollationImpl;
import org.eigenbase.rel.RelCollationTraitDef;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitDef;

public class DrillDistributionTraitDef extends RelTraitDef<DrillDistributionTrait>{
  public static final DrillDistributionTraitDef INSTANCE = new DrillDistributionTraitDef();
  
  private DrillDistributionTraitDef() {
    super();
  }
  
  public boolean canConvert(
      RelOptPlanner planner, DrillDistributionTrait fromTrait, DrillDistributionTrait toTrait) {
    return true;
  }  

  public Class<DrillDistributionTrait> getTraitClass(){
    return DrillDistributionTrait.class;
  }
  
  public DrillDistributionTrait getDefault() {
    return DrillDistributionTrait.DEFAULT;
  }

  public String getSimpleName() {
    return "DrillPartitionTrait";
  }

  // implement RelTraitDef
  public RelNode convert(
      RelOptPlanner planner,
      RelNode rel,
      DrillDistributionTrait toPartition,
      boolean allowInfiniteCostConverters) {
    
    DrillDistributionTrait currentPartition = rel.getTraitSet().getTrait(DrillDistributionTraitDef.INSTANCE);
    
    if (currentPartition.equals(toPartition)) {
      return rel;
    }
    
    switch(toPartition.getType()){
      case ANY:
        return rel;
      case SINGLETON:
          return new UnionExchangePrel(rel.getCluster(), rel.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(toPartition), rel);
      case HASH_DISTRIBUTED: 
        RelCollation collation = rel.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);
        RelNode exch = new HashToRandomExchangePrel(rel.getCluster(), planner.emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(toPartition), rel);
        if (!collation.equals(RelCollationImpl.EMPTY)) {
          RelNode sort = new SortPrel(rel.getCluster(), exch.getTraitSet().plus(rel.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE)), exch, collation);
          return sort;
        } else {
          return exch;
        }       
      default:
        return null;
    }

  }

}
