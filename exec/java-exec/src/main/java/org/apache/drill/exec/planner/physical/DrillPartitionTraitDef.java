package org.apache.drill.exec.planner.physical;

import java.util.Collections;

import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelCollationImpl;
import org.eigenbase.rel.RelCollationTraitDef;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitDef;

public class DrillPartitionTraitDef extends RelTraitDef<DrillPartitionTrait>{
  public static final DrillPartitionTraitDef INSTANCE = new DrillPartitionTraitDef();
  
  private DrillPartitionTraitDef() {
    super();
  }
  
  public boolean canConvert(
      RelOptPlanner planner, DrillPartitionTrait fromTrait, DrillPartitionTrait toTrait) {
    return true;
  }  

  public Class<DrillPartitionTrait> getTraitClass(){
    return DrillPartitionTrait.class;
  }
  
  public DrillPartitionTrait getDefault() {
    return DrillPartitionTrait.DEFAULT;
  }

  public String getSimpleName() {
    return "DrillPartitionTrait";
  }

  // implement RelTraitDef
  public RelNode convert(
      RelOptPlanner planner,
      RelNode rel,
      DrillPartitionTrait toPartition,
      boolean allowInfiniteCostConverters) {
    
    DrillPartitionTrait currentPartition = rel.getTraitSet().getTrait(DrillPartitionTraitDef.INSTANCE);
    
    if (currentPartition.equals(toPartition)) {
      return rel;
    }
    
    switch(toPartition.getType()){
      case SINGLETON:
          return new UnionExchangePrel(rel.getCluster(), rel.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(toPartition), rel);
      case HASH_PARTITIONED: 
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
