package org.apache.drill.exec.planner.physical;

import org.eigenbase.relopt.RelTrait;
import org.eigenbase.relopt.RelTraitDef;

import com.google.common.collect.ImmutableList;

public class DrillPartitionTrait implements RelTrait {
  public static enum PartitionType {SINGLETON, HASH_PARTITIONED, RANGE_PARTITIONED, RANDOM_PARTITIONED, NO_PARTITIONED};

  public static DrillPartitionTrait SINGLETON = new DrillPartitionTrait(PartitionType.SINGLETON);
  public static DrillPartitionTrait NO_PARTITIONED = new DrillPartitionTrait(PartitionType.NO_PARTITIONED);
  public static DrillPartitionTrait RANDOM_PARTITIONED = new DrillPartitionTrait(PartitionType.RANDOM_PARTITIONED);
  
  public static DrillPartitionTrait DEFAULT = NO_PARTITIONED;
  
  private PartitionType type;  
  private final ImmutableList<PartitionField> fields;
  
  private DrillPartitionTrait(PartitionType type) {
    assert (type == PartitionType.SINGLETON || type == PartitionType.NO_PARTITIONED || type == PartitionType.RANDOM_PARTITIONED);
    this.type = type;
    this.fields = null;    
  }

  public DrillPartitionTrait(PartitionType type, ImmutableList<PartitionField> fields) {
    assert (type == PartitionType.HASH_PARTITIONED || type == PartitionType.RANGE_PARTITIONED);   
    this.type = type;
    this.fields = fields;
  }

  public boolean subsumes(RelTrait trait) {
    if(trait == DEFAULT || this == DEFAULT) return true;
    return this.equals(trait);
  }
  
  public RelTraitDef<DrillPartitionTrait> getTraitDef() {
    return DrillPartitionTraitDef.INSTANCE;
  }

  public PartitionType getType() {
    return this.type;
  }
  
  public int hashCode() {
    return  fields == null ? type.hashCode() : type.hashCode() | fields.hashCode() << 4 ;
  }
  
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof DrillPartitionTrait) {
      DrillPartitionTrait that = (DrillPartitionTrait) obj;
      return this.fields == that.fields && this.type == that.type;
    }
    return false;
  }

  @Override
  public String toString() {
    return fields == null ? this.type.toString() : this.type.toString() + "(" + fields + ")";
  }

  public class PartitionField {
    
    /**
     * 0-based index of field being partitioned.
     */
    private final int fieldId;
    
    public PartitionField (int fieldId) {
      this.fieldId = fieldId;
    }

    public boolean equals(Object obj) {
      if (!(obj instanceof PartitionField)) {
        return false;
      }
      PartitionField other = (PartitionField) obj;
      return this.fieldId == other.fieldId;
    }
    
    public int hashCode() {
      return this.fieldId;
    }

  }
}
