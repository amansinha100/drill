
package org.apache.drill.exec.planner.cost;

import org.eigenbase.relopt.RelOptCost;



public class DrillRelOptCost implements RelOptCost {

  @Override
  public double getRows() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public double getCpu() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public double getIo() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean isInfinite() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean equals(RelOptCost cost) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isEqWithEpsilon(RelOptCost cost) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isLe(RelOptCost cost) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isLt(RelOptCost cost) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public RelOptCost plus(RelOptCost cost) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public RelOptCost minus(RelOptCost cost) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public RelOptCost multiplyBy(double factor) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public double divideBy(RelOptCost cost) {
    // TODO Auto-generated method stub
    return 0;
  }

}
