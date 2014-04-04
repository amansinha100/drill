package org.apache.drill.exec.planner.cost;

import org.eigenbase.relopt.RelOptCost;

public interface DrillRelOptCost extends RelOptCost {
	
	double getNetwork();

}

