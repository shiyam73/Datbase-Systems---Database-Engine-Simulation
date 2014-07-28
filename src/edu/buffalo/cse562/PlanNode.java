package edu.buffalo.cse562;

import java.util.ArrayList;

public class PlanNode{
//	private Operator op;	
	private ArrayList<PlanNode> children;
	
	public PlanNode()
	{
//		this.op = op;
	}

	public void setChild(PlanNode child)
	{
		if(children==null)
		{
			children = new ArrayList<PlanNode>();
		}
		children.add(child);
	}
}
