package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Select;

public class JoinOperator implements Operator {

	private Operator left, right;
	Tuple leftTuple, rightTuple;
	private ArrayList<Tuple> lt = null;
	private ArrayList<Tuple> rt = null;
	private Iterator<Tuple> leftIt = null;
	private Iterator<Tuple> rightIt = null;
	private Expression conditionWhere;
	private Table table;

	public JoinOperator(Operator left, Operator right)
	{
		this.left = left;
		this.right = right;
	
		init();
	}

	public void init()
	{
		if(lt == null)
		{
			lt = new ArrayList<Tuple>();
			LoadData.LoadArrayList(left,lt);
			if(lt==null)
				System.out.println("Left is null");
			leftIt = lt.iterator();
			leftTuple = leftIt.next();
		}

		if(rt == null)
		{
			rt = new ArrayList<Tuple>();
			LoadData.LoadArrayList(right,rt);
			rightIt = rt.iterator();
		}

//		System.out.println("I am a Join Operator and I have " + lt.size()+" of "+left+ "and "+rt.size()+" of "+right);
	}
	@Override
	public Tuple readOnetuple() 
	{
		// TODO Auto-generated method stub				
		Tuple resultTuple = new Tuple();

		if(lt==null || rt == null)
		{
			return null;
		}
		if(rightIt.hasNext())
		{
			//System.out.println(this + " " + this.rightIt);
			rightTuple = rightIt.next();
		}
		else
		{
			if(leftIt.hasNext())
			{
				leftTuple = leftIt.next();
				rightIt = rt.iterator();
				//System.out.println(this + " " + this.rightIt);
				rightTuple = rightIt.next();
			}
			else
			{
				rt.clear();
				LoadData.LoadArrayList(right,rt);

				if(rt.isEmpty())
				{
					lt.clear();
					LoadData.LoadArrayList(left,lt);

					if(lt.isEmpty())
					{
						//System.out.println("Reached the end of left!" + leftTuple + rightTuple);
						lt = rt = null;
						return null;
					}
					
					right.reset();
					LoadData.LoadArrayList(right,rt);
					rightIt = rt.iterator();


					leftIt = lt.iterator();
					leftTuple = leftIt.next();
					rightTuple = rightIt.next();
				}
				else
				{
					rightIt = rt.iterator();
					leftIt = lt.iterator();
					leftTuple = leftIt.next();
					rightTuple = rightIt.next();
				}
			}
		}

		resultTuple = merge(leftTuple,rightTuple);
		//System.out.println("MERGED "+resultTuple);
		return resultTuple;
	}
	@Override
	public void reset() {
		// TODO Auto-generated method stub
		left.reset();
		right.reset();
	}

	public Tuple merge(Tuple leftTuple,Tuple rightTuple)
	{

		Tuple result = new Tuple();
		for(Entry<String, Datum> entry : leftTuple.entrySet())
		{
			result.put(entry.getKey(), entry.getValue());
		}
		for(Entry<String, Datum> entry : rightTuple.entrySet())
		{
			result.put(entry.getKey(), entry.getValue());
		}
		
		return result;
	}

}
