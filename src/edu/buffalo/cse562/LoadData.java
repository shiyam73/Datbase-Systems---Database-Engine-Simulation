package edu.buffalo.cse562;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;

public class LoadData extends ExpressionEvaluator
{
	private Expression conditionWhere;
	private Table thistable;
	public static void LoadArrayList(Operator inp,ArrayList<Tuple> list) {
//		try
//		{
	  
			int count = 0;
			/*if(!list.isEmpty())
				list.clear();*/
			
			
			while (count < 1000) 
			{
				Tuple tup = inp.readOnetuple();
				if (tup == null) 
				{
					//System.out.println("REACHED THE END OF THE BLOCK "+ inp);
					break;
				}
			
				list.add(tup);
				count++;
				
			}
		/*}
		catch(Exception e)
		{
			System.out.println(count + " " + tup);
			throw new IllegalArgumentException("PANIC! "+ e);
		}*/
			
	}

}
