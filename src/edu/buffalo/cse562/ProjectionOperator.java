package edu.buffalo.cse562;

import java.util.*;
import java.util.Map.Entry;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;

public class ProjectionOperator extends ProjectionEvaluator implements Operator {
	ArrayList<SelectItem> selectItems;
	Operator operator;
	Tuple result;
	String exp;
	Table table;
	
	public ProjectionOperator(Operator root, List<SelectItem> selItems,Table table) {
		// TODO Auto-generated constructor stub
		this.operator = root;
		this.selectItems = (ArrayList<SelectItem>) selItems;
		result = new Tuple();
		this.table =  table;
	}

	@Override
	public Tuple readOnetuple() {
		// TODO Auto-generated method stub
		do
		{
			input = operator.readOnetuple();
			
			if(input != null)
			{
				for(SelectItem s : selectItems)
					s.accept(this);
				//System.out.println("In projection readone");
				for(int i=0;i<selectItems.size();i++)
				{
					//System.out.println(selectItems.get(i));
					if(Constants.subSelect)
					{
						//System.out.println("inside sub select");
						result = input;
					}
					else
					{
						System.out.println("no subselect");
						//System.out.println("inside else project");
						for (Map.Entry<String, Datum> entry : input.entrySet()) 
						{
							if(selectItems.get(i).toString().contains("."))
							{
								if(entry.getKey().equalsIgnoreCase(selectItems.get(i).toString()))
								{
									result.put(entry.getKey(), entry.getValue());

								}
							}
							else
							{
								if(tempCol != null)
								{
									if(selectItems.get(i).toString().contains(tempCol))
									{
										//System.out.println(selectItems.get(i)+" "+entry.getKey()+" "+col);
										if(entry.getKey().contains(tempCol))
											result.put(entry.getKey(), entry.getValue());
									}
								}
								else
								{
									String temp[] = entry.getKey().split("\\.");
									String check = temp[0]+"."+selectItems.get(i).toString();
									if(entry.getKey().contains(check))
									{
										result.put(entry.getKey(), entry.getValue());

									}
								}
							}
						}

					}
				}
				return result;
			}
			
		}while(input != null);
		return null;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		
	}

}
