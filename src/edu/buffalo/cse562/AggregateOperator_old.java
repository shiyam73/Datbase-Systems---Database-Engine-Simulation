package edu.buffalo.cse562;

import java.io.File;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.Collections;

import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.Join;

public class AggregateOperator_old extends AggregateEvaluator implements Operator{

	private ArrayList<Function> function;
	private ArrayList<Column> cols;
	private List groupby;
	private Operator operator;
	String col;
	String []groupbycol;
	private Table table = null;
	String[] colvalues=null;
	String[] typeValues = null;
	int size=0;
	List selectitems;	
	String type=null;
	
	 static HashMap<String,Integer> hmax, hmin;
	 static HashMap<String,Double> hcount;
	 static HashMap<String,Double> hsum;
	 static HashMap<String,Tuple> hresult;
	 static HashMap<String,Double> havg;
	 static HashMap<String,HashMap<String,Integer>> hagg;
	 static HashMap<String, HashMap<String, Double>> haggTemp;
	 private List<OrderByElement> orderby = null;
	 PlainSelect ps1;
	 
	public AggregateOperator_old(Operator operator, PlainSelect ps,ArrayList<Function> functionin, ArrayList<Column> colsin, List groupbyin,Table table,List selitems)
	{
		selectitems=selitems;
	
		function=functionin;
		cols=colsin;
		if(groupbyin != null)
		{
			groupby=groupbyin;	
			groupbycol=new String[groupby.size()];
		}
		ps1 = ps;
		this.operator=operator;
		havg = new HashMap<String,Double>();
		hcount = new HashMap<String,Double>();
		hsum = new HashMap<String,Double>();
		hmax = new HashMap<String,Integer>();
		hmin = new HashMap<String,Integer>();
		hresult = new HashMap<String,Tuple>();
		hagg= new  HashMap<String,HashMap<String,Integer>>();
		haggTemp= new  HashMap<String,HashMap<String,Double>>();
		
		this.table = table;
	}

public void groupByCount(Tuple input)
{
	String grpcol=null;
	//System.out.println("inside sum "+(int)input.get(col).getValue());
	for(int i=0;i<groupbycol.length;i++)
	{
		if(i==0)
		{
			grpcol=input.get(groupbycol[i]).getValue()+"";
			type = input.get(groupbycol[i]).getType()+"";
		}
		else
			{
			grpcol=grpcol +"|"+ (input.get((groupbycol[i])).getValue());
			type += "|"+input.get(groupbycol[i]).getType();
			
			}

	}
	
	if(!hcount.containsKey(grpcol))
		hcount.put(grpcol,1.0);
	else
		hcount.put(grpcol,hcount.get(grpcol)+1.0);
}

public void groupBySum(Tuple tuple,String function)
{
	String grpcol=null;
	Tuple result = new Tuple();
	if(groupby != null)
	{
		//System.out.println(groupbycol.length);
		for(int i=0;i<groupbycol.length;i++)
		{
			//System.out.println("AGG "+groupbycol[i]);
			if(i==0)
			{
				grpcol=input.get(groupbycol[i]).getValue().toString()+"";
				type = input.get(groupbycol[i]).getType()+"";
			}
			else
			{
				grpcol=grpcol +"|"+(input.get((groupbycol[i])).getValue().toString());
				type += "|"+input.get(groupbycol[i]).getType();
			}

		}

		//System.out.println(grpcol);
		if(!hsum.containsKey(grpcol))
		{
			//System.out.println("Inside "+grpcol);
			String column = col.replaceAll("\\(|\\)","");
			//System.out.println(column);
			hsum.put(grpcol,(double)input.get(col.replaceAll("\\(|\\)","")).getValue());
		}
		else
		{
		//	System.out.println("else "+grpcol);
			String column = col.replaceAll("\\(|\\)","");
			//System.out.println("inside else :"+column);
			hsum.put(grpcol,hsum.get(grpcol)+(double)input.get(col.replaceAll("\\(|\\)","")).getValue());
			
		}
		//System.out.println("Sum : "+hsum.get(grpcol));
	}
	else
	{
		function = function.replaceAll("\\(|\\)", "");
		//System.out.println(function);
		if(!hsum.containsKey(function))
		{
			hsum.put(function,(double)input.get(function).getValue());
		}
		else
		{
			hsum.put(function, hsum.get(function)+(double)input.get(function).getValue());
		}
		
	}
		
}
public void groupByAvg(Tuple input,String function)
{	
	groupBySum(input,function);
	groupByCount(input);
	
}
public void groupByMax(Tuple input)
{
	String grpcol=null;
	for(int i=0;i<groupbycol.length;i++)
	{
		if(i==0)
		{
			grpcol=input.get(groupbycol[i]).getValue()+"";
			type = input.get(groupbycol[i]).getType()+"";
		}
		else
			{
			grpcol=grpcol +"|"+ (input.get((groupbycol[i])).getValue())+"";
			type += "|"+input.get(groupbycol[i]).getType();
			
			}

	}
	
	if(!hmax.containsKey(grpcol))
	{
		hmax.put(grpcol,(int)input.get(col).getValue());
	}
	else
	{
		if((int)input.get(col).getValue()>hmax.get(grpcol))
		hmax.put(grpcol,(int)input.get(col).getValue());
	}
	
}

public void groupByMin(Tuple input)
{
	String grpcol=null;
	for(int i=0;i<groupbycol.length;i++)
	{
		if(i==0)
		{
			grpcol=input.get(groupbycol[i]).getValue()+"";
			type = input.get(groupbycol[i]).getType()+"";
		}
		else
			{
			grpcol=grpcol + "|"+(input.get((groupbycol[i])).getValue())+"";
			type += "|"+input.get(groupbycol[i]).getType();
			
			}

	}
	
	if(!hmin.containsKey(grpcol))
		hmin.put(grpcol,(int)input.get(col).getValue());
	else
		if((int)input.get(col).getValue()<hmin.get(grpcol))
		hmin.put(grpcol,(int)input.get(col).getValue());
}

@Override
public Tuple readOnetuple()
{

	// TODO Auto-generated method stub
	ArrayList arrResult = new ArrayList<LinkedHashMap<String,Datum>>();
	
	if(groupby!=null)
	{
		
		for(int j=0;j<groupby.size();j++)
		{
			if(groupby.get(j).toString().contains("."))
				groupbycol[j]=groupby.get(j).toString();
			else
				groupbycol[j]=table.getWholeTableName()+"."+groupby.get(j).toString();
		}
		
		
		for(int i=0;i<function.size();i++)
		{		
			String func=function.get(i).getName();
			//System.out.println("inside function : "+function.get(i).getParameters().toString());
			try
			{
				col=function.get(i).getParameters().toString();
			}
			catch(NullPointerException e)
			{
				e.printStackTrace();
				col=groupbycol[0];
			}
		
			col = col.replaceAll("\\(|\\)","");
			
			//if(!col.contains(".") && !col.contains("*") && !col.contains("+")))
			//	col = table.getWholeTableName()+"."+col;
			//System.out.println("COL "+col);
			
			//hsum = new HashMap<String,Double>();
		//	System.out.println("started "+col+" "+func);
			do
			{
				input=operator.readOnetuple();
				
				if(input!=null)
				{
					
					if(function.get(i).getName().equalsIgnoreCase("SUM"))
					{
						//System.out.println("inside sum: "+function.get(i).getParameters().toString());
						//System.out.println(function.get(i).toString());
						/*for(Entry<String,Datum> entry : input.entrySet())
						{
							System.out.print(entry.getKey()+ " "+entry.getValue().getValue()+"|");
						}*/
						if(Constants.aliasMap.get(function.get(i).toString()) != null)
						{
							System.out.println("MAP "+Constants.aliasMap.get(function.get(i).toString()));
							alias = Constants.aliasMap.get(function.get(i).toString());
						}
						function.get(i).getParameters().accept(this);
						
						groupBySum(input,function.get(i).getParameters().toString());
						
					}
					if(function.get(i).getName().equalsIgnoreCase("COUNT"))
					{
						groupByCount(input);
					}
					if(function.get(i).getName().equalsIgnoreCase("AVG"))
					{
						groupByAvg(input,function.get(i).getParameters().toString());
					}
					if(function.get(i).getName().equalsIgnoreCase("MAX"))
					{
						groupByMax(input);
					}
					if(function.get(i).getName().equalsIgnoreCase("MIN"))
					{
						groupByMin(input);
					}
				}
			}while(input!=null);
			operator.reset();
			/*for(Entry<String,Double> entry : hsum.entrySet())
				System.out.println(entry.getKey()+" "+entry.getValue());*/
			if(function.get(i).getName().equalsIgnoreCase("AVG"))
			{
				for (Entry<String, Double> entry : hcount.entrySet()) {
					for (Entry<String, Double> entry1 : hsum.entrySet())
					{
						if(entry.getKey().equalsIgnoreCase(entry1.getKey()))
						{
							double sum=entry1.getValue();
							//System.out.println("SUM IS : "+entry1.getValue());
							double count1=entry.getValue();
							//System.out.println("COUNT IS :"+entry.getValue());
							double avg = (float) (sum/count1);
							
							havg.put(entry.getKey(), avg);
						}
					}
				}
			}
			if(function.get(i).getName().equalsIgnoreCase("SUM"))
			{
				
				HashMap<String,Double> temMap = new HashMap<String,Double>();
				temMap.putAll(hsum);
				haggTemp.put(function.get(i).toString(),temMap);
				
				hsum.clear();
			}
			if(function.get(i).getName().equalsIgnoreCase("COUNT"))
			{
				HashMap<String,Double> temMap = new HashMap<String,Double>();
				temMap.putAll(hcount);
				haggTemp.put(function.get(i).toString(),temMap);
				hcount.clear();
			
				
			}
			if(function.get(i).getName().equalsIgnoreCase("AVG"))
			{
				HashMap<String,Double> temMap = new HashMap<String,Double>();
				temMap.putAll(havg);
				haggTemp.put(function.get(i).toString(),temMap);
				havg.clear();
				hsum.clear();
				hcount.clear();
			}
			if(function.get(i).getName().equalsIgnoreCase("MAX"))
			{
				
				hagg.put(function.get(i).toString(),hmax);
			}
			if(function.get(i).getName().equalsIgnoreCase("MIN"))
			{
				
				hagg.put(function.get(i).toString(),hmin);
			}
		}
		
		
		String aggvalue=null;
		int k=0;
		
		for (Entry<String, HashMap<String,Double>> entry : haggTemp.entrySet())
		{
			String aggcolumn=entry.getKey();
			HashMap<String,Double> value=entry.getValue();
			String coldel=null;
			//System.out.println(aggcolumn);
			for (Entry<String, Double> entry1 : value.entrySet())
			{
				coldel=entry1.getKey().toString();
				Tuple result = new Tuple();
				if(arrResult.size() > 0)
				{
					
				}
				for(int a=0;a<groupbycol.length;a++)
				{   
					colvalues = entry1.getKey().toString().split("\\|");
					typeValues = type.split("\\|");
					if(typeValues[a].equalsIgnoreCase("int"))
						result.put(groupbycol[a], new GetInteger(colvalues[a]));
					if(typeValues[a].equalsIgnoreCase("string"))
						result.put(groupbycol[a], new GetString(colvalues[a]));
					if(typeValues[a].equalsIgnoreCase("double"))
						result.put(groupbycol[a], new GetDouble(colvalues[a]));
					if(typeValues[a].equalsIgnoreCase("date"))
					{
						DateFormat outputDateFormat = new SimpleDateFormat("yyyy-MM-dd");
						DateFormat inputDateFormat = new SimpleDateFormat("E MMM d HH:mm:ss zzz yyyy");
						inputDateFormat.setLenient(false);
						outputDateFormat.setLenient(false);
						Date date = null;
						try {
							date = inputDateFormat.parse(colvalues[a]);
						} catch (ParseException e) {
							// TODO Auto-generated catch block
							System.out.println("Inside catch");
							e.printStackTrace();
						}
						
						//System.out.println(colvalues[a]);
						 
						result.put(groupbycol[a], new GetDate(outputDateFormat.format(date).toString()));
					}
				}
				//System.out.println("AAA"+aggcolumn);
				result.put(aggcolumn,new GetDouble(entry1.getValue().toString()));
				/*for(Entry<String,Datum> t : result.entrySet())
					System.out.print(t.getKey()+"|"+t.getValue().getValue()+" ");
				System.out.println();*/
				arrResult.add(result);
			}
		}
		
		/*-----------------------ADITYAS ORDER BY*******************/
		orderby=ps1.getOrderByElements();
		//System.out.println("print array");
		
		//printArrayDiff(arrResult);
		//System.out.println(arrResult.size());
		int b=0;
		if(orderby!=null)
		{
			Collections.reverse(orderby);
			for(OrderByElement obe : orderby)
			{
				String tempa;
				if(obe.isAsc())
				{
					String order = "asc";
					if(Constants.aliasMap.get(obe.getExpression().toString()) != null)
					{
						tempa = Constants.aliasMap.get(obe.getExpression().toString());
						String temp[] = tempa.split(".");
						if(Constants.tableAliasMap.containsKey(temp[0]))
							tempa = obe.getExpression().toString();
					}
					else
					{
						if(obe.getExpression().toString().contains("."))
							tempa=obe.getExpression().toString();
						else
							tempa = table.getWholeTableName().toString()+"."+obe.getExpression().toString();
					}
					System.out.println("IF RELA"+tempa);
					//Collections.sort(arrResult, new GetComparator(tempa,"ASC"));
					trySort1 ty = new trySort1(arrResult,tempa,order);
					arrResult = ty.externalSort();
				}
				else
				{
					String tempd=null;
					String order = "desc";
					//System.out.println("hello \n");
					if(Constants.aliasMap.get(obe.getExpression().toString()) != null)
						tempd = Constants.aliasMap.get(obe.getExpression().toString());
					else
					{
					if(obe.getExpression().toString().contains("."))
						tempd=obe.getExpression().toString();
					else
						tempd = table.getWholeTableName().toString()+"."+obe.getExpression().toString();
					}
					System.out.println("ELSE RELA"+tempd);
					//Collections.sort(arrResult, new GetComparator(tempd,"DESC"));
					trySort1 ty = new trySort1(arrResult,tempd,order);
					
					arrResult = ty.externalSort();

				}
			}
			reArrange(arrResult);
			b=1;
		}
		/*---------------------------------------------------------*/
		if(b != 1)
		printArray(arrResult);
		
		//printArray(arrResult);
		
	}
	else
	{
		for(int i=0;i<function.size();i++)
		{	
			
			String func=function.get(i).getName();
			try
			{
			col=function.get(i).getParameters().toString();
			}
			catch(NullPointerException e)
			{
				col=groupbycol[i];
				
			}
			col = col.replaceAll("\\(|\\)","");
			if(!col.contains("."))
				col = table.getWholeTableName()+"."+col;
			do
			{
				input=operator.readOnetuple();
				
				//System.out.println();
				if(input!=null)
				{//System.out.println("here");
					/*for(Entry<String,Datum> entry : input.entrySet())
					{
						System.out.print(entry.getKey()+ " "+entry.getValue().getValue()+"|");
					}
					System.out.println();*/
					if(function.get(i).getName().equalsIgnoreCase("SUM"))
					{
						//System.out.println("inside sum");
						
						function.get(i).getParameters().accept(this);
						if(function.get(i).toString().contains(" AS "))
						{ 
							String temp[] = function.get(i).toString().split(" AS "); 
							alias = temp[1]; 
							}
						groupBySum(input,function.get(i).getParameters().toString());
					}
					if(function.get(i).getName().equalsIgnoreCase("COUNT"))
					{
						groupByCount(input);
					}
					if(function.get(i).getName().equalsIgnoreCase("AVG"))
					{
						groupByAvg(input,function.get(i).getParameters().toString());
					}
					if(function.get(i).getName().equalsIgnoreCase("MAX"))
					{
						groupByMax(input);
					}
					if(function.get(i).getName().equalsIgnoreCase("MIN"))
					{
						groupByMin(input);
					}
				}
			}while(input!=null);
			operator.reset();
			/*if(function.get(i).getName().equalsIgnoreCase("AVG"))
			{
				for (Entry<String, Integer> entry : hcount.entrySet()) {
					for (Entry<String, Integer> entry1 : hsum.entrySet())
					{
						if(entry.getKey().equalsIgnoreCase(entry1.getKey()))
						{
							float sum=entry1.getValue();
							float count1=entry.getValue();
							Float avg = (float) (sum/count1);
							havg.put(entry.getKey(), avg);
						}
					}
				}
			}*/
			if(function.get(i).getName().equalsIgnoreCase("SUM"))
			{
				haggTemp.put(function.get(i).toString(),hsum);
				size=hsum.size();

			}
			if(function.get(i).getName().equalsIgnoreCase("COUNT"))
			{
				size=hcount.size();
				//hagg.put(function.get(i).toString(),hcount);
			}
			if(function.get(i).getName().equalsIgnoreCase("AVG"))
			{
				size=havg.size();
				//hagg.put(function.get(i).toString(),havg);
			}
			if(function.get(i).getName().equalsIgnoreCase("MAX"))
			{
				size=hmax.size();
				hagg.put(function.get(i).toString(),hmax);
			}
			if(function.get(i).getName().equalsIgnoreCase("MIN"))
			{
				size=hmin.size();
				hagg.put(function.get(i).toString(),hmin);
			}
		}
		
		for(Entry<String,Double> entry : hsum.entrySet())
		{
			System.out.println(entry.getValue());
		}

	}
	
	
	return null;
}

@Override
public void reset() {
	// TODO Auto-generated method stub
	
}

public void reArrange(List arrayList)
{

	ArrayList<Tuple> result = new ArrayList<Tuple>();
	int i=0,j=0;
	//System.out.println("array size"+arrayList.size());
	while(i<arrayList.size())
	{
		//System.out.println("i = "+i);
		Tuple test = (Tuple)arrayList.get(i);
		Tuple check = new Tuple();
		for(j=0;j<function.size();j++)
		{
			for(Entry<String,Datum> entry : test.entrySet())
			{
				check.put(entry.getKey(),entry.getValue());
				//System.out.println("CHECK "+check.get(entry.getKey()).getValue());	
			}

			test = (Tuple)arrayList.get(i+j);

		}
		i+=j;
		//if(i == arrayList.size())
		//{
			for(j=0;j<function.size();j++)
			{
				//System.out.println(function.get(j).toString());
				for(Entry<String,Datum> entry : test.entrySet())
				{
					//System.out.println(entry.getKey()+ " "+entry.getValue().getValue()+"|");
					//	if(!check.containsKey(entry.getKey()))
					check.put(entry.getKey(),entry.getValue());
					//System.out.println("CHECK "+check.get(entry.getKey()).getValue());	
				}

			}
		//}
		//System.out.println(i);
		result.add(check);
	}
	
	
	//System.out.println();
	printArray(result);
}
public void printArray(List arrayList)
{
	for(int i=0;i<arrayList.size();i++)
	{
		String separator="";
		Tuple test=(Tuple) arrayList.get(i);
		for(int j=0;j<selectitems.size();j++)
		{
			//System.out.println("SELECT "+selectitems.get(j).toString());
			for(Entry<String,Datum> entry : test.entrySet())
			{
				if(selectitems.get(j).toString().contains("COUNT(") || selectitems.get(j).toString().contains("count("))
				{
					if(selectitems.get(j).toString().contains(" AS "))
					{
						
						String temp[] = selectitems.get(j).toString().split("AS");
						if(entry.getKey().equalsIgnoreCase(temp[0].trim()))
						{
							double d = (double)entry.getValue().getValue();
							int a = (int)d;
							System.out.print(separator+a);
						}
					}
					else
					{
						if(entry.getKey().equalsIgnoreCase(selectitems.get(j).toString()))
						{
							double d = (double)entry.getValue().getValue();
							int a = (int)d;
							System.out.print(separator+a);
						}
					}
				}
				else
				{
					if(selectitems.get(j).toString().contains(" AS "))
					{
						String temp[] = selectitems.get(j).toString().split("AS");
						if(entry.getKey().equalsIgnoreCase(temp[0].trim()))
							System.out.print(separator+entry.getValue().getValue());
					}
					else
					{
						if(selectitems.get(j).toString().contains("."))
						{
							if(entry.getKey().equalsIgnoreCase(selectitems.get(j).toString()))
							{
								if(entry.getValue().getType().equalsIgnoreCase("date"))
								{
									 SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
							
									 
									 String date = simpleDateFormat.format((Date)entry.getValue().getValue());
									 
									 System.out.print(separator+date);
								}
								else
								System.out.print(separator+entry.getValue().getValue());
							}
						}
						else if(selectitems.get(j).toString().equalsIgnoreCase(entry.getKey()))
						{
							System.out.print(separator+entry.getValue().getValue());
						}
						else	
						{
							//System.out.println(table.getWholeTableName()+"."+selectitems.get(j).toString()+" check ");
							if(entry.getKey().equalsIgnoreCase(table.getWholeTableName()+"."+selectitems.get(j).toString()))
								System.out.print(separator+entry.getValue().getValue());
						}
					}
				}
				separator = "|";
			}

		}
		System.out.println();
	}
/*	File dir = new File(Main.swapDir+"");
	for(File file1: dir.listFiles()) 
	{
		System.out.println("abc");

		try
		{
		file1.delete();
		}
		catch(Exception e)
		{
			
			e.printStackTrace();
		}
	}*/

}

public void printArrayDiff(List arrayList)
{
	for(int i=0;i<arrayList.size();i++)
	{
		Tuple t = (Tuple)arrayList.get(i);
		for(Entry<String,Datum> entry : t.entrySet())
			System.out.print(entry.getKey()+" "+entry.getValue().getValue()+" ");
		System.out.println();
	}
}








}
