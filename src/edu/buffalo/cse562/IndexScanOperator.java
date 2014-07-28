package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import jdbm.PrimaryHashMap;
import jdbm.PrimaryStoreMap;
import jdbm.PrimaryTreeMap;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class IndexScanOperator extends ExpressionEvaluator implements Operator {

	private Table table;
	private String tableName;
	private String[] condition;
	private ArrayList<Datum> values;
	private ArrayList<Datum> dateValues;
	private ArrayList<String> conditions;
	private boolean[] dateFlags = {true,false};
	RecordManager manager;
	ArrayList<Tuple> tuples;
	PrimaryStoreMap<Long, Tuple> primaryIndex;
	private Iterator<Row> keySetList;
	//PrimaryTreeMap<Row, ArrayList<Long>> primaryTreeIndex;
	//PrimaryHashMap<Row, ArrayList<Long>> primaryHashIndex;
	private ArrayList<String> colType;
	private ArrayList<String> colName;
	private List<ColumnDefinition> dataType;
	private int index = 0;
	private Expression selection;
	private Iterator<String> conditionItr;
	private Iterator<String> valuesItr;
	private Iterator<Tuple> returnTuples;
	private Map<Row, ArrayList<Long>> keyReturn = null;
	
	public IndexScanOperator(Table table,List<ColumnDefinition> colDefs,String[] condition,Expression selection)
	{
		this.table = table;
		this.condition = condition;
		this.tuples = new ArrayList<Tuple>();
		this.values = new ArrayList<Datum>();
		this.dateValues = new ArrayList<Datum>();
		conditions = new ArrayList<String>();
		this.dataType = colDefs;
		this.selection = selection;
		
		//System.out.println(selection);
		
		init();
		split();
		//System.out.println(tableName);
		try
		{
			manager = RecordManagerFactory.createRecordManager(Main.indexDir+"\\"+tableName);
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		primaryIndex = manager.storeMap(tableName+"P",new RowSerializer(colType,colName));
		//System.out.println(primaryIndex.size());
		conditionItr = conditions.iterator();
		getTuples();
	}
	
	void init() 
	{
		colType = new ArrayList<String>();
		colName = new ArrayList<String>();
		
		if(table.getAlias()!=null)
		{
			tableName = table.getAlias().toLowerCase();
		}
		else
		{
			tableName = table.getWholeTableName().toLowerCase();
		}
		
		for(ColumnDefinition colDef : dataType)
		{
			String type = colDef.getColDataType().toString().toLowerCase();
			String key = null;
			key = tableName + "." + colDef.getColumnName();
			colType.add(type);
			colName.add(key);
		}
	}
	
	public void split()
	{
		String temp[] = null;
		for(int i=0;i<condition.length;i++)
		{
			//System.out.println(condition[i]);
			
			if(condition[i].contains("="))
			{
				temp = condition[i].split("=");
				conditions.add(temp[0].replaceAll("\\(|\\)","").trim());
			}
			else if(condition[i].contains("|"))
			{
				temp = condition[i].split("\\|");
				conditions.add(temp[0].trim());
			}

			if(temp[1].trim().equals("GTE"))
			{
				dateValues.add(new GetDate(temp[2].trim()));
				dateValues.add(new GetDate(temp[4].trim()));
				if(temp[3].trim().equals("LT"))
					dateFlags[1] = false;
				if(temp[3].trim().equals("LTE"))
					dateFlags[1] = true;
			}
			else
			{
				values.add(new GetString(temp[1].replaceAll("\\'","").trim()));
			}
		}
	}
	
	public void getTuples()
	{
		for(int i=0;i<conditions.size();i++)
		{
			String map = Constants.mapIndices.get(conditions.get(i));
			//System.out.println("CON "+conditions.get(i));
			if(map.equals("secondarytree"))
			{
				//System.out.println("INSIDE TREE");
				PrimaryTreeMap<Row, ArrayList<Long>> primaryTreeIndex = manager.treeMap(conditions.get(i));
				Datum query[] = new Datum[2];
				
				for(int j=0;j<dateValues.size();j++)
				{
					query[j] = dateValues.get(j);
				}

				keyReturn = primaryTreeIndex.subMap(new Row(query[0]),new Row(query[1]));
				//System.out.println(keyReturn.size());
				
				if(keyReturn!=null && !keyReturn.isEmpty())
				{
					keySetList=  keyReturn.keySet().iterator();
					while(keySetList.hasNext())
					{
						ArrayList<Long> primaryLongValues = primaryTreeIndex.get(keySetList.next());
						for(int q=0; q<primaryLongValues.size(); q++)
						{
							/*Tuple o = primaryIndex.get(primaryLongValues.get(q));
							for(Entry<String, Datum> e : o.entrySet())
							{
								System.out.print(e.getValue().getValue()+"|");
							}
							System.out.println();*/
							tuples.add(primaryIndex.get(primaryLongValues.get(q)));
						}
					}
				}
				
				if(dateFlags[1] == true)
				{
					ArrayList<Long> primaryLongValues = primaryTreeIndex.get(new Row(query[1]));
					for(int q=0; q<primaryLongValues.size(); q++)
					{
						/*Tuple o = primaryIndex.get(primaryLongValues.get(q));
						for(Entry<String, Datum> e : o.entrySet())
						{
							System.out.print(e.getValue().getValue()+"|");
						}
						System.out.println();*/
						tuples.add(primaryIndex.get(primaryLongValues.get(q)));
					}
				}
				
				//System.out.println("END");
			}
			else if(map.equals("secondaryhashmap"))
			{
				PrimaryHashMap<Row, ArrayList<Long>> primaryHashIndex = manager.hashMap(conditions.get(i));
				Datum query = values.get(i);
				//System.out.println(query.getValue()+" "+conditions.get(i));
				//System.out.println(primaryHashIndex.size());
				ArrayList<Long> primaryLongValues = primaryHashIndex.get(new Row(query));
				/*if(primaryLongValues == null)
					System.out.println("PPPP");*/
				for(int q=0; q<primaryLongValues.size(); q++)
				{
					//Tuple o = primaryTreeIndex.get(primaryLongValues.get(q));
					tuples.add(primaryIndex.get(primaryLongValues.get(q)));
				}
			}
		}
		
		/*System.out.println(tuples.size());
		
		for(int index=0;index<tuples.size();index++)
		{
			for(Entry<String, Datum> e : tuples.get(index).entrySet())
			{
				System.out.print(e.getValue().getValue()+"|");
			}
			System.out.println();
		}*/
		
		if(tuples != null)
			returnTuples = tuples.iterator();
	}
	
	/*public void loadTuples()
	{
		if(conditionItr.hasNext())
		{
			String map = Constants.mapIndices.get(conditionItr.next());
			System.out.println("GT "+conditions.get(i));
			if(map.equals("secondarytree"))
			{
				PrimaryTreeMap<Row, ArrayList<Long>> primaryTreeIndex = manager.treeMap(condition[i]);
				Datum query = values.get(i);
				ArrayList<Long> primaryLongValues = primaryTreeIndex.get(new Row(query));
				
				for(int q=0; q<primaryLongValues.size(); q++)
				{
					//Tuple o = primaryTreeIndex.get(primaryLongValues.get(q));
					tuples.add(primaryIndex.get(primaryLongValues.get(q)));
				}
			}
			else if(map.equals("secondaryhashmap"))
			{
				PrimaryHashMap<Row, ArrayList<Long>> primaryHashIndex = manager.hashMap(conditions.get(i));
				Datum query = values.get(i);
				System.out.println(query.getValue()+" "+conditions.get(i));
				System.out.println(primaryHashIndex.size());
				ArrayList<Long> primaryLongValues = primaryHashIndex.get(new Row(new GetString(values.get(i).getValue().toString())));
				if(primaryLongValues == null)
					System.out.println("PPPP");
				for(int q=0; q<primaryLongValues.size(); q++)
				{
					//Tuple o = primaryTreeIndex.get(primaryLongValues.get(q));
					tuples.add(primaryIndex.get(primaryLongValues.get(q)));
				}
			}
		}
	}*/
	
	@Override
	public Tuple readOnetuple() 
	{
		// TODO Auto-generated method stub

		if(returnTuples.hasNext())
		{
			input = returnTuples.next();
			return input;
			/*if(selection != null)
			{
				if(evaluate())
				{
					return input;
				}
				else
				{
					resetFlag();
					return readOnetuple();
				}
			}
			else
			{
				return input;
			}*/
		}

		return null;
	}

	@Override
	public void reset()
	{
		// TODO Auto-generated method stub

	}
	
	public boolean evaluate()
	{
		
			return getOutput(selection, table,input);
	}

}
