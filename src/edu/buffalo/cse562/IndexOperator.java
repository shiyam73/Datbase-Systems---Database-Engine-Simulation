package edu.buffalo.cse562;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;

import jdbm.PrimaryHashMap;
import jdbm.PrimaryStoreMap;
import jdbm.PrimaryTreeMap;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.SecondaryHashMap;
import jdbm.SecondaryKeyExtractor;
import jdbm.SecondaryTreeMap;

public class IndexOperator 
{
	private String key;
	ScanOperator scan;
	private String indexDir;
	RecordManager manager;
	ArrayList<String> colType;
	ArrayList<String> colName;
	ArrayList<String> columnNames;
	ArrayList<String> typeOfMaps;
	PrimaryStoreMap<Long, Tuple> primaryIndex;
	//SecondaryTreeMap<String, Tuple, Tuple> secondaryIndex;
	//SecondaryHashMap<String, Tuple, Tuple> secondaryHashIndex;
	ArrayList<HashMap<Row,ArrayList<Long>>> secondaryHashMaps;
	ArrayList<Tuple> tuples;
	
	public IndexOperator(ScanOperator scan, ArrayList<String> columnNames,ArrayList<String> typeOfMaps,String indexDir)
	{
		this.scan = scan;
		this.indexDir = indexDir;
		this.columnNames = columnNames;
		this.typeOfMaps = typeOfMaps;
		
		/*try
		{
			manager = RecordManagerFactory.createRecordManager(indexDir+"\\"+scan.getTableName());
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		colType = scan.getColType();
		colName = scan.getColName();
		
		secondaryHashMaps = new ArrayList<HashMap<Row,ArrayList<Long>>>();
		tuples = new ArrayList<Tuple>();
		
		index();
		//System.out.println(columnNames.size());
	}

	public void index()
	{
		try 
		{
			 
			
			//PrimaryTreeMap<Row, Tuple> lineItemIndex = manager.treeMap("lineItemIndex");
			
			
			/*PrimaryTreeMap<Tuple, Tuple> lineItemIndex = manager.treeMap("lineItemIndex",new RowSerializer(colType,colName),
					new RowSerializer(getKeyType(new String[]{"lineitem.orderkey","lineitem.linenumber"}), getKeyname(new String[]{"lineitem.orderkey","lineitem.linenumber"})));*/
			String temp[] = null;
			
		//	for(int i=0; i<columnNames.size();i++)
		//	{
				if(columnNames.get(0).contains("|"))
				{
					temp = columnNames.get(0).split("\\|");
				}
				else
				{
					temp = new String[]{columnNames.get(0)};
				}
				
				if(typeOfMaps.get(0).equals("primarytree"))
				{
					String column = columnNames.get(0).trim();
					try
					{
						manager = RecordManagerFactory.createRecordManager(indexDir+"\\"+scan.getTableName());
					} 
					catch (IOException e) 
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					primaryIndex = manager.storeMap(scan.getTableName()+"P", new RowSerializer(colType,colName));
				}
				/*else if(typeOfMaps.get(i).equals("secondarytree"))
				{
					final String column = columnNames.get(i).trim();
					secondaryIndex = primaryIndex.secondaryTreeMap(column, 
									new SecondaryKeyExtractor<String, Tuple, Tuple>() {
										public String extractSecondaryKey(Tuple key,Tuple value) {
											//System.out.println((String)value.get("orders.clerk").getValue().toString());
											return (String)value.get(column).getValue().toString();
										}
									});
				}
				else if(typeOfMaps.get(i).equals("secondaryhashmap"))
				{
					final String column = columnNames.get(i);
					secondaryHashIndex = primaryIndex.secondaryHashMap(column, 
									new SecondaryKeyExtractor<String, Tuple, Tuple>() {
										public String extractSecondaryKey(Tuple key,Tuple value) {
											//System.out.println((String)value.get("orders.clerk").getValue().toString());
											return (String)value.get(column).getValue().toString();
										}
									});
				}*/
		//	}
			
			
			/*PrimaryTreeMap<Tuple, Tuple> lineItemIndex = manager.treeMap("lineItemIndex",new RowSerializer(colType,colName),
					new RowSerializer(getKeyType(new String[]{"orders.orderkey"}), getKeyname(new String[]{"orders.orderkey"})));
			
			SecondaryTreeMap<String, Tuple, Tuple> orderDateIndex = 
					lineItemIndex.secondaryTreeMap("orderDateIndex", 
							new SecondaryKeyExtractor<String, Tuple, Tuple>() {
								public String extractSecondaryKey(Tuple key,Tuple value) {
									//System.out.println((String)value.get("orders.clerk").getValue().toString());
									return (String)value.get("orders.orderdate").getValue().toString();
								}
							});*/
			
			int i=0;
			int j=0;
			
			Tuple input = scan.readOnetuple();
			String[] primaryIndexArray = primaryIndexTuple(columnNames.get(0));
			
			
			for(int x=1;x<columnNames.size();x++)
			{
				String column = columnNames.get(i);
				HashMap<Row,ArrayList<Long>> maps = new HashMap<Row,ArrayList<Long>>();
				secondaryHashMaps.add(maps);
			}
			
			
			if(input != null)
			{
				do
				{
	
					Tuple key = new Tuple();

					long keyId = primaryIndex.putValue(input);
					
					
					for(int x=1;x<columnNames.size();x++)
					{
						Datum columnValue = input.get(columnNames.get(x));
						Row column = new Row(columnValue);
						if(!secondaryHashMaps.get(x-1).containsKey(column))
						{
						//	column.put(columnNames.get(x),columnValue);
							//tuples.add(column);
							ArrayList<Long> recIds = new ArrayList<Long>();
							recIds.add(keyId);
							secondaryHashMaps.get(x-1).put(column,recIds);
						}
						else
						{
						//	column.put(columnNames.get(x),columnValue);
							ArrayList<Long> recIds = secondaryHashMaps.get(x-1).get(column);
							recIds.add(keyId);
							secondaryHashMaps.get(x-1).put(new Row(columnValue),recIds);
						}
					}
					
					
					input = scan.readOnetuple();
					i++;
					j++;
					
					if(j > 10000)
					{
						j=0;
						manager.commit();
						manager.clearCache();
					}
					
				}while(input != null);
			}
			
			System.out.println(primaryIndex.size());
			manager.commit();
			//manager.close();
			
			//System.out.println("SHM SIZE "+secondaryHashMaps.size());
			
			/*for(int p=0; p< secondaryHashMaps.size();p++)
			{
				HashMap<Row, ArrayList<Long>> en = secondaryHashMaps.get(p);
				System.out.println(en.size());
				for(Entry<Row, ArrayList<Long>> e : en.entrySet())
				{
					
					System.out.println(e.getKey().getKey().getValue()+" "+e.getValue());
				}
				//System.out.println();
			}*/
			
			
			for(int p=0; p< secondaryHashMaps.size();p++)
			{
				//String condns[] = primaryIndexTuple(columnNames.get(p+1));
				System.out.println(columnNames.get(p+1));
				String type = typeOfMaps.get(p+1);
				if(type.equals("secondarytree"))
				{
					PrimaryTreeMap<Row, ArrayList<Long>> primaryTreeIndex = manager.treeMap(columnNames.get(p+1));
					HashMap<Row, ArrayList<Long>> en = secondaryHashMaps.get(p);
					primaryTreeIndex.putAll(en);	
				}
				else if(type.equals("secondaryhashmap"))
				{
					PrimaryHashMap<Row, ArrayList<Long>> primaryTreeIndex = manager.hashMap(columnNames.get(p+1));
					HashMap<Row, ArrayList<Long>> en = secondaryHashMaps.get(p);
					primaryTreeIndex.putAll(en);
				}
			}
			
			manager.close();
			/*for(int p=1;p<columnNames.size();p++)
			{
				PrimaryTreeMap<Row, ArrayList<Long>> ptIndex = manager.treeMap(columnNames.get(p));
				Datum query = new GetDate("1995-06-23");
				ArrayList<Long> a = ptIndex.get(new Row(query));
				
				for(int q=0; q<a.size(); q++)
				{
					Tuple b = primaryIndex.get(a.get(q));
					for(Entry<String, Datum> e : b.entrySet())
					{
						System.out.print(e.getValue().getValue()+"|");
					}
					System.out.println();
				}
			}*/
			
			/*System.out.println(columnNames.get(2));
			PrimaryHashMap<Row, ArrayList<Long>> ptIndex = manager.hashMap("orders.orderpriority");
			Datum query = new GetString("1-URGENT");
			ArrayList<Long> a = ptIndex.get(new Row(query));
			
			for(int q=0; q<a.size(); q++)
			{
				Tuple b = primaryIndex.get(a.get(q));
				for(Entry<String, Datum> e : b.entrySet())
				{
					System.out.print(e.getValue().getValue()+"|");
				}
				System.out.println();
			}		*/	
			//new Row(new GetString("1-URGENT"))
			//System.out.println(primaryIndex.size()+" "+i);
			
			/*System.out.println(lineItemIndex.size()+" "+i);
			System.out.println(orderDateIndex.size());
			
			Set<String> a = orderDateIndex.keySet();
			Iterator its = a .iterator();
			while(its.hasNext())
			{
				String clerk = (String) its.next();
				System.out.println(clerk+" "+);
			}
			Iterable<Tuple> a = orderDateIndex.get("1996-06-02");
			Iterator b = a.iterator();
			while(b.hasNext())
			{
				Tuple x = (Tuple) b.next();
				Datum d = x.get("orders.orderkey");
			//	x = lineItemIndex.get((int)d.getValue());
				Tuple y = lineItemIndex.get(x);
				//System.out.println(d.getValue());
				for(Entry<String, Datum> e : y.entrySet())
					System.out.print(e.getValue().getValue()+"|");
				System.out.println();
			}*/
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public ArrayList<String> getKeyType(String[] key)
	{
		ArrayList<String> a = new ArrayList<String>();
		
		for(int j=0;j<key.length;j++)
		{
			for(int i=0;i< colName.size();i++)
			{
				if(colName.get(i).equals(key[j]))
				{
					a.add(colType.get(i));
					break;
				}
			}
		}
		return a;
	}
	
	public ArrayList<String> getKeyname(String[] key)
	{
		ArrayList<String> a = new ArrayList<String>();
		
		for(int j=0;j<key.length;j++)
		{
			for(int i=0;i< colName.size();i++)
			{
				if(colName.get(i).equals(key[j]))
				{
					a.add(colName.get(i));
					break;
				}
			}
		}
		return a;
	}
	
	public String[] primaryIndexTuple(String primaryKey)
	{
		String temp[] = null;
		if(primaryKey.contains("|"))
		{
			temp = primaryKey.split("\\|");
		}
		else
		{
			temp = new String[]{primaryKey};
		}
		return temp;
	}
	
	
	
	
}
