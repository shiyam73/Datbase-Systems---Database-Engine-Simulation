package edu.buffalo.cse562;

import java.util.*;

public class Tuple extends LinkedHashMap<String,Datum> implements Comparable<Tuple>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	static int check=0;
	@Override
	public int compareTo(Tuple o)
	{
		// TODO Auto-generated method stub
		
		return compareRows(this, o, o.size());
	}
	
	public int compareRows(Tuple a,Tuple b,int count)
	{
		
		int result =0;
		int itr = 0;
		
		
		if(count == 1)
		{
			for(java.util.Map.Entry<String, Datum> entry : a.entrySet())
			{
				result =  entry.getValue().compareTo(b.get(entry.getKey()));
				break;
			}
		}
		else
		{
			//System.out.println("Inside else");
			for(java.util.Map.Entry<String, Datum> entry : a.entrySet())
			{
				//System.out.println(entry.getKey());
				result =  entry.getValue().compareTo(b.get(entry.getKey()));
				if(result == 0)
				{
					if(itr == 0)
					{
						itr = 1;
						//System.out.println("AA  "+entry.getKey());
						continue;
					}
				}
				else
					return result;
			}
		}
		return result;	
	}
	
	
	
	
}
