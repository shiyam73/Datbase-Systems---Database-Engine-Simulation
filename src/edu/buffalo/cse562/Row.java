package edu.buffalo.cse562;

import java.io.Serializable;


public class Row implements Serializable, Comparable<Row>
//public class Row implements Serializable
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Datum key;
	
	Row(Datum key)
	{
		this.key = key;
	}

	Datum getKey()
	{
		return key;
	}
	
	public boolean equals(Object obj)
	{
		if(obj == null)
			return false;
		if(getClass() != obj.getClass())
			return false;
		
		final Row other = (Row)obj;
		
		if(this.getKey().getValue() ==  null && other.getKey().getValue() == null)
			return true;
		if(this.getKey().getValue().equals(other.getKey().getValue()))
			return true;
		
		return false;
	}
	
	public int hashCode()
	{
		int hash = 1;
		hash = 53* hash + (this.key.getValue() != null ? this.key.getValue().hashCode() : 0);
		return hash;
	}
	
	@Override
	public int compareTo(Row o) 
	{
		// TODO Auto-generated method stub
		
		//return compareRows(this.getKey(), o.getKey());
		return this.getKey().compareTo(o.getKey());
	}
	
	/*public  static int compareRows(Datum a,Datum b)
	{
		int result,first = 0,second,index = 0;
		while(index < a.length)
		{
			first = a[index].compareTo(b[index]);
			if(first == 0)
			{
				index++;
				if(index < a.length)
					return  a[index].compareTo(b[index]);
			}
			else
				break;
		}
		return first;
	}*/
}