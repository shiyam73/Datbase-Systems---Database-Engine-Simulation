package edu.buffalo.cse562;

public class GetString implements Datum {
	String s;
	String type;
	
	GetString(String s)
	{
		this.s=s;
		type = "string";
	
	}
	
	@Override
	public String getValue() {
		// TODO Auto-generated method stub
		return this.s;
	}
	
	public String getType()
	{
		return this.type;
	}
	
	public int hashCode()
	{
		return s.hashCode();
	}
	
	public int compareTo(Datum o1)
	{
		
		return this.getValue().compareTo(o1.getValue().toString());
	}
}
