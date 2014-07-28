package edu.buffalo.cse562;

public class GetInteger implements Datum {

	Integer a;
	String type;
	GetInteger(String s)
	{
		a=Integer.parseInt(s);
		type = "int";
	}
	
	@Override
	public Integer getValue() {
		// TODO Auto-generated method stub
				return a;
	}
	
	public String getType()
	{
		return this.type;
	}

	public int hashCode()
	{
		return a.hashCode();
	}
	
	public int compareTo(Datum o1)
	{
		if(this.getValue() > (int)o1.getValue() )
			return 1;
		else if(this.getValue() < (int) o1.getValue())
			return -1;
		else 
			return 0;
	}
}
