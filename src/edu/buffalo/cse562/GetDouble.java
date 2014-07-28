package edu.buffalo.cse562;

public class GetDouble implements Datum {
	Double d;
	String type;
	
	GetDouble(String s)
	{
		d=Double.parseDouble(s);	
		type = "double";
	}
	
	@Override
	public Double getValue() {
		// TODO Auto-generated method stub
		return d;
	}
	
	public String getType()
	{
		return this.type;
	}

	public int hashCode()
	{
		return d.hashCode();
	}
	
	@Override
	public int compareTo(Datum d) {
		// TODO Auto-generated method stub
		int retval = Double.compare((double)this.getValue(),(double)d.getValue() );
		
		if(retval > 0 )
			return 1;
		else if(retval < 0)
			return -1;
		else 
			return 0;
	}
 
}
