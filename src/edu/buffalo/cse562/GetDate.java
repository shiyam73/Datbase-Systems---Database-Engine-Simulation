package edu.buffalo.cse562;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class GetDate implements Datum {
	String d;
	String type;
	//SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
	
	GetDate(String s){
		
			
			d= s;
			//System.out.println(d);
			type="date";
		
		
	}
	
	@Override 
	public String getValue() {
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
	
	
	public int compareTo(Datum rightexprDatum) {
		Date a = null,b = null;
		String aString = (String) this.getValue();
		String bString = (String) rightexprDatum.getValue();
		try {
		a = new SimpleDateFormat("yyyy-MM-dd").parse(aString);
		b = new SimpleDateFormat("yyyy-MM-dd").parse(bString);
		} catch(ParseException ex) {
			ex.printStackTrace();
		}
		if (a.after(b)) return 1;
		else if (a.before(b)) return -1;
		else return 0;
	}
	
	
}
