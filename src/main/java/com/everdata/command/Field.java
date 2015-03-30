package com.everdata.command;


public class Field {
	
	public final static int SINGLE = 1;
	public final static int SCRIPT = 2;
	
	public static String fieldsToScript(String[] fields){

			StringBuilder script = new StringBuilder("doc['" + fields[0] + "'].value");
			
				
			for(int i = 1; i< fields.length; i++){
				script.append("+ '| ' + doc['");
				script.append( fields[i] );
				script.append("'].value");
			}
			
			return script.toString();					
	}

}
