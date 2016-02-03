package com.everdata.parser;

public class Expression {
	//<O_EQ> | <O_NEQ> | <O_GT> | <O_GTE> | <O_LT> | <O_LTE>
	public final static int EQ = 1;
    public final static int NEQ = 2;
    public final static int GT = 3;
    public final static int GTE = 4;
    public final static int LT = 5;
    public final static int LTE = 6;

    public String field;
	public int oper;
	public String value;
	public int valueType;

	public Expression() {
	}

}