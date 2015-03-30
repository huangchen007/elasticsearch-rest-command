package com.everdata.command;

/**
 * General exception for query execution issues.
 */
public class CommandException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -916314713696048647L;

	/**
	 * Constructs a CommandException, given the error message.
	 */
	public CommandException(String message) {
		super(message);
	}

} // public class QueryException extends Exception
