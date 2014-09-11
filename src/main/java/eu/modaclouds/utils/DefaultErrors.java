/**
 * 
 */
package eu.modaclouds.utils;
/**
 * @author Marco Scavuzzo
 *
 */
public class DefaultErrors {
	public static String alreadyConnected = "Error 1: A connection is already in place. Disconnect first.";
	public static String connectionError = "Error 2: Unable to connect to the destination.";
	public static String notConnected = "Error 3: No active connection.";
	public static String credentialsStorageError = "Error 4: Unable to save credentials";
	public static String credentialsGetError = "Error 5: Credential not found";
	public static String credentialsDeleteError = "Error 6: Unable to delete credentials";
	public static String databaseNotSupported = "Error 7: One or all of the selected databases are not supported";
	public static String fewParameters = "Error 8: Too few parameters. Check the documentation";
	
	public static String getErrorNumber(String error){
		int start = error.lastIndexOf("Error ");
		int col = error.lastIndexOf(": ");
		
		return error.substring(start+6, col);
	}
	
	public static String getErrorMessage(String error){
		int col = error.lastIndexOf(": ");
		return error.substring(col+1);
	}
}
