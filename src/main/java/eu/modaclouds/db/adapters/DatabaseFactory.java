/**
 * !! FACTORY PATTERN
 */
package eu.modaclouds.db.adapters;

import java.util.Map;
import java.util.concurrent.BlockingQueue;

import eu.modaclouds.db.adapters.azuretables.Tables;
import eu.modaclouds.db.adapters.datastore.Datastore;
import eu.modaclouds.db.models.Metamodel;
import eu.modaclouds.utils.Constants;

/**
 * @author Marco Scavuzzo
 *
 */
public class DatabaseFactory {
	//DEPRECATED
//	public static AbstractDatabase getDatabase(String name, BlockingQueue<Metamodel> queue){
//		switch(name){
//			case Constants.GAE_DATASTORE:
//				return new Datastore(queue);
//			case Constants.AZURE_TABLES:
//				return new Tables(queue);
//			default:
//				return null;
//		}
//	}
	/**
	 * Get an instance of a database as a producer or as a consumer
	 * @param name Name of the database to be instantiated
	 * @param options A map containing the properties of the new db object to be created. 
	 * 				   <code>mode</code> Consumer or Producer.
	 * 				   <code>threads</code> The number of consumer threads.
	 * @return
	 */
	public static AbstractDatabase getDatabase(String name, Map<String, String> options){
		
		switch(name){
			case Constants.GAE_DATASTORE:
				return new Datastore(options);
			case Constants.AZURE_TABLES:
				return new Tables(options);
			default:
				return null;
		}
	}
}
