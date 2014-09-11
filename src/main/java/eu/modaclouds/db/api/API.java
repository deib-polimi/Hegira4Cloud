package eu.modaclouds.db.api;

import java.util.HashMap;
import java.util.List;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.PropertyConfigurator;

import eu.modaclouds.db.adapters.AbstractDatabase;
import eu.modaclouds.db.adapters.DatabaseFactory;
import eu.modaclouds.db.adapters.datastore.Datastore;
import eu.modaclouds.exceptions.ConnectException;
import eu.modaclouds.utils.Constants;
import eu.modaclouds.utils.DefaultErrors;
import eu.modaclouds.utils.PropertiesManager;
import eu.modaclouds.utils.PropertiesManager.Property;

//Sets the path to base URL + /api
@Path("/api")
public class API {
	//static BlockingQueue<Metamodel> queue = new LinkedBlockingDeque<Metamodel>();
	
	/*-----------------------------------------------------------------------------------*/
	/*------------------------------ SWITCHOVER HANDLING -------------------------------*/
	@POST
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	@Path("/switchover")
	public Status switchOver(@QueryParam("source") String source, 
			@QueryParam("destination") final List<String> destination,
			@QueryParam("threads") int threads){
		String logs = Thread.currentThread().getContextClassLoader().getResource(Constants.LOGS_PATH).getFile();
		PropertyConfigurator.configure(logs);
		//BasicConfigurator.configure();
		//Check input params number
		if(source != null && destination != null && destination.size()>=1){
			//check input content
			boolean source_supported = Constants.isSupported(source);
			List<String> supported_dest = Constants.getSupportedDBfromList(destination);
			if(source_supported && supported_dest.size() >= 1){
				
				String message = "Mapping from "+source+ " to ";
				StringBuilder sb = new StringBuilder(message);
				for(String sup : supported_dest){
					sb.append(sup+" ");
				}
				
				//TODO: Update when multiple switchover will be available
				
//				AbstractDatabase src = DatabaseFactory.getDatabase(source, queue);
//				AbstractDatabase dst = DatabaseFactory.getDatabase(supported_dest.get(0), queue);
				
				HashMap<String, String> options_producer = new HashMap<String,String>();
				options_producer.put("mode", Constants.PRODUCER);
				HashMap<String, String> options_consumer = new HashMap<String,String>();
				options_consumer.put("mode", Constants.CONSUMER);
				
				if(threads>=1){
					options_consumer.put("threads", ""+threads);
				}
				
				AbstractDatabase src = DatabaseFactory.getDatabase(source, options_producer);
				AbstractDatabase dst = DatabaseFactory.getDatabase(supported_dest.get(0), options_consumer);
				
				src.switchOver(dst);
				
				return new Status(Constants.STATUS_SUCCESS, sb.toString());
			} else {
				//Cannot switchover - Not supported databases
				return new Status(Constants.STATUS_ERROR, DefaultErrors.getErrorMessage(DefaultErrors.databaseNotSupported),
						DefaultErrors.getErrorNumber(DefaultErrors.databaseNotSupported));
			}
		} else{
			//Cannot switchover - Few parameters
			return new Status(Constants.STATUS_ERROR, DefaultErrors.getErrorMessage(DefaultErrors.fewParameters),
					DefaultErrors.getErrorNumber(DefaultErrors.fewParameters));
		}
	}
	/*-----------------------------------------------------------------------------------*/
	
	/*-----------------------------------------------------------------------------------*/
	/*---------------------------- DATASTORE STANDARDIZATION ----------------------------*/
	@GET
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	@Path("/standardizeDatastore")
	/**
	 * Adjusts datastore's entities in order to provide transactionality after switchover.
	 * It inserts a property <code>isRoot</code> on root entities.
	 * @param kinds A list containing the entities' kinds to be adjusted. If no kind is passed every entity in the datastore will be checked, independently from its kind. 
	 * @return The Status for the operation
	 */
	public Status standardizeDatastore(@QueryParam("kind") final List<String> kinds){
		String logs = Thread.currentThread().getContextClassLoader().getResource(Constants.LOGS_PATH).getFile();
		PropertyConfigurator.configure(logs);
		//final Datastore gae = (Datastore) DatabaseFactory.getDatabase(Constants.GAE_DATASTORE, queue);
		HashMap<String, String> options_consumer = new HashMap<String,String>();
		options_consumer.put("mode", Constants.CONSUMER);
		final Datastore gae = (Datastore) DatabaseFactory.getDatabase(Constants.GAE_DATASTORE, options_consumer);
		
		(new Thread() {
			@Override public void run() {
				try {
					gae.connect();
					gae.standardizeDatastore(kinds);
				} catch (ConnectException e) {
					e.printStackTrace();
				} finally{
					gae.disconnect();
				}     
			}
		}).start();
			
		
		return new Status(Constants.STATUS_SUCCESS, (kinds==null)?"Standardizing the whole datastore" :
			"Standardizing entities with the passed kinds");
	}
	
	/*-----------------------------------------------------------------------------------*/
	
	/*-----------------------------------------------------------------------------------*/
	/*------------------------------ CREDENTIALS HANDLING -------------------------------*/
	@GET
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	@Path("/credentials")
	public Property getCredentials(@QueryParam("name") String name){
		String logs = Thread.currentThread().getContextClassLoader().getResource(Constants.LOGS_PATH).getFile();
		PropertyConfigurator.configure(logs);
		return PropertiesManager.getCredentialsProperty(name);
	}
	
	@PUT
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	@Path("/credentials")
	public Status putCredentials(@QueryParam("name") String name, @QueryParam("value") String value){
		String logs = Thread.currentThread().getContextClassLoader().getResource(Constants.LOGS_PATH).getFile();
		PropertyConfigurator.configure(logs);
		boolean created = PropertiesManager.createCredentials(name, value);
		Status status;
		if(created){
			status = new Status(Constants.STATUS_SUCCESS);
		}else{
			status = new Status(Constants.STATUS_ERROR, DefaultErrors.getErrorMessage(DefaultErrors.credentialsStorageError),
					 DefaultErrors.getErrorNumber(DefaultErrors.credentialsStorageError));
		}
		return status;
	}

	@POST
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	@Path("/credentials")
	public Status updateCredentials(@QueryParam("name") String name, @QueryParam("value") String value){
		String logs = Thread.currentThread().getContextClassLoader().getResource(Constants.LOGS_PATH).getFile();
		PropertyConfigurator.configure(logs);
		boolean created = PropertiesManager.createCredentials(name, value);
		Status status;
		if(created){
			status = new Status(Constants.STATUS_SUCCESS);
		}else{
			status = new Status(Constants.STATUS_ERROR, DefaultErrors.getErrorMessage(DefaultErrors.credentialsStorageError)+" "+logs,
					 DefaultErrors.getErrorNumber(DefaultErrors.credentialsStorageError));
		}
		return status;
	}
	
	@DELETE
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	@Path("/credentials")
	public Status deleteCredentials(@QueryParam("name") String name){
		String logs = Thread.currentThread().getContextClassLoader().getResource(Constants.LOGS_PATH).getFile();
		PropertyConfigurator.configure(logs);
		boolean removed = PropertiesManager.removeCredentials(name);
		Status status;
		if(removed){
			status = new Status(Constants.STATUS_SUCCESS);
		}else{
			status = new Status(Constants.STATUS_ERROR, DefaultErrors.getErrorMessage(DefaultErrors.credentialsDeleteError),
					 DefaultErrors.getErrorNumber(DefaultErrors.credentialsDeleteError));
		}
		return status;
	}
	
	/*-----------------------------------------------------------------------------------*/
	  @GET
	  @Produces(MediaType.TEXT_PLAIN)
	  public String sayPlainTextHello() {
	    return "Hello World";
	  }
	 
	  
	  	@POST
		@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
		@Path("/prova")
		public Status prova(@QueryParam("name") String name, @QueryParam("value") String value){
			Status status = new Status(Constants.STATUS_SUCCESS, "POST is working params: "+name+" & "+value);
			return status;
		}
	  @GET
	  @Produces(MediaType.TEXT_HTML)
	  public String sayHtmlHello(){
//		  AbstractDatabase azure = DatabaseFactory.getDatabase(Constants.AZURE_TABLES, new LinkedBlockingDeque<Metamodel>());
//		  StringBuilder sb = new StringBuilder();
//		  try {
//			  azure.connect();
//			  Iterable<String> tablesList = ((eu.modaclouds.db.adapters.azuretables.Tables) azure).getTablesList();
//			  
//			  for(String table : tablesList){
//				  sb.append(table+" - ");
//			  }
//		  }catch(ConnectException e){
//			  System.err.println("Exception!");
//		  }finally{
//			  azure.disconnect();
//		  }
		  //return "<html><body><img src=http://modaclouds.eu/img/image4094_1.png /><h1>ModaCLOUD_DB</h1><br/>"+sb.toString()+"</body></html>";
		  return Constants.API_intro;
	  }
}
