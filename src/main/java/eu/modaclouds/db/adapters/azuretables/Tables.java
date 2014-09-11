/**
 * 
 */
package eu.modaclouds.db.adapters.azuretables;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import com.microsoft.windowsazure.services.core.storage.CloudStorageAccount;
import com.microsoft.windowsazure.services.core.storage.ResultContinuation;
import com.microsoft.windowsazure.services.core.storage.ResultSegment;
import com.microsoft.windowsazure.services.core.storage.StorageException;
import com.microsoft.windowsazure.services.table.client.CloudTable;
import com.microsoft.windowsazure.services.table.client.CloudTableClient;
import com.microsoft.windowsazure.services.table.client.DynamicTableEntity;
import com.microsoft.windowsazure.services.table.client.EntityProperty;
import com.microsoft.windowsazure.services.table.client.TableOperation;
import com.microsoft.windowsazure.services.table.client.TableQuery;
import com.microsoft.windowsazure.services.table.client.TableResult;
import com.rabbitmq.client.QueueingConsumer;

import eu.modaclouds.db.adapters.AbstractDatabase;
import eu.modaclouds.db.models.AzureTablesModel;
import eu.modaclouds.db.models.Metamodel;
import eu.modaclouds.db.transformers.AzureTablesTransformer;
import eu.modaclouds.exceptions.ConnectException;
import eu.modaclouds.utils.Constants;
import eu.modaclouds.utils.DefaultErrors;
import eu.modaclouds.utils.DefaultSerializer;
import eu.modaclouds.utils.ObjectSizeCalculator;
import eu.modaclouds.utils.PropertiesManager;
/**
 * @author Marco Scavuzzo
 *
 */
public class Tables extends AbstractDatabase<AzureTablesModel>{

	private transient Logger log = Logger.getLogger(Tables.class);
	
	private CloudStorageAccount account;
	private CloudTableClient tableClient;
	private BlockingQueue<Metamodel> queue;
	
//	public Tables(){
//		
//	}
//	
//	public Tables(BlockingQueue<Metamodel> queue){
//		this.queue = queue;
//	}
	
	public Tables(Map<String, String> options){
		super(options);
		if(options.get("threads")!=null)
			this.THREADS_NO = Integer.parseInt(options.get("threads"));
	}

	@Override
	public void connect() throws ConnectException {
		if(!isConnected()){
			String credentials = PropertiesManager.getCredentials(Constants.AZURE_PROP);
			//log.debug(Constants.AZURE_PROP+" = "+credentials);
			try {
				account = CloudStorageAccount.parse(credentials);
				tableClient = account.createCloudTableClient();
				log.debug("Connected");
			} catch (InvalidKeyException | URISyntaxException e) {
				e.printStackTrace();
				throw new ConnectException(DefaultErrors.connectionError);
			}
		}else{
			throw new ConnectException(DefaultErrors.alreadyConnected);
		}
	}

	@Override
	public void disconnect() {
		if(isConnected()){
			account=null;
			tableClient=null;
			log.debug("Disconnected");
		}else{
			log.warn(DefaultErrors.notConnected);
		}
	}

	@Override
	protected void persist() {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void merge() {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void remove() {
		// TODO Auto-generated method stub
		
	}
	
	long count=1;
	@Override
	protected AbstractDatabase<AzureTablesModel> fromMyModel(Metamodel mm) {
		
		
		//Consumer
		log.debug(Thread.currentThread().getName()+" Hi I'm the AZURE consumer!");
		
		//Instantiate the Thrift Deserializer
        TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
        
		try{
			while(true){
				//log.debug(Thread.currentThread().getName()+" Extracting from the queue");
				//Metamodel myModel = queue.take();
				QueueingConsumer.Delivery delivery = consumer.nextDelivery();
				if(delivery!=null){
					//OLD IMPLEMENTATION
					//Metamodel myModel = (Metamodel) DefaultSerializer.deserialize(delivery.getBody());
					
					//new Thrift implementation
					Metamodel myModel = new Metamodel();
					deserializer.deserialize(myModel, delivery.getBody());
					
					//log.debug(Thread.currentThread().getName()+" Consuming: "+myModel.getRowKey());
				
					AzureTablesTransformer att = new AzureTablesTransformer();
					AzureTablesModel fromMyModel = att.fromMyModel(myModel);
					//TODO: get the dynamicEntity from fromMyModel and
					//call insertEntity after having checked if the table exists
					//DynamicTableEntity entity = fromMyModel.getEntity();
					List<DynamicTableEntity> entities = fromMyModel.getEntities();
					
					channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
					
					String tableName = fromMyModel.getTableName();
					createTable(tableName);
					//TODO: tesisting output
					//long totalSize=0;
					for(DynamicTableEntity entity : entities){
						//TODO: tesisting output
						//long sizeOf = ObjectSizeCalculator.sizeOf(entity);
						//totalSize+=sizeOf;
						//log.debug(Thread.currentThread().getName()+" SizeOfOutput: "+sizeOf);
						insertEntity(tableName, entity);
						count++;
						if(count%2000==0) 
							log.debug(Thread.currentThread().getName()+" Inserted "+count+" entities");
					}
				}else{
					log.debug(Thread.currentThread().getName()+" The queue is empty.");
				}
				//TODO: tesisting output
//				if(queue.isEmpty()){
//					log.debug("--> Total SizeOfOutput: "+totalSize);
//				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (StorageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	//TODO: Remove - just for testing
	public Metamodel publicToMyModel(AbstractDatabase<AzureTablesModel> db) {
		return toMyModel(db);
	}
	
	@Override
	protected Metamodel toMyModel(AbstractDatabase<AzureTablesModel> db) {
		// Producer
		Tables azure = (Tables) db;
		
		//TODO:modifiche test
		Iterable<String> tablesList = azure.getTablesList();
		
		for(String table : tablesList){
		
			//TODO:uncomment for testing a given table.
			//String table = "Tweets";
			//int i = 100;
			//long queueSize = 0;
			/************/
			
			//Create a new instance of the Thrift Serializer
	        TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
			//TODO: prendere le entity a lotti per non caricare tutto in memoria!!
			//Iterable<DynamicTableEntity> entities = azure.getEntitiesByTable(table);
			
			ResultContinuation continuationToken = null;
			while(true){
				//log.debug("\n\t"+(i/1000)+" Token: "+continuationToken+"\n");
				try {
					ArrayList<DynamicTableEntity> results = new ArrayList<DynamicTableEntity>();
					ResultSegment<DynamicTableEntity> segment = null;
					/**
					 * Bullet proof reads from Tables.
					 */
					boolean proof = true;
					while(proof){
						try{
							segment = 
									getEntities_withRange(table, continuationToken, 1000);
							results = segment.getResults();
							proof = false;
						}catch(IOException e){
							log.error(Thread.currentThread().getName() + 
									" ERROR: error reading from Tables -> "+e.getMessage());
						}
					
					}
					
					
					
					for(DynamicTableEntity entity : results){
						
						AzureTablesModel model = new AzureTablesModel(table, entity);
						AzureTablesTransformer transformer = new AzureTablesTransformer();
						Metamodel myModel = (Metamodel) transformer.toMyModel(model);
						
						try {
							//long sizeOf = ObjectSizeCalculator.sizeOf(myModel);
							//log.debug("Producing: "+myModel.getRowKey()+" of size: "+sizeOf+" bytes");
							
							/** Non persistent message inserted in the queue **/
							channel.basicPublish( "", TASK_QUEUE_NAME, 
						            null,
						            serializer.serialize(myModel));
							
							//OLD IMPLEMENTATION
							//queue.put(myModel);
							
							//TODO:togliere dopo i test
							//i--;
							//queueSize += sizeOf;
							/************/
						} catch ( IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (TException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						//uscita dal for per test
//						if(i==0){ 
//							log.debug(" ==> Transferred "+i+" entities. For a total of "+queueSize+" Bytes = "+queueSize/1024+"KB = "+
//									queueSize/(1024*1024)+"MB in the Queue");
//							return null;
//						}
//						try {
//							ObjectsUtils.printObject(myModel);
//							List<String> columnFamilies = myModel.getColumnFamilies();
//							for(String family : columnFamilies){
//								log.debug("column family: "+family);
//							}
//							List<Column> columns = myModel.getColumns();
//							log.debug("printing columns: ");
//							for(Column column : columns){
//								ObjectsUtils.printObject(column);
//							}
//						} catch (IllegalArgumentException | IllegalAccessException e) {
//							// TODO Auto-generated catch block
//							e.printStackTrace();
//						}
						
					}
					if(segment != null){
						continuationToken = segment.getContinuationToken();
						if(!segment.getHasMoreResults()) break;
					}else{
						break;
					}
					
				} catch (InvalidKeyException | URISyntaxException
						| StorageException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}	
			
		}
		return null;
	}

	/**
	 * Creates the table in the storage service if it does not already exist.
	 *     
	 * @param tableName A String that represents the table name.
	 * @return The created table or null
	 * @throws URISyntaxException If the resource URI is invalid
	 */
	public CloudTable createTable(String tableName) throws URISyntaxException{
		
		if(tableName.indexOf("@")==0)
			tableName=tableName.substring(1);
		//log.debug("Creating table: "+tableName);
		if(isConnected()){
			CloudTable cloudTable = new CloudTable(tableName, tableClient);
			try {
				cloudTable.createIfNotExist();
			} catch (StorageException e) {
				e.printStackTrace();
			}
			return cloudTable;
		}else{
			log.info(DefaultErrors.notConnected);
			return null;
		}
	}

	/**
	 * Insert an entity in the given table, provided that the table exists
	 * @param tableName Name of the table where to insert the entity
	 * @param entity An instance of the DynamicEntity
	 * @throws StorageException - if an error occurs accessing the storage service, or the operation fails.
	 * 
	 * @return A TableResult containing the result of executing the TableOperation on the table. The TableResult class encapsulates the HTTP response and any table entity results returned by the Storage Service REST API operation called for a particular TableOperation.
	 */
	public TableResult insertEntity(String tableName, DynamicTableEntity entity) throws StorageException{
		if(tableName.indexOf("@")==0)
			tableName=tableName.substring(1);
		//log.debug("Inserting entity: "+entity.getRowKey()+" into "+tableName);
		if(isConnected()){
			/**
			 * Bullet proof write
			 */
			int retries = 0;
			
			while(true){
				try{
					// Create an operation to add the new entity.
					TableOperation insertion = TableOperation.insertOrMerge(entity);
			
					// Submit the operation to the table service.
					return tableClient.execute(tableName, insertion);
				}catch(Exception e){
					retries++;
					log.error(Thread.currentThread().getName() + 
							" ERROR -> insertEntity : "+e.getMessage());
					
					log.error("Entity "+entity.getRowKey(), e);
				}finally{
					if(retries>=5){
						log.error(Thread.currentThread().getName() + 
								"SKIPPING entity: "+entity.getRowKey());
						return null;
					}
				}
			}
			
		}else{
			log.info(DefaultErrors.notConnected);
			return null;
		}
	}
	/**
	 * Get a list of the tables contained by the Azure account
	 * @return A collection containing table's names as String
	 */
	public Iterable<String> getTablesList(){
		if(isConnected()){
			Iterable<String> listTables = tableClient.listTables();
//			
//			log.debug("------------- TABLES LIST ---------------");
//			int i=1;
//			for(String table : listTables){
//				log.debug(i+": "+table);
//				i++;
//			}i=0;
//			
			return listTables;
		}else{
			log.info(DefaultErrors.notConnected);
			return null;
		}
	}
	/**
	 * Extracts the entities contained in a given table
	 * @param tableName The table name
	 * @return A collection containing table entities
	 */
	public Iterable<DynamicTableEntity> getEntitiesByTable(String tableName){
		if(isConnected()){
			TableQuery<DynamicTableEntity> partitionQuery =
				    TableQuery.from(tableName, DynamicTableEntity.class);
			return tableClient.execute(partitionQuery);
		}else{
			log.info(DefaultErrors.notConnected);
			return null;
		}
	}
	/**
	 * Extracts the entities contained in a given table in batches
	 * @param tableName The table name
	 * @param continuationToken The next point where to start fetching entities. Pass <code>null</code> at the beginning.
	 * @param pageSize The number of entities to retrieve. NB: 1000 max otherwise throws a <code> StorageException </code>
	 * @return A collection containing the entities, the continuationToken and other stuff.
	 * @throws InvalidKeyException
	 * @throws URISyntaxException
	 * @throws IOException
	 * @throws StorageException
	 */
	public ResultSegment<DynamicTableEntity> getEntities_withRange(String tableName,
			ResultContinuation continuationToken,
			int pageSize) throws InvalidKeyException, URISyntaxException, IOException, StorageException{
		if(isConnected()){
			TableQuery<DynamicTableEntity> partitionQuery =
				    TableQuery.from(tableName, DynamicTableEntity.class).take(pageSize);
			
			return tableClient.executeSegmented(partitionQuery, continuationToken);
		}else{
			log.info(DefaultErrors.notConnected);
			return null;
		}
	}
	
	/**
	 * Prints a collection containing table's entities
	 * @param collection A collection containing table's entities
	 */
	public void printEntitiesCollection(Iterable<DynamicTableEntity> collection){
		int i=0,j=0;
		for(DynamicTableEntity entity : collection){
			i++;
			HashMap<String, EntityProperty> properties = entity.getProperties();
			log.debug("--------- ENTITY "+i+" ---------");
			log.debug("RowKey: "+entity.getRowKey()+" PartitionKey: "+entity.getPartitionKey());
			Iterator<String> propIter = properties.keySet().iterator();
			while( propIter.hasNext()){
				j++;
				String key = propIter.next();
				log.debug("Key: "+key+" Value: "+properties.get(key).getValueAsString()+" Entity="+i
						+" Property: "+j);
			}
			j=0;
		}
	}
	
	public CloudStorageAccount getAccount() {
		return account;
	}

	public void setAccount(CloudStorageAccount account) {
		this.account = account;
	}

	public CloudTableClient getTableClient() {
		return tableClient;
	}

	public void setTableClient(CloudTableClient tableClient) {
		this.tableClient = tableClient;
	}
	/**
	 * Checks if a connection has already been established
	 * @return true if connected, false if not.
	 */
	public boolean isConnected(){
		return (account==null || tableClient==null) ? false : true;
	}
}
