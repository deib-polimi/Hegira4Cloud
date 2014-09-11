package eu.modaclouds.db.adapters.datastore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import com.google.appengine.api.datastore.Cursor;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entities;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.QueryResultList;
import com.google.appengine.api.datastore.Query.FilterPredicate;
import com.google.appengine.tools.remoteapi.RemoteApiInstaller;
import com.google.appengine.tools.remoteapi.RemoteApiOptions;
import com.rabbitmq.client.QueueingConsumer;

import eu.modaclouds.db.adapters.AbstractDatabase;
import eu.modaclouds.db.models.DatastoreModel;
import eu.modaclouds.db.models.Metamodel;
import eu.modaclouds.db.transformers.DatastoreTransformer;
import eu.modaclouds.exceptions.ConnectException;
import eu.modaclouds.utils.Constants;
import eu.modaclouds.utils.DefaultErrors;
import eu.modaclouds.utils.PropertiesManager;

public class Datastore extends AbstractDatabase<DatastoreModel> {

	private static Logger log = Logger.getLogger(Datastore.class);
	
	private RemoteApiInstaller installer;
	private DatastoreService ds;
	//OLD IMPLEMENTATION
	//private BlockingQueue<Metamodel> queue;

//	public Datastore(){
//		
//	}
	
	/**
	 * Instantiate a consumer or a producer
	 * @param mode consumer or producer mode
	 */
	public Datastore(Map<String, String> options){
		super(options);
		if(options.get("threads")!=null)
			THREADS_NO = Integer.parseInt(options.get("threads"));
	}
	
//	public Datastore(BlockingQueue<Metamodel> queue){
//		this.queue = queue;
//	}
	
	@Override
	public void connect() throws ConnectException {
		if(!isConnected()){
			String username = PropertiesManager.getCredentials(Constants.DATASTORE_USERNAME);
			String password = PropertiesManager.getCredentials(Constants.DATASTORE_PASSWORD);
			String server = PropertiesManager.getCredentials(Constants.DATASTORE_SERVER);
			RemoteApiOptions options = new RemoteApiOptions()
        		.server(server, 443)
        		.credentials(username, password);
			try {
				log.debug("Logging into "+server);
				installer = new RemoteApiInstaller();
				installer.install(options);
				ds = DatastoreServiceFactory.getDatastoreService();
			} catch (IOException e) {
				log.error(DefaultErrors.connectionError+"\nStackTrace:\n"+e.getStackTrace());
				throw new ConnectException(DefaultErrors.connectionError);
			}
		}else{
			log.warn(DefaultErrors.alreadyConnected);
			throw new ConnectException(DefaultErrors.alreadyConnected);
		}
	}

	@Override
	public void disconnect() {
		if(isConnected()){
			if(installer!=null)
				installer.uninstall();
			installer = null;
			ds = null;
			log.debug(Thread.currentThread().getName() + " Disconnected");
		}else{
			log.warn(DefaultErrors.notConnected);
		}

	}

	@Override
	protected
	void persist() {
		// TODO Auto-generated method stub

	}

	@Override
	protected
	void merge() {
		// TODO Auto-generated method stub

	}

	@Override
	protected void remove() {
		// TODO Auto-generated method stub

	}

	private void putBatch(List<Entity> batch){
		boolean proof = true;
		while(proof){
			try{
				ds.put(batch);
				proof = false;
			}catch(ConcurrentModificationException ex){
				log.error(ex.getMessage()+"...retry");
			}
		}
	}
	
	/**
	 * Checks if a connection has already been established
	 * @return true if connected, false if not.
	 */
	public boolean isConnected(){
		return (installer==null || ds==null) ? false : true;
	}
	
	/**
     * Prints entities of a given kind
     * @param ds The datastore object to connect to the actual datastore
     * @param kind The kind used for the retrieval
     */
    @SuppressWarnings("unused")
	private void printEntitiesByKind(String kind){
    	Iterable<Entity> results = getEntitiesByKind(kind);
    	log.debug("------------------");
        int i=0;
        for (Entity result : results) {
        	Key entity_key = result.getKey();
        	long id = entity_key.getId();
        	String name = entity_key.getName();
        	log.debug("Key id: "+id+" - Key name: "+name);
	        Key parent_key = result.getParent();
	        if(parent_key!=null/* && (!parent_key.getName().equals("null")|| parent_key.getId()!=0L)*/){
		        long parent_id = parent_key.getId();
		        String parent_name = parent_key.getName();
		        log.debug("Parent Key id: "+parent_id+" - Parent Key name: "+
		        		parent_name);
		        	
	        }
        	printEntityProperties(result);
        	
        	printDescendents(entity_key);
        	i++;
        	log.debug("-----------------");
        }
        
        log.info("Result set: "+i);
    }
    
    private void printDescendents(Key ancestorKey){
    	Iterable<Entity> entities = getDescentents(ancestorKey);
    	log.debug("\n----------Printing descendents STARTING----------\n");
    	int i=0;
    	for(Entity ent : entities){
    		if(i>0)
    			log.debug("\n----------Next descendent----------\n");
    		
    		String parentKind = ent.getKind();
    		Key entity_key = ent.getKey();
        	long id = entity_key.getId();
        	String name = entity_key.getName();
        	log.debug("DescendentKind: "+parentKind+" \nKey id: "+id+" - Key name: "+name);
        	
    		i++;
    	}
    	log.debug("\n----------Printing descendents FINISHED----------\n");
    }
    /**
     * Query for a given entity type
     * @param ds The datastore object to connect to the actual datastore
     * @param kind The kind used for the retrieval
     * @return An iterable containing all the entities
     */
    private Iterable<Entity> getEntitiesByKind(String kind){
    	Query q = new Query(kind);
    	PreparedQuery pq = ds.prepare(q);
    	return pq.asIterable();
    }
    
    /**
     * Gets a batch of entities of a given kind
     * @param kind The Entity Kind 
     * @param cursor The point where to start fetching entities (<code>null</code> if entities should be fetched). Could be extracted from the returned object.
     * @param pageSize The number of entities to be retrieved in each batch (maximum 300).
     * @return An object containing the entities, the cursor and other stuff.
     */
    private QueryResultList<Entity> getEntitiesByKind_withCursor(String kind, 
    		Cursor cursor, int pageSize){
    	boolean proof = true;
    	QueryResultList<Entity> results = null;
    	/**
    	 * Bullet proof reads from the Datastore.
    	 */
    	while(proof){
    		try{
	    		FetchOptions fetchOptions = FetchOptions.Builder.withLimit(pageSize);
	    		if(cursor!=null)
	    			fetchOptions.startCursor(cursor);
	        	Query q = new Query(kind);
	        	PreparedQuery pq = ds.prepare(q);
	        	results = pq.asQueryResultList(fetchOptions);
	        	proof = false;
    		}catch(Exception e){
    			log.error(Thread.currentThread().getName() + 
    					"ERROR: getEntitiesByKind_withCursor -> "+e.getMessage());
    		}
        	
    	}
		
    	return results;
    }
    
    /**
     * Gets all the entities descending (even not directly connected) from the given key
     * @param ds the Datastore object
     * @param ancestorKey get descendents of this key
     * @return
     */
    private Iterable<Entity> getDescentents(Key ancestorKey){
	    	Query q = new Query().setAncestor(ancestorKey);
	    	PreparedQuery pq = ds.prepare(q);
	    	return pq.asIterable();
    }
    
    /**
     * Returns all the properties for a given entity
     * @param entity The entity
     */
    private static void printEntityProperties(Entity result){
    	Map<String, Object> properties = result.getProperties();
    	Iterator<Entry<String, Object>> iterator = properties.entrySet().iterator();
    	while(iterator.hasNext()){
    		Entry<String, Object> elem = iterator.next();
    		boolean unindexedProperty = result.isUnindexedProperty(elem.getKey());
    		if(unindexedProperty)
    			log.debug("Unindexed Property "+elem.getKey()+": "+elem.getValue().toString());
    		else
    			log.debug("Indexed Property "+elem.getKey()+": "+elem.getValue().toString());
    	}
    	
    }
    
    /**
     * Gets all the entities' kind statistics for a given datastore 
     * NB: Statistics are updated once per day, so they can be unreliable
     * @param datastore
     */
    @SuppressWarnings("unused")
	private void getKindStats(){
	    	Iterable<Entity> results = ds.prepare(new Query("__Stat_Kind__")).asIterable();
	    	log.debug("-----------------");
	    	for(Entity globalStat : results){
	    		Key key2 = globalStat.getKey();
	    		String name = key2.getName();
	    		log.debug("Properties for "+name+": ");
		    	printEntityProperties(globalStat);
		    	log.debug("-----------------");
	    	}
    }
    
    private void deleteByKind(String kind){
	    	Iterable<Entity> results = getEntitiesByKind(kind);
	    	log.debug("------------------");
	        int i=0;
	        for (Entity result : results) {
	        	Key entity_key = result.getKey();
	        	long id = entity_key.getId();
	        	String name = entity_key.getName();
	        	log.debug("REMOVING Key id: "+id+" - Key name: "+name);
	        	ds.delete(entity_key);
	        	i++;
	        	log.debug("-----------------");
	        }
	        
	        log.info("Removed: "+i+" keys");
    }

	@Override
	protected AbstractDatabase<DatastoreModel> fromMyModel(Metamodel mm) {
		//Consumer
		log.debug(Thread.currentThread().getName()+" Hi I'm the GAE consumer!");
		List<Entity> batch = new ArrayList<Entity>();
		TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
		
		long k=0;
		try {
			while(true){
				log.debug(Thread.currentThread().getName()+" Extracting from the queue");
				
				//OLD IMPLEMENTATION
				//Metamodel myModel = queue.take();
				
//				int messageCount = channel.queueDeclarePassive(TASK_QUEUE_NAME).getMessageCount();
//				log.debug(Thread.currentThread().getName()+" message count: "+messageCount);
				
				QueueingConsumer.Delivery delivery = consumer.nextDelivery(2000);
				if(delivery!=null){
					//OLD IMPLEMENTATION
					//Metamodel myModel = (Metamodel) DefaultSerializer.deserialize(delivery.getBody());
					
					// new thrift deserializer
					Metamodel myModel = new Metamodel();
					deserializer.deserialize(myModel, delivery.getBody());
					
					//log.debug(Thread.currentThread().getName()+" Consuming: "+myModel.getRowKey());
					DatastoreTransformer dt = new DatastoreTransformer(ds);
					DatastoreModel fromMyModel = dt.fromMyModel(myModel);
					
					/*** OPTIMIZATION ***/
					batch.add(fromMyModel.getEntity());
					//TODO: performance optimization
					batch.add(fromMyModel.getFictitiousEntity());
					//log.debug(Thread.currentThread().getName()+" ===> added entities to batch");
					channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
					k++;
					//try{
						if(k%100==0){
							//ds.put(batch);
							putBatch(batch);
							log.debug(Thread.currentThread().getName()+" ===>100 entities. putting normal batch");
							batch = new ArrayList<Entity>();
						}/*else if(channel.queueDeclarePassive(TASK_QUEUE_NAME).getMessageCount()==0 && !batch.isEmpty()){
							log.debug(Thread.currentThread().getName()+" ===>Nothing in the queue for me!");
							ds.put(batch);
							log.debug(Thread.currentThread().getName()+" ===>less than 100 entities. putting short batch");
							batch = new ArrayList<Entity>();
							k=0;
						}*/
					//}catch(Exception ex){
					//	
					//}
				}else{
					if(k>0){
						log.debug(Thread.currentThread().getName()+" ===>Nothing in the queue for me!");
						//ds.put(batch);
						putBatch(batch);
						log.debug(Thread.currentThread().getName()+" ===>less than 100 entities. putting short batch");
						batch = new ArrayList<Entity>();
						k=0;
					}
				}
				//ds.put(fromMyModel.getEntity());
			}
		} catch (InterruptedException e) {
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

	@Override
	protected Metamodel toMyModel(AbstractDatabase<DatastoreModel> db) {
		// Producer
		/**
		 * -Get Kinds
		 * 	- for every kind get root entities
		 * 		- for every root entity store every descendant in the same ancestor path
		 * 		  (need to modify datastore model to do that)
		 * 		- pass the model to the transformer that will generate the metamodel 
		 * 		- put the generated metamodel inside the blockingqueue
		 */
		
		Datastore datastore = (Datastore) db;

		List<String> kinds = datastore.getAllKinds();
		
//		/**OLD IMPLEMENTATION**/
////		for(String kind : kinds){
////			Iterable<Entity> rootEntities = ds.getRootEntities(kind);
////			for(Entity entity : rootEntities){
////				Key rootKey = entity.getKey();
////				Iterable<Entity> descendentes = ds.getDescentents(rootKey);
////				for(Entity en : descendentes){
////					DatastoreModel dsModel = new DatastoreModel(en);
////					dsModel.setAncestorPath(descendentes);
////					DatastoreTransformer dt = new DatastoreTransformer();
////					Metamodel myModel = dt.toMyModel(dsModel);
////					try {
////						if(myModel!=null){
////							log.debug("Producing: "+myModel.getRowKey());
////							queue.put(myModel);
////						}
////					} catch (InterruptedException e) {
////						// TODO Auto-generated catch block
////						e.printStackTrace();
////					}
////				}
////			}
////		}
		
//		/**SECOND OPTIMIZED APPROACH!**/
		for(String kind : kinds){
			//Iterable<Entity> entitiesByKind = datastore.getEntitiesByKind("UserRatings");
			
			//contatore test
			long i = 0, /*previousProductionTime=0,*/ previousQueueCheckTime=0;
			int queueElements=0;
			//contatore dati coda
			//long queueSize = 0;
			
			//Create a new instance of the Thrift Serializer
	        TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
	        Cursor cursor = null;
	        
	        while(true){
	        		//TEST
//				QueryResultList<Entity> results = datastore.getEntitiesByKind_withCursor(
//						"Tweets", cursor, 300);
	        	
				QueryResultList<Entity> results = datastore.getEntitiesByKind_withCursor(
						kind, cursor, 300);
				//log.info("\tresults contains "+results.size()+" elements");
				Cursor newcursor=null;
				
				boolean proofCursor = true;
				int timeout_ms=100;
				while(proofCursor){
					try{
						newcursor = results.getCursor();
						proofCursor = false;
					}catch(Exception e){
						log.error("\n\n\n\n\t\tUndocumented Error !!! "+e.getMessage()+"\n\n\n");
						try {
							Thread.sleep(timeout_ms);
							if(timeout_ms<5000) timeout_ms*=2;
						} catch (InterruptedException e1){}
					}
				}timeout_ms=100;
				
//				try{
//					log.info("\nCursor: "+cursor.toWebSafeString()+"\n");
//				}catch(NullPointerException ex){
//					log.info("\nCursor: null\n");
//				}
				
				/**
				 * newcursor is null if the query result cannot be resumed;
				 * newcursor is equal to cursor if all entities have been read.
				 */
				if(newcursor==null || newcursor.equals(cursor)) break;
				else cursor = newcursor;
				
				
				for(Entity entity : results){
					DatastoreModel dsModel = new DatastoreModel(entity);
					dsModel.setAncestorString(entity.getKey().toString());
					DatastoreTransformer dt = new DatastoreTransformer();
					Metamodel myModel = dt.toMyModel(dsModel);
					try {
						if(myModel!=null){
							//long sizeOf = ObjectSizeCalculator.sizeOf(myModel);
							//queueSize += sizeOf;
							//log.debug("Producing: "+myModel.getRowKey()/*+" of size: "+sizeOf+" bytes"*/);
							
							/** Non persistent message inserted in the queue **/
							channel.basicPublish( "", TASK_QUEUE_NAME, 
						            null,
						            serializer.serialize(myModel));
							//queue.put(myModel);
							i++;
						}
						//uscita dal metodo per test
//						if(i==0){ 
//							log.debug(" ==> Transferred "+i+" entities. For a total of "+queueSize+" Bytes = "+queueSize/1024+"KB = "+
//									queueSize/(1024*1024)+"MB in the Queue");
//							return null;
//						}
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (TException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
				log.debug(Thread.currentThread().getName()+" Produced: "+i+" entities");
				
				if(i%5000==0){
					//long currentProductionTime = System.currentTimeMillis();
					//long productionRate = 5000/(currentProductionTime - previousProductionTime);
					
					
					try {
						if(queueElements>0 && previousQueueCheckTime>0){
							
							int messageCount = channel.queueDeclarePassive(TASK_QUEUE_NAME).getMessageCount();
							
							//Producer is faster then consumers
							if(messageCount-queueElements>0 && messageCount>50000){
								long consumingRate = (messageCount-queueElements)/
										(System.currentTimeMillis() - previousQueueCheckTime);
							
								if(consumingRate<=0) consumingRate=1;
								/*
								 * How much time should I wait to lower the queue to 50'000entities?
								 * t = (messageCount(ent) - 50000(ent))/consumingRate(ms)
								 * Anyway, wait no more than 40s
								 */
								long t = (messageCount - 50000)/consumingRate;
								if(t>40000) t=40000;
								if(t<0) t=0;
								
								log.debug("Consuming rate: "+consumingRate+"ent/ms. \tSlowing down ... wait "+t+" ms");
								//Thread.currentThread().wait(t);
								Thread.sleep(t);
								
							}
						}
						
						queueElements = channel.queueDeclarePassive(TASK_QUEUE_NAME).getMessageCount();
						previousQueueCheckTime = System.currentTimeMillis();
						
					} catch (IOException | InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					//previousProductionTime = currentProductionTime;
				}
			}
			
			//we should arrive here when newcursor=cursor, i.e. we have read all entities for a kind
	        log.debug(Thread.currentThread().getName()+" ==> Transferred "+i+" entities.");
			
		}
		
		return null;
	}

	/**
     * Entities' kinds that contain (may contain) entity group root entities (have no ancestor parent); 
     * one stat entity for each kind of entity stored.
     * @return A list with all the Kinds that "contain" root entities, excluding kinds used for datastore's statistics
     */
    private List<String> getRootEntitiesStats(){
    	Iterable<Entity> results = ds.prepare(new Query("__Stat_Kind_IsRootEntity__")).asIterable();
    	//list containing kinds of the root entities
    	ArrayList<String> rootKinds = new ArrayList<String>();
    	System.out.println("---------Root Entities Stats--------");
    	for(Entity globalStat : results){
    		Key key2 = globalStat.getKey();
    		String name = key2.getName();
	    	System.out.println("Root Kind "+name);
	    	if(name.indexOf("_")!=0){
	    		rootKinds.add(name);
	    	}
	    	System.out.println("-----------------");
    	}
    	return rootKinds;
    }
    /**
     * All entities' kinds contained in the datastore, excluding statistic ones.
     * @return  A list containing all the kinds
     */
    public List<String> getAllKinds(){
    	Iterable<Entity> results = ds.prepare(new Query(Entities.KIND_METADATA_KIND)).asIterable();
    	//list containing kinds of the root entities
    	ArrayList<String> kinds = new ArrayList<String>();
    	for(Entity globalStat : results){
    		Key key2 = globalStat.getKey();
    		String name = key2.getName();
	    	if(name.indexOf("_")!=0){
	    		kinds.add(name);
	    	}
    	}
    	return kinds;
    }
    /**
     * Analyzes all datastore's entities and identifies root ones.
     * For root entities a property named isRoot will be added and set to true.
     * Since it uses datastore's statistics, changes to the data structure may be visible after some hours (max a day)
     * @param rootKinds A list containing all the kinds of the entities which are root. If <code>null</code> is passed every kind in the datastore is analyzed.
     */
    public void standardizeDatastore(List<String> rootKinds){
    	//If null is passed every kind in the datastore is analyzed.
    	if(rootKinds==null)
    		rootKinds = getRootEntitiesStats();
    	for(String kind : rootKinds){
    		Iterable<Entity> entities = getEntitiesByKind(kind);
    		for(Entity e : entities){
    			if(isRoot(e)){
    				e.setProperty("isRoot", true);
    				ds.put(e);
    				log.debug("Entity "+e.getKind()+"("+e.getKey().getName()+" - "+e.getKey().getId()+")"+
    						" set to be ROOT");
    			}
    		}
    	}
    }
    /**
     * Checks if an entity is root (i.e. it hasn't any parent)
     * @param e The entity to be checked
     * @return <code>true</code> if the given entity is root, <code>false</code> otherwise;
     */
    private boolean isRoot(Entity e){
    	Key key = e.getKey();
    	Key parentKey = key.getParent();
    	return (parentKey==null) ? true : false;
    }
    
    /**
     * Gets all the root entities for a given kind
     * @param kind
     * @return
     */
    public Iterable<Entity> getRootEntities(String kind){
    	Query q = new Query(kind);
		FilterPredicate filter = new Query.FilterPredicate("isRoot", Query.FilterOperator.EQUAL, true);
		q.setFilter(filter);
		PreparedQuery pq = ds.prepare(q);
    	return pq.asIterable();
    }
    
}
