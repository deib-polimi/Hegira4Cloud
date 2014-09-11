/**
 * Template pattern Part
 * This class must be extended from any supported DB
 * The copy class is made final so as not to be overridden, since it's tasks must be executed sequentially
 */
package eu.modaclouds.db.adapters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

import eu.modaclouds.db.models.Metamodel;
import eu.modaclouds.exceptions.ConnectException;
import eu.modaclouds.utils.Constants;

/**
 * @author Marco Scavuzzo
 *
 */
public abstract class AbstractDatabase<DBModel> implements Runnable{
	private transient Logger log = Logger.getLogger(AbstractDatabase.class);
	AbstractDatabase dest;
	protected int THREADS_NO = 10;
	protected static final String TASK_QUEUE_NAME = "task_queue";
	protected ConnectionFactory factory;
	protected Connection connection;
	protected Channel channel;
	protected QueueingConsumer consumer;
	
	/**
	 * Constructs a general database object
	 * @param options A map containing the properties of the new db object to be created. 
	 * 				   <code>mode</code> Consumer or Producer.
	 * 				   <code>threads</code> The number of consumer threads.
	 */
	protected AbstractDatabase(Map<String, String> options){
		String mode = options.get("mode");
		if(mode==null) return;
		//Common queue settings
		factory = new ConnectionFactory();
        factory.setHost("localhost");
        try{
	        connection = factory.newConnection();
	        channel = connection.createChannel();
	        /**
	         * Declaring a durable queue
	         * 2nd parameter = durable boolean
	         */
	        channel.queueDeclare(TASK_QUEUE_NAME, false, false, false, null);
        }catch(IOException e){
        	e.printStackTrace();
        }
        //queue settings differentiation
		switch(mode){
			case Constants.PRODUCER:
				consumer=null;
				break;
			case Constants.CONSUMER:
				if(options.get("threads")!=null)
					this.THREADS_NO = Integer.parseInt(options.get("threads"));
				
				log.debug("CONSTRUCTOR No. Consumer threads: "+this.THREADS_NO);
				try{
					channel.basicQos(1);
			        consumer = new QueueingConsumer(channel);
			        channel.basicConsume(TASK_QUEUE_NAME, false, consumer);
			        dest=this;
				}catch(IOException ex){
					
				}
				break;
			default:
				break;
		}
	}
    
	/**
	 * Encapsulate the logic contained inside the models to map to the intermediate model
	 * to a DB
	 * @param mm The intermediate model
	 * @return returns the converted model
	 */
	protected abstract AbstractDatabase<DBModel> fromMyModel(Metamodel mm);
	/**
	 * Encapsulate the logic contained inside the models to map a DB to the intermediate
	 * model 
	 * @param model The model to be converted
	 * @return returns The intermediate model
	 */
	protected abstract Metamodel toMyModel(AbstractDatabase<DBModel> model);
	/**
	 * TODO: Made public just to try. revert to protected ASAP
	 * Suggestion: To be called inside a try-finally block. Should always be disconnected
	 * @throws ConnectException
	 */
	public abstract void connect() throws ConnectException;
	/**
	 * Suggestion: To be called inside a finally block
	 */
	public abstract void disconnect();
	protected abstract void persist();
	protected abstract void merge();
	protected abstract void remove();
	
	/**
	 * Template method
	 * Non se ne può fare l'override
	 * la destinazione è l'oggetto su cui il metodo è chiamato
	 * @return
	 */
	public final boolean switchOver(AbstractDatabase destination){	
		final AbstractDatabase thiz = this;
		//final AbstractDatabase dest = destination;
		this.dest = destination;
		
		(new Thread() {
			@Override public void run() {
				try {
					thiz.connect();
					toMyModel(thiz);
				} catch (ConnectException e) {
					e.printStackTrace();
				} finally{
					thiz.disconnect();
				}     
			}
		}).start();

//		(new Thread() {
//			@Override public void run(){
//				try {
//					dest.connect();
//					log.debug("Starting consumer thread");
//					dest.fromMyModel(null);
//				} catch (ConnectException e) {
//					e.printStackTrace();
//				} finally {
//					dest.disconnect();
//				}		
//			}
//		}).start();
		
		//executing the consumers
		ExecutorService executor = Executors.newFixedThreadPool(destination.THREADS_NO);
		log.debug("EXECUTOR switchover No. Consumer threads: "+destination.THREADS_NO);
		for(int i=0;i<destination.THREADS_NO;i++){
			executor.execute(destination);
		}

		return true;
	}
	
	//Consumer runnable method
	@Override
	public void run() {
		try {
			dest.connect();
			log.debug("Starting consumer thread");
			dest.fromMyModel(null);
		} catch (ConnectException e) {
			e.printStackTrace();
		} finally {
			//dest.disconnect();
		}	
	}
}
