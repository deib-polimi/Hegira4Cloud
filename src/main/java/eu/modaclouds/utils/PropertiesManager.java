/**
 * Wrapper class that manages databases credentials
 */
package eu.modaclouds.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.xml.bind.annotation.XmlRootElement;

import org.apache.log4j.Logger;

import eu.modaclouds.db.api.Status;

/**
 * @author Marco Scavuzzo
 *
 */
public class PropertiesManager {
	private transient static Logger log = Logger.getLogger(PropertiesManager.class);
	
	
	public static String getCredentials(String property){
		Properties props = new Properties();
		try {
			//log.debug("Trying to read "+Constants.CREDENTIALS_PATH);
			URL systemResource = Thread.currentThread().getContextClassLoader().getResource(Constants.CREDENTIALS_PATH);
			
			InputStream isr = new FileInputStream(systemResource.getFile());
			if (isr == null){
                throw new FileNotFoundException(Constants.CREDENTIALS_PATH+" must exist.");
			}
			props.load(isr);
			return props.getProperty(property);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			return null;
		} finally {
			props=null;
		}
		return null;
	}
	
	public static Property getCredentialsProperty(String property){
		Properties props = new Properties();
		try {
			log.debug("Trying to read "+Constants.CREDENTIALS_PATH);
			URL systemResource = Thread.currentThread().getContextClassLoader().getResource(Constants.CREDENTIALS_PATH);
			
			InputStream isr = new FileInputStream(systemResource.getFile());
			if (isr == null){
                throw new FileNotFoundException(Constants.CREDENTIALS_PATH+" must exist.");
			}
			props.load(isr);
			String value = props.getProperty(property);
			if(value!=null){
				Property prop = new Property(property, value);
				prop.setStatus(new Status(Constants.STATUS_SUCCESS));
				return prop;
			}else{
				Status status = new Status(Constants.STATUS_ERROR,
						DefaultErrors.getErrorMessage(DefaultErrors.credentialsGetError),
						DefaultErrors.getErrorNumber(DefaultErrors.credentialsGetError));
				return new Property(status);
			}
		} catch (IOException e) {
			Status status = new Status(Constants.STATUS_ERROR,
					DefaultErrors.getErrorMessage(DefaultErrors.credentialsGetError),
					DefaultErrors.getErrorNumber(DefaultErrors.credentialsGetError));
			return new Property(status);
		}catch (Exception e) {
			return null;
		}   finally {
			props=null;
		}
	}
	
	/**
	 * Creates or updates a key-value pair inside the credentials file
	 * @param name The key for the property to be inserted
	 * @param value The value of the property to be inserted
	 * @return A boolean stating if the operation succeeded
	 */
	public static boolean createCredentials(String name, String value){
			Properties props = new Properties();
			try {
				log.debug("Trying to read "+Constants.CREDENTIALS_PATH);
				URL systemResource = Thread.currentThread().getContextClassLoader().getResource(Constants.CREDENTIALS_PATH);
				InputStream isr = new FileInputStream(systemResource.getFile());
				OutputStream osr = new FileOutputStream(Constants.CREDENTIALS_PATH, true);
				if (isr == null){
	                throw new FileNotFoundException(Constants.CREDENTIALS_PATH+" must exist.");
				}
				props.load(isr);
				props.setProperty(name, value);
				props.store(osr, null);
			} catch (IOException e) {
				e.printStackTrace();
				return false;
			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}
			return true;
	}
	
	public static boolean removeCredentials(String name){
		Properties props = new Properties();
		try {
			log.debug("Trying to read "+Constants.CREDENTIALS_PATH);
			URL systemResource = Thread.currentThread().getContextClassLoader().getResource(Constants.CREDENTIALS_PATH);
			InputStream isr = new FileInputStream(systemResource.getFile());
			
			if (isr == null){
                throw new FileNotFoundException(Constants.CREDENTIALS_PATH+" must exist.");
			}
			props.load(isr);
			Object removed = props.remove(name);
			OutputStream osr = new FileOutputStream(Constants.CREDENTIALS_PATH);
			
			if(removed!=null){
				props.store(osr, "");
				osr.flush();
				osr.close();
				return true;
			}else{
				return false;
			}
		} catch (IOException e) {
			return false;
		} catch (Exception e) {
			return false;
		}
	}
	
	public static boolean checkCredentials(){
		List<String> creds = Constants.getSupportedCredentials();
		Properties props = new Properties();
		try {
			log.debug("Trying to read "+Constants.CREDENTIALS_PATH);

			InputStream isr = new FileInputStream(Constants.CREDENTIALS_PATH);
			if (isr == null){
                throw new FileNotFoundException(Constants.CREDENTIALS_PATH+" must exist.");
			}
			props.load(isr);
			boolean returnValue = true;
			for(String cred : creds){
				if(!props.containsKey(cred)){
					returnValue = false;
					break;
				}
			}
			return returnValue;
		} catch (IOException e) {
			return false;
		} catch (Exception e) {
			return false;
		} finally {
			props=null;
		}
	}
	
	@XmlRootElement
	public static class Property {
		String name;
		String value;
		Status status;
		public Property(){};
		public Property(String name, String value){
			this.name = name;
			this.value = value;
		}
		public Property(Status status){
			this.status = status;
		}
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public String getValue() {
			return value;
		}
		public void setValue(String value) {
			this.value = value;
		}
		public Status getStatus() {
			return status;
		}
		public void setStatus(Status status) {
			this.status = status;
		}
	}
}
