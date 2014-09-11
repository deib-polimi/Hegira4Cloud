/**
 * 
  ● AT Entity = MM Row
  ● AT Row key = MM RowKey
  ● AT Table = MM ColumnFamily
  ● AT Partiton key = MM Partition Group
  ● AT Property → Name + Value
      – Property_name = MM ColumnName
	  – Property_value = MM ColumnValue
	  – Proprty_value.class = MM ColumnValueType
  ● AT (Row) Timestamp = MM (Column) Timestamp
  ● user_input() = MM Indexable
	  – Azure al momento non supporta gli indici
 *
 */
package eu.modaclouds.db.transformers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Logger;

import eu.modaclouds.utils.DateUtils;
import eu.modaclouds.utils.DefaultSerializer;

import com.microsoft.windowsazure.services.table.client.DynamicTableEntity;
import com.microsoft.windowsazure.services.table.client.EntityProperty;

import eu.modaclouds.db.models.AzureTablesModel;
import eu.modaclouds.db.models.Metamodel;
import eu.modaclouds.db.models.Column;

/**
 * @author Marco Scavuzzo
 *
 */
public class AzureTablesTransformer implements ITransformer<AzureTablesModel> {
	private transient Logger log = Logger.getLogger(AzureTablesTransformer.class);
	
	@Override
	public Metamodel toMyModel(AzureTablesModel model) {
		Metamodel metamodel = new Metamodel();
		mapRowKey(metamodel, model);
		mapPartitionGroup(metamodel, model);
		mapColumnFamilies(metamodel, model);
		//maps to just one column family
		mapColumns(metamodel, model, model.getTableName());
		
		return metamodel;
	}

	@Override
	public AzureTablesModel fromMyModel(Metamodel metamodel) {
		AzureTablesModel model = new AzureTablesModel();
		String partitionGroup = metamodel.getPartitionGroup();
		mapTable(partitionGroup, model);
		
		
		List<DynamicTableEntity> entities = mapRows(metamodel, partitionGroup);
		
		//Sets a list of entities to be contained by the same table
		model.setEntities(entities);
		return model;
	}
	
	
	/*---------------------------------------------------------------------------------*/
	/*------------------------- DIRECT MAPPING UTILITY METHODS ------------------------*/
	private void mapPartitionGroup(Metamodel metamodel, AzureTablesModel model){
		String partitionKey = model.getEntity().getPartitionKey();
		String tableName = model.getTableName();
		metamodel.setPartitionGroup("@"+tableName+"#"+partitionKey);
	}
	
	private void mapRowKey(Metamodel metamodel, AzureTablesModel model){
		metamodel.setRowKey(model.getEntity().getRowKey());
	}
	
	private void mapColumnFamilies(Metamodel metamodel, AzureTablesModel model){
		String tableName = model.getTableName();
		ArrayList<String> columnFamilies = new ArrayList<String>();
		columnFamilies.add(tableName);
		metamodel.setColumnFamilies(columnFamilies);
	}
	
	private void mapColumns(Metamodel metamodel, AzureTablesModel model, String columnFamily){
		HashMap<String, EntityProperty> properties = model.getEntity().getProperties();
		//getting the "list" of keys 
		Iterator<String> keyIterator = properties.keySet().iterator();
		ArrayList<Column> columns = new ArrayList<Column>();
		while(keyIterator.hasNext()){
			String key = keyIterator.next();
			//using a key to get the right property
			EntityProperty property = properties.get(key);
			Column column = new Column();
			column.setColumnName(key); 
			try {
				column.setColumnValue(getBytesFromProperty(property));
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			column.setColumnValueType(property.getEdmType().name());
			//Convert date into ISO8601-compliant String
			String timestamp = DateUtils.format(model.getEntity().getTimestamp());
			column.setTimestamp(timestamp);
			//TODO: Add some sort of mappings for indexes. Should we make the user choose?
			column.setIndexable(true);
			columns.add(column);
		}
		//metamodel.setColumnsByColumnFamily(columnFamily, columns);
		//TODO: this is the new implementation
		metamodel.getColumns().put(columnFamily, columns);
	}
	
	private byte[] getBytesFromProperty(EntityProperty prop) throws IOException{
		String type = prop.getEdmType().name();
		//System.out.println("type: "+type);
		switch(type){
			case "BINARY":
				return prop.getValueAsByteArray();
	
			case "BOOLEAN":
				return DefaultSerializer.serialize(prop.getValueAsBoolean());
				
			case "DATETIME":
				return DefaultSerializer.serialize(prop.getValueAsDate());
				
			case "DOUBLE":
				return DefaultSerializer.serialize(prop.getValueAsDouble());
				
			case "GUID":
				return DefaultSerializer.serialize(prop.getValueAsUUID());
				
			case "INT32":
				return DefaultSerializer.serialize(prop.getValueAsInteger());
				
			case "INT64":
				return DefaultSerializer.serialize(prop.getValueAsLong());
				
			case "STRING":
				return DefaultSerializer.serialize(prop.getValueAsString());
				
			default:
				throw new IOException("Can't serialize object.");
		}
	}
	
	/*---------------------------------------------------------------------------------*/
	
	/*---------------------------------------------------------------------------------*/
	/*------------------------- INVERSE MAPPING UTILITY METHODS ------------------------*/
	/**
	 * Maps the metamodel to Azure rowS in a table
	 * @param metamodel The metamodel
	 * @return returns a list of entities to be contained by the same table
	 */
	private ArrayList<DynamicTableEntity> mapRows(Metamodel metamodel, String partitionGroup){
		//cycle over column families
		List<String> columnFamilies = metamodel.getColumnFamilies();
		ArrayList<DynamicTableEntity> entities = new ArrayList<DynamicTableEntity>();
		for(String columnFamily : columnFamilies){
			//Since everything gets mapped into a single table (row by row which means column by column)
			HashMap<String, EntityProperty> properties = new HashMap<String, EntityProperty>();
			//List<Column> columns = metamodel.getColumnsByColumnFamily(columnFamily);
			//TODO: this is the new implementation
			List<Column> columns = metamodel.getColumns().get(columnFamily);
			for(Column column : columns){
				// Deserialize before putting into property	
				if(!column.getColumnName().equals("isRoot")){
					try {
						EntityProperty entityProperty = getPropertyFromColumn(column.getColumnValueType(), column.getColumnValue());
						
						if(/*column.getColumnValue().equals("byte[]")*/entityProperty.getEdmType().name().equals("BINARY")){
							// Object is not supported so we should keep it as it is and create a new property with the datatype
							//log.debug("==> Creating new column named: "+column.getColumnName()+"_Type. Inserting type: "+column.getColumnValueType());
							properties.put(column.getColumnName()+"_Type", 
									new EntityProperty(column.getColumnValueType()));
						}
						
						properties.put(column.getColumnName(), entityProperty);
						
					} catch (ClassNotFoundException | IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}else{
					//log.debug("=> Removing column isRoot");
				}
				
			}
			DynamicTableEntity dynamicEntity = new DynamicTableEntity(properties);
			dynamicEntity.setPartitionKey(extractPartitionKey(partitionGroup));
			//TODO: problema deve essere univoca
			dynamicEntity.setRowKey(metamodel.getRowKey());
			entities.add(dynamicEntity);
		}
		return entities;
	}
	
	/**
	 * Creates an Entity property of the right type
	 * @param datatype serializedObject datatype
	 * @param serializedObject An array of bytes containing a serialized object
	 * @return The Entity property
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	private EntityProperty getPropertyFromColumn(String datatype, byte[] serializedObject) throws ClassNotFoundException, IOException{
			
		switch(datatype){
			case "byte[]":
				//returning it as it is
				return new EntityProperty(serializedObject);
				
			case "Boolean":
				Boolean bool = (Boolean) DefaultSerializer.deserialize(serializedObject);
				return new EntityProperty(bool);
				
			case "Date":
				Date date = (Date) DefaultSerializer.deserialize(serializedObject);
				return new EntityProperty(date);
				
			case "Double":
				Double doub = (Double) DefaultSerializer.deserialize(serializedObject);
				return new EntityProperty(doub);
				
			case "UUID":
				UUID uuid = (UUID) DefaultSerializer.deserialize(serializedObject);
				return new EntityProperty(uuid);
				
			case "Integer":
				Integer integ = (Integer) DefaultSerializer.deserialize(serializedObject);
				return new EntityProperty(integ);
				
			case "Long":
				Long lng = (Long) DefaultSerializer.deserialize(serializedObject);
				return new EntityProperty(lng);
				
			case "String":
				String str = (String) DefaultSerializer.deserialize(serializedObject);
				return new EntityProperty(str);
				
			default:
				//returning it as it is
				return new EntityProperty(serializedObject);
		}
	}
	
	private void mapTable(String partitionGroup, AzureTablesModel model){
		String tableName = extractTableName(partitionGroup);
		model.setTableName(tableName);
	}
	
	private String extractTableName(String partitionGroup){
		/*
		 * Datastore should have removed the @
		 */
		int lastIndexOf = partitionGroup.lastIndexOf("#");
		if(lastIndexOf > -1){
			return partitionGroup.substring(0, lastIndexOf);
		}
		return null;
	}
	
	private String extractPartitionKey(String partitionGroup){
		int lastIndexOf = partitionGroup.lastIndexOf("#");
		if(lastIndexOf > -1){
			return partitionGroup.substring(lastIndexOf+1);
		}
		return null;
	}
	/*---------------------------------------------------------------------------------*/
}