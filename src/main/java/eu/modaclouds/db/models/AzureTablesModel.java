/**
 * 
 */
package eu.modaclouds.db.models;

import java.util.List;

import com.microsoft.windowsazure.services.table.client.DynamicTableEntity;

/**
 * @author Marco Scavuzzo
 *
 */
public class AzureTablesModel {
	private String tableName;
	private DynamicTableEntity entity;
	//Entities contained by the same table
	private List<DynamicTableEntity> entities;
	
	public AzureTablesModel(){
		
	}
	
	public AzureTablesModel(String tableName, DynamicTableEntity entity){
		this.tableName = tableName;
		this.entity = entity;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public DynamicTableEntity getEntity() {
		return entity;
	}

	public void setEntity(DynamicTableEntity entity) {
		this.entity = entity;
	}

	public List<DynamicTableEntity> getEntities() {
		return entities;
	}

	public void setEntities(List<DynamicTableEntity> entities) {
		this.entities = entities;
	}
}
