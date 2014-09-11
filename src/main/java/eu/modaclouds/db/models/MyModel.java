/**
 */
package eu.modaclouds.db.models;

import java.util.HashMap;
import java.util.List;

/**
 * @author Marco Scavuzzo
 *
 */
public class MyModel {	
	
	/**
	 * 
	 *
	 * @param <FamilyName> Data type of the column family key. Must implement Comparable.
	 * @param <RowKey> Data type of the row key. Must implement Comparable.
	 */
	public static class PartitionGroup<FamilyName, RowKey>{
		String partitionName;
		/**
		 * K = Family name
		 * V = ColumnFamily Object
		 */
		HashMap<Comparable<FamilyName>,ColumnFamily> columnFamilies;
		/**
		 * K = Row key
		 * V = Row Object
		 */
		HashMap<Comparable<RowKey>,Row<RowKey>> rows;
		public String getPartitionName() {
			return partitionName;
		}
		public void setPartitionName(String partitionName) {
			this.partitionName = partitionName;
		}
		public HashMap<Comparable<FamilyName>, ColumnFamily> getColumnFamilies() {
			return columnFamilies;
		}
		public void setColumnFamilies(
				HashMap<Comparable<FamilyName>, ColumnFamily> columnFamilies) {
			this.columnFamilies = columnFamilies;
		}
		public HashMap<Comparable<RowKey>, Row<RowKey>> getRows() {
			return rows;
		}
		public void setRows(HashMap<Comparable<RowKey>, Row<RowKey>> rows) {
			this.rows = rows;
		}
	}
	/**
	 *
	 * @param <RowKey> Data type of the row key. Must implement Comparable.
	 */
	public static class Row<RowKey>{
		Comparable<RowKey> rowKey;
		//Contains all the columns associated with the same RowKey, even from different column families
		List<Column> columns;
	}
	
	/**
	 * 
	 *
	 * @param <ColumnName> Data type of the column family key. Must implement Comparable.
	 */
	public static class Column<ColumnName>{
		Comparable<ColumnName> columnName;
		byte[] columnValue;
		String columnValueType;
		String timestamp;
		boolean indexable;
	}
	
	/**
	 *
	 * @param <FamilyName> Data type of the column family key. Must implement Comparable.
	 * @param <RowKey> Data type of the row key. Must implement Comparable.
	 */
	public static class ColumnFamily<FamilyName, RowKey>{
		Comparable<FamilyName> familyName;
		//Contains all the columns associated with the same RowKey, not including those from different column families
		HashMap<Comparable<RowKey>, List<Column>> columns;
	}
}
