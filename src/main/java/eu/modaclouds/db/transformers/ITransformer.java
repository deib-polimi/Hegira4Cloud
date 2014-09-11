/**
 * Interface to be implemented by any class which aims at migrating from one DB to another
 */
package eu.modaclouds.db.transformers;

/**
 * @author Marco Scavuzzo
 *
 */

import eu.modaclouds.db.models.Metamodel;

public interface ITransformer<DBModel> {
	public Metamodel toMyModel(DBModel model);
	public DBModel fromMyModel(Metamodel model);
}
