package eu.modaclouds.utils;

import java.lang.reflect.Field;

public class ObjectsUtils {
	/**
	 * Prints the content of an Object for debugging purposes
	 * @param obj The object whose attributes are to be read
	 * @throws IllegalAccessException 
	 * @throws IllegalArgumentException 
	 */
	public static void printObject(Object obj) throws IllegalArgumentException, IllegalAccessException{
		Class<?> parentClass = obj.getClass();
		System.out.println("####### Trying to print: "+parentClass.getSimpleName());
		Field[] fields = parentClass.getDeclaredFields();
		for (Field field : fields) {
			field.setAccessible(true);
			String name = field.getName();
		    Object value = field.get(obj);
		    System.out.println("Field name: "+name+", Field value: "+ value.toString());
		}
	}
}
