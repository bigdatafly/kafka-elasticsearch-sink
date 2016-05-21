/**
 * 
 */
package com.bigdatafly.factory;

/**
 * @author summer
 *
 */
public interface Factory<T> {

	public  T create();
	
	//public Factory<T> builder();
}
