/**
 * 
 */
package com.bigdatafly.factory;

import java.util.function.Supplier;

/**
 * @author summer
 *
 */
public class SingletonFactory {

	public static <T> Singleton<T> of(Supplier<T> supp) {
		return new Singleton<T>() {
			@Override
			public T init() {
				return supp.get();
			}
		};
	}

	public static abstract class Singleton<T> {
		private volatile T t;
		public abstract T init();
		public T get() {
			T local = t;
			if (local == null) {
				synchronized (this) {
					local = t;
					if (local == null) {
						t = local = init();
					}
				}
			}
			return local;
		}
	}

}
