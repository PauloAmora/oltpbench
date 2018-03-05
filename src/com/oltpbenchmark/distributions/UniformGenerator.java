
package com.oltpbenchmark.distributions;

import java.util.Random;

public class UniformGenerator extends IntegerGenerator
{     
	
	/**
	 * Number of items.
	 */
	int items;
	
	/**
	 * Min item to generate.
	 */
	int base;
	
	Random r;
	
	
	/******************************* Constructors **************************************/

	/**
	 * Create a zipfian generator for the specified number of items.
	 * @param _items The number of items in the distribution.
	 */
	public UniformGenerator(int _items)
	{
		this(0,_items-1);
	}

	/**
	 * Create a zipfian generator for items between min and max.
	 * @param _min The smallest integer to generate in the sequence.
	 * @param _max The largest integer to generate in the sequence.
	 */
	public UniformGenerator(int _min, int _max)
	{
		base = _min;
		items = _max;
		r = new Random();
	}

		/****************************************************************************************/
	
	/** 
	 * Generate the next item. this distribution will be skewed toward lower integers; e.g. 0 will
	 * be the most popular, 1 the next most popular, etc.
	 * @param itemcount The number of items in the distribution.
	 * @return The next item in the sequence.
	 */
	public int nextInt(int itemcount)
	{
		return r.nextInt(itemcount-base) + base;
	}


	/**
	 * Return the next value, skewed by the Zipfian distribution. The 0th item will be the most popular, followed by the 1st, followed
	 * by the 2nd, etc. (Or, if min != 0, the min-th item is the most popular, the min+1th item the next most popular, etc.) If you want the
	 * popular items scattered throughout the item space, use ScrambledZipfianGenerator instead.
	 */
	@Override
	public int nextInt() 
	{
		return nextInt(items);
	}
	@Override
	public double mean() {
		throw new UnsupportedOperationException("@todo implement ZipfianGenerator.mean()");
	}
}
