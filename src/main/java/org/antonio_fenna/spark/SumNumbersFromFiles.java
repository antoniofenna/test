package org.antonio_fenna.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SumNumbersFromFiles {
	public static void main( String[] args )
    {
	//STEP 1.
	SparkConf conf= new SparkConf(); 
    //STEP 2.
	JavaSparkContext jsc= new JavaSparkContext(conf);
	JavaRDD<String> lines= jsc.textFile(args[0]);
	JavaRDD<Integer> rddofIntegers=lines.map(s->Integer.valueOf(s));
	int suma= rddofIntegers.reduce((Integer,Integer2)->Integer+Integer2);
	System.out.println("The sum is:"+ suma);
	jsc.stop();
    }
}
