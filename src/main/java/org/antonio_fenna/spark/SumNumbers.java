package org.antonio_fenna.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;



public class SumNumbers 
{
    public static void main( String[] args )
    {
    	//STEP 1.
    	SparkConf conf= new SparkConf(); 
        //STEP 2.
    	JavaSparkContext jsc= new JavaSparkContext(conf);
    	Integer[] numbers={1,2,3,4,5,6,7,8,9};
    	List <Integer> data= Arrays.asList(numbers);
    	JavaRDD<Integer> distributedData=jsc.parallelize(data);
    	int suma= distributedData.reduce((Integer,Integer2)->Integer+Integer2);
    	System.out.println("The sum is:"+ suma);
    	jsc.stop();
    }
}
