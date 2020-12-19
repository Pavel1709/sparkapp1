/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ru.molchanov.sparkapp;

import java.util.*;
import java.util.stream.Collectors;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import ru.molchanov.sparkapp.exceptions.NoFileException;
import scala.Tuple2;

/**
 *
 * @author pavel1709
 */
public class SparkHadler {
    static long maxAmount;
    static String filePath;
    static SparkSession sparkSession;
    static JavaSparkContext context;
    static Dataset<Row> dataSet1;
    static HashMap<String, scala.collection.mutable.WrappedArray> hm; 
    static HashMap<Long, ArrayList<Long>> hm2; //HashMap with nesting = 1
    static HashMap<Long, ArrayList<Long>> hm3; //HashMap with nesting = 2
    static HashMap<Long, ArrayList<Long>> hm4; //HashMap with nesting = 3

    public static HashMap<Long, ArrayList<Long>> getHm2() {
        return hm2;
    }

    public static HashMap<Long, ArrayList<Long>> getHm3() {
        return hm3;
    }

    public static HashMap<Long, ArrayList<Long>> getHm4() {
        return hm4;
    }

    public static void setMaxAmount(long maxAmount) {
        SparkHadler.maxAmount = maxAmount;
    }
    
    public static void setfFilePath(String filePath) {
        SparkHadler.filePath = filePath;
    }
    
    
    
    public static void makeSparkEnvironment() {
        sparkSession = SparkSession  //Creating spark session
            .builder()
            .appName("SparkTest")
            .config("spark.sql.warehouse.dir", "/file:C:/temp")
            .master("local")
            .getOrCreate();
        context = new JavaSparkContext(sparkSession.sparkContext());  //Creating spark context using our session
    }
    
    
    public static void makeInfoFromFile() throws NoFileException {
        if (filePath == null) {
            throw new NoFileException("File was not found");
        }
        JavaRDD<String> stringJavaRDD = context.textFile(filePath); //Getting data from the file
        JavaRDD<String>  lines = stringJavaRDD.flatMap(s -> Arrays.asList(s.split("\n")).iterator()); // split lines
        PairFunction<String, String, String> keyData =
            new PairFunction<String, String, String>() {
                public Tuple2<String, String> call(String x) {
                    return new Tuple2(x.split(" ")[1],x.split(" ")[2]);  //split values
                }
            };
        JavaPairRDD<String, String> pairs = lines.mapToPair(keyData); //creating JavaPairRDD

        List<Tuple2<String, String>> output = pairs.collect(); //making list of tuples

        Dataset<Row> dataSet = sparkSession
                .createDataset(output, Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
                .toDF("a","b"); //Converts this dataSet to generic DataFrame with columns renamed
        dataSet.show();
        dataSet1 = dataSet.groupBy("a")  //groupBy and aggregate
                .agg(functions.collect_list("b"))
                .toDF("a","b"); // making table
        dataSet1.show();
        
    }
    
    
    public static void makeList() {
        JavaPairRDD<String, scala.collection.mutable.WrappedArray> jpRDD1 = dataSet1.toJavaRDD().mapToPair(new PairFunction<Row, String, scala.collection.mutable.WrappedArray>() {
            public Tuple2<String, scala.collection.mutable.WrappedArray> call(Row row) throws Exception {
                return new Tuple2<String, scala.collection.mutable.WrappedArray>((String) row.get(0), (scala.collection.mutable.WrappedArray) row.get(1)); //making javaPairRdd from dataset
            }
        });
        jpRDD1.foreach(data -> {
            System.out.println("model="+data._1() + " label=" + data._2());
        });
        Map<String, scala.collection.mutable.WrappedArray> map = jpRDD1.collectAsMap(); //making map from jpRDD1
        hm = new HashMap<>(map); //wrap to HashMap
    }
    
    
    public static void makeSingleNestedList() {
        hm2 = new HashMap<Long, ArrayList<Long>>();
        hm.keySet().forEach(x -> {
            String seq = hm.get(x).mkString(" ");
            String[] values = seq.split(" ");
            List<String> list = Arrays.asList(values);
            ArrayList<Long> arList = new ArrayList<Long>();
            list.forEach(y -> arList.add(Long.parseLong(y)));
            hm2.put(Long.parseLong(x), arList);
        });
    }
    
    
    public static void cut() {
        Map<Long, ArrayList<Long>> someMap = hm2.entrySet().stream()
                .filter(x -> x.getValue().size() <= maxAmount)
                .collect(Collectors.toMap(y -> y.getKey(), y -> y.getValue()));
        hm2 = new HashMap<Long, ArrayList<Long>>(someMap);
    }
    
    
    public static void makeDoubleNestedList() {
        hm3 = new HashMap<Long, ArrayList<Long>>();

        for(Long i: hm2.keySet()) {
            hm3.put(i, new ArrayList<Long>());
            hm2.get(i).forEach(y -> {
                hm3.get(i).add(y);
                if (hm2.containsKey(y))
                    hm2.get(y).forEach(z -> hm3.get(i).add(z));
            });
        }
    }
    
    public static void makeTrippleNestedList() {
        hm4 = new HashMap<Long, ArrayList<Long>>();
        for(Long i: hm2.keySet()) {
            hm4.put(i, new ArrayList<Long>());
            hm2.get(i).forEach(y -> {
                hm4.get(i).add(y);
            });
            hm2.get(i).forEach(x ->{
                if(hm2.containsKey(x)) {
                    hm2.get(x).forEach(s -> {
                        hm4.get(i).add(s);
                        if(hm2.containsKey(s)) {
                            hm2.get(s).forEach(t -> {
                                hm4.get(i).add(t);
                            });
                        }
                    });
                }
            });

        }
    }
}
