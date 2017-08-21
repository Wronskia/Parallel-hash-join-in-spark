import org.apache.hadoop.fs.FileUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

import java.io.File;

import scala.Tuple2;

import java.util.ArrayList;

import java.util.*;
import java.lang.*;

import java.io.Writer;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.BufferedWriter;

import java.io.OutputStream;
import java.io.InputStream;
import java.io.FileInputStream;

import org.apache.commons.io.FileUtils;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Created by Yassine on 30.04.17.
 */
public class task4 {


    public static void main(String[] args) throws IOException {
        String master = "local[4]";


        SparkConf conf = new SparkConf()
                .setAppName(task4.class.getName())
                .setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);


        String schema_customer = "C_CUSTKEY:Int,C_NAME:String,C_ADDRESS:String,C_NATIONKEY:Int,C_PHONE:String,C_ACCTBAL:Float,C_MKTSEGMENT:String,C_COMMENT:String";
        String schema_orders = "O_ORDERKEY:Int,O_CUSTKEY:Int,O_ORDERSTATUS:String,O_TOTALPRICE:Float,O_ORDERDATE:String,O_ORDERPRIORITY:String,O_CLERK:String,O_SHIPPRIORITY:Int,O_COMMENT:String";
        String input_file_customer = args[0];
        String input_file_order = args[1];
        String output_file= args[2];



        System.out.println(schema_customer.length());

        JavaRDD<Object> customerRDD = sc.textFile(args[0]).map(lines -> lines.split("\\|"));
        JavaRDD<Object> orderRDD = sc.textFile(args[1]).map(lines -> lines.split("\\|"));


        List<JavaPairRDD> RDD_list_order = new ArrayList<JavaPairRDD>();

        int taille_order=schema_orders.split(",").length;

        for(int i=0;i<taille_order;++i){
            final int j = i;
            RDD_list_order.add(orderRDD.zipWithIndex().mapToPair(x -> new Tuple2<>(x._2, ((String[]) x._1)[j])));

        }



        List<String> list_schema_order = new ArrayList<>();
        List<String> list_types_order = new ArrayList<>();

        for(int i=0;i<taille_order;++i){

            list_schema_order.add(schema_orders.split(",")[i].split(":")[0]);
            list_types_order.add(schema_orders.split(",")[i].split(":")[1]);


        }

        List<JavaPairRDD> RDD_list_customer = new ArrayList<JavaPairRDD>();

        int taille_customer=schema_customer.split(",").length;

        for(int i=0;i<taille_customer;++i){
            final int j = i;
            RDD_list_customer.add(customerRDD.zipWithIndex().mapToPair(x -> new Tuple2<>(x._2, ((String[]) x._1)[j])));

        }

        List<String> list_schema_customer = new ArrayList<>();
        List<String> list_types_customer = new ArrayList<>();

        for(int i=0;i<taille_customer;++i){
            list_schema_customer.add(schema_customer.split(",")[i].split(":")[0]);
            list_types_customer.add(schema_customer.split(",")[i].split(":")[1]);
        }


        JavaPairRDD<Object, Object> concat =orderRDD.zipWithIndex().mapToPair(x -> new Tuple2<>(x._2, new Tuple2(((String[]) x._1)[list_schema_order.indexOf("O_CUSTKEY")],((String[]) x._1)[list_schema_order.indexOf("O_COMMENT")] )));

        Map<Object, Object> key = RDD_list_customer.get(list_schema_customer.indexOf("C_CUSTKEY")).mapToPair(line -> new Tuple2(((Tuple2)line)._2, ((Tuple2) line)._1)).mapValues(x -> null).collectAsMap();

        JavaPairRDD<Object,Object> finalRDD = concat.mapToPair(x -> new Tuple2(((Tuple2)x._2)._1, new Tuple2(((Tuple2)x._2)._2,key.get(((Tuple2)x._2)._1))));

        finalRDD.map(x -> (String) (x._1 + "," + ((Tuple2) x._2)._1)).coalesce(1,true).saveAsTextFile("output");


        try {
            FileUtils.copyFile(new File("output/part-00000"), new File(output_file));
            FileUtils.deleteDirectory(new File("output"));

        }
        catch(IOException e)
        {
            e.printStackTrace();
        }


    }
}