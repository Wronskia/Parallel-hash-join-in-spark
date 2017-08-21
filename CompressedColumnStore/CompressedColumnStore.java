package org.sparkexample;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import java.util.*;
import java.lang.*;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import java.io.File;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import java.io.Writer;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import scala.Array;
import scala.Tuple2;

import java.util.*;
import java.util.ArrayList;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Example class
 */
/**
 * Created by Yassine on 25.04.17.
 */
public class task3 {

    public static String f(Tuple2 x,int taille_pr)
    {

        String finalString="";


        for(int j=0;j<taille_pr;++j) {

            Tuple2 temp = (Tuple2) ((Tuple2) x)._2;
            for (int k=taille_pr-1-j; k>0; k--)
            {
                temp = (Tuple2) ((Tuple2) temp)._1;
            }

            if(j==0) {
                finalString = finalString + "" + temp._2;
            }
            else{
                finalString = finalString + "," + temp._2;
            }

        }
        return finalString;
    }
    public static void main(String[] args) throws IOException {
        String master = "local[4]";


        SparkConf conf = new SparkConf()
                .setAppName(task2.class.getName())
                .setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);


        String input_file = args[0];
        String output_file = args[1];
        String schema = args[2];
        String projection_list = args[3];
        String where_list = args[4];
        String compressed = args[5];
        JavaRDD<String> inputRDD = sc.textFile(args[0]);

        JavaRDD<Object> RDD_col=inputRDD.map(lines -> lines.split(","));


        List<JavaPairRDD> RDD_list = new ArrayList<JavaPairRDD>();


        int taille=schema.split(",").length;

        for(int i=0;i<taille;++i){
            final int j = i;
                RDD_list.add(RDD_col.zipWithIndex().mapToPair(x -> new Tuple2<>(x._2, ((String[]) x._1)[j])));

        }

        List<String> schemas = new ArrayList<>();
        List<String> types = new ArrayList<>();

        for(int i=0;i<taille;++i){

            schemas.add(schema.split(",")[i].split(":")[0]);
            types.add(schema.split(",")[i].split(":")[1]);
        }


        JavaPairRDD<Object, Long> RDDcomp = RDD_list.get(schemas.indexOf(compressed)).mapToPair(x -> new Tuple2(((Tuple2)x)._2, ((Tuple2)x)._1));
        RDDcomp = RDDcomp.groupByKey().keys().zipWithIndex();
        Map<Object, Long> traduction = RDDcomp.collectAsMap();

        JavaPairRDD<Long, Long> RDD_compressed = RDD_list.get(schemas.indexOf(compressed)).mapValues(x -> traduction.get(x));

        //System.out.println(RDD_compressed.first());

        //RDD_compressed.filter(x -> x == objToId.get(val));




        int taille_where=where_list.split(",").length;


        List<JavaPairRDD> result = new ArrayList<JavaPairRDD>();

        for(int i=0;i<taille_where;++i){

            int index=schemas.indexOf(where_list.split(",")[i].split("\\|")[0]);


            if(where_list.split(",")[i].split("\\|")[1].equals(">")){

                if(types.get(index).equals("Int")){

                    int x = Integer.parseInt(where_list.split(",")[i].split("\\|")[2]);
                    Function<Tuple2<Long, String>, Boolean> comp =new Function<Tuple2<Long, String>, Boolean>()
                    {
                        public Boolean call(Tuple2<Long, String> keyValue) {



                            return (Integer.parseInt(keyValue._2()) > x);}
                    };
                    result.add(RDD_list.get(index).filter(comp));

                }
                else if(types.get(index).equals("Float")){

                    float x = Float.parseFloat(where_list.split(",")[i].split("\\|")[2]);
                    Function<Tuple2<Long, String>, Boolean> comp =new Function<Tuple2<Long, String>, Boolean>()
                    {
                        public Boolean call(Tuple2<Long, String> keyValue) {

                            return (Float.parseFloat(keyValue._2()) > x);}
                    };
                    result.add(RDD_list.get(index).filter(comp));

                }

                else if(types.get(index).equals("String")){

                    String x = where_list.split(",")[i].split("\\|")[2];
                    Function<Tuple2<Long, String>, Boolean> comp =new Function<Tuple2<Long, String>, Boolean>()
                    {
                        public Boolean call(Tuple2<Long, String> keyValue) {

                            return ((keyValue._2().compareTo(x))>0);}
                    };
                    result.add(RDD_list.get(index).filter(comp));

                }
            }


            if(where_list.split(",")[i].split("\\|")[1] == ">="){

                if(types.get(index).equals("Int")){

                    int x = Integer.parseInt(where_list.split(",")[i].split("\\|")[2]);
                    Function<Tuple2<Long, String>, Boolean> comp =new Function<Tuple2<Long, String>, Boolean>()
                    {
                        public Boolean call(Tuple2<Long, String> keyValue) {

                            return (Integer.parseInt(keyValue._2()) >= x);}
                    };
                    result.add(RDD_list.get(index).filter(comp));

                }
                else if(types.get(index).equals("Float")){

                    float x = Float.parseFloat(where_list.split(",")[i].split("\\|")[2]);
                    Function<Tuple2<Long, String>, Boolean> comp =new Function<Tuple2<Long, String>, Boolean>()
                    {
                        public Boolean call(Tuple2<Long, String> keyValue) {

                            return (Float.parseFloat(keyValue._2()) >= x);}
                    };
                    result.add(RDD_list.get(index).filter(comp));

                }

                else if(types.get(index).equals("String")){

                    String x = where_list.split(",")[i].split("\\|")[2];
                    Function<Tuple2<Long, String>, Boolean> comp =new Function<Tuple2<Long, String>, Boolean>()
                    {
                        public Boolean call(Tuple2<Long, String> keyValue) {

                            return (((keyValue._2().compareTo(x))>=0));}
                    };
                    result.add(RDD_list.get(index).filter(comp));

                }
            }




            if(where_list.split(",")[i].split("\\|")[1] == "<="){

                if(types.get(index).equals("Int")){

                    int x = Integer.parseInt(where_list.split(",")[i].split("\\|")[2]);
                    Function<Tuple2<Long, String>, Boolean> comp =new Function<Tuple2<Long, String>, Boolean>()
                    {
                        public Boolean call(Tuple2<Long, String> keyValue) {

                            return (Integer.parseInt(keyValue._2()) <= x);}
                    };
                    result.add(RDD_list.get(index).filter(comp));

                }
                else if(types.get(index).equals("Float")){

                    float x = Float.parseFloat(where_list.split(",")[i].split("\\|")[2]);
                    Function<Tuple2<Long, String>, Boolean> comp =new Function<Tuple2<Long, String>, Boolean>()
                    {
                        public Boolean call(Tuple2<Long, String> keyValue) {

                            return (Float.parseFloat(keyValue._2()) <= x);}
                    };
                    result.add(RDD_list.get(index).filter(comp));

                }

                else if(types.get(index).equals("String")){

                    String x = where_list.split(",")[i].split("\\|")[2];
                    Function<Tuple2<Long, String>, Boolean> comp =new Function<Tuple2<Long, String>, Boolean>()
                    {
                        public Boolean call(Tuple2<Long, String> keyValue) {

                            return (((keyValue._2().compareTo(x))<=0));}
                    };
                    result.add(RDD_list.get(index).filter(comp));

                }
            }





            if(where_list.split(",")[i].split("\\|")[1].equals("<")){



                if(types.get(index).equals("Int")){


                    int x = Integer.parseInt(where_list.split(",")[i].split("\\|")[2]);
                    Function<Tuple2<Long, String>, Boolean> comp =new Function<Tuple2<Long, String>, Boolean>()
                    {
                        public Boolean call(Tuple2<Long, String> keyValue) {

                            return (Integer.parseInt(keyValue._2()) < x);}
                    };
                    result.add(RDD_list.get(index).filter(comp));

                }
                else if(types.get(index).equals("Float")){

                    float x = Float.parseFloat(where_list.split(",")[i].split("\\|")[2]);
                    Function<Tuple2<Long, String>, Boolean> comp =new Function<Tuple2<Long, String>, Boolean>()
                    {
                        public Boolean call(Tuple2<Long, String> keyValue) {

                            return (Float.parseFloat(keyValue._2()) < x);}
                    };
                    result.add(RDD_list.get(index).filter(comp));

                }

                else if(types.get(index).equals("String")){

                    String x = where_list.split(",")[i].split("\\|")[2];
                    Function<Tuple2<Long, String>, Boolean> comp =new Function<Tuple2<Long, String>, Boolean>()
                    {
                        public Boolean call(Tuple2<Long, String> keyValue) {

                            return (((keyValue._2().compareTo(x))<0));}
                    };
                    result.add(RDD_list.get(index).filter(comp));

                }
            }


            if(where_list.split(",")[i].split("\\|")[1].equals("=") && schemas.indexOf(compressed)!= index){

                if(types.get(index)=="Int"){

                    int x = Integer.parseInt(where_list.split(",")[i].split("\\|")[2]);
                    Function<Tuple2<Long, String>, Boolean> comp =new Function<Tuple2<Long, String>, Boolean>()
                    {
                        public Boolean call(Tuple2<Long, String> keyValue) {

                            return (Integer.parseInt(keyValue._2()) == x);}
                    };
                    result.add(RDD_list.get(index).filter(comp));

                }
                else if(types.get(index).equals("Float")){

                    float x = Float.parseFloat(where_list.split(",")[i].split("\\|")[2]);
                    Function<Tuple2<Long, String>, Boolean> comp =new Function<Tuple2<Long, String>, Boolean>()
                    {
                        public Boolean call(Tuple2<Long, String> keyValue) {

                            return (Float.parseFloat(keyValue._2()) == x);}
                    };
                    result.add(RDD_list.get(index).filter(comp));

                }

                else if(types.get(index).equals("String")){

                    String x = where_list.split(",")[i].split("\\|")[2];
                    Function<Tuple2<Long, String>, Boolean> comp =new Function<Tuple2<Long, String>, Boolean>()
                    {
                        public Boolean call(Tuple2<Long, String> keyValue) {

                            return (((keyValue._2().compareTo(x))==0));}
                    };
                    result.add(RDD_list.get(index).filter(comp));

                }
            }
            else if (where_list.split(",")[i].split("\\|")[1].equals("=")){
                final int j = i;
                result.add(RDD_compressed.filter(x -> x._2 == traduction.get(where_list.split(",")[j].split("\\|")[2])));

            }

        }



        JavaPairRDD<Long,Object> finalRDD=result.get(0);


        for(int i=0;i<taille_where-1;++i){
            finalRDD = finalRDD.join(result.get(i+1));
        }


        List<String> proj = new ArrayList<>();
        List<String> where_ = new ArrayList<>();


        int taille_proj=projection_list.split(",").length;


        for(int i=0;i<taille_proj;++i){

            proj.add(projection_list.split(",")[i]);

        }

        for(int i=0;i<taille_where;++i){

            where_.add(where_list.split(",")[i].split("\\|")[0]);

        }

        int indexes=where_.indexOf(proj);

        //result.get(1).join(result.get(0)).foreach(x -> System.out.println(x));




        JavaPairRDD<Long,Object> joinedRDD=finalRDD.join(RDD_col.zipWithIndex().mapToPair(x -> new Tuple2<Long,Object>(x._2, ((String[]) x._1)[schemas.indexOf(proj.get(0))]))).mapValues(x -> (Object) x);
        for (int i = 1; i < taille_proj; ++i) {
            final int j = i;
            joinedRDD=joinedRDD.join(RDD_col.zipWithIndex().mapToPair(x -> new Tuple2<Long,Object>( x._2, (Object) ((String[]) x._1)[schemas.indexOf(proj.get(j))]))).mapValues(x -> (Object) x);
        }


        joinedRDD.map(x->f(x,taille_proj)).coalesce(1,true).saveAsTextFile("test");

        try {
            FileUtils.copyFile(new File("test/part-00000"), new File(output_file));
            FileUtils.deleteDirectory(new File("test"));

        }
        catch(IOException e)
        {
            e.printStackTrace();
        }


    }


}

