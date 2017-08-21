import sun.font.TrueTypeFont;

import java.util.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.*;




public class Skyline {


    public static ArrayList<Tuple> mergePartitions(ArrayList<ArrayList<Tuple>> partitions){

        ArrayList<Tuple> merge = new ArrayList<>();
        for(int i=0;i<partitions.size();++i){
            merge.addAll(nlSkyline(partitions.get(i)));
        }
        return merge;
    }


    public static ArrayList<Tuple> dcSkyline(ArrayList<Tuple> inputList, int blockSize) {

        while (inputList.size() > blockSize)
        {
            ArrayList<ArrayList<Tuple>> buildingPartitions = new ArrayList<>();
        for (int i = 0; i < inputList.size() / blockSize+1; ++i) {
            buildingPartitions.add(new ArrayList<>());
            for (int j = 0; j < blockSize; ++j) {
                if (i * blockSize + j < inputList.size()) {
                    buildingPartitions.get(i).add(inputList.get(i * blockSize + j));
                }
            }

        }

            inputList = mergePartitions(buildingPartitions);
        }
        return nlSkyline(inputList);
    }

    public static int indexOfboolArray(Boolean[] array,int counter) {
        int returnvalue = -1;
        int count=0;
        for (int i = 0; i < array.length; ++i) {
            if (array[i] == true) {
                count+=1;
                if(count==counter) {
                    returnvalue = i;
                    break;
                }
            }
        }
        return returnvalue;
    }

    public static ArrayList<Tuple> nlSkyline(ArrayList<Tuple> partition) {

        Boolean[] is_skyline = new Boolean[partition.size()];
        Arrays.fill(is_skyline, Boolean.TRUE);

        ArrayList<Tuple> skylines = new ArrayList<>();

        int i=0;
        int j=0;
        int count_j;

        while(i<partition.size()){

            i=indexOfboolArray(is_skyline,1);
            if(i==-1){
                break;
            }
            count_j=1;
            while(j<partition.size()){

                j=indexOfboolArray(is_skyline,1+count_j);
                if(j==-1){
                    break;
                }
                if(!partition.get(j).isIncomparable(partition.get(i))) {
                    if (partition.get(j).dominates(partition.get(i))) {
                        is_skyline[i] = false;
                        break;
                    }
                    else {
                        is_skyline[j] = false;
                        count_j-=1;
                    }
                }
                else{
                    count_j+=1;
                }
            }
            if(is_skyline[i]){
                is_skyline[i]=false;
                skylines.add(partition.get(i));
            }
        }
        return skylines;
    }

}

class Tuple {
    private int price;
    private int age;

    public Tuple(int price, int age){
        this.price = price;
        this.age = age;
    }

    public boolean dominates(Tuple other){
        if ((this.getPrice() <other.getPrice() && this.getAge()<=other.getAge()) || (this.getPrice() <=other.getPrice() && this.getAge()<other.getAge()) ){
            return true;
        }
        return false;
    }

    public boolean isIncomparable(Tuple other){
        if(!this.dominates(other) && !other.dominates(this)){
            return true;
        }
        return false;
    }

    public int getPrice() {
        return price;
    }

    public int getAge() {
        return age;
    }

    public String toString(){
        return price + "," + age;
    }

    public boolean equals(Object o) {
        if(o instanceof Tuple) {
            Tuple t = (Tuple)o;
            return this.price == t.price && this.age == t.age;
        } else {
            return false;
        }
    }

}