package com.redislabs.sa.ot.util;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Scanner;

public class LinesofTextFromTXTFile {

    private static LinesofTextFromTXTFile instance = null;

    String  filePath1 = PropertyFileFetcher.loadProps("dataload.properties").getProperty("polite.comments.english.path");
    String  filePath2 = PropertyFileFetcher.loadProps("dataload.properties").getProperty("polite.comments.french.path");
    //"/Users/owentaylor/wip/demos/graph_recommendations/polite_small_vocab_en.txt";

    private ArrayList<String> englishComments = new ArrayList<String>();
    private ArrayList<String> frenchComments = new ArrayList<String>();

    public void loadEnglishComments(){
        Reader in = null;
        try {
            in = new FileReader(filePath1);
            Scanner sc=new Scanner(in);
            while(sc.hasNextLine()){
                englishComments.add(sc.nextLine().replace("'",""));
            }
        }catch(Throwable t ){
            t.printStackTrace();
        }
    }

    public void loadFrenchComments(){
        Reader in = null;
        try {
            in = new FileReader(filePath2);
            Scanner sc=new Scanner(in);
            while(sc.hasNextLine()){
                frenchComments.add(sc.nextLine().replace("'",""));
            }
        }catch(Throwable t ){
            t.printStackTrace();
        }
    }

    public ArrayList<String> getEnglishComments(){
        if(englishComments.size()<1){
            loadEnglishComments();
            System.out.println("englishComments.size() == "+englishComments.size());
        }
        return englishComments;
    }

    public ArrayList<String> getFrenchComments(){
        if(frenchComments.size()<1){
            loadFrenchComments();
            System.out.println("frenchComments.size() == "+frenchComments.size());
        }
        return frenchComments;
    }

    public LinesofTextFromTXTFile() {
    }

    public static LinesofTextFromTXTFile getInstance() {
        if (instance == null) {
            instance = new LinesofTextFromTXTFile();
        }
        return instance;
    }


}
