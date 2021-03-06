package com.redislabs.sa.ot.util;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.Reader;
import java.util.ArrayList;

public class CSVCities {

    private static CSVCities instance = null;
    public static int TORONTO_INDEX = 0;
    public static int VANCOUVER_INDEX = 0;
    public static int MONTREAL_INDEX = 0;

    String  filePath = PropertyFileFetcher.loadProps("dataload.properties").getProperty("city.csvfilepath");
    //"/Users/owentaylor/wip/demos/graph_recommendations/simplemaps_canadacities_basicv1.7/canadacitiesNoQuotes.csv";

    private ArrayList<City> cities = new ArrayList<City>();

    public void loadCities(){
        Reader in = null;
        try {
            in = new FileReader(filePath);
            Iterable<CSVRecord> records = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in);
            for (CSVRecord record : records) {
                City city = new City();
                city.setCityName(record.get(1));
                city.setProvinceID(record.get(2));
                city.setProvinceName(record.get(3));
                city.setLat(record.get(4));
                city.setLng(record.get(5));
                city.setPostalCodes(record.get(10).split(" "));
                city.setId(record.get(11));
                cities.add(city);
                if(city.cityName.equalsIgnoreCase("Toronto")&&city.getProvinceID().equalsIgnoreCase("ON")){
                    TORONTO_INDEX= cities.size()-1;
                    System.out.println("TORONTO: "+cities.get(TORONTO_INDEX).cityName);
                }
                if(city.cityName.equalsIgnoreCase("Vancouver")&&city.getProvinceID().equalsIgnoreCase("BC")){
                    VANCOUVER_INDEX= cities.size()-1;
                    System.out.println("VANCOUVER: "+cities.get(VANCOUVER_INDEX).cityName);
                }
                if(city.cityName.equalsIgnoreCase("Montreal")&&city.getProvinceID().equalsIgnoreCase("QC")){
                    MONTREAL_INDEX= cities.size()-1;
                    System.out.println("MONTREAL: "+cities.get(MONTREAL_INDEX).cityName);
                }
            }
        }catch(Throwable t ){
            t.printStackTrace();
        }
    }

    //"city","city_ascii","province_id","province_name","lat","lng",
    // "population","density","timezone","ranking","postal","id"

    public ArrayList<City> getCities(){
        if(cities.size()<1){
            loadCities();
            System.out.println("cities.size() == "+cities.size());
        }
        return cities;
    }

    public CSVCities() {
    }

    public static CSVCities getInstance() {
        if (instance == null) {
            instance = new CSVCities();
        }
        return instance;
    }


}
