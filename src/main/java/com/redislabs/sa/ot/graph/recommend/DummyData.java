package com.redislabs.sa.ot.graph.recommend;

import com.github.javafaker.Faker;
import com.redislabs.sa.ot.util.CSVCities;
import com.redislabs.sa.ot.util.City;
import com.redislabs.sa.ot.util.LinesofTextFromTXTFile;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

public class DummyData {

    private static Faker faker = new Faker();
    private static long cityCounter=0;
    private static long languageCounter=0;

    public static String getFullName(){
        return (getFirstName()+" "+getLastName()).replaceAll("'","");
    }

    public static String getEmail(String fullName){
        String email =fullName.replaceAll(" ","");
        String domain = faker.company().buzzword()+faker.pokemon().name();
        domain = domain.replaceAll(" ","");
        domain = domain.replaceAll("'","");
        domain = domain.replaceAll("-","");
        String[] suffixOptions = {"com","org","net","ca","edu","gov"};
        if(System.nanoTime()%10==0){
            email = email+getRandomNumber(2000)+"@gmail.com";
        }else{
            int index = (int) (System.nanoTime()%5);
            email = email+"@"+domain+"."+suffixOptions[index];
        }
        return email.toLowerCase();
    }


    // 100, 20
    public static long getRandomTimeStamp(int daysBackinTimeMax, int daysBackInTimeMin){
        long random = getRandomNumber(daysBackinTimeMax-daysBackInTimeMin);
        random = random*86400000l;
        return System.currentTimeMillis()-(random+(86400000l*daysBackInTimeMin));
    }

    public static long getMillisForToday(){
        TimeZone timeZone = TimeZone.getTimeZone("UTC");
        Calendar cal = Calendar.getInstance(timeZone);
        Date date = new Date(System.currentTimeMillis());
        cal.setTime(date);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis();
    }

    public static int getYearForToday(){
        TimeZone timeZone = TimeZone.getTimeZone("UTC");
        Calendar cal = Calendar.getInstance(timeZone);
        Date date = new Date(System.currentTimeMillis());
        cal.setTime(date);
        return  cal.get(Calendar.YEAR);
    }

    public static int getYearForMillis(long millis){
        TimeZone timeZone = TimeZone.getTimeZone("UTC");
        Calendar cal = Calendar.getInstance(timeZone);
        Date date = new Date(millis);
        cal.setTime(date);
        return  cal.get(Calendar.YEAR);
    }

    public static long getMillisForPreviousDay(long dayToCompare){
        TimeZone timeZone = TimeZone.getTimeZone("UTC");
        Calendar cal = Calendar.getInstance(timeZone);
        Date date = new Date(dayToCompare-86400000l);
        cal.setTime(date);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis();
    }

    public static String getDayDateString(long dayMillis){
        SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd");
        return format1.format(dayMillis);
    }

    public static String getID(){
        return System.nanoTime()+"";
    }

    public static String getHexID(){
        return String.format("%x", System.nanoTime());
    }

    public static String getFirstName(){
        String name = faker.name().fullName(); // Miss Samanta Schmidt
        String firstName = faker.name().firstName(); // Emory
        return firstName;
    }

    public static String getLastName(){
        String lastName = faker.name().lastName(); // Barton
        return lastName;
    }

    public static String getZipCode(){
        String zipCode = faker.address().zipCode();
        return zipCode;
    }

    public static String getLong(){
        return faker.address().longitude();
    }

    public static String getLat(){
        return faker.address().latitude();
    }

    public static String getPaymentType(){
        String [] paymentTypes = {"Visa","Mastercard", "PayPal", "Apple Pay","Google Pay","ITEM_SWAP","NO_PAYMENT_REQUIRED"};
        int index = getRandomNumber(paymentTypes.length-1);
        return paymentTypes[index];
    }

    public static int getRandomNumber(int scale){
        double randNum =  (Math.random()*scale);
        int aNum = (int) randNum; // 0 and scale
        return aNum;
    }

    public static String getPostForProductName(String prodName){
        String post = "A rare opportunity to experience the "+prodName;
        return post;
    }

    static String getPostID(){
        return "post:"+System.nanoTime();
    }

    static String getPreferredLanguage(){
        String favLanguage = "English";//English
        if(languageCounter % 9 ==0){
            favLanguage="French"; // french
        }
        languageCounter++;
        return favLanguage;
    }

    static String getTwoColorItemPost(String itemName, String itemCategory,String preferredLanguage){
        String color = getColor();
        String color2 = getColor();
        String spacer = "   ";
        int timesUsed = getRandomNumber(10)+1;
        if(timesUsed>7){
            spacer = "  erotica  ";
        }
        String productPost = "A very cool offer (this "+itemName+" is shipped in a "+color+"-ish and truly "+color2+" box. - used only "+timesUsed+" times.  Hope to get full asking price or more - you pay for shipping.";
        String servicePost = "I endeavor to provide you with the very best customer experience as I work with you to build your "+itemName+"  we can change the color to "+color+" or even "+color2+" and I will work with you to get all the work finished in a timely fashion";
        if(preferredLanguage.equalsIgnoreCase("French")){
            ArrayList<String> frComments = LinesofTextFromTXTFile.getInstance().getFrenchComments();
            productPost = frComments.get(getRandomNumber(frComments.size()-1))+spacer+productPost;
            servicePost = frComments.get(getRandomNumber(frComments.size()-1))+spacer+servicePost;
        }else{
            ArrayList<String> enComments = LinesofTextFromTXTFile.getInstance().getEnglishComments();
            productPost = enComments.get(getRandomNumber(enComments.size()-1))+spacer+productPost;
            servicePost = enComments.get(getRandomNumber(enComments.size()-1))+spacer+servicePost;
        }
        String response = productPost;
        if(itemCategory.equalsIgnoreCase("service")){
            response = servicePost;
        }
        return response;
    }

    public static String getIPAddress(){
        return  faker.internet().publicIpV4Address();
    }

    public static City getCity(){
        CSVCities cHelper = CSVCities.getInstance();
        ArrayList<City> cities = cHelper.getCities();
        int randomCityIndex = getRandomNumber(cities.size());
        City city = cities.get(randomCityIndex);
        // Skew the results to live mostly near 3 cities:
        if(cityCounter%4==0){
            if(getRandomNumber(3)==1){
                city = cities.get(CSVCities.VANCOUVER_INDEX);
            }else if(getRandomNumber(3)==1){
                city = cities.get(CSVCities.MONTREAL_INDEX);
            }else{
                city = cities.get(CSVCities.TORONTO_INDEX);
            }
        }
        cityCounter++;
        return city;
    }

    public static String getPointStringLatLong(City value){
        City city=value;
        if(null == city){
            city = getCity();
        }
        double dlon = getRandomNumber(50)*.0012;
        double dlat = getRandomNumber(50)*.0012;
        double clat = Double.parseDouble(city.getLat());
        double clon = Double.parseDouble(city.getLng());

        if(System.nanoTime()%3==0){
            dlat = dlat*-1;
        }
        if(System.nanoTime()%2==0){
            dlon = dlon*-1;
        }

        String lats = (clat+dlat)+"0000001111";
        String lons = (clon+dlon)+"0000001111";
        lats = lats.substring(0,11);
        lons = lons.substring(0,12);
        String pointString = "point({latitude: "+lats+", longitude: "+lons+"})";
        return pointString;
    }

    public static String getProduct(){

        String product = faker.commerce().material()+" "+" #"+getRandomNumber(20);
        //faker.pokemon().name();
        return product.replaceAll("'","");
    }

    public static String getColor(){
       return faker.commerce().color();
    }

    public static String getDepartment(){
        String department = "&";
        while(department.contains("&")){
            department = faker.commerce().department();
        }
        return department;
    }

    public static String getPrice(){
        return faker.commerce().price();
    }

    public static String getCategory(){
        String category = "";
        if(System.nanoTime()%6==0){
            category = "home";
        }else if(System.nanoTime()%6==1){
            category = "work";
        }else if(System.nanoTime()%6==2){
            category = "gaming";
        }else if(System.nanoTime()%6==3){
            category = "auto";
        }else if(System.nanoTime()%6==4){
            category = "pets";
        }else {
            category = "service";
        }
        return category;
    }

    public static String getSeasons(){
        String [] fullSeasons = new String[]{"winter","spring","summer","fall"};
        String seasons = "";
        if(System.nanoTime()%7==0){
            seasons = "['"+fullSeasons[0]+"']";
        }else if(System.nanoTime()%7==1){
            seasons = "['"+fullSeasons[1]+"']";
        }else if(System.nanoTime()%7==2){
            seasons = "['"+fullSeasons[2]+"']";
        }else if(System.nanoTime()%7==3){
            seasons = "['"+fullSeasons[3]+"']";
        }else {
            seasons = "['"+fullSeasons[1]+"','"+fullSeasons[2]+"']";
        }
        return seasons;
    }

}
