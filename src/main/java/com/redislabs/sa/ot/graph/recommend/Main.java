package com.redislabs.sa.ot.graph.recommend;

import com.redislabs.redisgraph.ResultSet;
import com.redislabs.redisgraph.impl.api.RedisGraph;
import com.redislabs.redistimeseries.RedisTimeSeries;
import com.redislabs.sa.ot.util.CSVCities;
import com.redislabs.sa.ot.util.City;
import com.redislabs.sa.ot.util.JedisConnectionFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.HashMap;

public class Main {

    static JedisPool jPool = JedisConnectionFactory.getInstance().getJedisPool();
    static Jedis jedisClient = jPool.getResource();

    static RedisGraph graph = new RedisGraph(jPool);
    static RedisTimeSeries timeSeries = new RedisTimeSeries(jPool);
    static DummyData dd;
    static String GRAPH_NAME = "recommendations";
    static String CF_NAME = "posts";
    static String CF_cityKeyNAME = "cities";

    static io.rebloom.client.Client cfClient = new io.rebloom.client.Client(jPool);


    public static void main(String[] args) {
        jedisClient.flushDB();
        long startTime = System.currentTimeMillis();
        //deleteCFilters();
        //deleteGraph();
        prepareRecommendationDBData(3,500,250,3);
        long endTime = System.currentTimeMillis();
        System.out.println("\n\ttotal time to build graph was: "+(endTime-startTime));
        testPerf();
    }

    static void testPerf() {
        for(int x=0;x<10;x++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    long perfStartTime = System.currentTimeMillis();
                    HashMap<String,String> labels = new HashMap();
                    labels.put("group","GRAPH_PERF_TEST");
                    jedisClient = jPool.getResource();
                    jedisClient.del(("ts:perfTest:" + Thread.currentThread().getId()));
                    timeSeries.create(("ts:perfTest:" + Thread.currentThread().getId()), 0, labels);

                    ResultSet result = graph.query(GRAPH_NAME, "MATCH (s:Member)-[:NEAR]->(c:City {name: 'Toronto'}), (i:Item)-[:NEAR]->(c)  SET i.city_type='big city'");

                    System.out.println("perfTest: result.size() == "+result.size());
                    timeSeries.add(("ts:perfTest:"+Thread.currentThread().getId()),System.currentTimeMillis()-perfStartTime);
                }
            }).start();
        }
    }


    static void deleteGraph(){
        //try(jedisClient = jPool.getResource();) {
            if (jedisClient.exists(GRAPH_NAME)) {
                graph.deleteGraph(GRAPH_NAME);
            }
        //}
    }

    static void deleteCFilters(){
       // try (io.rebloom.client.Client cfClient = new io.rebloom.client.Client(jPool);
        //){
        try{
            cfClient.delete(CF_NAME); //posts
            cfClient.delete(CF_cityKeyNAME);
        }catch(Throwable t){
            t.printStackTrace();
        }
    }

    static void prepareRecommendationDBData(int years, int members, int listings, int randomCityCountForLoveAssignments){
        populateDays(years);
        createDayIndexes();
        System.out.println("Creating City nodes");
        populateGraphCities(); // using 1700 Canadian Cities
        createCityIndexes();
        System.out.println("Creating Member nodes");
        populateGraphMembers(members);
        System.out.println("Creating listing nodes");
        populateGraphItems(listings);
        System.out.println("Creating Graph Indexes");
        createGraphIndexes();
        System.out.println("Establishing NEAR Members");
        matchCitiesToMembers(7000);
        System.out.println("Establishing NEAR Items");
        matchItemsToCities(7000);

        System.out.println("Adding Posts from submitting Members for Items");
        for(int a=0;a < (50);a++) { // with 10,000 listings this means 100 iterations
            populateGraphAddPostedByWithComment();
        }
        System.out.println("creating posts indexes...");
        createPostsIndexes();

        System.out.println("Establishing PURCHASED Items");
        boolean shouldPrint = true;
        for(int count=1;count<100;count++) {
            int metersDistant = count*200;
            sleepForABit(10);
            matchItemsToPurchasers(metersDistant, shouldPrint);
            shouldPrint=false;
        }
        System.out.println("Creating Ranking nodes");
        populateGraphItemRankings(1);
        System.out.println("Creating Ranked index");
        createRankingIndex(); // had to wait for Ranks to exist

        System.out.println("Creating Member LOVES Listing relationships");
        for (int x = 0; x < randomCityCountForLoveAssignments; x++) {
            sleepForABit(10);
            populateGraphAddFavorite(30);
        }

        System.out.println("\n\t\tDone with all Graph Prep");
    }

    static void sleepForABit(long howLong){
        try{
            Thread.sleep(howLong);
        }catch(Throwable t){}
    }

    static void addPostOutsideGraph(){
        String post1 = dd.getTwoColorItemPost("Tall Cabinet","work","English");
        String key = dd.getPostID();
        if(isDuplicatePost(post1)) {
            //doNothing
        }else {
           // try(Jedis jedisClient = jPool.getResource()) {
                jedisClient.set(key, post1);
                System.out.println("\nPost with key: " + key + " successfully added!");
            //}
        }
    }

    static boolean isDuplicatePost(String post){
        boolean isDuplicate = false;
        boolean shouldDisplayMessage =true;
        //try(io.rebloom.client.Client cfClient = new io.rebloom.client.Client(jPool)){
          //  try(Jedis jedisClient = jPool.getResource()){
                if (jedisClient.exists(CF_NAME)){
                    shouldDisplayMessage=false;
                } else {
                    System.out.println("creating Cuckoo Filter with keyname: "+CF_NAME);
                    cfClient.cfCreate(CF_NAME, 100000);
                }
                if(cfClient.cfExists(CF_NAME,post)){
                    isDuplicate=true;
                    if(shouldDisplayMessage) {
                        System.out.println("\n\t<<<^!!!^>>> We discourage members from submitting the same post more than once - please refrain from duplicate posts." +
                                "\n\tYour post was this: \n\t" + post);
                    }
                }else{
                    cfClient.cfAddNx(CF_NAME,post);
                }
            //}
        //}
        return isDuplicate;
    }

    static boolean isDuplicateCity(String cityName){
        boolean isDuplicate = false;
        String CF_NAME = "cities";
        //try(io.rebloom.client.Client cfClient = new io.rebloom.client.Client(jPool)) {
        //    try (Jedis jedisClient = jPool.getResource()) {
                if (jedisClient.exists(CF_NAME)) {
                } else {
                    System.out.println("creating Cuckoo Filter with keyname: " + CF_NAME);
                    cfClient.cfCreate(CF_NAME, 10000);
                }
                if (cfClient.cfExists(CF_NAME, cityName)) {
                    isDuplicate = true;
                } else {
                    cfClient.cfAddNx(CF_NAME, cityName);
                }
          //  }
        //}
        return isDuplicate;
    }

    static void createRankingIndex() {
        String query = "create index ON :Ranking(NumberOfStars)";
        System.out.println(query);
        graph.query(GRAPH_NAME, query);
    }

    static void createPostsIndexes() {
        String query = "create index ON :Post(id)";
        System.out.println(query);
        graph.query(GRAPH_NAME, query);
        query = "CALL db.idx.fulltext.createNodeIndex('Post', 'content')";
        System.out.println(query);
        graph.query(GRAPH_NAME, query);
    }

    static void createGraphIndexes() {
        String query = "create index ON :Product(geo_point)";
        System.out.println(query);
        graph.query(GRAPH_NAME, query);
        query = "create index ON :Product(name)";
        System.out.println(query);
        graph.query(GRAPH_NAME, query);
        query = "create index ON :Member(geo_point)";
        System.out.println(query);
        graph.query(GRAPH_NAME, query);
        query = "create index ON :Member(id)";
        System.out.println(query);
        graph.query(GRAPH_NAME, query);
    }

    static void createCityIndexes(){
        String query = "create index ON :City(name)";
        System.out.println(query);
        graph.query(GRAPH_NAME, query);
        query = "create index ON :City(geo_point)";
        System.out.println(query);
        graph.query(GRAPH_NAME, query);
    }

    static void createDayIndexes(){
        String query = "create index ON :Day(id)";
        System.out.println(query);
        graph.query(GRAPH_NAME, query);
        query = "create index ON :Day(millis)";
        System.out.println(query);
        graph.query(GRAPH_NAME, query);
    }


    static void matchItemsToPurchasers(int meters, boolean shouldPrint) {
        String itemIDVname = "itemID";
        String memberIDVname = "memberID";
        // ## get list of items that have not been purchased then purchase them
        String query = "match (p:Post)-[:FEATURES]->(i:Item {claimed: 'false'})-[:ADDED_IN]->(c:City)," +
                "(m:Member)-[:NEAR]->(c),(x:Member)<-[:POSTED_BY]-(p)  WHERE distance(i.geo_point, m.geo_point) < "+meters+
                " with i,m,p,c,x limit 1 CREATE (i)<-[:PURCHASED]-(m) return i.id as "+itemIDVname+" , m.id as "+memberIDVname;
        ResultSet resultSet = graph.query(GRAPH_NAME,query);
        if(shouldPrint){
            System.out.println(resultSet.size()+" Records returned from query: "+query);
        }
        while(resultSet.hasNext()) {
            com.redislabs.redisgraph.Record record = resultSet.next();
            String itemID = record.getValue(itemIDVname);
            long memID = record.getValue(memberIDVname);
            /*
            query = "match (i:Item), (m:Member) where i.id = '" + itemID + "' AND m.id =" + memID + " CREATE (m)-[:PURCHASED]->(i)";
            if (shouldPrint) {
                System.out.println(query);
            }
            graph.query(GRAPH_NAME, query);
             */
            query = "match (i:Item) WHERE i.id = '" + itemID + "' SET i.claimed = 'true'";
            if (shouldPrint) {
                System.out.println(query);
            }
            graph.query(GRAPH_NAME, query);
        }
    }

    static void matchItemsToCities(int meters) {
        String query = "match (i:Item),(c:City) WHERE distance(i.geo_point, c.geo_point) < " + meters + " CREATE (i)-[:NEAR]->(c)";
        System.out.println(query);
        graph.query(GRAPH_NAME, query);
    }

    static void matchCitiesToMembers(int meters) {
        String query = "match (s:Member),(c:City) WHERE distance(s.geo_point, c.geo_point) < " + meters + " CREATE (s)-[:NEAR]->(c)";
        System.out.println(query);
        graph.query(GRAPH_NAME, query);
    }

    // This ignores the complexity of timeZones and
    // just uses whatever the service loading the data thinks the day is
    static void populateDays(int yearsCountingBackwards) {
        boolean firstTime = true;
        long millisToUse = dd.getMillisForToday();
        for(int x=0; x < 365*yearsCountingBackwards ; x++) {
            if (firstTime) {
                createDayNode(millisToUse, true);
                firstTime = false;
            } else {
                millisToUse = dd.getMillisForPreviousDay(millisToUse);
                createDayNode(millisToUse, false);
            }
        }
    }


    static void createDayNode(long millisToUse, boolean firstTime){
        String query = "CREATE (:Day { id: '"+dd.getDayDateString(millisToUse)+"', "+
                "millis: "+millisToUse+" , timezone: 'UTC' })";
        if(firstTime) {
            System.out.println("createDayNode() EXECUTING: \n" + query);
        }
        graph.query(GRAPH_NAME,query);
    }

    static void populateGraphMembers(int howMany){
        boolean firstTime = true;
        for(int x=0;x<howMany;x++) {
            String fullName = dd.getFullName();
            City city = dd.getCity();
            String prefLanguage = dd.getPreferredLanguage();
            if(city.getProvinceID().equalsIgnoreCase("QC")){
                prefLanguage = "French";
            }
            String query = "MATCH (d:Day { id: '"+dd.getDayDateString(dd.getRandomTimeStamp(729,365))+"'}) CREATE (:Member { id: "+dd.getID()+" , name: '"+fullName+"', email: '"+dd.getEmail(fullName)+"'," +
                    " country: 'Canada', province: '"+city.getProvinceID()+"', language: '"+prefLanguage+"', City: '"+city.getCityName()+"' "+
                    ", geo_point: "+dd.getPointStringLatLong(city)+"})" +
                    "-[:JOINED_ON]->(d)";
            if(firstTime) {
                System.out.println(query);
                firstTime=false;
            }
            graph.query(GRAPH_NAME, query);

        }
    }

    static void populateGraphCities(){
        boolean firstTime = true;
        CSVCities cityHelper = CSVCities.getInstance();
        ArrayList<City> cities = cityHelper.getCities();
        for(City c : cities){
            String query = "CREATE (:City { id: "+c.getId()+" , " + "name: '"+c.getCityName()+"', " +
                    "country: 'Canada', " + "provinceID: '"+c.getProvinceID()+"', " +
                    "postalCodes: '"+c.getProvinceID()+"', " +
                    "geo_point: point({latitude: "+c.getLat()+", longitude: "+c.getLng()+"})"+"})";
            if(firstTime) {
                System.out.println(query);
                firstTime=false;
            }
            graph.query(GRAPH_NAME,query);
        }
    }

    static void populateGraphItems(int howMany){
        boolean firstTime = true;
        for(int x=0;x<howMany;x++) {
            City city = dd.getCity();
            String query = "MATCH (c:City {name: '"+city.getCityName()+"'}), (d:Day{ id: '"+
                    dd.getDayDateString(dd.getRandomTimeStamp(364,4))+"'})" +
                    " CREATE (c)<-[:ADDED_IN]-(:Item {id: '"+dd.getHexID()+"'," +
                    " paymentType: '"+dd.getPaymentType()+"', "+
                    "name: '"+dd.getProduct()+"', color: '"+dd.getColor()+"', price: "+dd.getPrice()+", " +
                    "department: '"+dd.getDepartment()+"',claimed: 'false', seasons: "+dd.getSeasons()+"," +
                    " category: '"+dd.getCategory()+"', " +
                    "geo_point: "+dd.getPointStringLatLong(city)+"})-[:ADDED_ON]->(d)";
            if(firstTime) {
                System.out.println(query);
                firstTime=false;
            }
            graph.query(GRAPH_NAME,query);
        }
    }

    //here we add Post comment content to items posted by the poster:
    // all Items need a poster
    static void populateGraphAddPostedByWithComment(){
        boolean printToScreen = true;
        String cityName = dd.getCity().getCityName();
        // we don't want to hit the same city all the time so cuckoo it to reduce the # of dupes:
        // we short circuit later when duplicate itemIDs are used so here we can allow for occasional same city
        while(isDuplicateCity(cityName)){
            cityName = dd.getCity().getCityName();
            sleepForABit(10);
        }
        //String query = "MATCH (m:Member)-[:NEAR]->(c:City {name: '"++"'}), " + "(i:Item)-[:NEAR]->(c) WHERE NOT (m)-[:LOVES]->(i) return m.id as member_id, i.id as item_id";

        //adding filter for PURCHASED (any existing relationship between Members  and Items)
        String query = "MATCH (m:Member)-[:NEAR]->(c:City {name: '"+cityName+"'}), " +
                "(i:Item)-[:NEAR]->(c) return m.id as member_id, m.language as preferredLanguage, i.id as item_id, i.name as item_name, i.category as item_category";

        System.out.println("populateGraphAddPostedByWithComment() sending this query: \n"+query);
        ResultSet resultSet = graph.query(GRAPH_NAME,query);
        System.out.println(resultSet.size()+" Records returned from query: "+query);

        while(resultSet.hasNext()) { // checking for dupes allows us to move to the next city quickly
            com.redislabs.redisgraph.Record record = resultSet.next();
            Long memberID = record.getValue("member_id");
            String preferredLanguage = record.getValue("preferredLanguage");
            String itemID = record.getValue("item_id");
            String itemName = record.getValue("item_name");
            String itemCategory = record.getValue("item_category");
            createPostForItem(memberID, itemID, itemName, itemCategory, preferredLanguage, printToScreen);
            if (printToScreen) {
                printToScreen = false;
            }
        }
    }

    static void createPostForItem(long memberID, String itemID, String itemName,String itemCategory, String preferredLanguage,boolean printToScreen){
        String post = dd.getTwoColorItemPost(itemName,itemCategory,preferredLanguage);
        post = "In reference to Item: "+itemID+"   <post-comment>"+post+"</post-comment>";
        String postID = dd.getPostID();
        String query = "CREATE (p:Post {id: '"+postID+"' , content: '"+post+"', ipaddress: '"+DummyData.getIPAddress()+"'}) return p";
        String relationshipQuery = "MATCH (m:Member {id: "+memberID+"}), (i:Item {id: '"+itemID+"'}), " +
                "(p:Post {id: '"+postID+"'}) CREATE (m)<-[:POSTED_BY]-(p)-[:FEATURES]->(i)";

        if(printToScreen) {
            System.out.println("createPostForItem() issuing 2 queries :");
            System.out.println(query);
            System.out.println(relationshipQuery);
        }
        if(isDuplicatePost(itemID+":"+itemName+":"+itemCategory)){
            //don't write that post into system (Skip that entry)
        }else {
            graph.query(GRAPH_NAME, query);
            sleepForABit(10);
            graph.query(GRAPH_NAME,relationshipQuery);
        }
    }

    // users 'LOVE' items this is a chance to capture that relationship
    static void populateGraphAddFavorite(int nodeInvolvedLimit){
        boolean printToScreen = false;
        String query = "MATCH (s:Member)-[:NEAR]->(c:City {name: '"+dd.getCity().getCityName()+"'}), " +
                "(i:Item)-[:NEAR]->(c) WHERE NOT (s)-[:LOVES]->(i) return s.id as member_id, i.id as item_id limit "+nodeInvolvedLimit;
        ResultSet resultSet = graph.query(GRAPH_NAME,query);
        System.out.println(resultSet.size()+" Records returned from query: "+query);
        while(resultSet.hasNext()) {
            com.redislabs.redisgraph.Record record = resultSet.next();
            Long memberID = record.getValue("member_id");
            String itemID = record.getValue("item_id");
            if(System.nanoTime()%3==0) { // create only 33% rankings on purchases nearby
                createLoveForProduct(memberID, itemID, printToScreen);
                if(printToScreen){
                    printToScreen=false;
                }
            }
         }
    }

    static void createLoveForProduct(long memberID, String itemID, boolean printToScreen){
        String query = "MATCH (m:Member {id: "+memberID+"}), (i:Item {id: '"+itemID+"'}) " +
                "CREATE (m)-[:LOVES]->(i)";
        if(printToScreen) {
            System.out.println(query);
        }
        graph.query(GRAPH_NAME,query);
    }

    //This has to happen after PURCHASED relationship has been established
    // here we bind Items to ratings
    // get all Purchases
    // find a item id by proximity to user [once point() and distance() work]
    // create a userRating node that references the itemID and userID
    // later we can look within a region for users and their recommendations
    // we can look by season
    // we can also look by network user-referral
    static void populateGraphItemRankings(int howManyPerItem){
        boolean printToScreen = true;
        String query = "match (m:Member)-[:PURCHASED]->(i:Item) return m.id as member_id, i.id as item_id, i.name as item_name";
        ResultSet resultSet = graph.query(GRAPH_NAME,query);
        System.out.println(resultSet.size()+" Records returned from query: "+query);
        while(resultSet.hasNext()){
            com.redislabs.redisgraph.Record record = resultSet.next();
            Long memberID = record.getValue("member_id");
            String itemID = record.getValue("item_id");
            String itemName = record.getValue("item_name");
            for(int x = 0;x<howManyPerItem;x++){
                if(System.nanoTime()%10==0) { // create only 10% rankings on purchases made
                    createRanking(memberID, itemID, itemName, printToScreen);
                    if(printToScreen){
                        printToScreen=false;
                    }
                }
            }
        }
    }

    static void createRanking(Long memberID, String itemID, String itemName, boolean printToScreen){
        String query = "MATCH (m:Member {id: "+memberID+"}), (i:Item {id: '"+itemID+"'}) " +
                "CREATE (r:Ranking {id: '"+dd.getHexID()+"', itemName: '"+itemName+"',NumberOfStars: "+dd.getRandomNumber(6)+"})" +
                "-[:RANKS]->(i)-[:RANKED_BY]->(m)";
        if(printToScreen) {
            System.out.println(query);
        }
        graph.query(GRAPH_NAME,query);
    }

    static void printResults(String graphName, String query){
        System.out.println("Query: "+query+"  graph: "+graphName);
        ResultSet resultSet = graph.query(graphName,query);
        System.out.println(resultSet.size()+" Records returned from query: "+query);
        while(resultSet.hasNext()){
            com.redislabs.redisgraph.Record record = resultSet.next();
            for(String k : record.keys()){
                System.out.print("[Record] Key: "+k);
                System.out.println("  Value: "+record.getValue(k));
            }
        }
    }

    static void queryMembers(int id){
        String query = "match (s:Member) return s.name, s.id limit 10";
        query = "match (s:Member {id: "+id+"}) return s";
        printResults(GRAPH_NAME,query);
    }


    static void queryProducts(String id){
        String query = "match (p:Product) return p.name, p.id limit 10";
        query = "match (p:Product {id: "+id+"}) return p";
        printResults(GRAPH_NAME,query);
    }

}
