package org.istrsac.property;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.Properties;
import java.util.Random;

public class Property {
    public static void main(String[] args) {
        Logger logger =  LoggerFactory.getLogger(Property.class.getName());
        System.out.println("in the main method");
        JSONObject jsonObjCreated = new Property().getjsonObject();
        System.out.println(jsonObjCreated.getClass().getName());
        String jsonString = jsonObjCreated.toJSONString();
        System.out.println(jsonObjCreated +  "  "+ jsonString.getClass().getName());
        MongoClient mongo = new MongoClient("localhost",27017);
        while(true){
        new Property().checkMongoCOnnectivity(mongo);
        try{
        Thread.sleep(7000);
        }catch (InterruptedException e)
        {
            System.out.println(e);
        }
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Stopping application....");
                logger.info("Closing producer....");
                mongo.close();
                logger.info("Done....");

            }));

        }
    }

    public JSONObject getjsonObject(){
        Logger logger =  LoggerFactory.getLogger(Property.class.getName());
        String bootStrapServers1,bootStrapServers2 ,val,dateTime;
        JSONObject jsonObj = new JSONObject();
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        dateTime =(dtf.format(now)).toString();
        Random rand = new Random();
        try
        {
            FileReader reader=new FileReader("/home/koushik/Documents/includejar/prop.properties");
            Properties p=new Properties();
            p.load(reader);
            val = p.getProperty("hj");
            bootStrapServers1 = p.getProperty("bootstarp1");
            bootStrapServers2 = p.getProperty("bootstarp2");
            jsonObj.put("bootStrapServer1",bootStrapServers1);
            jsonObj.put("bootStrapServer2",bootStrapServers2);
            jsonObj.put("Time_now",dateTime);
            jsonObj.put("_id",rand.nextInt(1000000000));
        }
        catch (FileNotFoundException e){
            logger.info(e.toString());
        }
        catch (IOException e){
            logger.info(e.toString());
        }
        return jsonObj;
    }
    public void checkMongoCOnnectivity(MongoClient mongo){
        Logger logger =  LoggerFactory.getLogger(Property.class.getName());

        MongoCredential credential = MongoCredential.createCredential("testUser","javatpointdb","rooT_123".toCharArray());
        logger.info("Connected to the database successfully");
        MongoDatabase database = mongo.getDatabase("javatpointdb");
        logger.info("Credentials ::"+ credential);
        MongoCollection<Document> collection = database.getCollection("sampleCollection");
        logger.info("Collection myCollection selected successfully");
        JSONObject jsonObjCreated = new Property().getjsonObject();
        Document document = new Document(jsonObjCreated);
        try{
            collection.insertOne(document);
        }
        catch (com.mongodb.MongoWriteException ex)
        {
            if (ex.getCode() == 11000)
                System.out.println("Document already exists ");
            else
                System.out.println("Some issue");
        }
        logger.info("Document inserted successfully");
        MongoCollection<Document> collections = database.getCollection("sampleCollection");
        logger.info("Collection sampleCollection selected successfully");
        FindIterable<Document> iterDoc = collections.find();
        int i = 1;
        Iterator it = iterDoc.iterator();
        while (it.hasNext()) {
            System.out.println(it.next());
            i++;
        }
//mongo.close();
    }
}
