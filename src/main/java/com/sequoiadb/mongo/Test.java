package com.sequoiadb.mongo;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName Test
 * @Description 测试sequoiaDB对mongoDB的兼容性
 * @Author yangqi
 * @Date 2021/5/6 10:23 上午
 * @Version 1.0
 **/
public class Test {

    private MongoClient mongoClient = null;
    private MongoDatabase mongoDatabase;
    private MongoCollection mongoCollection;

    public MongoClient getConnection(String mongoUrl,int port){
        mongoClient = new MongoClient(mongoUrl, port);
        return mongoClient;
    }

    public void closeResource(){
        if (mongoClient != null){
            mongoClient.close();
        }
    }

    public void setDB(String dbName,String tbName){
        mongoDatabase = mongoClient.getDatabase(dbName);
        mongoCollection = mongoDatabase.getCollection(tbName);
    }

    public void insert(){
        System.out.println("insert statement...");
        List<Document> list = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            list.add(new Document("name", "Nike_"+i).append("id",i).append("age", i%10+20).append("salary", 10000+i*100));
        }
        mongoCollection.insertMany(list);
        select();
    }

    public void update(){
        System.out.println("update statement...");
        Bson bson = new Document("name", "Nike_"+1).append("id",1).append("age", 21).append("salary", 10100);
        Bson bson1 = new Document("name", "Json").append("id",1).append("age", 21).append("salary", 10100);
        mongoCollection.updateOne(bson,bson1);
        select();
    }

    public void delete(){
        System.out.println("delete statement...");
        Bson bson = new Document("name", "Json").append("id",1).append("age", 21).append("salary", 10100);
        mongoCollection.deleteOne(bson);
        select();
    }

    public void select(){
        System.out.println("select statement...");
        FindIterable findIterable = mongoCollection.find();
        MongoCursor iterator = findIterable.iterator();
        while (iterator.hasNext()){
            System.out.println(iterator.next());
        }
    }
}
