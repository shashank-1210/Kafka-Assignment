package com.knoldus.KafkaClasses;

import com.knoldus.UserPojo.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;

public class Producer {

    public static void main(String[] args) {

        Properties p=new Properties();
        p.put("bootstrap.servers","localhost:9092");
        p.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer","com.knoldus.SerializedDeserialized.ObjectSerialization");////Path for Serialized Class



      KafkaProducer<String,User> kp=new KafkaProducer<String, User>(p);

      //Setting values to the User Object.

      User user1=new User(101,21,"Shashank","B.Tech");
      User user2=new User(102,22,"Saurav","M.Tech");


//Code for generating random values in order to make key unique.

        Random rand = new Random();

      //  Code for generating random values in order to make key unique ends here.


        try {


           //Passing topic name,key  and value to the ProducerRecord

           kp.send(new ProducerRecord("User",Integer.toString(rand.nextInt(10000)),user1));

          ;

           kp.send(new ProducerRecord("User",Integer.toString(rand.nextInt(10000)),user2));




       }
       catch (Exception e)
       {
           System.out.println("Exception:"+e);
       }
       finally {
           kp.close();
       }


    }

}
