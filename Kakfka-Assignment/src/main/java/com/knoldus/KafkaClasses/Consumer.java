package com.knoldus.KafkaClasses;

import com.knoldus.UserPojo.User;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.FileWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Consumer {

    public static void main(String[] args) throws Exception {

        Properties p=new Properties();
        p.put("bootstrap.servers", "localhost:9092");
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer","com.knoldus.SerializedDeserialized.ObjectDeserialization"); //Path for Deserialized Class
        p.put("group.id","test-group");

        KafkaConsumer<String, User> kc=new KafkaConsumer<String,User>(p);

        List topics=new ArrayList();
        topics.add("User");      //User is the name of the topic.
        kc.subscribe(topics);

        while(true)
        {
            ConsumerRecords<String,User> rec= kc.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String,User> record : rec){

                //Printing Consumed Message onto the console.

                System.out.println(record.value().toString()); //Calling toString() Method to print the message as desired output format.

                System.out.println("Writing the Consumed Message into the file named as 'MyFile.txt'");

             //Program to add message into the file.

                FileWriter fw=new FileWriter("MyFile.txt",true);

                fw.write(record.value().toString());

                fw.write("\n");

                //Program to end message into the file ends here.


                System.out.println("The consumed message is successfully written into the file\n");

                fw.close();


            }
        }


    }

}
