package com.knoldus.SerializedDeserialized;

import com.knoldus.UserPojo.User;

import org.apache.kafka.common.serialization.Deserializer;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Map;

public class ObjectDeserialization implements Deserializer<User> {


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public User deserialize(String s, byte[] bytes) {

        if (bytes == null) {
            System.out.println("Null value received at deserialize");
            return null;
        }

            User user=null;

            ObjectMapper om=new ObjectMapper();
        try {
            user=om.readValue(bytes,User.class); ////Converting bytes array into User object
        } catch (IOException e) {
            e.printStackTrace();
        }


        return user;
    }



    @Override
    public void close() {

    }
}
