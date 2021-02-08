package com.knoldus.SerializedDeserialized;

import com.knoldus.UserPojo.User;
import org.apache.kafka.common.serialization.Serializer;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class ObjectSerialization implements Serializer<User> {


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String s, User user) {


        if (user == null)
            return null;

        byte user_array[]=null;

        ObjectMapper om=new ObjectMapper();
        String res="";
        try {
            res=om.writeValueAsString(user);   ////Converting object into string.
        } catch (IOException e) {
            e.printStackTrace();
        }
        user_array=res.getBytes(StandardCharsets.UTF_8);  //Converting string into bytes array.

       return user_array;
    }



    @Override
    public void close() {

    }
}
