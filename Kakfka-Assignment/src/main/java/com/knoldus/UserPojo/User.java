package com.knoldus.UserPojo;

public class User {

    //User class fields

    private int user_id;
    private int user_age;
    private String user_name;
    private String user_course;


    //Default Constructor

    public User()
    {

    }

    //Arg-constructor to initialize fields

    public User(int user_id, int age, String user_name, String user_course) {



        this.user_id = user_id;
        this.user_age = age;
        this.user_name = user_name;
        this.user_course = user_course;

    }

    //Getters for user class fields

    public int getUser_id() {
        return user_id;
    }

    public int getUser_age() {
        return user_age;
    }

    public String getUser_name() {
        return user_name;
    }

    public String getUser_course() {
        return user_course;
    }


//Overriding the default toString() method according the output format we want from our project,

    @Override
    public String toString() {
        return "User Message{"+"\"id\":" + '"'+user_id+'"' + ",\"name\":" + '"'+user_name+'"' + ",\"age\":" + '"'+user_age+'"' + ",\"course\":" + '"'+user_course+'"'  + "}";
    }


}
