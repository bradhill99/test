package com.example.tutorial;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import com.example.tutorial.PersonProto.Person;

public class PersonTester {
    static public String pbFile = "d:\\spn\\tsmc\\person.pb";
    static public void read() {
        Person person;
        try {
            person = Person.parseFrom(new FileInputStream(pbFile));       

            System.out.println("Person ID: " + person.getId());
            System.out.println("  Name: " + person.getName());
            System.out.println("  E-mail address: " + person.getEmail());
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    static public void write() {
        Person.Builder person = Person.newBuilder();        
        person.setId(Integer.valueOf("1"));
        person.setName("Brad");
        person.setEmail("123@123.com");
                
        try {
            FileOutputStream output = new FileOutputStream(pbFile);
            person.build().writeTo(output);
            output.close();
        } catch (IOException e) {
            e.printStackTrace();
        }        
    }
    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        write();
        read();
    }

}
