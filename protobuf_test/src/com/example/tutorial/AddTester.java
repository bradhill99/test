package com.example.tutorial;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import com.example.tutorial.AddressBookProtos.AddressBook;
import com.example.tutorial.AddressBookProtos.Person;

public class AddTester {
    static public String pbFile = "d:\\spn\\tsmc\\address.pb";
    static public void read() {
        AddressBook addressBook;
        try {
            addressBook = AddressBook.parseFrom(new FileInputStream(pbFile));       

            for (Person person: addressBook.getPersonList()) {
                System.out.println("Person ID: " + person.getId());
                System.out.println("  Name: " + person.getName());
                if (person.hasEmail()) {
                  System.out.println("  E-mail address: " + person.getEmail());
                }
    
                for (Person.PhoneNumber phoneNumber : person.getPhoneList()) {
                  switch (phoneNumber.getType()) {
                    case MOBILE:
                      System.out.print("  Mobile phone #: ");
                      break;
                    case HOME:
                      System.out.print("  Home phone #: ");
                      break;
                    case WORK:
                      System.out.print("  Work phone #: ");
                      break;
                  }
                  System.out.println(phoneNumber.getNumber());
                }
            }
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    static public void write() {
        AddressBook.Builder addressBook = AddressBook.newBuilder();
        
        Person.Builder person = Person.newBuilder();
        person.setId(Integer.valueOf("1"));
        person.setName("Brad");
        person.setEmail("123@123.com");
        
        Person.PhoneNumber.Builder phoneNumber = Person.PhoneNumber.newBuilder().setNumber("1234444");
        phoneNumber.setType(Person.PhoneType.MOBILE);
        person.addPhone(phoneNumber);

        addressBook.addPerson(person.build());
        
        try {
            FileOutputStream output = new FileOutputStream(pbFile);
            addressBook.build().writeTo(output);
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
