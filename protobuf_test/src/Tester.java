import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;


public class Tester {
    public static void write() {
        Test.Person.Builder person = Test.Person.newBuilder();        
        person.setId(123);
        person.setName("Bob");
        person.setEmail("bob@example.com");
       
        FileOutputStream output;
        try {
            output = new FileOutputStream("person.pb");
            person.build().writeTo(output);        
            output.close();
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
    
    public static void read() {
        try {
            Test.Person person = Test.Person.parseFrom(new FileInputStream("person.pb"));
            System.out.println("Person ID: " + person.getId());
            System.out.println("  Name: " + person.getName());
            System.out.println("  Email: " + person.getEmail());
        }
        catch (FileNotFoundException ex) {
            ex.printStackTrace();
        }
        catch (IOException ex) {
            ex.printStackTrace();
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
