import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.InetAddress;
import java.nio.ByteBuffer;

public class Cidr {

    /**
     * @param args
     * @throws UnknownHostException 
     */
    private static void test(String addr) {
        try {
            String[] parts = addr.split("/");
    
            InetAddress bar = InetAddress.getByName(parts[0]);
            int network = ByteBuffer.wrap(bar.getAddress()).getInt();
            int subnet_length = Integer.parseInt(parts[1]);    
            int mask = 0xfffffff << (32 - subnet_length);
            int start_ip = (network & mask) + 1;
            int end_ip = (network | ~mask) - 1; 
            
            System.out.println(Integer.toHexString(start_ip));
            System.out.println(Integer.toHexString(end_ip));
        }
        catch(UnknownHostException ex) {
            ex.printStackTrace();
        }
    }
    
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        test("103.10.4.0/25");        
    }

}
