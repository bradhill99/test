import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;


public class TestDataGenerator {
	// id=sha1 weight=float price=float popularity=int
	public static void main(String[] args) throws Exception {
	  FileWriter fstream = null;
	  BufferedWriter out = null;

	  try{
		  // Create file 
		fstream = new FileWriter("d:\\out.txt");
		out = new BufferedWriter(fstream);

		int cnt = 100;		
		while(cnt-- > 0) {
			String str = "id=%sha1% weight=%weight_float% price=%price_float% popularity=%po_int% title=%title%";
			// id=
			String sha1 = getSha1(Integer.toString(cnt));
			str = str.replaceFirst("%sha1%", sha1);

    		Random generator = new Random();
    		float f = generator.nextFloat();
			// weight=
    		str = str.replaceFirst("%weight_float%", Float.toString(f));
    		// price=
    		str = str.replaceFirst("%price_float%", Float.toString(f));
    		// popularity
    		int i = generator.nextInt(100) + 1;
    		str = str.replaceFirst("%po_int%", Integer.toString(i));
    		
    		// title 
    		str = str.replaceFirst("%title%", Integer.toString(cnt));
    		out.write(str + "\n");
		}
		
		out.close();
	  }
	  catch(IOException ex) {
		  
	  }
	}		

	static String getSha1(String input) throws NoSuchAlgorithmException {
	        MessageDigest mDigest = MessageDigest.getInstance("SHA1");
	        byte[] result = mDigest.digest(input.getBytes());
	        StringBuffer sb = new StringBuffer();
	        for (int i = 0; i < result.length; i++) {
	            sb.append(Integer.toString((result[i] & 0xff) + 0x100, 16).substring(1));
	        }
	         
	        return sb.toString();
	    }
}
