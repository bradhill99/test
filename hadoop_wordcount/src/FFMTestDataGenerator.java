import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

public class FFMTestDataGenerator {
	// id=sha1 weight=float price=float popularity=int
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			throw new Exception("usage: TestDataGenerator #records output_file_name");
		}
		
	  FileWriter fstream = null;
	  BufferedWriter out = null;

	  try{
		int cnt = Integer.parseInt(args[0]);
		  // Create file 
		fstream = new FileWriter(args[1]);
		out = new BufferedWriter(fstream);
		while(cnt-- > 0) {
			String str = "path=%sha1% sha1=%path%";

			String sha1 = getSha1(Integer.toString(cnt));
			str = str.replaceFirst("%sha1%", sha1);

    		Random generator = new Random();
    		float f = generator.nextFloat();
    		str = str.replaceFirst("%path%", Float.toString(f));    		
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
