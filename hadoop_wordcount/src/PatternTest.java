import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class PatternTest {
	static public void main(String[] args) {
		String input = "tes= id=6H500F0 name=Maxtor%20DiamondMax%2011%20-%20hard%20drive%20-%20500%20GB%20-%20SATA-300 manu=Maxtor%20Corp. manu_id_s=maxtor cat=electronics cat=hard%20drive features=SATA%203.0Gb%2Fs%2C%20NCQ features=8.5ms%20seek features=16MB%20cache price=350 popularity=6 inStock=true";
    	StringTokenizer tokenizer = new StringTokenizer(input);
		while (tokenizer.hasMoreTokens()) {
			Pattern datePatt = Pattern.compile("(.*)=(.*)");
			Matcher m = datePatt.matcher(tokenizer.nextToken());
			if (m.matches()) {
			  String key2 = m.group(1);
			  String value = m.group(2);
			  System.out.println("key=" + key2 + ", value=" + value);
			}
			//LOG.info("input values:" + tokenizer.nextToken());
		}
	}
}
