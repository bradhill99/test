import java.util.Calendar;
import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class Test {
    public static final String DATE_FORMAT_NOW = "yyyyMMddHHmmssSSS";

    public static String now() {
    Calendar cal = Calendar.getInstance();
    SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_NOW);
    return sdf.format(cal.getTime());
    }
    
	public static void main(String[] args) throws Exception {		
	    System.out.println("byebye");
	    System.out.println(now());
	    
	    
//diff test
	    SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_NOW);
        Date d1 = null;
        Date d2 = null;
        try {
                            
            d2 = sdf.parse("20130814161831012");
            d1 = sdf.parse("20130814161830965");
        } catch (ParseException ex) {
            System.out.println("ex:" + ex);
        }    
        
        long diff = d2.getTime() - d1.getTime();
        int diffSeconds = (int)diff / 1000;
        
        System.out.println("diff=" + diffSeconds);
	}
}
