import org.apache.solr.common.util.Base64;

import com.trendmicro.spn.solr.mapreduce.SolrOutputFormat;


public class TestBase64 {

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        String token = "ad_hoc1:test123";
        String encode = Base64.byteArrayToBase64(token.getBytes(), 0, token.length());
        System.out.println("encode=" + encode);

        String decode = new String(Base64.base64ToByteArray(encode));
        System.out.println("decode=" + decode);
    }

}
