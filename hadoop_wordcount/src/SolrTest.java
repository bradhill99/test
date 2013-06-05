import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

public class SolrTest {
    public static void main(String[] args) throws Exception {
		//String url = "http://brad01.spn.tw.trendnet.org:8983/solr/collection2";
    	String url = "http://10.1.144.250:8983/solr/";
		SolrServer server = new HttpSolrServer( url );
		    
		SolrInputDocument doc1 = new SolrInputDocument();		
//	    doc1.addField( "domain", "tw.com.brad" );
//	    doc1.addField( "first_seen", 1358553600L );
//	    doc1.addField( "last_seen", 1358553600L );
//	    doc1.addField( "sip", "" );
		
		doc1.addField( "id", "frank_chao" );
		doc1.addField( "content", "frank_chao@trendmicro.com.tw" );
	    UpdateResponse res = server.add(doc1);
	    System.out.println("res=" + res.toString());
	    server.commit();
    }
}
