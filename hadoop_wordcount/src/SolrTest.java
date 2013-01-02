import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

public class SolrTest {
    public static void main(String[] args) throws Exception {
		String url = "http://brad01.spn.tw.trendnet.org:8983/solr";
		SolrServer server = new HttpSolrServer( url );
		    
		SolrInputDocument doc1 = new SolrInputDocument();		
	    doc1.addField( "id", "id1", 1.0f );
	    doc1.addField( "name", "doc1", 1.0f );
	    doc1.addField( "price", 10 );
	    
	    UpdateResponse res = server.add(doc1);
	    System.out.println("res=" + res.toString());
	    server.commit();
    }
}
