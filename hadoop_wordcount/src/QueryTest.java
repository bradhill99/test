import java.net.URI;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;

public class QueryTest {
    public static void main(String[] args) throws Exception {
    	// String url = "http://10.31.66.58:8983/solr/census";
    	String url = "http://spn-s-solrcloud-reverse-1v.sjdc:8983/solr/domain_ip_census-domain_full_2013_06_01_18";
    	URI uri = new URI("http://spn-s-solrcloud-reverse-1v.sjdc:8983/solr/domain_ip_census-domain_full_2013_06_01_18");
    	DefaultHttpClient httpclient = new DefaultHttpClient();

    	httpclient.getCredentialsProvider().setCredentials(
				new AuthScope(uri.getHost(), uri.getPort(),AuthScope.ANY_SCHEME),
				new UsernamePasswordCredentials("solr_cloud", "$pn0ps123"));
    	
		SolrServer server = new HttpSolrServer(url, httpclient);
		SolrQuery solrQuery = new  SolrQuery().
									setQuery("*:*").
									setFields("domain", "first_seen").
									setStart(0).
									setRows(1);
		QueryResponse rsp = server.query(solrQuery);
		System.out.println("result:" + rsp.toString());
    }
}
