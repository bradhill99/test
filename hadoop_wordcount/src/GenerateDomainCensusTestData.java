import java.io.BufferedWriter;
import java.io.FileWriter;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

public class GenerateDomainCensusTestData {
	public static void main(String[] args) throws Exception {
		int shardId=4;
		int solrPort = 8983 + shardId-1;
		FileWriter fstream = new FileWriter("d:\\solrmeter\\domain_census_shard" + Integer.toString(shardId) + ".txt");
		BufferedWriter out = new BufferedWriter(fstream);
		
		//280774
		for(int cnt = 0; cnt < 29; ++cnt) {
		   	String url = "http://10.31.66.58:" + Integer.toString(solrPort) + "/solr/domain_census";
			SolrServer server = new HttpSolrServer( url );
			SolrQuery solrQuery = new  SolrQuery().
										setQuery("*:*").
										setFields("domain").
										setStart(cnt*10000).
										setRows(1000);
			solrQuery.set("shards", "shard" + Integer.toString(shardId));
			QueryResponse rsp = server.query(solrQuery);
			
			SolrDocumentList list = rsp.getResults();
			
	
			for (SolrDocument doc : list) {
				//System.out.println(doc.getFieldValue("domain"));
				out.write("domain:" + doc.getFieldValue("domain") + "\n");
			}
		}
		out.close();
	}
}
