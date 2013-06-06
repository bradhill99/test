import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.text.DateFormat;
import java.util.Calendar;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.AuthScheme;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.AuthState;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.protocol.ClientContext;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.ExecutionContext;
import org.apache.http.protocol.HttpContext;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;

/**
 * Read sample
 * @author brad_liu
 *
 */
public class StressTest {
	static class PreemptiveAuthInterceptor implements HttpRequestInterceptor {

	    public void process(final HttpRequest request, final HttpContext context) throws HttpException, IOException {
	        AuthState authState = (AuthState) context.getAttribute(ClientContext.TARGET_AUTH_STATE);

	        // If no auth scheme avaialble yet, try to initialize it
	        // preemptively
	        if (authState.getAuthScheme() == null) {
	            CredentialsProvider credsProvider = (CredentialsProvider) context.getAttribute(ClientContext.CREDS_PROVIDER);
	            HttpHost targetHost = (HttpHost) context.getAttribute(ExecutionContext.HTTP_TARGET_HOST);
	            Credentials creds = credsProvider.getCredentials(new AuthScope(targetHost.getHostName(), targetHost.getPort()));
	            if (creds == null)
	                throw new HttpException("No credentials for preemptive authentication");
	            authState.setAuthScheme(new BasicScheme());
	            authState.setCredentials(creds);
	        }
	    }
	}
	
	private String getUrl() {
		// POC
		//String[] urls = {"10.31.66.58:8983","10.31.66.59:8983","10.31.66.71:8983","10.31.66.72:8983"};
		
		// office lab
		String[] urls = {"10.31.66.71:8080"};
		// String[] urls = {"10.31.66.59:9900"};
		
		Random generator = new Random();    		
		int r = generator.nextInt(urls.length);
		// return this format http://10.31.66.58:8983/solr
		return "http://" + urls[r] + "/solr/ad_hoc_test1";
	}
	
	public void run(String[] args) throws InterruptedException, URISyntaxException {		
    	URI uri = new URI(this.getUrl());
    	DefaultHttpClient httpClient = new DefaultHttpClient(new ThreadSafeClientConnManager());

    	httpClient.getCredentialsProvider().setCredentials(
				new AuthScope(uri.getHost(), uri.getPort()),
				new UsernamePasswordCredentials("solrcloud_trial", "pass"));
    	
    	httpClient.addRequestInterceptor(new PreemptiveAuthInterceptor(), 0);
    	
//	      ModifiableSolrParams params = new ModifiableSolrParams();
//	      params.set(HttpClientUtil.PROP_MAX_CONNECTIONS, 128);
//	      params.set(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, 32);
//	      params.set(HttpClientUtil.PROP_BASIC_AUTH_USER, "solrcloud_trial");
//	      params.set(HttpClientUtil.PROP_BASIC_AUTH_PASS, "pass");
//	      HttpClient httpClient =  HttpClientUtil.createClient(params);
	      
	      
        // init solr server
        SolrServer solrServer = new ConcurrentUpdateSolrServer(this.getUrl(), httpClient, 3000, 5);
		//SolrServer solrServer = new HttpSolrServer(this.getUrl());
       
        //File file = new File("./out.txt");
        File file = new File(args[0]);
        FileInputStream fis = null;
        BufferedInputStream bis = null;

        try {
          fis = new FileInputStream(file);

          // Here BufferedInputStream is added for fast reading.
          bis = new BufferedInputStream(fis);
          BufferedReader d = new BufferedReader(new InputStreamReader(bis));

          String valueStr;
          while ( (valueStr = d.readLine()) != null ) {
			//System.out.println(valueStr);
			StringTokenizer tokenizer = new StringTokenizer(valueStr);
			SolrInputDocument doc = new SolrInputDocument();        

			while (tokenizer.hasMoreTokens()) {
				Pattern datePatt = Pattern.compile("(.*)=(.*)");
				Matcher m = datePatt.matcher(tokenizer.nextToken());
				if (m.matches()) {
				  //System.out.println("key=" + m.group(1) + ", value=" + m.group(2));		            
		          doc.addField(m.group(1), URLDecoder.decode(m.group(2), "UTF-8"));
				}
			}
			
			//Thread.sleep(200);
        	//UpdateResponse res = solrServer.add(doc, 10000);
			UpdateResponse res = solrServer.add(doc);
    	    //System.out.println("res=" + res.toString());
    	    //solrServer.commit();
          }

          // dispose all the resources after using them.
          fis.close();
          bis.close();
          
          System.out.println("before commit");
          //Thread.sleep(10000);
          solrServer.commit();
          System.out.println("after commit");
        }
    	catch (FileNotFoundException e) {
    		e.printStackTrace();
    	} catch (IOException e) {
	        e.printStackTrace();
    	}
    	catch (SolrServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}        
	}
	
	public static void main(String[] args) throws Exception {
		StressTest test = new StressTest();
		test.run(args);
		System.out.println("bye~~");
	    Calendar cal = Calendar.getInstance();
	    DateFormat df = DateFormat.getDateTimeInstance(DateFormat.FULL,
	        DateFormat.MEDIUM);

	    System.out.println(df.format(cal.getTime()));
	}
}
