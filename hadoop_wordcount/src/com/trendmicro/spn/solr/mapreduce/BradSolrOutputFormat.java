package com.trendmicro.spn.solr.mapreduce;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

public class BradSolrOutputFormat extends FileOutputFormat<NullWritable, SolrInputDocument>
{
	private static final Log LOG = LogFactory.getLog(BradSolrOutputFormat.class);
	private static final String SOLR_SERVER_LIST = "urls";
	
	@Override
	public RecordWriter<NullWritable, SolrInputDocument> getRecordWriter(
			TaskAttemptContext context) throws IOException, InterruptedException {
		
		Configuration config = context.getConfiguration();		
		String urls[] = config.get(SOLR_SERVER_LIST).split(",");
		LOG.info("urls=" + urls.toString());
		
		return new SolrRecordWriter(urls);
	}
	
	public static void setSolrServerList(Job job, String urls) {
        job.getConfiguration().set(SOLR_SERVER_LIST, urls);
    }

    class SolrRecordWriter extends RecordWriter<NullWritable, SolrInputDocument> {
    	private SolrServer server = null; 

    	private String getUrl(String[] urls) {
    		Random generator = new Random();    		
    		int r = generator.nextInt(urls.length);
    		LOG.info("random number=" + r + ", return server:" + urls[r]);
    		// return this format http://10.31.66.58:8983/solr
    		return "http://" + urls[r] + "/solr";
    	}
    	
        public SolrRecordWriter(String[] urls) {
        	this.server = new HttpSolrServer(this.getUrl(urls));
        }

        @Override
        public void write(NullWritable key, SolrInputDocument value) throws IOException, InterruptedException {
            try {
            	UpdateResponse res = server.add(value);
        	    LOG.info("res=" + res.toString());
            }
            catch (SolrServerException e) {
                throw new IOException(e.getMessage(), e);
            }
        }

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			try {
				this.server.commit();
			} catch (SolrServerException ex) {
				LOG.error("ex:", ex);
			}
		}
    }
}
