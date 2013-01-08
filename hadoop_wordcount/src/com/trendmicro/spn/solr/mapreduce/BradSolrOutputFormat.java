package com.trendmicro.spn.solr.mapreduce;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
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
	
	@Override
	public RecordWriter<NullWritable, SolrInputDocument> getRecordWriter(
			TaskAttemptContext job) throws IOException, InterruptedException {
		LOG.info("invoke getRecordWriter");
		return new SolrRecordWriter();
	}
	
    class SolrRecordWriter extends RecordWriter<NullWritable, SolrInputDocument>
    {
    	private SolrServer server = null;

    	private String getUrl() {
    		String urls[] = {"http://10.31.66.58:8983/solr",
    						"http://10.31.66.59:8983/solr",
    						"http://10.31.66.71:8983/solr",
    						"http://10.31.66.72:8983/solr"};
    		
    		Random generator = new Random();    		
    		int r = generator.nextInt(urls.length);
    		LOG.info("random number=" + r + ", return server:" + urls[r]);
    		return urls[r];
    	}
    	
        public SolrRecordWriter() {
        	this.server = new HttpSolrServer(this.getUrl());
        }

        @Override
        public void write(NullWritable key, SolrInputDocument value) throws IOException, InterruptedException
        {
            try {
            	UpdateResponse res = server.add(value);
        	    LOG.info("res=" + res.toString());
            }
            catch (SolrServerException e) {
                throw new IOException(e.getMessage(), e);
            }
        }

		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			try {
				this.server.commit();
			} catch (SolrServerException ex) {
				// TODO Auto-generated catch block
				LOG.error("ex:", ex);
			}
		}
    }
}
