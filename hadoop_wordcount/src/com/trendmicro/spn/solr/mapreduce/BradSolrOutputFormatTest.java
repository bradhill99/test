package com.trendmicro.spn.solr.mapreduce;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.solr.common.SolrInputDocument;

public class BradSolrOutputFormatTest extends Configured implements Tool
{
	private static final Log LOG = LogFactory.getLog(BradSolrOutputFormatTest.class);
	
    public static final class MyReducer extends Reducer<LongWritable, Text, NullWritable, SolrInputDocument>
    {
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException
        {
        	LOG.info("input key:" + key.toString());
        	String valueStr = "";
        	for(Text val : values) {
        		LOG.info("input values:" + val.toString());
        		valueStr = val.toString();
        	}

        	SolrInputDocument doc = new SolrInputDocument();        	
        	StringTokenizer tokenizer = new StringTokenizer(valueStr);
			while (tokenizer.hasMoreTokens()) {
				Pattern datePatt = Pattern.compile("(.*)=(.*)");
				Matcher m = datePatt.matcher(tokenizer.nextToken());
				if (m.matches()) {
				  LOG.info("key=" + m.group(1) + ", value=" + m.group(2));		            
		          doc.addField(m.group(1), URLDecoder.decode(m.group(2), "UTF-8"));
				}
			}
			
			if (!doc.isEmpty()) {
				context.write(NullWritable.get(), doc);
			}
        }
    }

    @Override
    public int run(String[] args) throws Exception
    {
        if (args.length != 3) {
            System.err.format("Usage: %s <in> <out> <solrip:port[,...]>\n", this.getClass().getName());
            return 2;
        }

        Job job = new Job(getConf(), "BradSolrOutputFormatTest");
        job.setJarByClass(this.getClass());

        job.setMapperClass(Mapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(MyReducer.class);
        job.setOutputFormatClass(BradSolrOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(BradSolrOutputFormat.class);
        job.setNumReduceTasks(10);

        FileInputFormat.addInputPaths(job, args[0]);
        BradSolrOutputFormat.setOutputPath(job, new Path(args[1]));
        BradSolrOutputFormat.setSolrServerList(job, args[2]);

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception
    {
        System.exit(ToolRunner.run(new BradSolrOutputFormatTest(), args));
    }
}
