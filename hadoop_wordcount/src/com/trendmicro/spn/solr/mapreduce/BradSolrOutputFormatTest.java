package com.trendmicro.spn.solr.mapreduce;

import java.io.IOException;

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
        	for(Text val : values) {
        		LOG.info("input values:" + val.toString());
        	}
        	
//        	doc1.addField( "id", "id1", 1.0f );
//    	    doc1.addField( "name", "doc1", 1.0f );
//    	    doc1.addField( "price", 10 );
//    	    
            SolrInputDocument doc = new SolrInputDocument();
            doc.setField("id", "id1");
            doc.setField( "name", "doc_brad", 1.0f );
//            for (Text val : values) {
//                doc.setField("content", val.toString());
//            }
            context.write(NullWritable.get(), doc);
        }
    }

    @Override
    public int run(String[] args) throws Exception
    {
        if (args.length != 2) {
            System.err.format("Usage: %s <in> <out>\n", this.getClass().getName());
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
        job.setNumReduceTasks(2);

        FileInputFormat.addInputPaths(job, args[0]);
        BradSolrOutputFormat.setOutputPath(job, new Path(args[1]));

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception
    {
        System.exit(ToolRunner.run(new BradSolrOutputFormatTest(), args));
    }
}
