package com.trendmicro.spn.solr.mapreduce;

import java.io.IOException;

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
    public static final class MyReducer extends Reducer<LongWritable, Text, NullWritable, SolrInputDocument>
    {
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException
        {
            SolrInputDocument doc = new SolrInputDocument();
            doc.setField("id", key.get());
            for (Text val : values) {
                doc.setField("content", val.toString());
            }
            context.write(NullWritable.get(), doc);
        }
    }

    @Override
    public int run(String[] args) throws Exception
    {
        if (args.length != 3) {
            System.err.format("Usage: %s <in> <out> <core_name>\n", this.getClass().getName());
            return 2;
        }

        Job job = new Job(getConf(), "SolrOutputFormatTest");
        job.setJarByClass(this.getClass());

        job.setMapperClass(Mapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(MyReducer.class);
        job.setOutputFormatClass(SolrOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(SolrInputDocument.class);
        job.setNumReduceTasks(2);

        FileInputFormat.addInputPaths(job, args[0]);
        SolrOutputFormat.setOutputPath(job, new Path(args[1]));
        SolrOutputFormat.setSolrCoreName(job, args[2]);

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception
    {
        System.exit(ToolRunner.run(new SolrOutputFormatTest(), args));
    }
}
