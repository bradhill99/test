import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Export HFile of a specified HBase table.  
 * Use {@link com.trendmicro.spn.ops.hbase.HFileImporter} to import.
 * 
 */

public class HFileGenerationJob extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(HFileGenerationJob.class);
    private static Pattern datPatt = Pattern.compile("(.*)=(.*)");
    final static String NAME = "HFileGenerationJob";

    /**
     * Mapper.
     */
    static class Exporter extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        /**
         * @param row
         *            The current table row key.
         * @param value
         *            The columns.
         * @param context
         *            The current context.
         * @throws IOException
         *             When something is broken with the data.
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
         *      org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException {
            LOG.info("key=" + key + "," + "value=" + value);
         
            try {
            String[] nameValueArray = value.toString().split(",");
            for (String nameValue : nameValueArray) {
                Matcher m = datPatt.matcher(nameValue);
                if (m.matches()) {
                    LOG.info("key=" + m.group(1) + "," + "value=" + m.group(2));
                    String keyStr = m.group(1);
                    String valueStr = m.group(2);
                    Put put = new Put(keyStr.getBytes());
                    put.add("grade".getBytes(), keyStr.getBytes(), valueStr.getBytes());
                    context.write(new ImmutableBytesWritable(keyStr.getBytes()), put);
                }
            }
            }
            catch(Exception ex) {
                LOG.error("ex:", ex);
            }
        }
    }

    /**
     * Sets up the actual job.
     * 
     * @param conf
     *            The current configuration.
     * @param args
     *            The command line parameters.
     * @return The newly created job.
     * @throws IOException
     *             When setting up the job fails.
     */
    public static Job createSubmittableJob(Configuration conf, String[] args) throws IOException {
        String tableName = args[0];
        Path inputDir = new Path(args[1]);
        Path outputDir = new Path(args[2]);
        Job job = new Job(conf, NAME + "_" + tableName);
        job.setJobName(NAME + "_" + tableName);
        job.setJarByClass(Exporter.class);

        FileInputFormat.setInputPaths(job, inputDir);
        FileOutputFormat.setOutputPath(job, outputDir);
                      
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);      
   
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        
        job.setInputFormatClass(TextInputFormat.class);        
        
        job.setMapperClass(Exporter.class);
        job.setReducerClass(PutSortReducer.class);
        
        HTable table = new HTable(conf, tableName);
        HFileOutputFormat.configureIncrementalLoad(job, table);

        return job;
    }

    /*
     * @param errorMsg Error message. Can be null.
     */
    private static void usage(final String errorMsg) {
        if (errorMsg != null && errorMsg.length() > 0) {
            System.err.println("ERROR: " + errorMsg);
        }
        System.err
                .println("Usage: HFileGenerationJob [-D <property=value>]* <tablename> <inputdir> <outputdir> [<versions> "
                        + "[<starttime> [<endtime>]]]\n");
        System.err.println("  Note: -D properties will be applied to the conf used. ");
        System.err.println("  For example: ");
        System.err.println("   -D mapred.output.compress=true");
        System.err
                .println("   -D mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec");
        System.err.println("   -D mapred.output.compression.type=BLOCK");
        System.err.println("  Additionally, the following SCAN properties can be specified");
        System.err.println("  to control/limit what is exported..");
        System.err.println("   -D " + TableInputFormat.SCAN_COLUMN_FAMILY + "=<familyName>");
    }

    @Override
    public int run(String[] args) {
        try {
            Configuration conf = HBaseConfiguration.create();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (otherArgs.length < 2) {
                usage(null);
                return -1;
            }

            // setup and submit the job
            Job job = createSubmittableJob(conf, otherArgs);
            return job.waitForCompletion(true) ? 0 : 1;
        } catch (Exception ex) {
            LOG.error("Reading HFile failed.", ex);
            return 1;
        }
    }

    /**
     * Pls see {@link #usage(String)}.
     * 
     * @param args The command line parameters.
     * @throws Exception When running the job fails.
     */
    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new HFileGenerationJob(), args);
        System.exit(ret);
    }
}
