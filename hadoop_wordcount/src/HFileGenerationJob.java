import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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
    private static Pattern dataPattern = Pattern.compile("(.*)=(.*)");
    private static String FAMILY_COMMAND_LINE = "family.value";
    final static String NAME = "HFileGenerationJob";

    /**
     * Mapper.
     */
    static class Exporter extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        private static String family = "";
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            family = context.getConfiguration().get(FAMILY_COMMAND_LINE);
            LOG.info("passed family is :" + family);
        }
        
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException {
            java.util.Map<String, String> nameMap = new HashMap<String, String>(); // field name convert map
            
            // parse input string into map
            String[] nameValueArray = value.toString().split(",");
            for (String nameValue : nameValueArray) {
                Matcher m = dataPattern.matcher(nameValue);
                if (m.matches()) {
                    nameMap.put(m.group(1), m.group(2));
                }
            }
            
            try {
                // extract row key value: EQP_ID and CH_ID
                String rowKey = nameMap.remove("EQP_ID")+nameMap.remove("CH_ID")+nameMap.remove("EVENT_TIME_START");
                Put put = new Put(rowKey.getBytes());
                
                Iterator iter = nameMap.entrySet().iterator();
                while (iter.hasNext()) {
                    java.util.Map.Entry entry = (java.util.Map.Entry) iter.next();
                    String keyStr = (String)entry.getKey();
                    String valStr = (String)entry.getValue();
                    put.add(family.getBytes(), keyStr.getBytes(), valStr.getBytes());
                }
                context.write(new ImmutableBytesWritable(rowKey.getBytes()), put);
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
    public static Job createSubmittableJob(Configuration conf, CommandLine line) throws IOException {
        String tableName = line.getOptionValue("t");
        Path inputDir = new Path(line.getOptionValue("i"));
        Path outputDir = new Path(line.getOptionValue("o"));
        conf.set(FAMILY_COMMAND_LINE, line.getOptionValue("f"));
        Job job = new Job(conf, NAME + "_" + tableName);
        job.setJobName(NAME + "_" + tableName);
        job.setJarByClass(Exporter.class);

        // filter input file
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] listStatus = fs.globStatus(new Path(inputDir + "/part-r*"));
        List<Path> inputhPaths = new ArrayList<Path>();
        for (FileStatus fstat : listStatus) {
            inputhPaths.add(fstat.getPath());
        }

        FileInputFormat.setInputPaths(job,
                (Path[]) inputhPaths.toArray(new Path[inputhPaths.size()]));
        
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
    private static void usage(final String errorMsg) throws ParseException {
        if (errorMsg != null && errorMsg.length() > 0) {
            System.err.println("ERROR: " + errorMsg);
        }
        System.err.println("Usage: HFileGenerationJob -f <column family> -t <tablename> -i <inputdir> -o <outputdir>");
    }

    private static CommandLine parseCommaneLine(String[] otherArgs) throws ParseException {
        CommandLine line = null; 
        try {
            CommandLineParser parser = new BasicParser();
            Options options = new Options();
            options.addOption("f", null, true, "column family");
            options.addOption("t", null, true, "table name");
            options.addOption("i", null, true, "input path");
            options.addOption("o", null, true, "output path");
            
            line = parser.parse( options, otherArgs );
        }
        catch(ParseException ex) {
            System.err.println("ex:" +ex);
            throw ex;
        }
        
        return line;
    }
    
    @Override
    public int run(String[] args) {
        try {
            Configuration conf = HBaseConfiguration.create();
            //System.out.println("pass args=" + args);
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (otherArgs.length < 2) {
                usage(null);
                return -1;
            }

            CommandLine line = parseCommaneLine(otherArgs);
            // setup and submit the job
            Job job = createSubmittableJob(conf, line);
            return job.waitForCompletion(true) ? 0 : 1;
        } catch (Exception ex) {
            LOG.error("ex:", ex);
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
