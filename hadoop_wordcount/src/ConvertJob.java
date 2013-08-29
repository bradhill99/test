import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class ConvertJob extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(ConvertJob.class);
    
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private Set<String> fields = new HashSet<String>(); // necessary fields set
        private java.util.Map<String, String> nameMap = new HashMap<String, String>(); // field name convert map
        private static Pattern datePatt = Pattern.compile("(.*)=(.*)");
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // init set
            String[] fieldArray = {"eqp_id", "unit_name", "event_name", "event_create_dt", "chmbr_recipe_id", "lot_id", "slot_id", 
                                "wafer_id", "load_port_id", "phys_recipe_id", "step_seq", "eqp_state"};

            for (String field : fieldArray) {
                fields.add(field);
            }
 
            // init name mapping
            nameMap.put("unit_name", "ch_id");
            nameMap.put("event_create_dt", "event_time_start");
            nameMap.put("chmbr_recipe_id", "recipe_id");
            nameMap.put("phys_recipe_id", "phy_rcp_id");
            nameMap.put("eqp_state", "state_start");
        }
         
         private String doNameConvert(String name) {
             String matchedName = nameMap.get(name);
             
             if (matchedName != null) {
                 return matchedName;
             }
             else {
                 return name;
             }
         }
         
         public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {         
           LOG.info("key=" + key + "," + "value=" + value);

           String eqp_id = ""; // reducer key 1
           String unit_name = ""; // reducer key 2
           
           StringBuffer outputString = new StringBuffer();
           String[] nameValueArray = value.toString().split(",");
           for (String nameValue : nameValueArray) {
               Matcher m = datePatt.matcher(nameValue);
               if (m.matches()) {
                   if (!fields.contains(m.group(1))) {
                       continue;
                   }
                   
                   // filter out empty eqp_state
                   if (m.group(1).equalsIgnoreCase("eqp_state") && m.group(2).isEmpty()) {
                       continue;
                   }
                   
                   // find value for reducer key
                   if (m.group(1).equalsIgnoreCase("eqp_id")) {
                       eqp_id = m.group(2);
                   }                   
                   if (m.group(1).equalsIgnoreCase("unit_name")) {
                       unit_name = m.group(2);
                   }
                   
                   // convert name
                   String name = doNameConvert(m.group(1));
                   
                   // write out the final value
                   outputString.append(name + "=" + m.group(2) + ",");
               }
           }
           
           context.write(new Text(eqp_id + unit_name), new Text(outputString.toString()));           
         }
    }
    
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private static Pattern eventTimePatt = Pattern.compile(".*event_time_start=(\\d+),.*");
        
        /*
         * sort by event_time_start
         */
        class EventTimeComparator implements Comparator<String> {
            @Override
            public int compare(String o1, String o2) {
                Matcher m1 = eventTimePatt.matcher(o1);
                Matcher m2 = eventTimePatt.matcher(o2);
                if (!m1.matches() || !m2.matches()) {                    
                    return -1;
                }
                
                LOG.info("o1 value=" + m1.group(1) + "," + "o2 value=" + m2.group(1));
                
                return o1.compareTo(o2);
            }
        }
        
        private List<String> sortByEvent_create_dt(Iterable<Text> values) {
            List<String> sortedList = new LinkedList<String>();
            for (Text value:values) {
                sortedList.add(value.toString());
            }
            
            Collections.sort(sortedList, new EventTimeComparator());
            return sortedList;
        }
                
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 1. remove line with empty eqp_state   -- done in mapper
            // 2. sort all values by event_create_dt
            // 3. pick two consecutive lines and do the calculation
            
            List<String> sortedList = sortByEvent_create_dt(values);
            
            for (String value:sortedList) {
                context.write(key, new Text(value));
            }
            //doCalc(sortedList);
            
//            for (Text value:values) {
//               context.write(key, value);
//            }
        }
    }
    
    public int run(String [] args) throws Exception {
         Job job = new Job(getConf());
         job.setJarByClass(ConvertJob.class);
         job.setJobName("ConvertJob");
    
         job.setOutputKeyClass(Text.class);
         job.setOutputValueClass(Text.class);
        
         job.setMapperClass(Map.class);
         job.setReducerClass(Reduce.class);
    
         job.setInputFormatClass(TextInputFormat.class);
         job.setOutputFormatClass(TextOutputFormat.class);
    
         FileInputFormat.setInputPaths(job, new Path(args[0]));
         FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
         boolean success = job.waitForCompletion(true);
         return success ? 0 : 1;
    }
    
   public static void main(String[] args) throws Exception {
         int ret = ToolRunner.run(new ConvertJob(), args);
         System.exit(ret);
   }
}
