import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class CommandLineTest {
    
    public static void main(String[] args) throws Exception {
        Options options = new Options();
        // add threadCount and queueSize option
        Option threadCount = OptionBuilder.withArgName("t").withLongOpt("thread-count").hasArg().withDescription("solr indexing concurrent thread number").create("t");
        Option queueSize = OptionBuilder.withArgName("q").withLongOpt("queue-size").hasArg().withDescription("solr indexing queue number").create("q");
        Option userName = OptionBuilder.withArgName("u").withLongOpt("uasername").isRequired().hasArg().withDescription("collection user name").create("u");
        Option passWord = OptionBuilder.withArgName("p").withLongOpt("password").isRequired().hasArg().withDescription("collection password").create("p");
        Option solrServer = OptionBuilder.withArgName("s").withLongOpt("solr-server").isRequired().hasArg().withDescription("solr server(HA Proxy)").create("s");
        Option collection = OptionBuilder.withArgName("c").withLongOpt("collection").isRequired().hasArg().withDescription("collection name").create("c");
        
        options.addOption(userName);
        options.addOption(passWord);
        options.addOption(threadCount);
        options.addOption(queueSize);
        options.addOption(solrServer);
        options.addOption(collection);
        
        
                
//        options.addOption("t", false, "solr indexing concurrent thread number");
//        options.addOption("q", false, "solr indexing queue number");
//        options.addOption("user", true, "username");
//        options.addOption("password", false, "password");
//        options.addOption("s", true, "HA proxy server");
//        options.addOption("c", true, "collection name");
        
        try {
            CommandLineParser parser = new BasicParser();
            CommandLine cmd = parser.parse( options, args);
            if(cmd.hasOption("t")) {
                System.out.println(cmd.getOptionValue("t"));
            }
            if(cmd.hasOption("q")) {
                System.out.println(cmd.getOptionValue("q"));
            }
            if(cmd.hasOption("u")) {
                System.out.println(cmd.getOptionValue("u"));
            }
            if(cmd.hasOption("p")) {
                System.out.println(cmd.getOptionValue("p"));
            }
        }
        catch(ParseException exp) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("CommandLineTest", options, true);
            
            System.out.println( "Unexpected exception:" + exp.getMessage() );
        }
    }
}
