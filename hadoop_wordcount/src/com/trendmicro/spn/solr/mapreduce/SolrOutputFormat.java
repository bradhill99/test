package com.trendmicro.spn.solr.mapreduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Timer;
import java.util.zip.ZipOutputStream;

public class SolrOutputFormat extends FileOutputFormat<NullWritable, SolrInputDocument>
{
    private static final String CONF_CORE_NAME = "mapreduce.solroutputformat.corename";
    private static final String CONF_COMPRESS_OUTPUT = "mapreduce.solroutputformat.compress";

    @Override
    public RecordWriter<NullWritable, SolrInputDocument> getRecordWriter(TaskAttemptContext context)
        throws IOException, InterruptedException
    {
        Configuration config = context.getConfiguration();
        FileOutputCommitter committer = (FileOutputCommitter)getOutputCommitter(context);

        Path hdfsOutputDir = committer.getWorkPath();
        if (!hdfsOutputDir.isAbsolute()) {
            FileSystem fs = hdfsOutputDir.getFileSystem(config);
            hdfsOutputDir = hdfsOutputDir.makeQualified(fs);
        }

        Path localWorkDir = FileSystem.getLocal(config).getWorkingDirectory();
        if (!localWorkDir.isAbsolute()) {
            FileSystem fs = localWorkDir.getFileSystem(config);
            localWorkDir = localWorkDir.makeQualified(fs);
        }

        String coreName = config.get(CONF_CORE_NAME);
        String zipName = String.format("%s.zip", coreName);
        Path coreZipFile = null;
        for (Path file : DistributedCache.getLocalCacheFiles(config)) {
            if (file.getName().endsWith(zipName)) {
                coreZipFile = file;
                break;
            }
        }
        if (coreZipFile == null) {
            throw new IOException(String.format("%s not found", zipName));
        }

        String shardID = String.format("part-%05d", context.getTaskAttemptID().getTaskID().getId());
        File instanceDir = new File(localWorkDir.toUri().getPath(), shardID);
        if (!instanceDir.mkdirs()) {
            throw new IOException(String.format("Create dir '%s' failed", instanceDir.toString()));
        }
        FileUtil.unZip(new File(coreZipFile.toUri().getPath()), instanceDir);

        try {
            return new SolrRecordWriter(hdfsOutputDir, shardID, coreName, instanceDir);
        }
        catch (SAXException e) {
            throw new IOException(e.getMessage(), e);
        }
        catch (ParserConfigurationException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public void checkOutputSpecs(JobContext job) throws FileAlreadyExistsException, IOException
    {
        super.checkOutputSpecs(job);

        if (job.getConfiguration().get(CONF_CORE_NAME) == null) {
            throw new IOException(String.format("Config '%s' not found", CONF_CORE_NAME));
        }
    }

    public static void setSolrCoreName(Job job, String coreName)
    {
        job.getConfiguration().set(CONF_CORE_NAME, coreName);
    }

    class SolrRecordWriter extends RecordWriter<NullWritable, SolrInputDocument>
    {
        private Path outputDir;
        private String shardID;
        private File instanceDir;

        private CoreContainer coreContainer;
        private SolrServer solrServer;

        public SolrRecordWriter(Path outputDir, String shardID, String coreName, File instanceDir)
            throws IOException, SAXException, ParserConfigurationException
        {
            this.outputDir = outputDir;
            this.shardID = shardID;
            this.instanceDir = instanceDir;

            coreContainer = new CoreContainer("./");

            CoreDescriptor descriptor = new CoreDescriptor(coreContainer, coreName, instanceDir.getAbsolutePath());
            coreContainer.register(coreContainer.create(descriptor), false);

            solrServer = new EmbeddedSolrServer(coreContainer, coreName);
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException
        {
            Timer timer = new Timer("heartbeat", true);

            try {
                try {
                    solrServer.commit(true, true);
                    solrServer.optimize(true, true);
                }
                finally {
                    coreContainer.shutdown();
                }

                FileSystem fs = outputDir.getFileSystem(context.getConfiguration());
                if (!context.getConfiguration().getBoolean(CONF_COMPRESS_OUTPUT, false)) {
                    fs.copyFromLocalFile(new Path(instanceDir.getAbsolutePath()), new Path(outputDir, shardID));
                }
                else {
                    FSDataOutputStream dos = fs.create(new Path(outputDir, shardID + ".zip"), true);
                    ZipOutputStream zos = new ZipOutputStream(dos);
                    Queue<File> q = new LinkedList<File>();
                    q.add(instanceDir);
                    while (q.size() > 0) {
                        File f = q.remove();
                        if (f.isDirectory()) {
                            for (File c : f.listFiles()) {
                                q.add(c);
                            }
                        }
                        else {
                            // TODO: compress output
                            System.out.format("Adding: (%s, %s)\n", f.getName(), f.getCanonicalPath());
                        }
                    }
                    zos.close();
                    dos.close();
                }
            }
            catch (SolrServerException e) {
                throw new IOException(e.getMessage(), e);
            }
            finally {
                timer.cancel();
            }
        }

        @Override
        public void write(NullWritable key, SolrInputDocument value) throws IOException, InterruptedException
        {
            try {
                solrServer.add(value);
            }
            catch (SolrServerException e) {
                throw new IOException(e.getMessage(), e);
            }
        }
    }
}
