import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class SequenceFileRead {
    /*
     * This is the old version to write protobuf byte array to file, but seems like this will produce some garbage bytes, and file content is
     * not protobuf (can't be decoded by protoc command by using protoc --decode_raw < file
    public class SequenceFileRead {  
    private static void writeFileAsByte(byte[] data, String filepath) throws IOException {
        File file = new File(filepath);
        
        DataOutputStream dis = new DataOutputStream((new FileOutputStream(file)));
        dis.write(data);
        dis.close();
    }*/
    
    private static void writeFileAsByte(Configuration conf, Path localDest, BytesWritable val) throws IOException {
        FileSystem rawLocalFs = ((LocalFileSystem) FileSystem.getLocal(conf)).getRaw();
        OutputStream out = rawLocalFs.create(localDest);
        System.out.println("value length is=" + val.getLength());
        out.write(val.getBytes(), 0, val.getLength());
        out.close();
    }
    
    private static void dumpSequenceFileMetaData(SequenceFile.Reader reader) {
//        TreeMap<Text, Text> metaData = reader.getMetadata().getMetadata();
//        for (Map.Entry<Text, Text> entry : metaData.entrySet()) {
//            System.out.println("Key: " + entry.getKey() + ". Value: " + entry.getValue());
//       }
        System.out.println(reader.getMetadata().toString());
    }
    
  //java SequenceFileRead <sequence_file> [start key] [end key]
    public static void main(String[] args) throws IOException {
        String uri = args[0];
        Configuration conf = new Configuration();
        //FileSystem fs = FileSystem.get(URI.create(uri), conf);
        FileSystem fs = FileSystem.getLocal(conf);
        Path sequencefilePath = new Path(uri);
        
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, sequencefilePath, conf);
        dumpSequenceFileMetaData(reader);
        
        Text key = new Text();
        BytesWritable val = new BytesWritable();
        long cnt = 1;
        while (reader.next(key, val)) {
            System.out.print(key + "\t");
            //System.out.write(val.getBytes());
            //System.out.println("");
            //writeFileAsByte(val.getBytes(), key.toString());
            writeFileAsByte(conf, new Path(String.valueOf(cnt++)), val);           
        }
        
        reader.close();
    }
}