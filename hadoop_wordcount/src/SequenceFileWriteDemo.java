import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class SequenceFileWriteDemo {  
    private static byte[] readFileAsByte(String filePath) throws IOException {
        File file = new File(filePath);
        byte[] fileData = new byte[(int)file.length()];
        DataInputStream dis = new DataInputStream((new FileInputStream(file)));
        dis.readFully(fileData);
        dis.close();
        return fileData;
    }
    
  public static void main(String[] args) throws IOException {
    String uri = args[0];
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(uri), conf);
    Path path = new Path(uri);

    Text key = new Text();

    SequenceFile.Writer writer = null;
    try {
      writer = SequenceFile.createWriter(fs, conf, path,
          key.getClass(), BytesWritable.class);
      
      for (int i = 1; i < args.length; i++) {
        key.set(args[i]);
        BytesWritable value = new BytesWritable(readFileAsByte(args[i]));        
        writer.append(key, value);
      }
    } finally {
      IOUtils.closeStream(writer);
    }
  }
}