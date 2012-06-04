package pruebas;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, Text, Text, Text> {
    protected ZipFileWriter zipFileWriter = new ZipFileWriter();
    Logger log = Logger.getLogger("log_file");
    
	@Override
    @SuppressWarnings("unchecked")
    protected void setup(Reducer.Context context)
            throws IOException, InterruptedException {
		zipFileWriter.setup();
		zipFileWriter.openZipForWriting();
	}
	 @Override
	 public void reduce(Text key, Iterable<Text> values, Context context) {
		 FileSystem fs;
		 try {
			 fs = FileSystem.get(new Configuration());

			 for (Text t: values) {
				 Path path = new Path(t.toString());
				 if (fs.exists(path)) {
					 FSDataInputStream in = fs.open(path);
					 zipFileWriter.addTextFile(t.toString(), in.readLine());
				 }
			 }
		 } catch (IOException e) {
		 }
	 }
	 
	 @Override
	    @SuppressWarnings("unchecked")
	    protected void cleanup(Reducer.Context context)
	            throws IOException, InterruptedException {
		 zipFileWriter.closeZip();
		}
}
