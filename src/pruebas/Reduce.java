package pruebas;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Logger;
import java.util.zip.ZipOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, Text, Text, Text> {
    protected ZipFileWriter zipFileWriter = null;
    private HashMap<String, ZipFileWriter> zips = new HashMap<String, ZipFileWriter>();
    Logger log = Logger.getLogger("log_file");
    
	@Override
    @SuppressWarnings("unchecked")
    protected void setup(Reducer.Context context)
            throws IOException, InterruptedException {
	}
	 @Override
	 public void reduce(Text key, Iterable<Text> values, Context context) {
		 FileSystem fs;
		 try {
			 String jobKey = key.toString();
			 zipFileWriter = zips.get(jobKey);
			 if (zipFileWriter == null) {
				 log.info("CREA " + jobKey);
				zipFileWriter = new ZipFileWriter(jobKey);
				zipFileWriter.setup();
				zipFileWriter.openZipForWriting();
				zips.put(jobKey, zipFileWriter);
			 } else {
				 log.info("GET " + jobKey);
			 }
				
			 fs = FileSystem.get(new Configuration());
			 for (Text t: values) {
				 Path path = new Path(t.toString());
				 if (fs.exists(path)) {
					 FSDataInputStream in = fs.open(path);
					 zipFileWriter.addTextFile(t.toString(), in.readLine());
					 fs.deleteOnExit(path);
				 }
			 }
		 } catch (IOException e) {
		 }
	 }
	 
	 @Override
	    @SuppressWarnings("unchecked")
	    protected void cleanup(Reducer.Context context)
	            throws IOException, InterruptedException {
		Iterator<String> it =  zips.keySet().iterator();
		while (it.hasNext()) {
			zips.get(it.next()).closeZip();
		}
	 }
}
