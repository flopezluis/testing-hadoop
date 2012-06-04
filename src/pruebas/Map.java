package pruebas;

import java.io.IOException;
import java.io.OutputStream;
import java.util.UUID;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text, Text> {
	Logger log = Logger.getLogger("log_file");
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
		FileSystem fs = FileSystem.get(new Configuration());
		Path path = new Path("/data/" + UUID.randomUUID());
		OutputStream os = fs.create(path);
		
		os.write(value.getBytes());
		// write to os
		os.close();
		context.write(new Text(path.getName()), new Text(path.toString()));
    }
}
