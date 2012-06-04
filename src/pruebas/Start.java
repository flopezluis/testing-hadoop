package pruebas;
/*

 * map reduce sample code

 */



import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;

import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;





public class Start extends Configured implements Tool {
	Logger log = Logger.getLogger("log_file");
	@Override
    public int run(String[] args) throws Exception {
        // inventory dir holds all package (zip) files resulting from stage
        Configuration configuration = getConf();
        configuration.set("mapred.reduce.tasks.speculative.execution", "false");        
        // TODO even in local mode, the first argument should not be the inventory
        // but write a complete project file instead
        Job job = new Job(configuration);
        job.setJobName("toZip");

        // Hadoop processes key-value pairs
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // set map and reduce classes
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        // Hadoop TextInputFormat class
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("/messages"));
        FileOutputFormat.setOutputPath(job, new Path("/gzip"));

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

	/**

	 * Runs the demo.

	 */
	public static void main(String[] args) throws IOException {
		int mapTasks = 20;
		int reduceTasks = 1;
		try {
			ToolRunner.run(new Start(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		JobConf conf = new JobConf(Prue.class);
//		conf.setJobName("testing");
//		conf.setNumMapTasks(mapTasks);
//		conf.setNumReduceTasks(reduceTasks);
//		MultipleInputs.addInputPath(conf, new Path("/colas"), TextInputFormat.class, MapClass.class);
//
//		conf.setOutputKeyClass(Text.class);
//		conf.setOutputValueClass(Text.class);
//        FileOutputFormat.setOutputPath(conf, new Path("/czip"));
//        FileOutputFormat.setCompressOutput(conf, true);
//        FileOutputFormat.setOutputCompressorClass(conf, org.apache.hadoop.io.compress.GzipCodec.class);
//
//		conf.setMapperClass(MapClass.class);
//		conf.setCombinerClass(ReduceClass.class);
//		conf.setReducerClass(theClass)
//		// Delete the output directory if it exists already
//        Configuration conf = new Configuration();
//        MultipleInputs.addInputPath(conf, new Path("/colas"), TextInputFormat.class, MapClass.class);
		
	}

}