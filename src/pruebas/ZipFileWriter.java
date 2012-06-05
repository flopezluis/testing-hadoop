package pruebas;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Opens a zip file for output and writes the text, native files, and exceptions
 * into it.
 */
public class ZipFileWriter {
	Logger log = Logger.getLogger("log_file");
    private String rootDir;
    private String zipFileName;
    private ZipOutputStream zipOutputStream;
    private FileOutputStream fileOutputStream;
    OutputStream os;
    
    
    public ZipFileWriter(String name) {
    	zipFileName = "/data/" + name + ".zip";
    }

    public void setup() {
    	FileSystem fs;
    	try {
    		fs = FileSystem.get(new Configuration());
    		Path path = new Path(zipFileName);
    		os = fs.create(path);
    		rootDir = "/data";
    	} catch (IOException e) {
    		e.printStackTrace();
    	}
    	log.info("CREADO " + zipFileName);
    }

    public void openZipForWriting() throws IOException {
    	
        //fileOutputStream = new FileOutputStream(zipFileName);
        zipOutputStream = new ZipOutputStream(new BufferedOutputStream(os));
    }

    public void closeZip() throws IOException {
        zipOutputStream.close();
        os.close();
    }

    public void addTextFile(String entryName, String textContent) throws IOException {
        ZipEntry zipEntry = new ZipEntry(entryName);
        zipOutputStream.putNextEntry(zipEntry);
        if (textContent == null) {
            textContent = "No text extracted";
        }
        zipOutputStream.write(textContent.getBytes());
    }

    public void addBinaryFile(String entryName, byte[] fileContent, int length) throws IOException {
        ZipEntry zipEntry = new ZipEntry(entryName);
        zipOutputStream.putNextEntry(zipEntry);
        zipOutputStream.write(fileContent, 0, length);
    }

    public String getZipFileName() {
        return zipFileName;
    }
}