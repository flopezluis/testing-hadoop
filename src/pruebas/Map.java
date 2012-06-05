package pruebas;

import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.util.UUID;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import net.sf.json.JSONObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;


public class Map extends Mapper<LongWritable, Text, Text, Text> {
	Logger log = Logger.getLogger("log_file");
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
		FileSystem fs = FileSystem.get(new Configuration());
		Path path = new Path("/data/" + UUID.randomUUID());
		OutputStream os = fs.create(path);
        String message = null;
        String jobKey = null;
        try {
        	JSONObject jsonO = JSONObject.fromObject(value.toString());
			System.out.println(jsonO.get("body"));
			message = getMessage(jsonO.get("body").toString());
			jobKey = getJobKey(jsonO.get("body").toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		os.write(message.getBytes());
		// write to os
		os.close();
		context.write(new Text(jobKey), new Text(path.toString()));
    }
    
	/**
	 * It gets the message from an . 
	 * @param body
	 * @return the message 
	 * @throws ParserConfigurationException
	 * @throws SAXException
	 * @throws IOException
	 * @throws XPathExpressionException
	 * @throws CodecException
	 */
	public static String getMessage(String xml) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException {
		return getXpath("//storage", xml);
	}
	
	/**
	 * It gets the message from an . 
	 * @param body
	 * @return the message 
	 * @throws ParserConfigurationException
	 * @throws SAXException
	 * @throws IOException
	 * @throws XPathExpressionException
	 * @throws CodecException
	 */
	public static String getXpath(String expression, String xml) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
        InputSource is = new InputSource(new StringReader(xml));
		Document doc = builder.parse(is);
		XPathFactory xPathfactory = XPathFactory.newInstance();
		XPath xpath = xPathfactory.newXPath();
		XPathExpression expr = xpath.compile(expression);
		String message = expr.evaluate(doc, XPathConstants.STRING).toString();
		return message;
	}
	
	
	/**
	 * It gets the message from an . 
	 * @param body
	 * @return the message 
	 * @throws ParserConfigurationException
	 * @throws SAXException
	 * @throws IOException
	 * @throws XPathExpressionException
	 * @throws CodecException
	 */
	public static String getJobKey(String xml) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException {
		return getXpath("//@jobKey", xml);
	}
}
