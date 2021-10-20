package cc4lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.LambdaLogger;

import java.io.InputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.zip.GZIPInputStream;
import java.util.HashSet;
import java.util.ArrayList;
import java.io.IOException;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.AmazonServiceException;

// Handler value: cc4lambda.HandlerString
public class HandlerString implements RequestHandler<String, String>{
  // *** hashset ***
  private static final HashSet<Character> cantoneseChars = new HashSet<Character>();
  static{
      cantoneseChars.add('冇');
      cantoneseChars.add('𡃉');
      cantoneseChars.add('褸');
      cantoneseChars.add('喎');
      cantoneseChars.add('嗌');
      cantoneseChars.add('㗎');
      cantoneseChars.add('噉');
      cantoneseChars.add('嗰');
      cantoneseChars.add('喺');
      cantoneseChars.add('嚟');
      cantoneseChars.add('揾');
      cantoneseChars.add('咗');
      cantoneseChars.add('啱');
      cantoneseChars.add('諗');
      cantoneseChars.add('攞');
      cantoneseChars.add('佢');
      cantoneseChars.add('啲');
      cantoneseChars.add('哋');
      cantoneseChars.add('畀');
      cantoneseChars.add('咁');
      cantoneseChars.add('乸');
      cantoneseChars.add('瞓');
  }

  @Override
  public String handleRequest(String event, Context context)
  {
    LambdaLogger logger = context.getLogger();
    // process s3_url
    logger.log("WET S3 URL: " + event);

    S3Object s3o = null;
    ArrayList<String> filtered = new ArrayList<String>();
    int status = -1;
    try {
      AmazonS3URI s3URI = new AmazonS3URI(event);
      String s3bucket = s3URI.getBucket();
      String s3key = s3URI.getKey();
      logger.log("S3 bucket: " + s3bucket + "\n");
      logger.log("S3 key: " + s3key + "\n");

      logger.log("Building unzip stream\n");
      AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
      s3o = s3Client.getObject(s3bucket, s3key);
      InputStream gzipStream = new GZIPInputStream(s3o.getObjectContent(), 65535);
      BufferedReader in = new BufferedReader(new InputStreamReader(gzipStream, "UTF-8"), 65535);

      logger.log("Start reading S3 object line by line with BufferedReader\n");
      String line;
      int hit_n = 0;
      int tot_n = 0;
      while ((line = in.readLine()) != null)  {
        char [] cs = line.toCharArray();
        for (char c : cs){
          if (cantoneseChars.contains(c)){
            filtered.add(line);
            hit_n += 1;
            break;
          }
        }
        tot_n += 1;
      }
      logger.log("Read lines: " + tot_n + "\n");
      logger.log("Hit lines: " + hit_n + "\n");
      status = 0;
    } catch (AmazonServiceException e) {
      logger.log(e.getErrorMessage());
      status = -1;
    } catch (IOException e) {
      logger.log(e.getMessage());
      status = -1;
    }
    finally {
        // To ensure that the network connection doesn't remain open, 
        // close any open input streams.
        if(s3o != null) {
          logger.log("Trying to close s3 object\n");
          try {
            s3o.close();
          } catch (IOException e) {
            logger.log(e.getMessage());
          }
        }
    }
    String response = null;
    logger.log("Responding\n");
    if (status == 0)
      response = String.join("\n", filtered);
    else
      response = "-1";
    return response;
  }
}