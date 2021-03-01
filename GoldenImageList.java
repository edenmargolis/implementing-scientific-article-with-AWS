import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Scanner;

public class GoldenImageList {

    public static HashMap <String, Integer> golden_standard;

    public static void main() throws IOException {

        golden_standard = new HashMap<>();

        try{
            File golden = new File("word-relatedness.txt");
            Scanner myReader = new Scanner(golden);
            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
                String [] words = data.split("\t");
                golden_standard.put(words[0],1);
                golden_standard.put(words[1],1);
            }
            myReader.close();
        }
        catch (FileNotFoundException e){
            System.out.println("wrong inupt");
        }

        PrintWriter pw = null;
        try {
            pw = new PrintWriter(
                    new OutputStreamWriter(new FileOutputStream("golden_new_file.txt"), "UTF-8"));
            for (String s : golden_standard.keySet()) {
                pw.println(s);
            }
            pw.flush();
        } finally {
            pw.close();

            S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();
            s3.putObject(PutObjectRequest.builder().bucket("files-guy-eden").key("golden_standard_list")
                    .build(), RequestBody.fromFile(new File("golden_new_file.txt")));

            s3.putObject(PutObjectRequest.builder().bucket("files-guy-eden").key("golden_standard")
                    .build(), RequestBody.fromFile(new File("word-relatedness.txt")));
        }

    }


}
