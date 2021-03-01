import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import javafx.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

public class VectorSimilarity_MapReduce {

    public static class MapperClass extends Mapper<LongWritable, Text, IntWritable, Text> {

        public static ArrayList<Pair<Pair <String, String>, String>> golden_standard;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            golden_standard = new ArrayList<>();
            S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();
            ResponseInputStream<GetObjectResponse> object = s3.getObject(GetObjectRequest.builder().bucket("files-guy-eden").key("golden_standard").build());
            BufferedReader reader = new BufferedReader(new InputStreamReader(object));
            String line;
            while ((line = reader.readLine()) != null) {
                String [] split_line = line.split("\t");
                golden_standard.add(new Pair <> (new Pair <> (split_line[0], split_line[1]), split_line[2]));
            }
            super.setup(context);
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] split_vector = value.toString().split("\t", 2);
            String word = split_vector[0];
            String array = split_vector[1];
            for (Pair <Pair <String, String>, String> pair : golden_standard){
                Pair tmp = pair.getKey();
                if (tmp.getKey().equals(word))
                    context.write(new IntWritable(golden_standard.indexOf(pair)), new Text (pair.getValue()+ " 0 "+ word +" " + array));
                if (tmp.getValue().equals(word))
                    context.write(new IntWritable(golden_standard.indexOf(pair)), new Text (pair.getValue()+ " 1 " +word +" "+ array));
            }
        }
    }

    public static class ReducerClass extends Reducer<IntWritable, Text, Text, Text> {

        //public static ArrayList<String> golden_standard_rank;
        HashMap<Integer, double []> vectors1;
        HashMap<Integer, double []> vectors2;
        String [] returnVector;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            returnVector = new String [24];
            vectors1 = new HashMap<>();
            vectors2 = new HashMap<>();
            String bool = "";
            String word1 = "";
            String word2 = "";
            //get the vectors of each word
            for (Text value:values){
                String [] value_split = value.toString().split(" ",5);
                bool = value_split[0];
                double [] vector = fromString(value_split[4]);
                if(value_split[1].equals("0")) {
                    vectors1.put(Integer.parseInt(value_split[3]), vector);
                    word1= value_split[2];
                }
                else {
                    vectors2.put(Integer.parseInt(value_split[3]), vector);
                    word2= value_split[2];
                }
            }

            if(vectors1.size()==4 && vectors2.size()==4) {

                //creating vector for each combination

                int counter = 0; //the place for the value in the returnArray

                for (int j = 0; j < 4; j++) {

                    double[] vector1 = vectors1.get(j);
                    double[] vector2 = vectors2.get(j);

                    //calc9
                    double sum = 0;
                    for (int i = 0; i < vector1.length; i++)
                        sum += Math.abs(vector1[i] - vector2[i]);
                    if(Double.isNaN(sum) | Double.isInfinite(sum))
                        returnVector[counter] = "?";
                    else
                        returnVector[counter] = String.valueOf(sum);
                    counter++;

                    //calc10
                    sum = 0;
                    for (int i = 0; i < vector1.length; i++)
                        sum += Math.pow(vector1[i] - vector2[i], 2);
                    sum = Math.pow(sum, 0.5);
                    if(Double.isNaN(sum) | Double.isInfinite(sum))
                        returnVector[counter] = "?";
                    else
                        returnVector[counter] = String.valueOf(sum);
                    counter++;

                    //calc11
                    double sum1 = 0;
                    double sum2 = 0;
                    double sum3 = 0;
                    for (int i = 0; i < vector1.length; i++) {
                        sum3 += vector1[i] * vector2[i];
                        sum1 = Math.pow(vector1[i], 2);
                        sum2 = Math.pow(vector2[i], 2);
                    }
                    sum1 = Math.pow(sum1, 0.5);
                    sum2 = Math.pow(sum2, 0.5);
                    sum = sum3 / (sum1 * sum2);
                    if(Double.isNaN(sum) | Double.isInfinite(sum))
                        returnVector[counter] = "?";
                    else
                        returnVector[counter] = String.valueOf(sum);
                    counter++;

                    //calc13
                    double numerator = 0;
                    double denominator = 0;
                    for (int i = 0; i < 1000; i++) {
                        numerator += Math.min(vector1[i], vector2[i]);
                        denominator += Math.max(vector1[i], vector2[i]);
                    }
                    sum = numerator / denominator;
                    if(Double.isNaN(sum) | Double.isInfinite(sum))
                        returnVector[counter] = "?";
                    else
                        returnVector[counter] = String.valueOf(sum);
                    counter++;

                    //calc15
                    numerator = 0;
                    denominator = 0;
                    for (int i = 0; i < vector1.length; i++) {
                        numerator += Math.min(vector1[i], vector2[i]);
                        denominator += vector1[i] + vector2[i];
                    }
                    numerator *= 2;
                    sum = numerator / denominator;
                    if(Double.isNaN(sum) | Double.isInfinite(sum))
                        returnVector[counter] = "?";
                    else
                        returnVector[counter] = String.valueOf(sum);
                    counter++;

                    //calc17
                    double[] tmpVector = new double[vector1.length];
                    for (int i = 0; i < vector1.length; i++) {
                        tmpVector[i] = vector1[i] + vector2[i];
                        tmpVector[i] /= 2;
                    }
                    sum = DCalculation(vector1, tmpVector) + DCalculation(vector2, tmpVector);
                    if(Double.isNaN(sum) | Double.isInfinite(sum))
                        returnVector[counter] = "?";
                    else
                        returnVector[counter] = String.valueOf(sum);
                    counter++;

                }

                context.write(new Text(Arrays.toString(returnVector)), new Text(bool+"\t"+word1+"\t"+word2));

            }
        }

        private double DCalculation (double [] vec1, double [] vec2){
            double sum = 0;
            double sum1;
            for(int i = 0; i< vec1.length; i++){
                sum1 = Math.log(vec1[i]/vec2[i]);
                sum += vec1[i] * sum1;
            }
            return sum;
        }

        private static double[] fromString(String string) {
            String[] strings = string.replace("[", "").replace("]", "").split(", ");
            double result[] = new double[strings.length];
            for (int i = 0; i < result.length; i++) {
                result[i] = Double.parseDouble(strings[i]);
            }
            return result;
        }
    }

    public static class PartitionerClass extends Partitioner<IntWritable,Text> {

        @Override
        public int getPartition(IntWritable key, Text value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }

    }
}

