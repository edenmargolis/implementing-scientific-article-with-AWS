import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Vector;

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

public class BuildVectors_MapReduce {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        Vector<String> myVec;
        HashMap<String, Integer> myHash;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            myHash = new HashMap<>();
            myVec = new Vector<>();
            //get myHash from s3
            S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();
            ResponseInputStream<GetObjectResponse> object = s3.getObject(GetObjectRequest.builder().bucket("files-guy-eden").key("golden_standard_list").build());
            BufferedReader reader = new BufferedReader(new InputStreamReader(object));
            String line;
            while ((line = reader.readLine()) != null) {
                myHash.put(line, 1);
            }

            //get vector from s3
            object = s3.getObject(GetObjectRequest.builder().bucket("output-ass3").key("best1000/part-r-00000").build());
            reader = new BufferedReader(new InputStreamReader(object));
            while ((line = reader.readLine()) != null) {
                String [] split_line = line.split("\t") ;
                myVec.add(split_line[0]);
            }

            super.setup(context);
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split("\t");
            String[] sentence = values[1].split(" ");   //as that/IN/compl/3 patients/NNS/nsubj/3 experience/VB/ccomp/0
            for (int i = 0; i < sentence.length; i++) {   //for each part of the example above
                String[] curr_word = sentence[i].split("/");   //split the current structure
                if (myHash.containsKey(curr_word[0])) { //check if the word is part of the golden image
                    int next_word = Integer.parseInt(curr_word[curr_word.length-1]);   //get the index of the connected word
                    if (next_word != 0) {   // if the index is 0 it means that it's not a word in this sentence and we can pass it
                        String[] connected_word = sentence[next_word-1].split("/");  //get the connected word structure
                        String word = connected_word[0] + "-" + curr_word[curr_word.length-2]; //add the word with her feature
                        int index = myVec.indexOf(word); //get the index of this word - in reduce we want to create an array for each word and put the value of this word in the index place
                        if (index!= -1) { //if index == -1 means it's not part of the best 1000
                            context.write(new Text("*"), new Text(curr_word[0]+" "+ values[2])); //all the words
                            context.write(new Text("F"), new Text(curr_word[curr_word.length-2]+" "+ values[2] + " " + curr_word[0])); //all the features
                            context.write(new Text("L"), new Text(connected_word[0]+" "+values[2] + " " + curr_word[0])); //all the lexemes
                            context.write(new Text("Z " + curr_word[0]), new Text(index + " " + connected_word[0] + " "+curr_word[curr_word.length-2] +" "+ values[2])); //a value to put in word array
                        }
                    }
                }
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        HashMap<String, Long> featureHash; //save feature and its number of occurrences
        HashMap<String, Long> lexemeHash; //save lexeme and its number of occurrences
        HashMap<String, Long> numOfElementHash; //number of occurrences for each word

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            featureHash = new HashMap<>();
            lexemeHash = new HashMap<>();
            numOfElementHash = new HashMap<>();
            super.setup(context);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] key_split = key.toString().split(" ");
            String tag = key_split[0]; //get the tag of this key
            if (tag.equals("F") | tag.equals("L") | tag.equals("*")) {
                for (Text value : values) {
                    String[] value_split = value.toString().split(" ");
                    String word = value_split[0]; //the feature / lexeme
                    String occurs = value_split[1]; //number of occurrences of this feature/lexeme
                    switch (tag) {
                        case "*":
                            if (!numOfElementHash.containsKey(word))
                                numOfElementHash.put(word, Long.parseLong(occurs));
                            else
                                numOfElementHash.put(word, numOfElementHash.get(word) + Long.parseLong(occurs));
                            break;
                        case "F":
                            String primaryWord = value_split[2]; //number of occurrences of this feature/lexeme
                            word = word + " " + primaryWord;
                            if (!featureHash.containsKey(word))
                                featureHash.put(word, Long.parseLong(occurs));
                            else
                                featureHash.put(word, featureHash.get(word) + Long.parseLong(occurs));
                            break;
                        case "L":
                            primaryWord = value_split[2]; //number of occurrences of this feature/lexeme
                            word = word + " " + primaryWord;
                            if (!lexemeHash.containsKey(word))
                                lexemeHash.put(word, Long.parseLong(occurs));
                            else
                                lexemeHash.put(word, lexemeHash.get(word) + Long.parseLong(occurs));
                            break;
                    }
                }
            } else { //means we got Z - the last value. we already calculated the others table

                /*
                assumptions for calculations:

                p(f) = how many times features occured / number of lines at all
                p(l) = how many times lexama occured / number of lines at all
                p(f|l) = (how many times lexama + feature occured) / how many times lexama occured
                p(l|f) = (how many times lexama + feature occured) / how many times feature occured

                 */
                double[] vector5 = new double[1000]; //formula 5
                double[] vector6 = new double[1000]; //formula 6
                double[] vector7 = new double[1000]; //formula 7
                double[] vector8 = new double[1000]; //formula 8

                String word = key_split[1];
                for (Text value : values) {
                    String[] value_split = value.toString().split(" ");
                    int index = Integer.parseInt(value_split[0]); //represent the index of the word in the array
                    String lexeme = value_split[1];
                    String feature = value_split[2];
                    long occurs = Long.parseLong(value_split[3]);

                    //update vector5
                    vector5[index] += occurs;

                    //update vector6
                    long lexemeOccurs = lexemeHash.get(lexeme + " " + word);
                    vector6[index] += ((double) occurs / (double) lexemeOccurs);



                    //update vector7
                    long featureOccurs = featureHash.get(feature  + " " + word);
                    double lf = (double) occurs / (double) featureOccurs;
                    double f = (double) featureOccurs / (double) numOfElementHash.get(word);
                    double l = (double) lexemeOccurs / (double) numOfElementHash.get(word);
                    double calculation = lf / (f * l);
                    vector7[index] += Math.log(calculation) / Math.log(2); //log2 N = log10 N / log10 2


                    //update vector8
                    double tmp2 = Math.sqrt(l * f);
                    double tmp = (lf - (f * l)) / tmp2;
                    vector8[index] += tmp;

                }

                context.write(new Text(word), new Text("0 "+Arrays.toString(vector5)));
                context.write(new Text(word), new Text("1 "+Arrays.toString(vector6)));
                context.write(new Text(word), new Text("2 "+Arrays.toString(vector7)));
                context.write(new Text(word), new Text("3 "+Arrays.toString(vector8)));

            }
        }
    }

        public static class PartitionerClass extends Partitioner<Text,Text> {

            @Override
            public int getPartition(Text key, Text value, int numPartitions) {
                return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
            }

        }
    }

