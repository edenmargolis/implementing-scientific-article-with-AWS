import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class Counter_MapReduce {

    public static class MapperClass extends Mapper<LongWritable,Text,Text,LongWritable> {


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            String [] values = value.toString().split("\t");
            String [] sentence = values[1].split(" ");   //get the example structure
            for (int i = 0 ; i< sentence.length ; i++){   //for each part of the example
                String [] curr_word = sentence[i].split("/");   //get the current word structure
                int next_word = Integer.parseInt(curr_word[curr_word.length-1]);   //get the index of the connected word
                if(next_word!=0){   // if the index is 0 it means that it's not a word in this example and we can pass it
                    String [] connected_word = sentence[next_word-1].split("/");  //get the connected word structure
                    context.write(new Text(connected_word[0]+"-"+curr_word[curr_word.length-2]), new LongWritable(Long.parseLong(values[2])));    //send it to reducer
                }
            }
        }
    }

    public static class ReducerClass extends Reducer<Text,LongWritable,Text,Text> {

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            long sum = 0;
            for (LongWritable num : values){
                sum+=num.get();
            }
            context.write(key, new Text(Long.toString(sum)));
        }
    }

    public static class PartitionerClass extends Partitioner<Text,LongWritable> {

        @Override
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }

    }
}
