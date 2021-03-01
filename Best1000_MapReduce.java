import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class Best1000_MapReduce {

    public static class MapperClass extends Mapper<LongWritable,Text,LongWritable,Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            String [] word = value.toString().split("\t");
            context.write(new LongWritable(-1*Long.parseLong(word[1])), new Text (word[0]));
        }
    }

    public static class ReducerClass extends Reducer<LongWritable,Text,Text,Text> {

        private int counter;
        Vector <String> best1000;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            counter = 0;
            best1000 = new Vector <> ();
            super.setup(context);
        }

        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            long sum = key.get()*-1;   //get the actual number
            for (Text word : values){
                if(counter>99 && counter<1100) {    //omit the first 100
                    context.write(word, new Text(Long.toString(sum)));
                    best1000.add(word.toString());
                }
                counter++;
            }
        }
    }

    public static class PartitionerClass extends Partitioner<LongWritable,Text> {

        @Override
        public int getPartition(LongWritable key, Text value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE)  % numPartitions;
        }

    }
}
