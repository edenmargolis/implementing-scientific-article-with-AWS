import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MapReduce {

    public static void main(String[] args)  {
        System.out.println("Starting MapReduce Program\n\n");
        try {

            BasicConfigurator.configure();

            Configuration conf1 = new Configuration();
            Job job1 = Job.getInstance(conf1, "counter");
            job1.setJarByClass(Counter_MapReduce.class);

            job1.setMapperClass(Counter_MapReduce.MapperClass.class);
            job1.setReducerClass(Counter_MapReduce.ReducerClass.class);
            job1.setPartitionerClass(Counter_MapReduce.PartitionerClass.class);

            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(LongWritable.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job1, new Path("s3n://assignment3dsp/biarcs"));
            FileOutputFormat.setOutputPath(job1, new Path("s3n://output-ass3/counter"));
            job1.setInputFormatClass(SequenceFileInputFormat.class);
            job1.setOutputFormatClass(TextOutputFormat.class);

            if(job1.waitForCompletion(true)){}

            Configuration conf2 = new Configuration();
            Job job2 = Job.getInstance(conf2, "best1000");
            job2.setJarByClass(Best1000_MapReduce.class);
            job2.setMapperClass(Best1000_MapReduce.MapperClass.class);
            job2.setReducerClass(Best1000_MapReduce.ReducerClass.class);
            job2.setPartitionerClass(Best1000_MapReduce.PartitionerClass.class);

            job2.setMapOutputKeyClass(LongWritable.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            job2.setInputFormatClass(TextInputFormat.class);
            job2.setOutputFormatClass(TextOutputFormat.class);
            FileInputFormat.addInputPath(job2, new Path("s3n://output-ass3/counter"));
            FileOutputFormat.setOutputPath(job2, new Path("s3n://output-ass3/best1000"));

            if(job2.waitForCompletion(true)){}

            Configuration conf3 = new Configuration();
            Job job3 = Job.getInstance(conf3, "vectors");
            job3.setJarByClass(BuildVectors_MapReduce.class);
            job3.setMapperClass(BuildVectors_MapReduce.MapperClass.class);
            job3.setReducerClass(BuildVectors_MapReduce.ReducerClass.class);
            job3.setPartitionerClass(BuildVectors_MapReduce.PartitionerClass.class);

            job3.setMapOutputKeyClass(Text.class);
            job3.setMapOutputValueClass(Text.class);
            job3.setOutputKeyClass(Text.class);
            job3.setOutputValueClass(Text.class);
            job3.setInputFormatClass(SequenceFileInputFormat.class);
            job3.setOutputFormatClass(TextOutputFormat.class);
            FileInputFormat.addInputPath(job3, new Path("s3n://assignment3dsp/biarcs"));
            FileOutputFormat.setOutputPath(job3, new Path("s3n://output-ass3/vectors"));

            if(job3.waitForCompletion(true)){}

            Configuration conf4 = new Configuration();
            Job job4 = Job.getInstance(conf4, "vectorsSimilarity");
            job4.setJarByClass(VectorSimilarity_MapReduce.class);
            job4.setMapperClass(VectorSimilarity_MapReduce.MapperClass.class);
            job4.setReducerClass(VectorSimilarity_MapReduce.ReducerClass.class);
            job4.setPartitionerClass(VectorSimilarity_MapReduce.PartitionerClass.class);

            job4.setMapOutputKeyClass(IntWritable.class);
            job4.setMapOutputValueClass(Text.class);
            job4.setOutputKeyClass(Text.class);
            job4.setOutputValueClass(Text.class);
            job4.setInputFormatClass(TextInputFormat.class);
            job4.setOutputFormatClass(TextOutputFormat.class);
            FileInputFormat.addInputPath(job4, new Path("s3n://output-ass3/vectors"));
            FileOutputFormat.setOutputPath(job4, new Path("s3n://output-ass3/vectorsSimilarity"));

            if(job4.waitForCompletion(true)){}

            Weka_program.main(new String [1]);

            System.exit(0);
        }
        catch (Exception e) {
            System.out.println("got an error: " + e.toString());
            System.out.println("got an error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
