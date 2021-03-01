import java.io.*;
import java.util.Random;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.bayes.NaiveBayes;
import weka.classifiers.evaluation.Prediction;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ArffLoader;


public class Weka_program {

    public static String data_set;
    public static void main(String[] args) throws Exception {

        //Parsing our input file into an .arff file
        //BufferedReader inputReader = null;
        data_set = "@relation myWEKA\n\n@ATTRIBUTE data1 REAL\n@ATTRIBUTE data2 REAL\n@ATTRIBUTE data3 REAL\n@ATTRIBUTE data4 REAL\n@ATTRIBUTE data5 REAL\n@ATTRIBUTE data6 REAL\n@ATTRIBUTE data7 REAL\n@ATTRIBUTE data8 REAL\n@ATTRIBUTE data9 REAL\n@ATTRIBUTE data10 REAL\n@ATTRIBUTE data11 REAL\n@ATTRIBUTE data12 REAL\n@ATTRIBUTE data13 REAL\n@ATTRIBUTE data14 REAL\n@ATTRIBUTE data15 REAL\n@ATTRIBUTE data16 REAL\n@ATTRIBUTE data17 REAL\n@ATTRIBUTE data18 REAL\n@ATTRIBUTE data19 REAL\n@ATTRIBUTE data20 REAL\n@ATTRIBUTE data21 REAL\n@ATTRIBUTE data22 REAL\n@ATTRIBUTE data23 REAL\n@ATTRIBUTE data24 REAL\n@ATTRIBUTE related {False,True}\n\n@DATA";

        S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();
        ResponseInputStream<GetObjectResponse> object = s3.getObject(GetObjectRequest.builder().bucket("output-ass3").key("vectorsSimilarity/part-r-00000").build());
        BufferedReader reader = new BufferedReader(new InputStreamReader(object));
        String data;
        while ((data = reader.readLine()) != null) {
            String[] words = data.split("\t"); //words[0] is the array, words[1] is True\False
            String datas = words[0].replace("[", "").replace("]", ""); //substring those ] and [
            data_set = data_set + "\n"+ datas+ ","+ words[1];
        }


            try {
                BufferedWriter writer = new BufferedWriter(new FileWriter("input.arff"));
                writer.write(data_set);
                writer.close();
            } catch (FileNotFoundException e) {
                System.out.println("wrong input");
            } catch (IOException e) {
                e.printStackTrace();
            }
            // Load our new arff file
            ArffLoader loader = new ArffLoader();
            loader.setSource(new File("input.arff"));

            // Create instances
            Instances instance = loader.getDataSet();
            instance.setClassIndex(instance.numAttributes()-1);
            // Create classifier (model) and evaluate it
            Classifier naive = new NaiveBayes();
            Evaluation eval = new Evaluation(instance);
            // 10-fold cross validation as required
            eval.crossValidateModel(naive,instance,10,new Random(1));

            String ans = eval.toSummaryString() + "\n" + eval.toClassDetailsString();
            s3.putObject(PutObjectRequest.builder().bucket("output-ass3").key("output")
                .build(), RequestBody.fromString(ans));

//            int tp =0;
//            int tn =0;
//            int fp =0;
//            int fn =0;
//
//            naive.buildClassifier(instance);
//            double [] tmp3;
//
//            for (Instance inc : instance){
//                try {
//                    double pred = naive.classifyInstance(inc);
//                    tmp3 = inc.toDoubleArray();
//                    double actual =tmp3[24];
//                    if (actual == 1){
//                        if (pred == 1){
//
//                            if(tp<=2 && tmp3[0]<1000){
//                                System.out.print("true positive: ");
//                                System.out.println(instance.indexOf(inc));
//                                System.out.print(tmp3[0]+ " "+tmp3[1]+ " "+tmp3[2]+ " "+tmp3[3]+ " "+tmp3[4]+ " "+tmp3[24]+"\n\n");
//                                tp++;
//                            }
//                        } else{
//                            if(fn<=3 & tmp3[0]>500000){
//                                System.out.print("false negative: ");
//                                System.out.println(instance.indexOf(inc)+1);
//                                System.out.print(tmp3[0]+ " "+tmp3[1]+ " "+tmp3[2]+ " "+tmp3[3]+ " "+tmp3[4]+ " "+tmp3[24]+"\n\n");
//                                fn++;
//
//                            }
//                        }
//                    }
//                    else{
//                        if (pred == 1){
//                            if(fp<=3 && tmp3[0]<1000){
//                                System.out.print("false positive: ");
//                                System.out.println(instance.indexOf(inc));
//                                System.out.print(tmp3[0]+ " "+tmp3[1]+ " "+tmp3[2]+ " "+tmp3[3]+ " "+tmp3[4]+ " "+tmp3[24]+"\n\n");
//                                fp++;
//                            }
//                        } else{
//                            if(tn<=3 & tmp3[0]>500000){
//                                System.out.print("true negative: ");
//                                System.out.println(instance.indexOf(inc));
//                                System.out.print(tmp3[0]+ " "+tmp3[1]+ " "+tmp3[2]+ " "+tmp3[3]+ " "+tmp3[4]+ " "+tmp3[24]+"\n\n");
//                                tn++;
//                            }
//                        }
//                    }
//                } catch (Exception e){}
//            }

    }
}
