import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class myjar {

    public static void main(String[] args) throws IOException {

        //create golden image list and upload them to s3
        GoldenImageList.main();

        BasicConfigurator.configure();

        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClient.builder().withRegion(Regions.US_EAST_1).build();

        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar("s3n://files-guy-eden/ass3.jar")
                .withMainClass("MapReduce");

        StepConfig stepConfig = new StepConfig()
                .withName("DeletedEstimation")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(2)
                .withMasterInstanceType(InstanceType.M5Xlarge.toString())
                .withSlaveInstanceType(InstanceType.M5Xlarge.toString())
                .withHadoopVersion("3.2.1")
                .withEc2KeyName("DSP211")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("Words_Similarity")
                .withInstances(instances)
                .withSteps(stepConfig)
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withLogUri("s3n://output-ass3/Log/")
                .withReleaseLabel("emr-6.2.0");

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }

}
