import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import software.amazon.awssdk.services.ec2.model.InstanceType;

public class MainFlow {
    public static void main(String[] args) {
        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder
                .standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(credentialsProvider)
                .build();

        // ---------------------------------- Step0 ---------------------------------- //
        // ---------------------- 1000 most frequent features ------------------------ //
        HadoopJarStepConfig hadoopJarStep0 = new HadoopJarStepConfig()
                .withJar("s3n://ass3-jar-bucket/MostFrequentFeatures.jar")
                .withMainClass("Others.MostFrequentFeatures")
                .withArgs("s3n://ass3-corpus-bucket/Corpus/*",
                        "s3n://ass3-small-output-bucket/All-features/",
                        "s3n://ass3-small-output-bucket/Top-1000-features/",
                        "s3n://ass3-small-output-bucket/Top-1000-one-line/");

        StepConfig stepConfig0 = new StepConfig()
                .withName("Step0")
                .withHadoopJarStep(hadoopJarStep0)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // --------------------------------- Step0_5 --------------------------------- //
        // ------------------------------- Gold stem --------------------------------- //
        HadoopJarStepConfig hadoopJarStep0_5 = new HadoopJarStepConfig()
                .withJar("s3n://ass3-jar-bucket/ReadGold.jar")
                .withMainClass("Others.ReadGold")
                .withArgs("s3n://ass3-corpus-bucket/word-relatedness.txt",
                        "s3n://ass3-corpus-bucket/Word-relatedness-stemmed/");

        StepConfig stepConfig0_5 = new StepConfig()
                .withName("Step0_5")
                .withHadoopJarStep(hadoopJarStep0_5)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // ---------------------------------- Step1 ---------------------------------- //
        // --------------------------------- Vector1 --------------------------------- //
        HadoopJarStepConfig hadoopJarStep1 = new HadoopJarStepConfig()
                .withJar("s3n://ass3-jar-bucket/JFS_calc_vector1.jar")
                .withMainClass("Others.JFS_calc_vector1")
                .withArgs("s3n://ass3-corpus-bucket/Corpus-partial/*",
                        "s3n://ass3-small-output-bucket/Temp-vectors/",
                        "s3n://ass3-output-bucket/Top-1000-one-line/*",
                        "s3n://ass3-small-output-bucket/Vectors/Vector1/");

        StepConfig stepConfig1 = new StepConfig()
                .withName("Step1")
                .withHadoopJarStep(hadoopJarStep1)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // ---------------------------------- Step2 ---------------------------------- //
        // --------------------------------- Vector2 --------------------------------- //
        HadoopJarStepConfig hadoopJarStep2 = new HadoopJarStepConfig()
                .withJar("s3n://ass3-jar-bucket/JFS_calc_vector2.jar")
                .withMainClass("Others.JFS_calc_vector2")
                .withArgs("s3n://ass3-small-output-bucket/Vectors/Vector1/*",
                        "s3n://ass3-small-output-bucket/Count_l/",
                        "s3n://ass3-small-output-bucket/Vectors/Vector2/");

        StepConfig stepConfig2 = new StepConfig()
                .withName("Step2")
                .withHadoopJarStep(hadoopJarStep2)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // ---------------------------------- Step3 ---------------------------------- //
        // -------------------------------- Vectors3+4 ------------------------------- //
        HadoopJarStepConfig hadoopJarStep3 = new HadoopJarStepConfig()
                .withJar("s3n://ass3-jar-bucket/JFS_calc_vector3.jar")
                .withMainClass("Others.JFS_calc_vector3")
                .withArgs("s3n://ass3-small-output-bucket/Count_l/*",
                        "s3n://ass3-small-output-bucket/Count_L/",
                        "s3n://ass3-small-output-bucket/Vectors/Vector1/*",
                        "s3n://ass3-small-output-bucket/P_l_f/",
                        "s3n://ass3-small-output-bucket/P_l/",
                        "s3n://ass3-small-output-bucket/P_f/",
                        "s3n://ass3-small-output-bucket/Vectors/Vector3/",
                        "s3n://ass3-small-output-bucket/Vectors/Vector4/");

        StepConfig stepConfig3 = new StepConfig()
                .withName("Step3")
                .withHadoopJarStep(hadoopJarStep3)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // ---------------------------------- Step4 ---------------------------------- //
        // ------------------------------ Distances vec1 ----------------------------- //
        HadoopJarStepConfig hadoopJarStep4 = new HadoopJarStepConfig()
                .withJar("s3n://ass3-jar-bucket/JFS_vec_distances.jar")
                .withMainClass("Others.JFS_vec_distances")
                .withArgs("s3n://ass3-small-output-bucket/Vectors/Vector1/*",
                        "s3n://ass3-small-output-bucket/Distances/Vector1/",
                        "1");

        StepConfig stepConfig4 = new StepConfig()
                .withName("Step4")
                .withHadoopJarStep(hadoopJarStep4)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // ---------------------------------- Step5 ---------------------------------- //
        // ------------------------------ Distances vec2 ----------------------------- //
        HadoopJarStepConfig hadoopJarStep5 = new HadoopJarStepConfig()
                .withJar("s3n://ass3-jar-bucket/JFS_vec_distances.jar")
                .withMainClass("Others.JFS_vec_distances")
                .withArgs("s3n://ass3-small-output-bucket/Vectors/Vector2/*",
                        "s3n://ass3-small-output-bucket/Distances/Vector2/",
                        "2");

        StepConfig stepConfig5 = new StepConfig()
                .withName("Step5")
                .withHadoopJarStep(hadoopJarStep5)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // ---------------------------------- Step6 ---------------------------------- //
        // ------------------------------ Distances vec3 ----------------------------- //
        HadoopJarStepConfig hadoopJarStep6 = new HadoopJarStepConfig()
                .withJar("s3n://ass3-jar-bucket/JFS_vec_distances.jar")
                .withMainClass("Others.JFS_vec_distances")
                .withArgs("s3n://ass3-small-output-bucket/Vectors/Vector3/*",
                        "s3n://ass3-small-output-bucket/Distances/Vector3/",
                        "3");

        StepConfig stepConfig6 = new StepConfig()
                .withName("Step6")
                .withHadoopJarStep(hadoopJarStep6)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // ---------------------------------- Step7 ---------------------------------- //
        // ------------------------------ Distances vec4 ----------------------------- //
        HadoopJarStepConfig hadoopJarStep7 = new HadoopJarStepConfig()
                .withJar("s3n://ass3-jar-bucket/JFS_vec_distances.jar")
                .withMainClass("Others.JFS_vec_distances")
                .withArgs("s3n://ass3-small-output-bucket/Vectors/Vector4/*",
                        "s3n://ass3-small-output-bucket/Distances/Vector4/",
                        "4");

        StepConfig stepConfig7 = new StepConfig()
                .withName("Step7")
                .withHadoopJarStep(hadoopJarStep7)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // ---------------------------------- Step8 ---------------------------------- //
        // ------------------------------- Vectors final ----------------------------- //
        HadoopJarStepConfig hadoopJarStep8 = new HadoopJarStepConfig()
                .withJar("s3n://ass3-jar-bucket/JFS_vec_final.jar")
                .withMainClass("Others.JFS_vec_final")
                .withArgs("s3n://ass3-small-output-bucket/Distances/Vector1/*",
                        "s3n://ass3-small-output-bucket/Distances/Vector2/*",
                        "s3n://ass3-small-output-bucket/Distances/Vector3/*",
                        "s3n://ass3-small-output-bucket/Distances/Vector4/*",
                        "s3n://ass3-small-output-bucket/Distances/Vectors-final/");

        StepConfig stepConfig8 = new StepConfig()
                .withName("Step8")
                .withHadoopJarStep(hadoopJarStep8)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // ---------------------------------- RunJobFlow ---------------------------------- //
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(6)
                .withMasterInstanceType(InstanceType.C5_XLARGE.toString())
                .withSlaveInstanceType(InstanceType.C5_XLARGE.toString())
                .withHadoopVersion("2.6.0")
                .withEc2KeyName("dsp-ass3-keypair")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("Ass3AdeeOfek")
                .withReleaseLabel("emr-5.14.0")
                .withInstances(instances)
                .withSteps(stepConfig1, stepConfig2, stepConfig3, stepConfig4, stepConfig5, stepConfig6,
                        stepConfig7, stepConfig8)
                .withLogUri("s3n://ass3-log-bucket/")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withServiceRole("EMR_DefaultRole");

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }
}
