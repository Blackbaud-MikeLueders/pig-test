package com.blackbaud.emr;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.ActionOnFailure;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsResult;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;
import com.amazonaws.services.s3.AmazonS3;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class EmrService {

    private AmazonS3 s3;
    private AmazonElasticMapReduce emr;

    public JobDefinition createPigJobDefinition(String name, String bucketName) {
        return JobDefinition.builder()
                .id(UUID.randomUUID())
                .name(name)
                .bucketName(bucketName)
                .type(JobType.PIG)
                .build();
    }

    public VersionedJobDefinition createVersionedJobDefinition(JobDefinition jobDefinition, String scriptContent) {
        VersionedJobDefinition versionedJobDefinition = VersionedJobDefinition.builder()
                .id(UUID.randomUUID())
                .scriptContent(scriptContent)
                .version(1)
                .description("initial script")
                .jobDefinition(jobDefinition)
                .build();

        s3.putObject(jobDefinition.getBucketName(), versionedJobDefinition.getScriptPath(), scriptContent);
        return versionedJobDefinition;
    }

    public AddJobFlowStepsResult submitJobs(JobInstance ... jobs) {
        StepFactory stepFactory = new StepFactory();
        List<StepConfig> steps = new ArrayList<>();
        for (JobInstance job : jobs) {
            String pigScript = job.getS3ScriptPath();
            String[] args = new String[]{
                    "-p", "INPUT=" + job.getS3InputPath(),
                    "-p", "OUTPUT=" + job.getS3OutputPath()
            };
            HadoopJarStepConfig hadoopJarStep = stepFactory.newRunPigScriptStep(pigScript, args);

            StepConfig stepConfig = new StepConfig()
                    .withName(job.getName())
                    .withActionOnFailure(ActionOnFailure.CONTINUE)
                    .withHadoopJarStep(hadoopJarStep);

            steps.add(stepConfig);
        }

        String clusterId = "j-12007AYUJTF9O";
        AddJobFlowStepsRequest jobRequest = new AddJobFlowStepsRequest()
                .withJobFlowId(clusterId)
                .withSteps(steps.toArray(new StepConfig[steps.size()]));

        return emr.addJobFlowSteps(jobRequest);
    }

}
