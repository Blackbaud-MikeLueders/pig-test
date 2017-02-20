package com.blackbaud.emr;

import com.amazonaws.services.s3.AmazonS3;

import java.util.UUID;

public class EmrService {

    private AmazonS3 s3;

    public JobDefinition createPigJobDefinition(String name, String bucketName) {
        return JobDefinition.builder()
                .id(UUID.randomUUID())
                .name(name)
                .bucketName(bucketName)
                .type(JobType.PIG)
                .build();
    }

    public VersionedJobDefinition createVersionedJobDefinition(JobDefinition jobDefinition, String scriptContent) {
        VersionedJobDefinition versionedJobDefinition =  VersionedJobDefinition.builder()
                .id(UUID.randomUUID())
                .scriptContent(scriptContent)
                .version(1)
                .description("initial script")
                .jobDefinition(jobDefinition)
                .build();

        String scriptName = versionedJobDefinition.getScriptName();
        s3.putObject(jobDefinition.getBucketName(), "scripts/" + scriptName, scriptContent);
        return versionedJobDefinition;
    }

}
