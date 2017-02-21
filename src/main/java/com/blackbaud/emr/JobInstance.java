package com.blackbaud.emr;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class JobInstance {

    private UUID id;
    private String name;
    private VersionedJobDefinition versionedJobDefinition;

    public String getS3ScriptPath() {
        return getS3Path(getScriptPath());
    }

    public String getS3InputPath() {
        return getS3Path(getInputPath());
    }

    public String getS3OutputPath() {
        return getS3Path(getOutputPath());
    }

    private String getS3Path(String keyPath) {
        return "s3://" + getBucketName() + "/" + keyPath;
    }

    public String getBucketName() {
        return versionedJobDefinition.getBucketName();
    }

    public String getScriptPath() {
        return versionedJobDefinition.getScriptPath();
    }

    public String getInputPath() {
        return "jobs/" + name + "/input";
    }

    public String getOutputPath() {
        return "jobs/" + name + "/output";
    }

    public String getInputKey(String fileName) {
        return getInputPath() + "/" + fileName;
    }

}

