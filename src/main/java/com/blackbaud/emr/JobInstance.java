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

    public String getBucketName() {
        return versionedJobDefinition.getBucketName();
    }

    public String getInputKey(String fileName) {
        return "jobs/" + name + "/input/" + fileName;
    }

}

