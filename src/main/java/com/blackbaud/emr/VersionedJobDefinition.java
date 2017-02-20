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
public class VersionedJobDefinition {

    private UUID id;
    private int version;
    private String scriptContent;
    private String description;
    private JobDefinition jobDefinition;

    public String getScriptName() {
        return jobDefinition.getName().replaceAll("\\s+", "_") + "_V" + version;
    }

    public String getBucketName() {
        return jobDefinition.getBucketName();
    }

}
