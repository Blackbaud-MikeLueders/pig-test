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
public class JobDefinition {

    private UUID id;
    private String name;
    private String bucketName;
    private JobType type;

}
