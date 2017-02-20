package com.blackbaud.aws

import com.amazonaws.auth.SystemPropertiesCredentialsProvider
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.blackbaud.emr.EmrService
import com.blackbaud.emr.EmrClient
import com.blackbaud.emr.JobDefinition
import com.blackbaud.emr.JobInstance
import com.blackbaud.emr.VersionedJobDefinition
import com.blackbaud.testsupport.RandomGenerator
import spock.lang.Specification

import java.util.stream.IntStream
import java.util.stream.Stream

class EmrTest extends Specification {

    RandomGenerator aRandom = new RandomGenerator()
    EmrService emrService = new EmrService()
    EmrClient emrClient = new EmrClient()
    int maxId = 10000

    def setup() {
        AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(new SystemPropertiesCredentialsProvider())
                .build()
        emrService.s3 = s3
        emrClient.s3 = s3
    }

    def "should upload to s3"() {
        given:
        File pigScript = new File(getClass().getClassLoader().getResource("diff_groups.pig").file)

        JobDefinition jobDefinition = emrService.createPigJobDefinition("Group Rebuild", "bb-emr-group-rebuild")
        VersionedJobDefinition versionedJobDefinition = emrService.createVersionedJobDefinition(jobDefinition, pigScript.text)

        String siteId = "123"
        String groupId = "45678"
        JobInstance job = emrClient.createJobInstance(versionedJobDefinition, siteId, groupId)

        emrClient.gzipAndUploadFile(job, "source_group.gz", createIdStream())
        emrClient.gzipAndUploadFile(job, "target_group.gz", createIdStream())


        expect:
        true
    }

    private Stream createIdStream() {
        IntStream.range(0, maxId)
                .filter({ aRandom.coinFlip() })
                .boxed()
    }

}
