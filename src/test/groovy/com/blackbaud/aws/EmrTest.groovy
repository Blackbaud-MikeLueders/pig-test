package com.blackbaud.aws

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.SystemPropertiesCredentialsProvider
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsResult
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.blackbaud.emr.EmrClient
import com.blackbaud.emr.EmrService
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

    def setup() {
        AWSCredentialsProvider credentialsProvider = new SystemPropertiesCredentialsProvider()

        AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .build()

        AmazonElasticMapReduce emr = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .build()

        emrClient.s3 = s3
        emrService.s3 = s3
        emrService.emr = emr;
    }

    def "should upload to s3"() {
        given:
        File pigScript = new File(getClass().getClassLoader().getResource("diff_groups.pig").file)

        JobDefinition jobDefinition = emrService.createPigJobDefinition("Group Rebuild", "bb-emr-group-rebuild")
        VersionedJobDefinition versionedJobDefinition = emrService.createVersionedJobDefinition(jobDefinition, pigScript.text)

        int jobCount = 4
        List<JobInstance> jobs = []
        for (int i = 0; i < jobCount; i++) {
            String siteId = aRandom.siteId()
            String groupId = aRandom.intBetween(0, 100000)
            JobInstance job = emrClient.createJobInstance(versionedJobDefinition, siteId, groupId)

            int groupSize = getGroupSize()
            println "Creating job, ${job.id}, size=${groupSize}"
            emrClient.gzipAndUploadFile(job, "source_group.gz", createIdStream(groupSize))
            emrClient.gzipAndUploadFile(job, "target_group.gz", createIdStream(groupSize))
            jobs << job
        }

        jobs.each { JobInstance job ->
            println "Submitting job, id=${job.id}"
        }
        AddJobFlowStepsResult result = emrService.submitJobs(jobs.toArray(new JobInstance[jobs.size()]))
        println "Jobs submitted, stepIds=${result.stepIds}"

        expect:
        true
    }

    private int getGroupSize() {
        if (aRandom.weightedCoinFlip(5)) {
            return aRandom.intBetween(10000000, 40000000)
        } else if (aRandom.weightedCoinFlip(5)) {
            return aRandom.intBetween(1000000, 10000000)
        } else if (aRandom.weightedCoinFlip(10)) {
            return aRandom.intBetween(100000, 1000000)
        } else {
            return aRandom.intBetween(100, 100000)
        }
    }

    private Stream createIdStream(int maxId) {
        IntStream.range(0, maxId)
                .filter({ aRandom.coinFlip() })
                .boxed()
    }

}
