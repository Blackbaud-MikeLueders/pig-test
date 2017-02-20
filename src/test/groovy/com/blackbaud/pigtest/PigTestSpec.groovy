package com.blackbaud.pigtest

import com.blackbaud.exec.ExecutionResult
import com.blackbaud.exec.pig.PigLocalExecutor
import com.blackbaud.testsupport.RandomGenerator
import spock.lang.Specification

import java.util.stream.IntStream
import java.util.stream.Stream

class PigTestSpec extends Specification {

    RandomGenerator aRandom = new RandomGenerator()
    PigLocalExecutor executor = new PigLocalExecutor()
            .hadoopBuildDir(new File("/Users/mike.lueders/IdeaProjects/Blackbaud/pig-test/build/hadoop/"))
    File pigScript

    def setup() {
        pigScript = new File(getClass().getClassLoader().getResource("diff_groups.pig").file)
    }

    def "should diff files"() {
        given:
        int maxGroupSize = 1000
        List<Integer> sourceIds = []
        List<Integer> targetIds = []

        when:
        ExecutionResult result = executor.pigScript(pigScript)
                .gzipInputFile("source_group.gz", createIdStream(maxGroupSize, sourceIds))
                .gzipInputFile("target_group.gz", createIdStream(maxGroupSize, targetIds))
                .execute()

        then:
        assert result.exitCode == 0
        assertExpectedResults(executor, sourceIds, targetIds)
    }

    private Stream createIdStream(int maxId, List idList) {
        IntStream.range(0, maxId)
                .filter({ aRandom.coinFlip() })
                .peek({ i -> idList << i })
                .boxed()
    }

    private void assertExpectedResults(PigLocalExecutor executor, List<Integer> sourceIds, List<Integer> targetIds) {
        List<Integer> actualIdsToAdd = readOutputFile(executor, "members_to_add.gz")
        List<Integer> actualIdsToRemove = readOutputFile(executor, "members_to_remove.gz")

        List<Integer> expectedIdsToAdd = targetIds - sourceIds
        List<Integer> expectedIdsToRemove = sourceIds - targetIds

        assert expectedIdsToAdd == actualIdsToAdd
        assert expectedIdsToRemove == actualIdsToRemove
    }

    private List<Integer> readOutputFile(PigLocalExecutor executor, String fileName) {
        executor.readGzipOutputFile(fileName).collect {
            Integer.parseInt(it)
        }
    }

}
