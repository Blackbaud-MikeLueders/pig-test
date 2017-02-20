package com.blackbaud.pigtest

import com.blackbaud.exec.ExecutionResult
import com.blackbaud.pig.PigLocalExecutor
import com.blackbaud.testsupport.RandomGenerator
import spock.lang.Specification

import java.util.zip.GZIPOutputStream

class PigTestSpec extends Specification {

    RandomGenerator aRandom = new RandomGenerator()
    File tmpDir
    File sourceFile
    File targetFile
    File pigScript

    def setup() {
        tmpDir = File.createTempDir()
        sourceFile = new File(tmpDir, "source_group.gz")
        targetFile = new File(tmpDir, "target_group.gz")
        pigScript = new File(getClass().getClassLoader().getResource("diff_groups.pig").file)
    }

    def cleanup() {
        tmpDir.deleteDir()
    }

    def "should diff files"() {
        given:
        List<Integer> sourceIds = createFileWithMaxIds(sourceFile, 1000)
        List<Integer> targetIds = createFileWithMaxIds(targetFile, 1000)

        when:
        PigLocalExecutor executor = new PigLocalExecutor()
                .hadoopBuildDir(new File("/Users/mike.lueders/IdeaProjects/Blackbaud/pig-test/build/hadoop/"))
                .pigScript(pigScript)
                .inputFiles(sourceFile, targetFile)
        ExecutionResult result = executor.execute()

        then:
        assert result.exitCode == 0

        and:
        assertExpectedResults(executor, sourceIds, targetIds)
    }

    private List<Integer> createFileWithMaxIds(File file, int maxId) {
        List<Integer> idList = []
        file.withOutputStream { OutputStream os ->
            GZIPOutputStream gzipOs = new GZIPOutputStream(os);

            StringBuilder builder = new StringBuilder()
            for (int i = 0; i < maxId; i++) {
                if (aRandom.coinFlip()) {
                    idList << i
                    builder.append(i).append('\n')
                }
                if (builder.size() > 100000) {
                    gzipOs.write(builder.toString().bytes)
                    builder.setLength(0)
                }
            }
            if (builder.size() > 0) {
                gzipOs.write(builder.toString().bytes)
            }
            gzipOs.close()
        }
        idList
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
