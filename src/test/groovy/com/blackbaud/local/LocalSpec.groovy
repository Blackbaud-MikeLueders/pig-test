package com.blackbaud.local

import com.blackbaud.exec.SystemExecutor
import com.blackbaud.testsupport.RandomGenerator
import spock.lang.Specification

import java.nio.file.Files
import java.nio.file.Paths
import java.util.stream.IntStream
import java.util.stream.Stream

class LocalSpec extends Specification {

    File localDir = new File("/Users/mike.lueders/IdeaProjects/Blackbaud/pig-test/build/localtest")
    RandomGenerator aRandom = new RandomGenerator()
    SystemExecutor executor = new SystemExecutor()
    int maxId = 40000000

    def "one"() {
        given:
        Iterator<Integer> sourceIterator = [1, 2, 3].iterator()
        Iterator<Integer> targetIterator = [4, 5, 6].iterator()

        when:
        GroupDiff diff = diff(sourceIterator, targetIterator)

        then:
        assert diff.idsToAdd == [4, 5, 6]
        assert diff.idsToRemove == [1, 2, 3]
    }

    def "two"() {
        Iterator<Integer> sourceIterator = [4, 5, 6].iterator()
        Iterator<Integer> targetIterator = [1, 2, 3].iterator()

        when:
        GroupDiff diff = diff(sourceIterator, targetIterator)

        then:
        assert diff.idsToAdd == [1, 2, 3]
        assert diff.idsToRemove == [4, 5, 6]
    }

    def "three"() {
        Iterator<Integer> sourceIterator = [1, 2, 3].iterator()
        Iterator<Integer> targetIterator = [2, 3, 4].iterator()

        when:
        GroupDiff diff = diff(sourceIterator, targetIterator)

        then:
        println diff.idsToAdd
        println diff.idsToRemove
        assert diff.idsToAdd == [4]
        assert diff.idsToRemove == [1]
    }

    def "should diff files"() {
        given:
        println "Generating source and target files..."
        File sourceFile = createSortedMemberFile("source.txt")
        File targetFile = createSortedMemberFile("target.txt")

        println "Starting..."
        long start = System.currentTimeMillis()
        Stream<String> sourceStream = Files.lines(Paths.get(sourceFile.absolutePath))
        Stream<String> targetStream = Files.lines(Paths.get(targetFile.absolutePath))
        Iterator<String> sourceIterator = sourceStream.iterator()
        Iterator<String> targetIterator = targetStream.iterator()

        when:
        GroupDiff diff = diff(sourceIterator, targetIterator)
        println "DONE: ${System.currentTimeMillis() - start}"

        then:
        println "ADD: " + diff.idsToAdd.size()
        println "REM: " + diff.idsToRemove.size()
        true
    }

    private File createSortedMemberFile(String fileName) {
        File outFile = new File(localDir, fileName)
        outFile.withWriter { BufferedWriter writer ->
            createIdStream().forEach({ int id ->
                writer.writeLine(id.toString())
            })
        }
        executor.execute("sort -n ${outFile.absolutePath} -o ${outFile.absolutePath}.sorted")
        new File(outFile.parentFile, "${outFile.name}.sorted")
    }

    private Stream createIdStream() {
        IntStream.range(0, maxId)
                .map({ i -> maxId - i - 1 })
                .filter({ aRandom.coinFlip() })
                .boxed()
    }


    private GroupDiff diff(Iterator sourceIterator, Iterator targetIterator) {
        GroupDiff diff = new GroupDiff()
        Integer source = null
        Integer target = null

        while (sourceIterator.hasNext() || targetIterator.hasNext()) {
            if (source == target) {
                source = sourceIterator.hasNext() ? sourceIterator.next() as Integer : null
                target = targetIterator.hasNext() ? targetIterator.next() as Integer : null
            }

            while ((target != null) && (source == null || target < source)) {
                diff.idsToAdd << target
                target = targetIterator.hasNext() ? targetIterator.next() as Integer : null
            }

            while ((source != null && (target == null || source < target))) {
                diff.idsToRemove << source
                source = sourceIterator.hasNext() ? sourceIterator.next() as Integer : null
            }
        }
        diff
    }

    static class GroupDiff {
        List<Integer> idsToAdd = []
        List<Integer> idsToRemove = []
    }

}
