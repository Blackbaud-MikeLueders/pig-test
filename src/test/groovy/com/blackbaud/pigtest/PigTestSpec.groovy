package com.blackbaud.pigtest

import com.blackbaud.testsupport.RandomGenerator
import spock.lang.Specification

import java.util.zip.GZIPOutputStream

class PigTestSpec extends Specification {

    RandomGenerator aRandom = new RandomGenerator()
    File sourceFile = new File("/Users/mike.lueders/IdeaProjects/Blackbaud/pig-test/test-data/source_group.txt")
    File targetFile = new File("/Users/mike.lueders/IdeaProjects/Blackbaud/pig-test/test-data/target_group.txt")
    File pigScript = new File("/Users/mike.lueders/IdeaProjects/Blackbaud/pig-test/test-data/diff_groups.pig")

//    @Ignore
    def "create test files"() {
        given:
        createFileWithMaxIds(sourceFile, 40000000)
        createFileWithMaxIds(targetFile, 40000000)

        pigScript << """
-- Process Inputs
source = LOAD '\$INPUT/source_group.txt' USING PigStorage('\n') AS source_member: long;
target = LOAD '\$INPUT/target_group.txt' USING PigStorage('\n') AS target_member: long;

-- Combine Data
combined = JOIN source BY source_member FULL OUTER, target BY target_member;

-- Output Data
SPLIT combined INTO member_to_remove IF target_member IS NULL,
                    member_to_add IF source_member IS NULL;

members_to_remove = FOREACH member_to_remove GENERATE source_member;
members_to_add = FOREACH member_to_add GENERATE target_member;

STORE members_to_remove INTO '\$OUTPUT/members_to_remove' USING PigStorage();
STORE members_to_add INTO '\$OUTPUT/members_to_add' USING PigStorage();
"""

        PigLocalExecutor executor = new PigLocalExecutor()
        executor.setHadoopDir(new File("/Users/mike.lueders/IdeaProjects/Blackbaud/pig-test/build/hadoop/"))
        executor.execute(pigScript, [sourceFile, targetFile])

        expect:
        true
    }

    def "should have a basic test"() {
        given:
        PigLocalExecutor executor = new PigLocalExecutor()
        executor.setHadoopDir(new File("/Users/mike.lueders/IdeaProjects/Blackbaud/pig-test/build/hadoop/"))
        executor.execute(pigScript, [sourceFile, targetFile])

        expect:
        true
    }

    private void createFileWithMaxIds(File file, int maxId) {
        file.withWriter { BufferedWriter writer ->
            StringBuilder builder = new StringBuilder()
            for (int i = 0; i < maxId; i++) {
                if (aRandom.coinFlip()) {
                    builder.append(i).append('\n')
                }
                if (builder.size() > 100000) {
                    writer.append(builder.toString())
                    builder.setLength(0)
                }
            }
            if (builder.size() > 0) {
                writer.append(builder.toString())
            }
        }
    }

    /*
    private static void compressGzipFile(String file, String gzipFile) {
        try {
            FileInputStream fis = new FileInputStream(file);
            FileOutputStream fos = new FileOutputStream(gzipFile);
            GZIPOutputStream gzipOS = new GZIPOutputStream(fos);
            byte[] buffer = new byte[1024];
            int len;
            while((len=fis.read(buffer)) != -1){
                gzipOS.write(buffer, 0, len);
            }
            //close resources
            gzipOS.close();
            fos.close();
            fis.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
     */
    /*

package com.journaldev.files;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GZIPExample {

    public static void main(String[] args) {
        String file = "/Users/pankaj/sitemap.xml";
        String gzipFile = "/Users/pankaj/sitemap.xml.gz";
        String newFile = "/Users/pankaj/new_sitemap.xml";

        compressGzipFile(file, gzipFile);

        decompressGzipFile(gzipFile, newFile);
    }

    private static void decompressGzipFile(String gzipFile, String newFile) {
        try {
            FileInputStream fis = new FileInputStream(gzipFile);
            GZIPInputStream gis = new GZIPInputStream(fis);
            FileOutputStream fos = new FileOutputStream(newFile);
            byte[] buffer = new byte[1024];
            int len;
            while((len = gis.read(buffer)) != -1){
                fos.write(buffer, 0, len);
            }
            //close resources
            fos.close();
            gis.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static void compressGzipFile(String file, String gzipFile) {
        try {
            FileInputStream fis = new FileInputStream(file);
            FileOutputStream fos = new FileOutputStream(gzipFile);
            GZIPOutputStream gzipOS = new GZIPOutputStream(fos);
            byte[] buffer = new byte[1024];
            int len;
            while((len=fis.read(buffer)) != -1){
                gzipOS.write(buffer, 0, len);
            }
            //close resources
            gzipOS.close();
            fos.close();
            fis.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
     */
}
