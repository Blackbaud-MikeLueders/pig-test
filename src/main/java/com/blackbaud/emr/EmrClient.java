package com.blackbaud.emr;

import com.amazonaws.services.s3.AmazonS3;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

public class EmrClient {

    private AmazonS3 s3;

    public JobInstance createJobInstance(VersionedJobDefinition versionedJobDefinition, String... nameSegments) {
        if (nameSegments.length == 0) {
            throw new IllegalArgumentException("Empty name");
        }

        String name = String.join("/", nameSegments);
        return JobInstance.builder()
                .id(UUID.randomUUID())
                .name(name)
                .versionedJobDefinition(versionedJobDefinition)
                .build();
    }

    public void uploadInputFile(JobInstance job, File inputFile) {
        s3.putObject(job.getBucketName(), job.getInputKey(inputFile.getName()), inputFile);
    }

    public void gzipAndUploadFile(JobInstance job, String fileName, Stream inputStream) throws IOException {
        if (fileName.endsWith(".gz") == false) {
            throw new IllegalStateException("File name ${fileName} must end with .gz");
        }

        File gzipInputFile = writeGzipInputFile(inputStream);
        s3.putObject(job.getBucketName(), job.getInputKey(fileName), gzipInputFile);
    }

    File writeGzipInputFile(Stream stream) throws IOException {
        GzipFileWriter writer = new GzipFileWriter();
        stream.forEach(writer::addLine);
        return writer.finalizeStreamAndGetFile();
    }

    private static class GzipFileWriter {

        private File file;
        private FileOutputStream os;
        private GZIPOutputStream gzipOs;
        private StringBuilder builder = new StringBuilder();

        public GzipFileWriter() throws IOException {
            file = File.createTempFile("input", ".gz");
            os = new FileOutputStream(file);
            gzipOs = new GZIPOutputStream(os);
            builder = new StringBuilder(110000);
        }

        public void addLine(Object line) {
            builder.append(line).append('\n');
            if (builder.length() > 10000) {
                writeBuilderContentToStream();
            }
        }

        private void writeBuilderContentToStream() {
            if (builder.length() > 0) {
                try {
                    gzipOs.write(builder.toString().getBytes());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            builder.setLength(0);
        }

        public File finalizeStreamAndGetFile() throws IOException {
            writeBuilderContentToStream();
            gzipOs.close();
            os.close();
            return file;
        }

    }

}
