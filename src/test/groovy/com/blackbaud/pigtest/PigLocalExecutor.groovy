package com.blackbaud.pigtest

import com.google.common.io.Files
import lombok.Data


@Data
class PigLocalExecutor {

    private File inputDir
    private File outputDir
    private File scriptDir

    void setHadoopDir(File hadoopBuildDir) {
        inputDir = new File(hadoopBuildDir, "input")
        outputDir = new File(hadoopBuildDir, "output")
        scriptDir = new File(hadoopBuildDir, "scripts")
    }

    public File execute(File script, List<File> inputFiles) {
        inputFiles.each { File inputFile ->
            Files.copy(inputFile, new File(inputDir, inputFile.name))
        }
        Files.copy(script, new File(scriptDir, script.name))

        null
    }

}
