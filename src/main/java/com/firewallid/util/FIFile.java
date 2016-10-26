/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URISyntaxException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class FIFile {

    public static final Logger LOG = LoggerFactory.getLogger(FIFile.class);

    public static File getWorkingDirectory(Class clazz) throws URISyntaxException {
        return new File(clazz.getProtectionDomain().getCodeSource().getLocation().toURI().getPath()).getParentFile();
    }

    public static String getWorkingDirectoryPath(Class clazz) throws URISyntaxException {
        return getWorkingDirectory(clazz).getPath();
    }

    public static void copyFolderToWorkingDirectory(Class clazz, String folderPath, boolean override) throws IOException, URISyntaxException {
        File folder = new File(folderPath);
        File workingDirectory = getWorkingDirectory(clazz);
        if (override || !(new File(workingDirectory.getPath() + "/" + folder.getName()).exists())) {
            FileUtils.copyDirectoryToDirectory(folder, workingDirectory);
        }
        LOG.info("Copy folder to working directory: " + folderPath + " -> " + workingDirectory.getPath());
    }

    public static void copyFolder(String sourceFolderPath, String destFolderPath, boolean override) throws IOException, URISyntaxException {
        File sourceFolder = new File(sourceFolderPath);
        File destFolder = new File(destFolderPath);
        if (override || !(new File(destFolderPath + "/" + sourceFolder.getName()).exists())) {
            FileUtils.copyDirectoryToDirectory(sourceFolder, destFolder);
        }
        LOG.info("Copy folder: " + sourceFolderPath + " -> " + destFolderPath);
    }

    public static void copyMergeDeleteHDFS(String srcDir, String dstFile) throws IOException {
        deleteExistHDFSPath(dstFile);

        Configuration hadoopConf = new Configuration();
        try (FileSystem fileSystem = FileSystem.get(hadoopConf)) {
            FileUtil.copyMerge(fileSystem, new Path(srcDir), fileSystem, new Path(dstFile), true, hadoopConf, null);
            LOG.info("File " + dstFile + " created. Temporary directory " + srcDir + " deleted");
        }
    }

    /* Generate full path */
    public static String generateFullPath(String folderfullpath, String name) {
        String fullPath;
        if (folderfullpath.endsWith(File.separator)) {
            fullPath = folderfullpath + name;
        } else {
            fullPath = folderfullpath + File.separator + name;
        }
        return fullPath;
    }

    /* Return true if path exist */
    public static boolean isExistsHDFSPath(String fullPath) throws IOException {
        Configuration hadoopConf = new Configuration();
        FileSystem fileSystem = FileSystem.get(hadoopConf);
        Path path = new Path(fullPath);

        return fileSystem.exists(path);
    }

    /* If path exist, it deleted */
    public static void deleteExistHDFSPath(String fullPath) throws IOException {
        Configuration hadoopConf = new Configuration();
        FileSystem fileSystem = FileSystem.get(hadoopConf);
        Path path = new Path(fullPath);

        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
            LOG.info("Deleted " + fullPath);
        }
    }

    /* Write text to hdfs file. If path is exist, path deleted */
    public static void writeStringToHDFSFile(String pathFile, String text) throws IOException {
        Configuration hadoopConf = new Configuration();
        FileSystem fileSystem = FileSystem.get(hadoopConf);
        Path path = new Path(pathFile);

        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }

        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fileSystem.create(path)))) {
            bw.write(text);
        }
        LOG.info("Created file: " + pathFile);
    }

}
