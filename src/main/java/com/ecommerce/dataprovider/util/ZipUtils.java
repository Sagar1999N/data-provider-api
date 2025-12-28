package com.ecommerce.dataprovider.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Utility for secure ZIP file extraction.
 * Prevents Zip Slip vulnerability by validating target paths.
 */
@Component
public class ZipUtils {

    private static final Logger logger = LoggerFactory.getLogger(ZipUtils.class);
    private static final int BUFFER_SIZE = 8192;

    /**
     * Extracts a ZIP file to the specified destination directory.
     *
     * @param zipFilePath Path to the ZIP file
     * @param destDirPath Destination directory path
     * @throws IOException if extraction fails
     */
    public void unzip(String zipFilePath, String destDirPath) throws IOException {
        logger.info("📦 Extracting ZIP file: {} to {}", zipFilePath, destDirPath);
        
        Path destDir = Paths.get(destDirPath);
        Files.createDirectories(destDir);

        try (ZipInputStream zis = new ZipInputStream(new FileInputStream(zipFilePath))) {
            ZipEntry entry = zis.getNextEntry();
            
            while (entry != null) {
                Path targetPath = sanitizePath(destDir, entry.getName());
                
                if (entry.isDirectory()) {
                    Files.createDirectories(targetPath);
                } else {
                    Files.createDirectories(targetPath.getParent());
                    extractFile(zis, targetPath);
                }
                
                zis.closeEntry();
                entry = zis.getNextEntry();
            }
        }
        
        logger.info("✅ Successfully extracted files to {}", destDirPath);
    }

    /**
     * Prevents Zip Slip vulnerability by ensuring target path is within destination.
     */
    private Path sanitizePath(Path destDir, String entryName) throws IOException {
        Path targetPath = destDir.resolve(entryName).normalize();
        
        if (!targetPath.startsWith(destDir)) {
            throw new IOException("Zip Slip detected! Entry is outside target directory: " + entryName);
        }
        
        return targetPath;
    }

    /**
     * Extracts a single file from the ZIP input stream.
     */
    private void extractFile(ZipInputStream zis, Path targetPath) throws IOException {
        try (OutputStream fos = Files.newOutputStream(targetPath)) {
            byte[] buffer = new byte[BUFFER_SIZE];
            int len;
            while ((len = zis.read(buffer)) > 0) {
                fos.write(buffer, 0, len);
            }
        }
    }
}