package com.vulcan.sau.util;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class RawCatalogFileUtil {
	private static final SimpleDateFormat date_pattern = new SimpleDateFormat("yyyyMMddHHmm", Locale.ENGLISH);
	private final String quarantineDirectory;

	public RawCatalogFileUtil(String quarantineDirectory) {
		this.quarantineDirectory = quarantineDirectory;

		File qd = new File(quarantineDirectory);
		if(!qd.exists()) qd.mkdirs();
	}

	public boolean assertCatalogFileSize(File newCatalog, File existingCatalog, long fileSizeMinimumThresholdInBytes) throws MalformedURLException, IOException {
		if (existingCatalog.exists()) {
			// If new catalog file size is more than 10% smaller, we quarantine it and signal caller.
			if (newCatalog.length() < (existingCatalog.length() * 0.90)) {
				quarantineCatalogFile(newCatalog);
				return false;
			}
		}
		else {
			if (newCatalog.length() < fileSizeMinimumThresholdInBytes) {
				quarantineCatalogFile(newCatalog);
				return false;
			}
		}

		return true;
	}

	public File archivePreviousCatalog(File catalogToArchive) {
		if (catalogToArchive.exists()) {
			File archiveFile = new File(catalogToArchive.getParent(), catalogToArchive.getName() + "." + date_pattern.format(new Date(catalogToArchive.lastModified())));
			if (archiveFile.exists()) {
				catalogToArchive.delete();
			}
			else {
				catalogToArchive.renameTo(archiveFile);
			}
			return archiveFile;
		}
		else {
			return null;
		}
	}

	public void quarantineCatalogFile(File catalogFile) {
		catalogFile.renameTo(new File(quarantineDirectory, catalogFile.getName() + "." + date_pattern.format(new Date(catalogFile.lastModified()))));
	}
}
