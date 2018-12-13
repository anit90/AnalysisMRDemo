package packageDemo;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class HadoopConnections {
	private static final Configuration configuration = new Configuration();

	private static boolean bInit = false;

	static {
		try {
			Intialize();
		} catch (Exception er) {
		}
	}

	private static void Intialize() throws IllegalArgumentException, Exception {
		if (bInit)
			return;
		synchronized (HadoopConnections.class) {
			configuration.addResource(new BufferedInputStream(new FileInputStream("/etc/hadoop/conf/core-site.xml")));
			configuration.addResource(new BufferedInputStream(new FileInputStream("/etc/hadoop/conf/hdfs-site.xml")));
			configuration.addResource(new BufferedInputStream(new FileInputStream("/etc/hadoop/conf/yarn-site.xml")));
			bInit = true;
		}
	}

	public static Configuration getHadoopConfiguration() throws Exception {
		Intialize();
		return configuration;
	}

	public static FileSystem getFileSystemObject(FileSystemType fsType) throws IllegalArgumentException, Exception {
		Intialize();
		FileSystem fs = null;

		switch (fsType) {

		case LOCAL:
			fs = FileSystem.getLocal(configuration);
			break;
		case HDFS:
		default:
			fs = FileSystem.get(configuration);
			break;

		}

		return fs;
	}

	public enum FileSystemType {
		HDFS, LOCAL
	}

	public static FileSystem getFileSystem(String strFilePath) throws IOException, URISyntaxException {

		FileSystem fs = null;

		if (strFilePath.startsWith("hdfs://")) {
			fs = FileSystem.get(new URI(strFilePath), configuration);
		} else {
			fs = FileSystem.getLocal(configuration).getRawFileSystem();
		}

		return fs;
	}

}
