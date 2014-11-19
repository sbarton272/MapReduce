package tests;

import java.io.File;
import java.io.IOException;
import java.util.List;

import fileIO.FileServer;
import fileIO.Partition;
import fileIO.RemoteFile;
import fileIO.RemoteFileException;

public class testRemoteFile {

	public static void main(String[] args) throws IOException {

		// Test with partition
		List<Partition<String>> partitions = Partition.fileToPartitions("resources/testRemote.txt", 5);
		Partition<String> p = partitions.get(0);
		p.setHostName("localhost");

		// Move the physical partition so that the partition thinks it needs to load remote when opened for read
		File curF = new File(p.getFilePath());
		File newF = new File("resources", p.getFile().getName());
		System.out.println("Moving " + curF + " to " + newF);
		curF.renameTo(newF);
		System.out.println(p.readAllContents());

		// Clean up both copies of file
		p.delete();
		newF.deleteOnExit();

		// Test with random file
		RemoteFile file = new RemoteFile("/tmp/testRemote.txt", "localhost", FileServer.DEFAULT_PORT, (int)new File("resources/testRemote.txt").length());
		file.load();
		file.getFile().deleteOnExit();
		System.out.println(file.getFile().exists());
		System.out.println(file.getFile().length());

		// Test with empty file
		file = new RemoteFile("/tmp/testRemoteEmpty.txt", "localhost", FileServer.DEFAULT_PORT, (int)new File("resources/testRemoteEmpty.txt").length());
		try {
			file.load();
		} catch (RemoteFileException e) {
			System.out.println("Threw correct error");
		}

		// Test with missing
		file = new RemoteFile("/tmp/testRemoteMissing.txt", "localhost", FileServer.DEFAULT_PORT, 42);
		try {
			file.load();
		} catch (RemoteFileException e) {
			System.out.println("Threw correct error");
		}

	}

}
