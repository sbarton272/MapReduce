package tests;

import java.io.IOException;

import fileIO.FileServer;
import fileIO.Partition;

public class testFileServer {

	public static void main(String[] args) throws IOException {

		// Start file server thread
		FileServer fileServer = new FileServer();
		fileServer.start();

		// TODO determine permissions

		Partition<String> partition = new Partition<String>(5);
		partition.setHostName("localhost");
		partition.openRead();
		System.out.println(partition.readAllContents());
		partition.closeRead();

	}

}
