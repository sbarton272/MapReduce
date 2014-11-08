package tests;

import java.io.IOException;

import fileIO.FileServer;

public class testFileServer {

	public static void main(String[] args) throws IOException {

		// Start file server thread
		FileServer fileServer = new FileServer("resources");
		fileServer.start();

	}

}
