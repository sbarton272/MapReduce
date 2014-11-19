package fileIO;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import master.ConfigLoader;
import messages.FileRequest;

public class FileServer extends Thread {

	public static final int DEFAULT_PORT = 5232;
	public static final String DEFAULT_REMOTE_FILE_DIR = "/tmp";
	public static final String FILE_SERVER_CONFIG_FILE = "fileIO/fileserver.config";
	private final int port;
	private final String remoteFileDir;

	public FileServer(String remoteFileDir) {
		this.remoteFileDir = remoteFileDir;
		port = DEFAULT_PORT;
		initDir();
	}

	public FileServer() throws Exception {
		ConfigLoader config = new ConfigLoader(FILE_SERVER_CONFIG_FILE);
		if (config.getFileServerPort() != 0) {
			port = config.getFileServerPort();
		} else {
			port = DEFAULT_PORT;
		}

		if (config.getFileServerDir() != null) {
			remoteFileDir = config.getFileServerDir();
		} else {
			remoteFileDir = DEFAULT_REMOTE_FILE_DIR;
		}

		initDir();
	}

	@Override
	public void run() {

		ServerSocket serverSoc = null;
		try {
			serverSoc = new ServerSocket(port);

			// Run indefinitely
			while(true){

				// Accept request
				final Socket soc = serverSoc.accept();

				// Spawn new thread to handle request
				Thread serveFileThread = new Thread(new Runnable() {
					@Override
					public void run() {
						serveFile(soc);
					}
				});
				serveFileThread.start();
			}

		} catch (IOException e) {
			//System.out.println("File Server Failure: an error occurred in the file server.");

		} finally {
			try {
				if (serverSoc != null) serverSoc.close();
			} catch (IOException e) {
				// Do nothing b/c server socket cannot close because it only closes on termination
			}
		}

	}

	private void serveFile(Socket soc) {

		ObjectInputStream reqStream = null;
		BufferedInputStream fileStream = null;
		try {
			// Decode request
			reqStream = new ObjectInputStream(soc.getInputStream());
			FileRequest req = (FileRequest) reqStream.readObject();

			// Find file
			File requestedFile = new File(remoteFileDir, req.getFileName());

			// Ensure file exists, length is same as request and that it is a non-empty file (cannot transmit empty file)
			byte[] fileBuf = new byte[(int) requestedFile.length()];
			int writeLen = 0;
			if (requestedFile.exists()
					&& (requestedFile.length() == req.getByteSize())
					&& (requestedFile.length() != 0)) {

				// Read in file to buffer
				fileStream = new BufferedInputStream(new FileInputStream(requestedFile));
				fileStream.read(fileBuf, 0, fileBuf.length);
				writeLen = fileBuf.length;
			}

			// Write buffer over socket, writes nothing if error
			OutputStream outputStream = soc.getOutputStream();
			outputStream.write(fileBuf,0, writeLen);
			outputStream.flush();

		} catch (IOException | ClassNotFoundException e) {
		} finally {

			// Close files
			try {
				if (reqStream != null) reqStream.close();
				if (fileStream != null) fileStream.close();
				soc.close();
			} catch (IOException e) {
				System.out.println("File Server Error: Unable to close connection.");
			}
		}

	}

	private void initDir() {
		// Make directory if not present
		File tmpDir = new File(remoteFileDir);
		tmpDir.mkdir();
	}

}
