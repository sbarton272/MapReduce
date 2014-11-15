package fileIO;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import messages.FileRequest;

// TODO logger for print statements
public class FileServer extends Thread {

	public static final int PORT = 5267;
	public static final String DEFAULT_REMOTE_FILE_DIR = "/tmp";
	private final String remoteFileDir;

	public FileServer(String remoteFileDir) {
		this.remoteFileDir = remoteFileDir;
	}

	public FileServer() {
		this(DEFAULT_REMOTE_FILE_DIR);
	}

	@Override
	public void run() {

		ServerSocket serverSoc = null;
		try {
			serverSoc = new ServerSocket(PORT);

			System.out.println("File server online");

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
			System.out.println("File server ran into trouble");
			e.printStackTrace();

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

			System.out.println("FileServer: Recieved request for " + req.getFileName() + ":" + req.getByteSize());

			// Find file
			File requestedFile = new File(remoteFileDir, req.getFileName());

			System.out.println("FileServer: Requested file available " + requestedFile + ":" + requestedFile.length());

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
				System.out.println("FileServer: Read file to buf " + req.getFileName());
			}

			// Write buffer over socket, writes nothing if error
			OutputStream outputStream = soc.getOutputStream();
			outputStream.write(fileBuf,0, writeLen);
			outputStream.flush();

			System.out.println("FileServer: Completed request for " + req.getFileName());

		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		} finally {

			// Close files
			try {
				if (reqStream != null) reqStream.close();
				if (fileStream != null) fileStream.close();
				soc.close();
				System.out.println("FileServer: Connection closed");
			} catch (IOException e) {
				System.out.println("FileServer: Unable to close connection.");
			}
		}

	}

}
