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


public class FileServer extends Thread {

	public static final int PORT = 13267;
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

				Socket soc = null;
				ObjectInputStream reqStream = null;
				BufferedInputStream fileStream = null;
				OutputStream outputStream = null;
				try {
					// Accept request
					soc = serverSoc.accept();

					// TODO spawn new thread

					// Decode request
					reqStream = new ObjectInputStream(soc.getInputStream());
					FileRequest req = (FileRequest) reqStream.readObject();

					System.out.println("Recieved request for " + req.getFileName());

					// Find file
					File requestedFile = new File(remoteFileDir, req.getFileName());

					System.out.println("Requested file available " + requestedFile + ":" + requestedFile.length());

					// Ensure length is same as request
					if (requestedFile.length() != req.getByteSize()) {
						// TODO respond with error
					}

					// Read in file to buffer
					fileStream = new BufferedInputStream(new FileInputStream(requestedFile));
					byte[] fileBuf = new byte[(int) requestedFile.length()];
					fileStream.read(fileBuf, 0, fileBuf.length);

					// TODO bug if request sent for empty file, hangs

					// Write buffer over socket
					outputStream = soc.getOutputStream();
					outputStream.write(fileBuf,0, fileBuf.length);
					outputStream.flush();

					System.out.println("Completed request for " + req.getFileName());


				} catch (IOException | ClassNotFoundException e) {
					e.printStackTrace();
				} finally {
					if (reqStream != null) reqStream.close();
					if (fileStream != null) fileStream.close();
					if (soc != null) soc.close();
					System.out.println("Connection closed");
				}
			}

		} catch (IOException e) {
			// TODO

		} finally {
			try {
				if (serverSoc != null) serverSoc.close();
			} catch (IOException e) {
				// Do nothing if server socket cannot close because it only closes on termination
			}
		}

	}

}
