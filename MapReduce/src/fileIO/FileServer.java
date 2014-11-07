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

	public static final int PORT = 9945;

	@Override
	public void run() {
		// TODO Auto-generated method stub

		ServerSocket serverSoc = null;
		try {
			serverSoc = new ServerSocket(PORT);

			// Run indefinitely
			while(true){

				ObjectInputStream reqStream = null;
				BufferedInputStream fileStream = null;
				OutputStream outputStream = null;
				try {
					// Accept request
					Socket soc = serverSoc.accept();

					// Decode request
					reqStream = new ObjectInputStream(
							soc.getInputStream());
					FileRequest req = (FileRequest) reqStream.readObject();

					// Find file
					File requestedFile = new File(req.getFilePath());

					// Ensure length is same as request
					if (requestedFile.length() != req.getByteSize()) {
						// TODO respond with error
					}

					// Read in file to buffer
					fileStream = new BufferedInputStream(new FileInputStream(requestedFile));
					byte[] fileBuf = new byte[(int)req.getByteSize()];
					fileStream.read(fileBuf, 0, fileBuf.length);

					// Write buffer over socket
					outputStream = soc.getOutputStream();
					outputStream.write(fileBuf,0, fileBuf.length);
				} catch (IOException | ClassNotFoundException e) {

				} finally {
					if (reqStream != null) reqStream.close();
					if (fileStream != null) fileStream.close();
					if (outputStream != null) outputStream.close();
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
