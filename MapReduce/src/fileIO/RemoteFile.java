package fileIO;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.Socket;

import messages.FileRequest;

public class RemoteFile implements Serializable {

	// TODO just extend File?

	private static final long serialVersionUID = -3103109845670699168L;
	protected final File file;
	private int fileByteSize;
	private String hostName;

	public RemoteFile(String filePath, String hostName, int fileByteSize) {
		this.file = new File(filePath);
		this.hostName = hostName;
		this.fileByteSize = fileByteSize;
	}

	public void load() throws IOException {

		Socket soc = null;
		BufferedOutputStream fileOutStream = null;
		try {

			System.out.println("Sending request for file " + file.getName() + " to " + hostName + ":" + FileServer.PORT);

			// Open socket to host machine
			soc = new Socket(hostName, FileServer.PORT);

			// Send request for file
			sendRequest(soc);

			// Get file stream
			fileOutStream = new BufferedOutputStream(new FileOutputStream(file));

			// Read in file over stream and write to disk
			getFile(soc, fileOutStream);

			// Update host machine to this machine as this partition changes the local copy
			hostName = InetAddress.getLocalHost().getHostName();

			System.out.println("Loaded remote file " + file.getPath());

		} catch (IOException e) {

			e.printStackTrace();

		} finally {
			// Close streams and socket
			if (fileOutStream != null) {
				fileOutStream.close();
			}
			if (soc != null) {
				soc.close();
			}
		}
	}

	private void sendRequest(Socket soc) throws IOException {
		// Generate partition request message
		FileRequest req = new FileRequest(file.getName(), fileByteSize);

		// Send message
		ObjectOutputStream objOutStream = new ObjectOutputStream(soc.getOutputStream());
		objOutStream.writeObject(req);
	}

	private void getFile(Socket soc, BufferedOutputStream fileOutStream) throws IOException {

		if (fileByteSize <= 0) {
			System.out.println("Remote file has no contents so not loading");
			return;
		}

		InputStream inStream = soc.getInputStream();

		// Read stream and write to file
		byte[] bytes = new byte[fileByteSize + 1]; // Note +1 important as leaves room for input stream to realize it is empty
		int nBytesRead;
		int current = 0;
		while( (nBytesRead = inStream.read(bytes, current, (bytes.length - current))) > -1) {
			current += nBytesRead;
		}
		fileOutStream.write(bytes, 0, bytes.length);
		fileOutStream.flush();

	}

	//------------------------------------------

	public void setHostName(String hostName) {
		// To help with corrected lookup mistakes in testing
		this.hostName = hostName;
	}

	public File getFile() {
		return file;
	}

	protected void setFileByteSize(int fileByteSize) {
		this.fileByteSize = fileByteSize;
	}

}
