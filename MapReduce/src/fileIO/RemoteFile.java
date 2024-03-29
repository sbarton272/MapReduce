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

	private static final long serialVersionUID = -3103109845670699168L;
	protected File file;
	private int fileByteSize;
	private String hostName;
	private final int hostPort;

	public RemoteFile(String filePath, String hostName, int hostPort, int fileByteSize) {
		this.file = new File(filePath);
		this.hostName = hostName;
		this.hostPort = hostPort;
		this.fileByteSize = fileByteSize;
	}

	public void load() throws IOException {

		// Open socket to host machine
		Socket soc = new Socket(hostName, hostPort);

		// Send request for file
		sendRequest(soc);

		// Get file stream
		BufferedOutputStream fileOutStream = new BufferedOutputStream(new FileOutputStream(file));

		// Read in file over stream and write to disk
		getFile(soc, fileOutStream);

		// Update host machine to this machine as this partition changes the local copy
		hostName = InetAddress.getLocalHost().getHostName();

		// Close streams and socket
		fileOutStream.close();
		soc.close();

		// Check if file is size zero, in which case delete and throw an exception to signal
		//  that the file contains no data
		if (file.length() == 0) {
			file.delete();
			throw(new RemoteFileException("Remote file was not loaded (zero size)"));
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
			// Remote file has no contents so not loading
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
		fileOutStream.write(bytes, 0, current);
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
