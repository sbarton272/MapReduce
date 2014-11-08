package messages;

import java.io.Serializable;

public class FileRequest implements Serializable {

	private static final long serialVersionUID = 6644202438816724816L;
	private final String fileName;
	private final long byteSize;

	public FileRequest(String fileName, long byteSize) {
		this.fileName = fileName;
		this.byteSize = byteSize;
	}

	public long getByteSize() {
		return byteSize;
	}

	public String getFileName() {
		return fileName;
	}
}
