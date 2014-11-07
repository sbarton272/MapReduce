package messages;

import java.io.Serializable;

public class FileRequest implements Serializable {

	private static final long serialVersionUID = 6644202438816724816L;
	private final String filePath;
	private final long byteSize;

	public FileRequest(String filePath, long byteSize) {
		this.filePath = filePath;
		this.byteSize = byteSize;
	}

	public long getByteSize() {
		return byteSize;
	}

	public String getFilePath() {
		return filePath;
	}
}
