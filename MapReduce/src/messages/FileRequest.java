package messages;

public class FileRequest {

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
