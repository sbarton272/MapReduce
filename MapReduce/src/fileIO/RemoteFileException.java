package fileIO;

import java.io.IOException;

public class RemoteFileException extends IOException {

	private static final long serialVersionUID = -524043980622365648L;

	public RemoteFileException(String message) {
		super(message);
	}

	public RemoteFileException(String message, Throwable throwable) {
		super(message, throwable);
	}

}
