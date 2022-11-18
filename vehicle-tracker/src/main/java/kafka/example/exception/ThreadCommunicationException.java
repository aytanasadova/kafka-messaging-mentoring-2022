package kafka.example.exception;

public class ThreadCommunicationException extends RuntimeException {

    public ThreadCommunicationException(String message) {
        super(message);
    }
    public ThreadCommunicationException(String message, Throwable cause) {
        super(message, cause);
    }

}
