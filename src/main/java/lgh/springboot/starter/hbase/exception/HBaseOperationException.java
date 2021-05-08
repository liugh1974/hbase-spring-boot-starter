package lgh.springboot.starter.hbase.exception;

/**
 * 
 * @author Liuguanghua
 *
 */
public class HBaseOperationException extends RuntimeException {
    private static final long serialVersionUID = -6220780513639666862L;

    public HBaseOperationException(String message, Throwable cause) {
        super(message, cause);
    }

    public HBaseOperationException(String message) {
        super(message);
    }
    
    public HBaseOperationException(String message, Object...params) {
        super(String.format(message, params));
    }

    public HBaseOperationException(Throwable cause) {
        super(cause);
    }
}
