package com.ric.cmn.excp;

/**
 * Exception возникающий если задана пустая услуга 
 * @author lev
 *
 */
@SuppressWarnings("serial")
public class StorageFileNotFoundException  extends Exception {

	public StorageFileNotFoundException(String message) {
        super(message);
    }
}
