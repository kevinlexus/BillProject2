package com.ric.cmn.excp;

/**
 * Exception возникающий если задана пустая услуга 
 * @author lev
 *
 */
@SuppressWarnings("serial")
public class ErrorWhileDist  extends Exception {

	public ErrorWhileDist(String message) {
        super(message);
    }
}
