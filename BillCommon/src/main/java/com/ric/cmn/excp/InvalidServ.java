package com.ric.cmn.excp;

/**
 * Exception возникающий если услуга некорректно настроена 
 * @author lev
 *
 */
@SuppressWarnings("serial")
public class InvalidServ  extends Exception {

	public InvalidServ(String message) {
        super(message);
    }
}
