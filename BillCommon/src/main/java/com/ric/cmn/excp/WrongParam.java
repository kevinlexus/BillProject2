package com.ric.cmn.excp;

/**
 * Exception возникающий если задан некорректный параметр 
 * @author lev
 *
 */
@SuppressWarnings("serial")
public class WrongParam  extends Exception {

	public WrongParam(String message) {
        super(message);
    }
}
