package com.ric.cmn.excp;

/**
 * Exception возникающий если счетчик не найден 
 * @author lev
 *
 */
@SuppressWarnings("serial")
public class NotFoundNode  extends Exception {

	public NotFoundNode(String message) {
        super(message);
    }
}
