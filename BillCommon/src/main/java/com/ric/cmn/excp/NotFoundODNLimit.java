package com.ric.cmn.excp;

/**
 * Exception если не установлен лимит ОДН 
 * @author lev
 *
 */
@SuppressWarnings("serial")
public class NotFoundODNLimit  extends Exception {

	public NotFoundODNLimit(String message) {
        super(message);
    }
}
