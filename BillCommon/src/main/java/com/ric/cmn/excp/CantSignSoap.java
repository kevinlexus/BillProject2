package com.ric.cmn.excp;

/**
 * Exception возникающий при невозможности подписать SOAP запрос
 * @author lev
 *
 */
public class CantSignSoap  extends Exception {

	public CantSignSoap(String message) {
        super(message);
    }
}
