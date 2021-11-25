package com.ric.cmn.excp;

/**
 * Exception возникающий при невозможности отправить SOAP запрос
 * @author lev
 *
 */
public class CantSendSoap  extends Exception {

	public CantSendSoap(String message) {
        super(message);
    }
}
