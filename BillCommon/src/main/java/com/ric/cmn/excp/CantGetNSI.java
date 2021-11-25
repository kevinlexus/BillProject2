package com.ric.cmn.excp;

/**
 * Exception возникающий при невозможности получить справочник NSI
 * @author lev
 *
 */
public class CantGetNSI  extends Exception {

	public CantGetNSI(String message) {
        super(message);
    }
}
