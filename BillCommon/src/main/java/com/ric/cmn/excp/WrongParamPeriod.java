package com.ric.cmn.excp;

/**
 * Exception возникающий если задан некорректный параметр
 * @author lev
 *
 */
@SuppressWarnings("serial")
public class WrongParamPeriod extends Exception {

	public WrongParamPeriod(String message) {
        super(message);
    }
}
