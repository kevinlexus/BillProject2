package com.dic.app.gis.service.maintaners;

import java.math.BigDecimal;
import java.text.ParseException;

import com.ric.dto.SumSaldoRecDTO;

/**
 * Интерфейс сервиса получения данных о задолженности, пени из разных источников
 * @author Lev
 *
 */
public interface DebMng {

	SumSaldoRecDTO getSumSaldo(String lsk, String period, Integer appTp);
}
