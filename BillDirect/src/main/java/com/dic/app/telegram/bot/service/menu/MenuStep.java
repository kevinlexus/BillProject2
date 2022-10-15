package com.dic.app.telegram.bot.service.menu;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class MenuStep {

    private Menu menu;
    private String callBackData;

}

