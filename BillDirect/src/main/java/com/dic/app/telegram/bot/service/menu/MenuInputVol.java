package com.dic.app.telegram.bot.service.menu;

public enum MenuInputVol {
        INPUT_UNDEFINED("Неопределено"),
        INPUT_VOL_COLD_WATER("Холодная вода"),
        INPUT_VOL_HOT_WATER("Горячая вода"),
        INPUT_VOL_EL_EN("Электроэнергия"),
        INPUT_VOL_HEAT("Отопление");

        private final String name;

        MenuInputVol(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }

    }
