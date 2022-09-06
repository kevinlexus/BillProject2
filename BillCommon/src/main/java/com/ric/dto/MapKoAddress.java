package com.ric.dto;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Value;

import java.util.Map;

@Value
@NoArgsConstructor(force = true)
@AllArgsConstructor()
public class MapKoAddress {
    Map<Long, KoAddress> mapKoAddress;
}
