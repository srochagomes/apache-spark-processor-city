package br.com.search.cities.controler;

import br.com.search.cities.data.City;
import br.com.search.cities.service.CityService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.UUID;

//@RestController
//@RequestMapping("/domains")
//@Slf4j
//@AllArgsConstructor
//public class CityController {
//
//    private CityService cityService;
//
//    //@GetMapping(value = "/cities",  produces = {MediaType.APPLICATION_JSON_VALUE})
//    public ResponseEntity<List<City>> getCities(
//            @RequestParam(name = "search") String search
//    ) throws Exception  {
//        return ResponseEntity.ok().body(cityService.searchCities(search));
//    }
//
//}
