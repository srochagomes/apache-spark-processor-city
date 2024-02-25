package br.com.search.cities.service;

import br.com.search.cities.data.City;
import lombok.AllArgsConstructor;
import org.apache.commons.codec.language.Soundex;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.levenshtein;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.split;

//@Service
@AllArgsConstructor
public class CityService implements Serializable {

    private SparkSession sparkSession;


    public List<City> searchCities(String searchTerm){
        int limiteDistancia = 3;
        int pageNumber = 1;
        int pageSize = 10;
        // Calculando os índices de início e fim para a página solicitada
        int startIndex = (pageNumber - 1) * pageSize;
        int endIndex = pageNumber * pageSize;

        // Executando a consulta SQL com a paginação
        Dataset<Row> paginatedCities = sparkSession.sql("SELECT * FROM cities");

        paginatedCities = paginatedCities.withColumn("parameter", lit(searchTerm));
        paginatedCities = paginatedCities.withColumn("parameters", explode(split(col("parameter"), "\\s+")));


        paginatedCities = paginatedCities.filter(
                levenshtein(col("palavras"), col("parameters")).$less(limiteDistancia)
        );





        paginatedCities = paginatedCities.select("linha", "municipio", "Tipo", "H", "CEP", "sku_pais", "sku_municipio").distinct().limit(10);

        Dataset<City> cityDTOs = paginatedCities.map(
                    new MapFunction<Row, City>() {
                        @Override
                        public City call(Row row) throws Exception {

                            return City.builder()
                                    .linha(row.getInt(row.fieldIndex("linha")))
                                    .cep(row.getString(row.fieldIndex("CEP")))
                                    .h(row.getString(row.fieldIndex("H")))
                                    .municipios(row.getString(row.fieldIndex("municipio")))
                                    .paisSKU(row.getString(row.fieldIndex("sku_pais")))
                                    .municipioSKU(row.getString(row.fieldIndex("sku_municipio")))
                                    .build();
                        }
                },
                Encoders.bean(City.class)
        );

        return cityDTOs.collectAsList();

    }

    private String soundexFunction(String str) {
        if (str != null) {
            Soundex soundex = new Soundex();
            return soundex.soundex(str);
        }
        return null;
    }

}
