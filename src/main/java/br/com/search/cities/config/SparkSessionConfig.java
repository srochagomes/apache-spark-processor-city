package br.com.search.cities.config;

import br.com.search.cities.data.City;
import br.com.search.cities.functions.PhoneticUDF;
import org.apache.commons.codec.language.Soundex;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import static org.apache.spark.sql.functions.*;

import java.io.File;

//@Component
public class SparkSessionConfig {

    public static final String DATABASE_PATH  = "dataset/";


    //@Bean
    public SparkSession getSparkSession() throws Exception {
        SparkSession spark = null;
        System.out.println("Executando");
        var warehouseLocation = new File("/spark-databases").getAbsolutePath();
        var pathDataSet = DATABASE_PATH.concat("*.xlsx");

        spark = SparkSession.builder()
                .appName("search-cities")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .config("spark.dynamicAllocation.enabled", true)
                .config("spark.worker.cleanup.enabled", true)
                .config("spark.worker.cleanup.interval", "1800") // Limpa o diretório a cada 30 minutos
                .config("spark.locality.disk.fraction", 0.5)
                .getOrCreate();


        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("linha", DataTypes.IntegerType, true),
                DataTypes.createStructField("Municípios", DataTypes.StringType, true),
                DataTypes.createStructField("Tipo", DataTypes.StringType, true),
                DataTypes.createStructField("H", DataTypes.StringType, true),
                DataTypes.createStructField("CEP", DataTypes.StringType, true),
                DataTypes.createStructField("SKU País", DataTypes.StringType, true),
                DataTypes.createStructField("SKU Município", DataTypes.StringType, true)
                // Adicionar mais campos conforme necessário
        });

        Dataset<Row>  excelSource = spark.read()
                .format("excel")
                .option("inferSchema","false")
                .option("treatEmptyValuesAsNulls", "false")
                .option("header", "true")
                .schema(schema)
                .load(pathDataSet);

        Dataset<Row> renamedDF = excelSource
                .withColumnRenamed("Municípios", "municipio")
                .withColumnRenamed("SKU Município", "sku_municipio")
                .withColumnRenamed("SKU País", "sku_pais");

        Dataset<Row>  datasetWithSearch = renamedDF.withColumn("search_phonetic",
                concat_ws(" ",col("linha"),
                        col("municipio"),
                        col("Tipo"),
                        col("H"),
                        col("CEP"),
                        col("sku_municipio"),
                        col("sku_municipio")));

        UserDefinedFunction phoneticUDF = udf(
                (String str) -> PhoneticUDF.phoneticCode(str), // Supondo que você tenha uma função chamada soundexFunction
                DataTypes.StringType
        );
        UserDefinedFunction soundexUDF = udf(
                (String str) -> City.soundexFunction(str), // Supondo que você tenha uma função chamada soundexFunction
                DataTypes.StringType
        );

        spark.udf().register("soundex_udf", soundexUDF);
        spark.udf().register("phonetic_code", phoneticUDF);



        datasetWithSearch = datasetWithSearch.withColumn("palavras", explode(split(col("search_phonetic"), "\\s+")));
        datasetWithSearch.createOrReplaceTempView("cities");
        datasetWithSearch.show(2);

        return spark;
    }


}
