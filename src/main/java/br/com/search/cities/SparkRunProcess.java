package br.com.search.cities;

import br.com.search.cities.data.City;
import org.apache.commons.codec.language.Soundex;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import br.com.search.cities.functions.PhoneticUDF;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.initcap;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.regexp_replace;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.substring;
import static org.apache.spark.sql.functions.typedLit;
import static org.apache.spark.sql.functions.udf;
import static org.apache.spark.sql.functions.levenshtein;

@Component
public class SparkRunProcess implements ApplicationRunner {

    public static final String DATABASE_PATH  = "dataset/";
    private SparkSession spark;
    private Dataset<Row> dataset;
    private Dataset<Row> datasetRoot;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        this.carregaElasticSearchV1(args);
    }

    public void carregaElasticSearchV1(ApplicationArguments args) throws Exception {
        System.out.println("Executando");
        var warehouseLocation = new File("/spark-databases").getAbsolutePath();
        var pathDataSetRegions = DATABASE_PATH.concat("regions.csv");
        var pathDataSet = DATABASE_PATH.concat("SuperBaseMunicipiosMundo.xlsx");

        this.spark = SparkSession.builder()
                .appName("search-cities")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .config("spark.dynamicAllocation.enabled", true)
                .config("spark.worker.cleanup.enabled", true)
                .config("spark.worker.cleanup.interval", "1800") // Limpa o diretório a cada 30 minutos
                .config("spark.locality.disk.fraction", 0.5)
                .getOrCreate();

        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("SKU IBGE", DataTypes.StringType, true),
                DataTypes.createStructField("Sigla País", DataTypes.StringType, true),
                DataTypes.createStructField("Tipo", DataTypes.StringType, true),
                DataTypes.createStructField("Ordem Alfabética País", DataTypes.StringType, true),
                DataTypes.createStructField("SKU Fériaz", DataTypes.StringType, true),
                DataTypes.createStructField("Município", DataTypes.StringType, true),
                DataTypes.createStructField("País", DataTypes.StringType, true),
                DataTypes.createStructField("Região", DataTypes.StringType, true),
                DataTypes.createStructField("UF", DataTypes.StringType, true),
                DataTypes.createStructField("Região Turística", DataTypes.StringType, true),
                DataTypes.createStructField("Arrecadação", DataTypes.StringType, true),
                DataTypes.createStructField("Cluster", DataTypes.StringType, true)
        });
        Dataset<Row>  dataset = spark.read()
                .format("excel")
                .option("inferSchema","false")
                .option("treatEmptyValuesAsNulls", "false")
                .option("header", "true")
                .schema(schema)
                .load(pathDataSet);

        dataset = dataset
                .withColumnRenamed("SKU IBGE", "skuIBGE")
                .withColumnRenamed("Sigla País", "siglaPais")
                .withColumnRenamed("Tipo", "type")
                .withColumnRenamed("Ordem Alfabética País", "ordemAlfabetica")
                .withColumnRenamed("SKU Fériaz", "skuFeriaz")
                .withColumnRenamed("Município", "municipio")
                .withColumnRenamed("País", "pais")
                .withColumnRenamed("Região", "regiao")
                .withColumnRenamed("UF", "uf")
                .withColumnRenamed("Região Turística", "regiaoTuristica")
                .withColumnRenamed("Arrecadação", "arrecadacao")
                .withColumnRenamed("Cluster", "cluster");





        dataset.createOrReplaceTempView("cities");



        Dataset<Row>  datasetStates = spark.read()
                .option("inferSchema","false")
                .option("treatEmptyValuesAsNulls", "false")
                .option("header", "true")
                .csv(pathDataSetRegions);



        datasetStates = datasetStates
                .withColumnRenamed("local_code", "localCode")
                .withColumnRenamed("name", "nomeEstado")
                .withColumnRenamed("continent", "continente")
                .withColumnRenamed("iso_country", "siglaPaisEstado");





        datasetStates.createOrReplaceTempView("states");

        Dataset<Row> joinedData = dataset.join(datasetStates,
                dataset.col("siglaPais").equalTo(datasetStates.col("siglaPaisEstado"))
                        .and(dataset.col("uf").equalTo(datasetStates.col("localCode"))),
                "left_outer");

        joinedData.createOrReplaceTempView("cities_states");

        Dataset<Row> result = spark.sql("SELECT " +
                "c.skuIBGE, " +
                "c.siglaPais, " +
                "c.type, " +
                "c.ordemAlfabetica, " +
                "c.skuFeriaz, " +
                "c.municipio, " +
                "c.pais, " +
                "c.regiao, " +
                "c.uf, " +
                "c.regiaoTuristica, " +
                "c.arrecadacao, " +
                "c.cluster, " +
                "initcap(c.pais) AS paisCamel, " +
                "s.nomeEstado, " +
                "s.continente " +
                "FROM cities_states AS c " +
                "LEFT JOIN states AS s " +
                "ON c.siglaPais = s.siglaPaisEstado AND c.uf = s.localCode ");

        result.show(10);


        // Configure as configurações do Elasticsearch
//        Map<String, String> esConfig = new HashMap<>();
//        esConfig.put("es.nodes", "localhost"); // Endereço do nó Elasticsearch
//        esConfig.put("es.port", "9200"); // Porta Elasticsearch
//        esConfig.put("es.index.auto.create", "true"); // Criar índice automaticamente se não existir

        // Configure as configurações do Elasticsearch
        Map<String, String> esConfig = new HashMap<>();
        esConfig.put("es.nodes", "https://3a3acaba08d1485d8a6705418dad3793.us-east-1.aws.found.io"); // Endereço do nó Elasticsearch
        esConfig.put("es.port", "443");
        esConfig.put("es.index.auto.create", "true"); // Criar índice automaticamente se não existir
        esConfig.put("es.net.http.auth.user", "elastic"); // Criar índice automaticamente se não existir
        esConfig.put("es.net.http.auth.pass", "3HOVrpl1WzHndGz3myjYEWi7"); // Criar índice automaticamente se não existir
        esConfig.put("es.net.ssl", "true");
        esConfig.put("es.nodes.wan.only","true");

        // Escreva os dados no Elasticsearch
        result.write()
                .format("org.elasticsearch.spark.sql") // Use o formato Elasticsearch
                .options(esConfig)
                .mode(SaveMode.Append) // Modo de gravação (Append, Overwrite, Ignore, ErrorIfExists)
                .save("city"); // Nome do índice Elasticsearch

        this.spark.close();

    }

    public void carregaElasticSearchV2(ApplicationArguments args) throws Exception {
        System.out.println("Executando");
        var warehouseLocation = new File("/spark-databases").getAbsolutePath();
        var pathDataSet = DATABASE_PATH.concat("SuperBaseMunicipiosMundo_old.xlsx");

        this.spark = SparkSession.builder()
                .appName("search-cities")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .config("spark.dynamicAllocation.enabled", true)
                .config("spark.worker.cleanup.enabled", true)
                .config("spark.worker.cleanup.interval", "1800") // Limpa o diretório a cada 30 minutos
                .config("spark.locality.disk.fraction", 0.5)
                .getOrCreate();



        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("SKU IBGE", DataTypes.StringType, true),
                DataTypes.createStructField("Sigla País", DataTypes.StringType, true),
                DataTypes.createStructField("Tipo", DataTypes.StringType, true),
                DataTypes.createStructField("Ordem Alfabética País", DataTypes.StringType, true),
                DataTypes.createStructField("SKU Fériaz", DataTypes.StringType, true),
                DataTypes.createStructField("Município", DataTypes.StringType, true),
                DataTypes.createStructField("País", DataTypes.StringType, true),
                DataTypes.createStructField("Região", DataTypes.StringType, true),
                DataTypes.createStructField("UF", DataTypes.StringType, true),
                DataTypes.createStructField("Região Turística", DataTypes.StringType, true),
                DataTypes.createStructField("Arrecadação", DataTypes.StringType, true),
                DataTypes.createStructField("Cluster", DataTypes.StringType, true)
        });
        Dataset<Row>  dataset = spark.read()
                .format("excel")
                .option("inferSchema","false")
                .option("treatEmptyValuesAsNulls", "false")
                .option("header", "true")
                .schema(schema)
                .load(pathDataSet);

        dataset = dataset
                .withColumnRenamed("SKU IBGE", "skuIBGE")
                .withColumnRenamed("Sigla País", "siglaPais")
                .withColumnRenamed("Tipo", "type")
                .withColumnRenamed("Ordem Alfabética País", "ordemAlfabetica")
                .withColumnRenamed("SKU Fériaz", "skuFeriaz")
                .withColumnRenamed("Município", "municipio")
                .withColumnRenamed("País", "pais")
                .withColumnRenamed("Região", "regiao")
                .withColumnRenamed("UF", "uf")
                .withColumnRenamed("Região Turística", "regiaoTuristica")
                .withColumnRenamed("Arrecadação", "arrecadacao")
                .withColumnRenamed("Cluster", "cluster");





        dataset.createOrReplaceTempView("cities");


        // Executando a consulta SQL com a paginação
        dataset = this.spark.sql("SELECT * FROM cities");


        dataset.select(
                col("skuIBGE"),
                col("siglaPais"),
                col("type"),
                col("ordemAlfabetica"),
                col("skuFeriaz"),
                col("municipio"),
                col("pais"),
                col("regiao"),
                col("uf"),
                col("regiaoTuristica"),
                col("arrecadacao"),
                col("cluster"),
                initcap(col("pais")).alias("paisCamel")
        ).distinct().show(10);


        // Configure as configurações do Elasticsearch
//        Map<String, String> esConfig = new HashMap<>();
//        esConfig.put("es.nodes", "localhost"); // Endereço do nó Elasticsearch
//        esConfig.put("es.port", "9200"); // Porta Elasticsearch
//        esConfig.put("es.index.auto.create", "true"); // Criar índice automaticamente se não existir

        // Escreva os dados no Elasticsearch
//        paginatedCities.write()
//                .format("org.elasticsearch.spark.sql") // Use o formato Elasticsearch
//                .options(esConfig)
//                .mode(SaveMode.Append) // Modo de gravação (Append, Overwrite, Ignore, ErrorIfExists)
//                .save("city"); // Nome do índice Elasticsearch

        this.spark.close();

    }

    public void processoTeste(ApplicationArguments args) throws Exception {
        System.out.println("Executando");
        var warehouseLocation = new File("/spark-databases").getAbsolutePath();
        var pathDataSet = DATABASE_PATH.concat("*.xlsx");

        this.spark = SparkSession.builder()
                .appName("search-cities")
                .master("local[*]")
                .config("spark.sql.warehouse.dir",warehouseLocation)
                .config("spark.dynamicAllocation.enabled",true)
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

        excelSource.createOrReplaceTempView("xlsTable");



        long xlsCount = this.spark.sql("select * from xlsTable").count();

        //this.spark.sql("select * from csvTable").toJSON().javaRDD().foreach(json -> System.out.println(json));
        this.spark.sql("select * from xlsTable").toJSON().javaRDD().foreach(json -> System.out.println(json));

        System.out.println("Total XLS "+xlsCount);



    }


    public void validaçãoArquivos(ApplicationArguments args) throws Exception {
        System.out.println("Executando");
        var warehouseLocation = new File("/spark-databases").getAbsolutePath();

        this.spark = SparkSession.builder()
                .appName("search-cities")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .config("spark.dynamicAllocation.enabled", true)
                .config("spark.sql.sources.partitionColumnTypeInference.enabled", true)
                .getOrCreate();


        // Listar arquivos CSV no diretório
        String[] csvFiles = new File(DATABASE_PATH).list();
        var listaGeral = new ArrayList<List<String>>();
        if (csvFiles != null) {
            for (String file : csvFiles) {
                if (file.endsWith(".xlsx")) {
                    String filePath = DATABASE_PATH + file;

                    // Carregar arquivo CSV
                    Dataset<Row> csvSource = spark.read()
                            .format("excel")
                            .option("inferSchema","true")
                            .option("treatEmptyValuesAsNulls", "false")
                            .option("header", "true")
                            .load(filePath);

                    // Obter os nomes das colunas
                    String[] columns = csvSource.columns();
                    System.out.println("Colunas do arquivo " + file + ":");
                    var listaInterna = new ArrayList<String>();
                    listaInterna.add("Arquivo:"+filePath);
                    for (String column : columns) {
                        listaInterna.add(column);
                        System.out.println(column);
                    }
                    listaGeral.add(listaInterna);
                }
            }
        }

        for (List<String> lista: listaGeral){
            System.out.println(lista);
        }
    }

    public void importacaoArquivoElasticSearch(ApplicationArguments args) throws Exception {
        System.out.println("Executando");
        var warehouseLocation = new File("/spark-databases").getAbsolutePath();
        var pathDataSet = DATABASE_PATH.concat("*.xlsx");

        this.spark = SparkSession.builder()
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
                        col("sku_pais"),
                        col("sku_municipio")));

        UserDefinedFunction phoneticUDF = udf(
                (String str) -> PhoneticUDF.phoneticCode(str), // Supondo que você tenha uma função chamada soundexFunction
                DataTypes.StringType
        );
        UserDefinedFunction soundexUDF = udf(
                (String str) -> City.soundexFunction(str), // Supondo que você tenha uma função chamada soundexFunction
                DataTypes.StringType
        );

        this.spark.udf().register("soundex_udf", soundexUDF);
        this.spark.udf().register("phonetic_code", phoneticUDF);

        int pageNumber = 1;
        int pageSize = 10;
        // Calculando os índices de início e fim para a página solicitada
        int startIndex = (pageNumber - 1) * pageSize;
        int endIndex = pageNumber * pageSize;
        int limiteDistancia = 3;

        String consultaString = "Barcela";

        //datasetWithSearch = datasetWithSearch.withColumn("palavras", explode(split(col("search_phonetic"), "\\s+")));
        datasetWithSearch.createOrReplaceTempView("cities");


        // Executando a consulta SQL com a paginação
        Dataset<Row> paginatedCities = this.spark.sql("SELECT * FROM cities");

//        paginatedCities = paginatedCities.withColumn("parameter", lit(consultaString));
//        paginatedCities = paginatedCities.withColumn("parameters", explode(split(col("parameter"), "\\s+")));
//
//
//        paginatedCities = paginatedCities.filter(
//                levenshtein(col("palavras"), col("parameters")).$less(limiteDistancia)
//        );





        paginatedCities.select("linha", "municipio", "Tipo", "H", "CEP", "sku_pais", "sku_municipio").distinct().show(10);
        paginatedCities = paginatedCities
                .withColumnRenamed("Tipo", "tipo")
                .withColumnRenamed("H", "h")
                .withColumnRenamed("CEP", "cep")
                .withColumnRenamed("sku_municipio", "skuMunicipio")
                .withColumnRenamed("sku_pais", "skuPais");

        // Configure as configurações do Elasticsearch
        Map<String, String> esConfig = new HashMap<>();
        esConfig.put("es.nodes", "localhost"); // Endereço do nó Elasticsearch
        esConfig.put("es.port", "9200"); // Porta Elasticsearch
        esConfig.put("es.index.auto.create", "true"); // Criar índice automaticamente se não existir

        // Escreva os dados no Elasticsearch
        paginatedCities.write()
                .format("org.elasticsearch.spark.sql") // Use o formato Elasticsearch
                .options(esConfig)
                .mode(SaveMode.Append) // Modo de gravação (Append, Overwrite, Ignore, ErrorIfExists)
                .save("city"); // Nome do índice Elasticsearch

        this.spark.close();

    }


}