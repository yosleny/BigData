package bigdata;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.spark_project.dmg.pmml.DataType;
import org.apache.commons.lang3.Range;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.MinMaxScaler;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.OneHotEncoderModel;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.StandardScalerModel;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.RandomForestRegressionModel;
import org.apache.spark.ml.regression.RandomForestRegressor;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.mean;
import static org.apache.spark.sql.functions.stddev;


import java.io.File;
import java.util.ArrayList;

public class sparkJob {
	public static void main(String []args) {
		
		SparkSession spark = SparkSession 
			    .builder() 
			    .appName("test") 
			    .config("hive.metastore.uris", "thrift://hn0-bigdat.tgn20x0yyz2ehkqxewu5rqkkgb.px.internal.cloudapp.net:9083") 
			    .config("spark.sql.warehouse.dir", "/hive/warehouse/external") 
			    .enableHiveSupport() 
			    .getOrCreate();
			spark.sql("show tables").show();
		
        //cluster
		
		 //String warehouseLocation = new File("/hive/warehouse/managed/data").getAbsolutePath();
		    /*SparkSession spark = SparkSession
		      .builder()
		      .appName("Java Spark Hive Example")
		      //.config("spark.sql.warehouse.dir", warehouseLocation)
		      .enableHiveSupport()
		      .getOrCreate();
		
        //SparkSession spark = SparkSession.builder().appName("examplespark").enableHiveSupport().getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        System.out.println("Session created");
		Dataset<Row> rows = spark.sql("SELECT gender, age,hypertension,heart_disease,work_type,Residence_type,avg_glucose_level,bmi,smoking_status,stroke from data");
        //Dataset<Row> rows = spark.read().format("csv").option("header","true").load("healthcare-dataset-stroke-data.csv");
        //select columns
        rows = rows.select(
        		col("gender"),
        		col("age").cast(DataTypes.DoubleType),
        		col("hypertension").cast(DataTypes.IntegerType),
        		col("heart_disease").cast(DataTypes.IntegerType),
        		col("work_type"),
        		col("Residence_type"),
        		col("avg_glucose_level").cast(DataTypes.DoubleType),
        		col("bmi").cast(DataTypes.DoubleType),
        		col("smoking_status"),
        		col("stroke").cast(DataTypes.IntegerType)
        		);
        
        //rows.show();
        //normalize the dataset 
        rows=rows
        .select(mean("age").alias("mean_age"), stddev("age").alias("stddev_age"))
        .crossJoin(rows)
        .withColumn("age_scaled" , (col("age").$minus(col("mean_age"))).$div(col("stddev_age")));
        
        rows=rows
        .select(mean("bmi").alias("mean_bmi"), stddev("bmi").alias("stddev_bmi"))
        .crossJoin(rows)
        .withColumn("bmi_scaled" , (col("bmi").$minus(col("mean_bmi"))).$div(col("stddev_bmi")));
        
       rows=rows
       .select(mean("avg_glucose_level").alias("mean_avg_glucose_level"), stddev("avg_glucose_level").alias("stddev_avg_glucose_level"))
       .crossJoin(rows)
       .withColumn("avg_glucose_level_scaled" , (col("avg_glucose_level").$minus(col("mean_avg_glucose_level"))).$div(col("stddev_avg_glucose_level")));
       //drop column after normalization
       rows.drop("age");
       rows.drop("bmi");
       rows.drop("avg_glucose_level");
       // conversion categorical attribute in numerical
       StringIndexer indexer = new StringIndexer()
    		      .setInputCol("gender")
    		      .setOutputCol("genderIndex");
       rows = indexer.fit(rows).transform(rows);
       
       StringIndexer indexer1 = new StringIndexer()
 		      .setInputCol("work_type")
 		      .setOutputCol("work_typeIndex");
       rows = indexer1.fit(rows).transform(rows);
       
       StringIndexer indexer2 = new StringIndexer()
  		      .setInputCol("Residence_type")
  		      .setOutputCol("Residence_typeIndex");
       rows = indexer2.fit(rows).transform(rows);
       
       StringIndexer indexer3 = new StringIndexer()
   		      .setInputCol("smoking_status")
   		      .setOutputCol("smoking_statusIndex");
       rows = indexer3.fit(rows).transform(rows);
       //drop column after conversion categorical attribute in numerical
       rows.drop("gender");
       rows.drop("work_type");
       rows.drop("Residence_type");
       rows.drop("smoking_status");
     //model
        
       VectorAssembler assembler = new VectorAssembler().setInputCols(new String[]{"genderIndex", "age_scaled", "hypertension", "heart_disease", "work_typeIndex","Residence_typeIndex", "avg_glucose_level_scaled", "bmi_scaled", "work_typeIndex","smoking_statusIndex"}).setOutputCol("features");
       rows=assembler.transform(rows);
       Dataset<Row>[]bothTrainTestDfs=rows.randomSplit(new double[] {0.8d,0.2d});
            
       Dataset<Row>trainDF=bothTrainTestDfs[0];
       Dataset<Row>testDF=bothTrainTestDfs[1];
       RandomForestClassifier estimator= new RandomForestClassifier()
    		   .setLabelCol("stroke")
    		   .setFeaturesCol("features")
    		   .setMaxDepth(5);
       RandomForestClassificationModel model= estimator.fit(trainDF);
       Dataset<Row> predictions= model.transform(testDF);
       predictions.printSchema();
       predictions.show();
       
       //Evaluation
       
       MulticlassClassificationEvaluator evaluator= new MulticlassClassificationEvaluator()
    		   .setLabelCol("stroke")
    		   .setPredictionCol("prediction");
       double accuracy = evaluator.evaluate(predictions);
       System.out.println("Accuracy:"+accuracy);
       
	   spark.stop();*/
	}
}
