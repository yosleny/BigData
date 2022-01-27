
package bigdata;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.mean;
import static org.apache.spark.sql.functions.stddev;
 
public class SparkJob {
	public static void main(String []args) {
		
        SparkSession spark = SparkSession.builder().appName("StrokePrediction").enableHiveSupport().getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        
		Dataset<Row> rows = spark.sql("SELECT gender,age,hypertension,heart_disease,work_type,Residence_type,avg_glucose_level,bmi,smoking_status,stroke from stroke_data where age>0");
        
		// Dataset<Row> rows = spark.read().format("csv").option("header","true").load("healthcare-dataset-stroke-data.csv");
        
		// Select columns
        /*rows = rows.select(
        		col("gender").cast(DataTypes.IntegerType),
        		col("age").cast(DataTypes.DoubleType),
        		col("hypertension").cast(DataTypes.IntegerType),
        		col("heart_disease").cast(DataTypes.IntegerType),
        		col("work_type").cast(DataTypes.IntegerType),
        		col("Residence_type").cast(DataTypes.IntegerType),
        		col("avg_glucose_level").cast(DataTypes.DoubleType),
        		col("bmi").cast(DataTypes.DoubleType),
        		col("smoking_status").cast(DataTypes.IntegerType),
        		col("stroke").cast(DataTypes.IntegerType)
        		);*/
		
        // Oversampling
        Dataset<Row> train1=rows.filter(col("stroke").equalTo("0"));
        Dataset<Row> train2=rows.filter(col("stroke").equalTo("1"));

        System.out.println(train1.count());
        System.out.println(train2.count());

        float ratio=train1.count()/train2.count();
        Dataset<Row> train2ov=train2.sample(true, ratio, 1);
        rows=train1.union(train2ov);
        
        System.out.println(rows.where(col("stroke").equalTo("0")).count());
        System.out.println(rows.where(col("stroke").equalTo("1")).count());
        
       // Standardization
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
       
       // Drop column after standardization
       rows.drop("age");
       rows.drop("bmi");
       rows.drop("avg_glucose_level");
       
       
       StringIndexer indexer1 = new StringIndexer()
 		      .setInputCol("work_type")
 		      .setOutputCol("work_typeIndex");
       rows = indexer1.fit(rows).transform(rows);
       rows.drop("work_type");
       
       // Conversion categorical features into numerical
       /*StringIndexer indexer = new StringIndexer()
    		      .setInputCol("gender")
    		      .setOutputCol("genderIndex");
       rows = indexer.fit(rows).transform(rows);
  
       
       StringIndexer indexer2 = new StringIndexer()
  		      .setInputCol("Residence_type")
  		      .setOutputCol("Residence_typeIndex");
       rows = indexer2.fit(rows).transform(rows);
       
       StringIndexer indexer3 = new StringIndexer()
   		      .setInputCol("smoking_status")
   		      .setOutputCol("smoking_statusIndex");
       rows = indexer3.fit(rows).transform(rows);
       
       // Drop column after conversion categorical attribute in numerical
       rows.drop("gender");
       rows.drop("Residence_type");
       rows.drop("smoking_status");*/
       
       // Model
       VectorAssembler assembler = new VectorAssembler().setInputCols(new String[]{"gender", "age_scaled", "hypertension", "heart_disease", "work_typeIndex","Residence_type", "avg_glucose_level_scaled", "bmi_scaled","smoking_status"}).setOutputCol("features");
       rows=assembler.transform(rows);
       Dataset<Row>[]bothTrainTestDfs=rows.randomSplit(new double[] {0.8d,0.2d});
       Dataset<Row>trainDF=bothTrainTestDfs[0];
       Dataset<Row>testDF=bothTrainTestDfs[1];
       
       /*RandomForestClassifier estimator= new RandomForestClassifier()
    		   .setLabelCol("stroke")
    		   .setFeaturesCol("features")
    		   .setMaxDepth(5);
       RandomForestClassificationModel model = estimator.fit(trainDF);
       Dataset<Row> predictions = model.transform(testDF);
       predictions.printSchema();
       predictions.show();
       
       // Evaluation
       MulticlassClassificationEvaluator evaluator= new MulticlassClassificationEvaluator()
    		   .setLabelCol("stroke")
    		   .setPredictionCol("prediction");
       double accuracy = evaluator.evaluate(predictions);
       System.out.println("Accuracy: "+accuracy);*/
       
       
       
       
       // Estimator
       RandomForestClassifier rf = new RandomForestClassifier()
    		   .setLabelCol("stroke")
    		   .setFeaturesCol("features");

       // Evaluator
       MulticlassClassificationEvaluator rfevaluator = new MulticlassClassificationEvaluator()
    		   .setLabelCol("stroke")
    		   .setPredictionCol("prediction");

       // ParamGrid for Cross Validation
       ParamMap[] rfparamGrid = new ParamGridBuilder()
                    .addGrid(rf.maxDepth(), new int[] {2, 5,10,20})
                    .addGrid(rf.numTrees(), new int[] {5, 20,40,100})
                    .build();

       // 5-fold CrossValidator
       CrossValidator rfcv = new CrossValidator()
    		        .setEstimator(rf)
    		        .setEstimatorParamMaps(rfparamGrid)
    		        .setEvaluator(rfevaluator)
    		        .setNumFolds(5);

       // Run cross validations
       CrossValidatorModel rfcvModel = rfcv.fit(trainDF);
       //System.out.println("Best model params: " + rfcvModel.bestModel().extractParamMap());
       System.out.println("maxDepth: " + rfcvModel.bestModel().extractParamMap().apply(rfcvModel.bestModel().getParam("maxDepth")));
       System.out.println("numTrees: " + rfcvModel.bestModel().extractParamMap().apply(rfcvModel.bestModel().getParam("numTrees")));

       // Use test set here so we can measure the accuracy of our model on new data
       Dataset<Row> rfpredictions = rfcvModel.transform(testDF);
       rfpredictions=rfpredictions.select(
       		col("prediction"),
       		col("stroke").cast(DataTypes.DoubleType)

       		
       		);;
       // rfcvModel uses the best model found from the Cross Validation       
       // Evaluate best model
       //System.out.println("Accuracy: "+rfevaluator.evaluate(rfpredictions.javaRDD()));
       //MulticlassMetrics metrics = new MulticlassMetrics(rfpredictions.toJavaRDD().rdd()));
      
       MulticlassMetrics metrics = new MulticlassMetrics(rfpredictions.select("prediction", "stroke"));
       
       	
       // output the Confusion Matrix
       System.out.println("Confusion Matrix");
       System.out.println(metrics.confusionMatrix());
       System.out.println("accuracy");
       System.out.println(metrics.accuracy());
       System.out.println("weightedPrecision");
       System.out.println(metrics.weightedPrecision());
       System.out.println("weightedRecall");
       System.out.println(metrics.weightedRecall());
       System.out.println("weightedFMeasure");
       System.out.println(metrics.weightedFMeasure());
       System.out.println("weightedTruePositiveRate");
       System.out.println(metrics.weightedTruePositiveRate());
       System.out.println("weightedFalsePositiveRate");
       System.out.println(metrics.weightedFalsePositiveRate());
      
       
	   spark.stop();
	}
}
