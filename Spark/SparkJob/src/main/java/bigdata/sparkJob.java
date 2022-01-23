package bigdata;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import cleaning.DataCleaning;
import data.StrokeInfo;
import model.DecisionTreeModel;
import model.NaiveBayesClassification;
import utils.ProjectConstants;

import org.apache.commons.lang3.Range;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.MinMaxScaler;
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
		
		SparkSession spark = SparkSession.builder().appName("Stroke Prediction").getOrCreate();
		
		Dataset<Row> rows = spark.read()
        		                 .format("csv")
        		                 .option("header","false")
        		                 .load("hdfs://master:9000/user/user/project/map/part-r-00000")
        		                 .toDF("id", ProjectConstants.Gender, ProjectConstants.Age, ProjectConstants.SufferHypertension, ProjectConstants.SufferHeartDesease, ProjectConstants.IsEverMarried, ProjectConstants.WorkType, ProjectConstants.IsUrbanResident, ProjectConstants.AvgGlucoseLevel, ProjectConstants.Bmi, ProjectConstants.IsSmoker, ProjectConstants.Stroke);

       rows = rows.select(
    		   col(ProjectConstants.Gender).cast(DataTypes.IntegerType),
    		   col(ProjectConstants.Age).cast(DataTypes.DoubleType),
    		   col(ProjectConstants.SufferHypertension).cast(DataTypes.IntegerType),
    		   col(ProjectConstants.SufferHeartDesease).cast(DataTypes.IntegerType),
    		   col(ProjectConstants.IsEverMarried).cast(DataTypes.IntegerType),
    		   //col(ProjectConstants.WorkType).cast(DataTypes.StringType),
    		   col(ProjectConstants.IsUrbanResident).cast(DataTypes.IntegerType),
    		   col(ProjectConstants.AvgGlucoseLevel).cast(DataTypes.DoubleType),
    		   col(ProjectConstants.Bmi).cast(DataTypes.DoubleType),
    		   col(ProjectConstants.IsSmoker).cast(DataTypes.IntegerType),
    		   col(ProjectConstants.Stroke).cast(DataTypes.IntegerType)
    		   );

       
       //Dataset<Row> cleanData = DataCleaning.clean(rows);
       
       
       
       NaiveBayesClassification nb = new NaiveBayesClassification(rows);
       
       
       spark.stop();
       
	}
}
