package model;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import data.StrokeInfo;
import utils.ProjectConstants;

public class DecisionTreeModel {

	OneHotEncoder workTypeEncoder;
	StandardScaler stdScaler;
	VectorAssembler vectorAssembler;
	DecisionTreeClassifier decisionTreeModel;
	Pipeline pipeline;
	MulticlassClassificationEvaluator evaluator;
	CrossValidator validator;
	CrossValidatorModel validatorModel;
	
	PipelineModel pipelineModel;
	
	int numClasses = 2;
	Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
	String impurity = "gini";
	int maxDepth = 5;
    int maxBins = 32;
	
	public DecisionTreeModel() {
		
		/*workTypeEncoder = new OneHotEncoder()
							  .setInputCols(new String[]{ ProjectConstants.WorkType })
							  .setOutputCols(new String[]{ ProjectConstants.WorkTypeEncoded });
		
		vectorAssembler = new VectorAssembler()
				              .setInputCols(new String[] { 
				            		  					   ProjectConstants.Gender, 
				            		  					   ProjectConstants.Age,
				            		                       ProjectConstants.SufferHypertension, 
				            		                       ProjectConstants.SufferHeartDesease, 
				            		                       ProjectConstants.IsEverMarried, 
				            		                       //ProjectConstants.WorkTypeEncoded,
				            		                       ProjectConstants.IsUrbanResident,
				            		                       ProjectConstants.AvgGlucoseLevel,
				            		                       ProjectConstants.Bmi,
				            		                       ProjectConstants.IsSmoker,
				            		                       ProjectConstants.Stroke })
				              .setOutputCol("features");
		
		stdScaler = new StandardScaler()
	            		.setInputCol("features")
	            		.setOutputCol("scaledFeatures")
	            		.setWithStd(true)
	            		.setWithMean(false);*/
		
		
		decisionTreeModel = new DecisionTreeClassifier()
			         			.setLabelCol(ProjectConstants.Stroke)
			         			.setFeaturesCol("indexedFeatures")
			         			.setImpurity(impurity)
			         			.setMaxDepth(maxDepth)
			         			.setMaxBins(maxBins);
		
		//pipeline = new Pipeline()
			      	   //.setStages(new PipelineStage[]{ vectorAssembler, decisionTreeModel}); //workTypeEncoder,  stdScaler,
		
		/*evaluator = new MulticlassClassificationEvaluator()
			      		.setLabelCol(ProjectConstants.Stroke)
			      		.setPredictionCol("prediction")
			      		.setMetricName("accuracy");
		
		validator = new CrossValidator()
						.setEstimator(pipeline)
						.setEvaluator(evaluator)
						.setNumFolds(10)
						.setSeed(1L);*/
        
		
	}
	
	
	public void fit(Dataset<StrokeInfo> data) {
		
        
		pipelineModel = pipeline.fit(data);

		
	}
	
	public void transform(Dataset<StrokeInfo> data) {
			
		// Make predictions.
		Dataset<Row> predictions = pipelineModel.transform(data);
		
		evaluator = new MulticlassClassificationEvaluator()
	      		.setLabelCol(ProjectConstants.Stroke)
	      		.setPredictionCol("prediction")
	      		.setMetricName("accuracy");

		double accuracy = evaluator.evaluate(predictions);
	    System.out.println("Test Error = " + (1.0 - accuracy));
			
		}
}
