package model;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import utils.ProjectConstants;

public class NaiveBayesClassification {
	
	NaiveBayes naiveBayes;
	NaiveBayesModel model;
	MulticlassClassificationEvaluator evaluator;
	Pipeline pipeline;
	PipelineModel pipelineModel;
	StandardScaler stdScaler;
	
	Dataset<Row> trainingData;
	Dataset<Row> testData;
	
	public NaiveBayesClassification(Dataset<Row> data) {
		
		VectorAssembler vectorAssembler = new VectorAssembler().setInputCols(new String[]{
				ProjectConstants.Gender, 
				ProjectConstants.Age, 
				ProjectConstants.SufferHypertension, 
				ProjectConstants.SufferHeartDesease, 
				ProjectConstants.IsEverMarried, 
				ProjectConstants.IsUrbanResident, 
				ProjectConstants.AvgGlucoseLevel, 
				ProjectConstants.Bmi, 
				ProjectConstants.IsSmoker, 
				ProjectConstants.Stroke
		}).setOutputCol("features");
		
		stdScaler = new StandardScaler()
        		.setInputCol("features")
        		.setOutputCol("scaledFeatures")
        		.setWithStd(true)
        		.setWithMean(false);
		
		naiveBayes = new NaiveBayes()
		         .setLabelCol(ProjectConstants.Stroke)
		         .setFeaturesCol("scaledFeatures");
       
       Dataset<Row>[] splits = data.randomSplit(new double[]{ 0.6, 0.4}, 1234L);
       trainingData = splits[0];
       testData = splits[1];
       
       pipeline = new Pipeline().setStages(new PipelineStage[]{ vectorAssembler, stdScaler, naiveBayes});
       
       fit();
       transform();
       
	}
	
	public void fit() {
		
		pipelineModel = pipeline.fit(trainingData);

		
	}
	
	public void transform() {
			
		Dataset<Row> predictions = pipelineModel.transform(testData);
		
		predictions.show();
		
		evaluator = new MulticlassClassificationEvaluator()
	      		.setLabelCol(ProjectConstants.Stroke)
	      		.setPredictionCol("prediction")
	      		.setMetricName("accuracy");

		double accuracy = evaluator.evaluate(predictions);
		System.out.println("_----------------------------------------------------------------------------------------------------------");
	    System.out.println("Test Error = " + (1.0 - accuracy));
			
		}

}
