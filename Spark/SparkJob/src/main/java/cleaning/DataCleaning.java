package cleaning;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import utils.ProjectConstants;
import data.StrokeInfo;



public class DataCleaning {

	public static Dataset<Row> clean(Dataset<Row> data) {
		
		return data.filter((FilterFunction<Row>) row -> {
															try {
																	if(row.anyNull())
																		return false;
																	
																	double age = row.getAs(ProjectConstants.Age);
																	String workType = row.getAs(ProjectConstants.WorkType);
																	
																	if(age <= 20 && workType.equals("children"))
																		return false;
																	
																	return true;
																}
															catch (Exception e) {
																	
																	return false;
																}
		});
	}
	
	
	/*public static Dataset<StrokeInfo> transform(Dataset<Row> data){
		
		return data.map((MapFunction<Row, StrokeInfo>) row -> {
																	return new StrokeInfo(
																			row.getAs(ProjectConstants.Gender),
																			row.getAs(ProjectConstants.Age),
																			row.getAs(ProjectConstants.SufferHypertension),
																			row.getAs(ProjectConstants.SufferHeartDesease),
																			row.getAs(ProjectConstants.IsEverMarried),
																			row.getAs(ProjectConstants.WorkType),
																			row.getAs(ProjectConstants.IsUrbanResident),
																			row.getAs(ProjectConstants.AvgGlucoseLevel),
																			row.getAs(ProjectConstants.Bmi),
																			row.getAs(ProjectConstants.IsSmoker)
																			);
		}, Encoders.bean(StrokeInfo.class));
		
	}*/
	
}
