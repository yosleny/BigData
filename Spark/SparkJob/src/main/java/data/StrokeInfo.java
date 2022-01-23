package data;

public class StrokeInfo {
	
	int gender;
	int age;
	int sufferHypertension;
	int sufferHeartDesease;
	int isEverMarried;
	int workType;
	int isUrbanResident;
	double avgGlucoseLevel;
	double bmi;
	int isSmoker;
	
	
	public StrokeInfo() {}
	
	public StrokeInfo(int gender, double age, int hypertension, int heartDesease, int everMarried, int workType, int residenceType, double avgGlucoseLevel, double bmi, int smoker) {
		super();
		this.gender = gender;
		this.age = (int)age;
		this.sufferHypertension = hypertension;
		this.sufferHeartDesease = heartDesease;
		this.isEverMarried = everMarried;
		this.workType = workType;
		this.isUrbanResident = residenceType;
		this.avgGlucoseLevel = avgGlucoseLevel;
		this.bmi = bmi;
		this.isSmoker = smoker;
	}
	
	public int getGender() { return this.gender; }
	
    public void setGender(int value) { this.gender = value; }
    
    public int getAge() { return this.age; }
    
    public void setAge(double value) { this.age = (int)value; }
    
    public int getSufferHypertension () { return this.sufferHypertension; }
	
    public void setSufferHypertension(int value) { this.sufferHypertension = value; }
    
    public int getSufferHeartDesease () { return this.sufferHeartDesease; }
	
    public void setGenderSufferHeartDesease(int value) { this.sufferHeartDesease = value; }
    
    public int getIsEverMarried() { return this.isEverMarried; }
	
    public void setIsEverMarried(int value) { this.isEverMarried = value; }
    
    public int getWorkType() { return this.workType; }
	
    public void setWorkType(int value) { this.workType = value; }
    
    public int getIsUrbanResident() { return this.isUrbanResident; }
	
    public void setIsUrbanResident(int value) { this.isUrbanResident = value; }
    
    public double getAvgGlucoseLevel() { return this.avgGlucoseLevel; }
	
    public void setAvgGlucoseLevel(double value) { this.avgGlucoseLevel = value; }
    
    public double getBmi() { return this.bmi; }
	
    public void setBmi(double value) { this.bmi = value; }
    
    public int getIsSmoker() { return this.isSmoker; }
	
    public void setIsSmoker(int value) { this.isSmoker = value; }

}
