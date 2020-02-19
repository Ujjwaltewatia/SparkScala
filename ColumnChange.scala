import org.apache.spark.sql.Functions._
import org.apache.spark.sql.types.DecimalType
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.types.DataType 

var listCoIName=new ListBuffer[String]
var listCoIType=new ListBuffer[String]



//Creating DataFrame by Reading From Source
var newTargetDF=??

//Taking column names of newTargetDF Dataframe
val allcolumns = newTargetDF.columns

//Creating columnName and columnType Variable
var columnName:String=""
var columnType:String=""

//Creating a List that contains all columns DataTypes
val columnTypesList = newTargetDF.schema.map(_.datatype).toList

//Converting columnTypes List into String Type and putting Values into new ListBuffer listCoIType
for(columnType <- columnTypesList){
	listCoIType +=columnType.toString()
}


//Converting Array into List by iterating through values of array and storing it into new ListBuffer listColName
for (columnName <- allcolumns){
  listColName += columnName
}

//Converting ListBuffer listCoIType and listCoIName into List of String Type
var listColTypeString:List[String]=listCoIType.toList
var listColNameString:List[String]=listCoIName.toList


//zip of listColNameString and listColTypeString
val colNameType = listColNameString zip listColTypeString

//now we can access columnsNames and ColumnTypes simultaneously where x is column Name and y is ColumnType
// And changing DataTypes of columns with DecimalTypes to DecimalTypes(11,2) where setting scale of them
// If DataType of Column is Not DecimalType then keeping columns as it as.

for((x,y) <- colNameType){
	if(y.startsWith("DecimalType")){
		newTargetDF=newTargetDF.withColumn(x,col(x).cast(DecimalType(11,2)))
	}else{
		newTargetDF=newTargetDF.withColumn(x,col(x)))
	}
}
