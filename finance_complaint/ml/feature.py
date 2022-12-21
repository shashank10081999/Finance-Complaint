from pyspark import keyword_only  ## < 2.0 -> pyspark.ml.util.keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import Param, Params, TypeConverters, HasOutputCols, \
    HasInputCols
# Available in PySpark >= 2.3.0 
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.ml import Estimator
from pyspark.sql import DataFrame
from pyspark.sql.functions import desc
from pyspark.sql.functions import col, abs
from typing import List
from pyspark.sql.types import TimestampType, LongType
from finance_complaint.logger import logger
from finance_complaint.config.spark_manager import spark_session




class DerivedFeatureGenerator(Transformer, HasInputCols, HasOutputCols,DefaultParamsReadable, DefaultParamsWritable):

    @keyword_only
    def __init__(self,inputCols:List[str] = None , outputCols:Lits[str] = None):
        super(DerivedFeatureGenerator,self).__init__()
        kwargs = self._input_kwargs
        self.seconds_in_day = 60*60*24
        self.setParams(**kwargs)
    
    def setParams(self,inputCols:List[str] = None , outputCols:Lits[str] = None):
        kwargs = self._input_kwargs
        self._set(**kwargs)

    def setInputCols(self , value : List[str]=None):
        return self._set(inputCols=value)

    def setOutputCols(self,value:List[str] = None):
        return self._set(outputCols = value)
    
    def _fit(self , dataframe: DataFrame):
        return self

    def _transform(self,dataframe:DataFrame):
        inputCols = self.getInputCols()

        for i in inputCols:
            dataframe = dataframe.withColumn(i ,col(column).cast(TimestampType()))

        dataframe = dataframe.withColumn(self.getOutputCols()[0], abs(
            col(inputCols[1]).cast(LongType()) - col(inputCols[0]).cast(LongType())) / (
                                             self.second_within_day))
    




class FrequencyImputer(Estimator ,HasInputCols, HasOutputCols,DefaultParamsReadable, DefaultParamsWritable):

    """
    To build a custom transformer in pyspark , we need write 2 diff class one is 'FrequencyImputer' and 'FrequencyImputerModel' 
    2nd class name should end with 'Model'
    1st class will have the fit function and 2nd class will have the transform function
    """
    @keyword_only
    def __init__(self, inputCols: List[str] = None, outputCols: List[str] = None, ):
        """
        @keyword_only - A decorator that forces keyword arguments in the wrapped method and saves actual input keyword arguments in _input_kwargs
        """
        super(FrequencyImputer, self).__init__()
        self.topCategorys = Param(self, "topCategorys", "") # here we have to declare the parameter that we learned in the fit function
        self._setDefault(topCategorys="") # here we are giving the default value for the parameters
        kwargs = self._input_kwargs # taking all the parameters 
        print(kwargs)
        self.setParams(**kwargs) # here we are going to a below function , where are set the inputcols and outputcols

    @keyword_only
    def setParams(self, inputCols: List[str] = None, outputCols: List[str] = None, ):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @keyword_only
    def setTopCategorys(self, value: List[str]):
        "here are setting value to the variable topCategorys"
        return self._set(topCategorys=value)

    @keyword_only
    def getTopCategorys(self):
        "function will return the value in the topCategorys variable"
        return self.getOrDefault(self.topCategorys)

    @keyword_only
    def setInputCols(self, value : Lits[str] = None):
        return self._set(inputCols = value)
    
    @keyword_only
    def setOutputCols(self,value:List[str] = None):
        return self._set(outputCols = value)
    
    def _fit(self,dataset : DataFrame):
        inputCols = self.getInputCols()
        topCategorys = []

        for col in inputCols:
            categoryCountByDesc  = dataset.groupBy(col).count().sort(desc("count"))
            topCategorys.append(categoryCountByDesc.take[1][0][col])
        
        self.setTopCategorys(value=topCategorys)

        estimator = FrequencyImputerModel(inputCols=self.getInputCols(),
                                          outputCols=self.getOutputCols())

        estimator.setTopCategorys(value=topCategorys)
        return estimator

class FrequencyImputerModel(FrequencyImputer, Transformer):

    def __init__(self, inputCols: List[str] = None, outputCols: List[str] = None, ):
        super(FrequencyImputerModel, self).__init__(inputCols=inputCols, outputCols=outputCols)

    def _transform(self,dataset : DataFrame):

        topCategorys = self.getTopCategorys()
        outputCols = self.getOutputCols()

        updateMissingValue = dict(zip(outputCols, topCategorys))

        inputCols = self.getInputCols()
        for outputColumn, inputColumn in zip(outputCols, inputCols):
            dataset = dataset.withColumn(outputColumn, col(inputColumn))

        dataset = dataset.na.fill(updateMissingValue)

        return dataset


