##LEMATIZATION
import sparknlp
sparknlp.start()

from sparknlp.pretrained import PretrainedPipeline
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from pyspark.sql.functions import lower, col
from pyspark.ml.feature import StringIndexer

pipeline = PretrainedPipeline('explain_document_ml', 'en')
s_df2 = pipeline.annotate(s_df,"review_text")
s_df2=s_df2.drop(*["document","sentence","token","spell","stems","pos","text"])

def mkString(line):    
    return " ".join([str(x[3]) for x in line])
string_udf= udf(lambda z: mkString(z), StringType())
s_df2=s_df2.withColumn("lemmatizedText",string_udf("lemmas"))
s_df2=s_df2.withColumn("lemmatizedText", lower(col("lemmatizedText")))


# define processing 4 steps and execute them with a trsnformation pipeline
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer, IDF
from pyspark.ml import Pipeline
##LEMATIZATION


# 1. Tokenizer, .setPattern("\\p{L}+") means that it remove accent from words (check it has no impact on the smileys !!!)
tokenizer = RegexTokenizer().setGaps(False)\
  .setPattern("\\p{L}+")\
  .setInputCol("lemmatizedText")\
  .setOutputCol("words")

# 2. filter out stop words
sw_filter = StopWordsRemover()\
  .setStopWords(stop_words)\
  .setCaseSensitive(False)\
  .setInputCol("words")\
  .setOutputCol("filtered")

# 3. TF: TF vectorization + remove words that appear in 20 docs or less
cv = CountVectorizer(minTF=1., minDF=20., vocabSize=2**17)\
  .setInputCol("filtered")\
  .setOutputCol("tf")
# 4. TF-IDF transform
idf = IDF().\
    setInputCol('tf').\
    setOutputCol('tfidf')
# Create a pipelined transformer
tfidf_pipeline = Pipeline(stages=[tokenizer, sw_filter, cv, idf]).fit(s_df2)