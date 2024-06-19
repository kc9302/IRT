import pyspark
from girth_execute import execute_girth_3pl
from py4j.protocol import Py4JJavaError
from pyspark.errors import PySparkException, PythonException, PySparkAttributeError, PySparkValueError, PySparkTypeError
from pyspark.sql import functions as F

"""
spark 환경에서 3pl model 을 분산 처리와 result data 를 전처리 하기 위한 함수.
:param dataframe: pyspark.sql.DataFrame -> raw_shape: {학생 ID, 시험 ID, 문항 ID, 문항 번호, 정오}
:return: pyspark.rdd.RDD -> DataFrame[test_id: bigint,
                                      ques_id: string,
                                      discrimination: double,
                                      difficulty: double,
                                      guessing: double]
                                      
                            [Row(test_id=31, ques_id='14', discrimination=0.25000000000000977,
                                difficulty=-5.596415436499063, guessing=0.3299999850988382),
                             Row(test_id=31, ques_id='2062', discrimination=2.1892236422589395,
                                difficulty=-0.7113258608999211, guessing=5.960464499743515e-08)]                                      
"""


def distributed_processing(dataframe, error_code) -> pyspark.rdd.RDD and dict:
    preprocessed_result_data = ""
    try:
        preprocessed_result_data = dataframe\
            .groupBy('test_id', 'ques_id').agg(F.collect_list("crt_yn").alias("crt_yn_list"))\
            .groupBy("test_id").agg(F.collect_list("ques_id").alias("ques_id_list"),
                                    F.collect_list("crt_yn_list").alias("crt_yn_list")).rdd.flatMap(execute_girth_3pl)\
            .toDF(["test_id", "ques_id", "discrimination", "difficulty", "guessing"])
    except PySparkAttributeError:
        error_code["ERROR"] = "[PySparkAttributeError] Attribute error"
    except PySparkValueError:
        error_code["ERROR"] = "[PySparkValueError] Attribute error"
    except PySparkTypeError:
        error_code["ERROR"] = "[PySparkTypeError] Attribute error"
    except PythonException:
        error_code["ERROR"] = "[PythonException] Python error"
    except PySparkException as spark_E:
        error_code["ERROR"] = f"[PySparkException] error : {spark_E}"
    except ValueError:
        error_code["ERROR"] = "[ValueError] check def distributed_processing()"
    except IndexError:
        error_code["ERROR"] = "[IndexError] check config.py host"
    except KeyError:
        error_code["ERROR"] = "[KeyError] check config.py keys or def connect_spark()"
    except AttributeError:
        error_code["ERROR"] = "[AttributeError] check def distributed_processing()"
    except NameError:
        error_code["ERROR"] = "[NameError] check def distributed_processing()"
    except Py4JJavaError as py:
        error_code["ERROR"] = "[Py4JJavaError] PySpark와 Java 간의 연결 문제 -> ValueError 가능성 있음 " + py.errmsg
    except Exception as err:
        error_code["ERROR"] = f"Unexpected {err=}, {type(err)=}"
    finally:
        return preprocessed_result_data, error_code
