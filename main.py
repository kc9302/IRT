from data_service_cassandra import get_raw_data, put_3pl_result_data
from spark_connection import connect_spark
from preprocessing_data import distributed_processing
from validation import check_spark_session, check_data_count, check_distributed_processing, check_error_code
from util import set_logging


def run_model() -> dict:
    result_code = ""

    # connect to spark
    spark, error_code = connect_spark()

    # check_spark_session
    check_spark_session(error_code=error_code)

    # select data
    raw_data_df, error_code = get_raw_data(spark, error_code)

    # distributed_processing
    preprocessed_result_data, error_code = distributed_processing(raw_data_df, error_code)

    # check_spark_session
    check_distributed_processing(error_code)

    # check_data_count
    check_data_count(raw_data_df, preprocessed_result_data)

    # insert preprocessed_result_data
    error_code = put_3pl_result_data(preprocessed_result_data, error_code)

    # spark 종료
    spark.stop()

    # check_error_code
    result_code = check_error_code(error_code)

    return result_code


if __name__ == "__main__":
    # config logging
    set_logging()

    # run model
    response_dict = run_model()
