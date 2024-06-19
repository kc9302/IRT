import numpy as np
from girth.unidimensional.dichotomous import threepl_mml, ability_3pl_map
import pyspark

"""
3pl model 을 동작 하는 함수.
:param spark: SparkSession
:param dataframe: pyspark.sql.DataFrame -> raw_shape: {학생 ID, 시험 ID, 문항 ID}
:return: list -> raw_shape: [시험 ID, 문항 ID, 변별도, 난이도, 추측도]
"""


def execute_girth_3pl(df: pyspark.sql.DataFrame) -> list:
    result_threepl_mml = threepl_mml(np.array(df['crt_yn_list']))
    girth_3pl_result_list = []
    ques_list = list(df["ques_id_list"])
    discrimination_list = result_threepl_mml["Discrimination"].tolist()
    difficulty_list = result_threepl_mml["Difficulty"].tolist()
    guessing_list = result_threepl_mml["Guessing"].tolist()

    for ques in range(len(ques_list)):
        girth_3pl_result_list.append((
            df["test_id"],
            ques_list[ques],
            discrimination_list[ques],
            difficulty_list[ques],
            guessing_list[ques]
        ))

    return girth_3pl_result_list


"""
ability model 을 동작 하는 함수.
:param spark: SparkSession
:param dataframe: pyspark.sql.DataFrame -> raw_shape: {학생 ID, 시험 ID, 문항 ID}
:return: list -> raw_shape: [시험 ID, 문항 ID, 변별도, 난이도, 추측도]
"""


def execute_girth_ability(df: pyspark.sql.DataFrame) -> list:

    dataset = np.array([[df.crt_yn_list]])
    diff = np.array([df.difficulty_list])
    disc = np.array([df.discrimination_list])
    guss = np.array([df.guessing_list])
    result_ability_3pl = float(ability_3pl_map(dataset=dataset, difficulty=diff, discrimination=disc, guessing=guss))
    ability_ability_list = []

    for i in range(len(dataset)):
        ability_ability_list.append((df["stu_id"],
                                     df["test_id"],
                                     result_ability_3pl))
    return ability_ability_list
