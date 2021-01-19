from contextlib import redirect_stdout
from timeit import default_timer
from typing import List

from pyspark.ml.fpm import FPGrowth
from pyspark.sql.functions import col
from pyspark.sql.types import Row, DoubleType, ArrayType, StringType
from pyspark.sql.udf import UserDefinedFunction

from association_common import clear_output, load_data


def print_result(result: List[Row], source_shape: (int, int)) -> None:
    max_tuple_size = max([len(r['items']) for r in result])
    for r in result:
        if len(r['items']) > 1 or max_tuple_size < 2:
            support = r['freq'] / source_shape[0]
            print(f'{list_to_string(r["items"])}; supp={support}')


def printer() -> None:
    print('\n#################################### NEW YORK ###################################')
    resny.associationRules.orderBy('confidence', ascending=False).show(resny.associationRules.count(), False)
    resny.freqItemsets.withColumn(
        'support', ny_supp(col('freq').cast(DoubleType()), col('items').cast(ArrayType(StringType())))
    ).orderBy('freq', ascending=False).show(resny.freqItemsets.count(), False)
    print('\n#################################### CHICAGO ####################################')
    resch.associationRules.orderBy('confidence', ascending=False).show(resch.associationRules.count(), False)
    resch.freqItemsets.withColumn(
        'support', ch_supp(col('freq').cast(DoubleType()), col('items').cast(ArrayType(StringType())))
    ).orderBy('freq', ascending=False).show(resch.freqItemsets.count(), False)
    print('\n################################## LOS ANGELES ##################################')
    resla.associationRules.orderBy('confidence', ascending=False).show(resla.associationRules.count(), False)
    resla.freqItemsets.withColumn(
        'support', la_supp(col('freq').cast(DoubleType()), col('items').cast(ArrayType(StringType())))
    ).orderBy('freq', ascending=False).show(resla.freqItemsets.count(), False)


def list_to_string(list_: list) -> str:
    return f'[{", ".join(x for x in list_)}]'


# CALCULATIONS ################################################################
dfny, dfch, dfla, timer_l = load_data()
timer_c = default_timer()

fpm = FPGrowth(itemsCol='features', minSupport=0.15, minConfidence=0.5)
dfny_len = dfny.count()
dfch_len = dfch.count()
dfla_len = dfla.count()
ny_supp = UserDefinedFunction(lambda x, y: x / dfny_len if len(y) == 1 else '-', StringType())
ch_supp = UserDefinedFunction(lambda x, y: x / dfch_len if len(y) == 1 else '-', StringType())
la_supp = UserDefinedFunction(lambda x, y: x / dfla_len if len(y) == 1 else '-', StringType())

resny = fpm.fit(dfny)
del dfny
resch = fpm.fit(dfch)
del dfch
resla = fpm.fit(dfla)
del dfla

# OUTPUT ######################################################################
clear_output()
with open('result_FPGrowth.txt', 'w') as file:
    with redirect_stdout(file):
        printer()
        print(f'Data loading time:\t{timer_c - timer_l:.2f} s')
        print(f'Computing time:   \t{default_timer() - timer_c:.2f} s')
