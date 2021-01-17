from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, lit, col
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_theme(style="darkgrid")


def get_most_common_crimes(df, city):
    crimes = df.groupBy('Crime Category').count().sort(
        desc('count')).limit(10)

    sns.barplot(y='Crime Category', x='count', data=crimes.toPandas())
    plt.title('Most common crimes - ' + city)
    plt.show()


def get_most_common_locations(df, city):
    locations = df.groupBy('Location Type').count().sort(
        desc('count')).limit(10)

    sns.barplot(y='Location Type', x='count', data=locations.toPandas())
    plt.title('Most common crime locations - ' + city)
    plt.show()


def get_victim_age(df1, df2, cols):
    df = df1.union(df2).groupBy(cols).count().replace('<18', '0-18')
    df = df.filter(col('Victim Age Group') !=
                   'UNKNOWN').orderBy('Victim Age Group')
    return df.replace('0-18', '<18')


def get_suspect_sex_plot(df, title):
    df.set_index('Suspect Sex', inplace=True)
    df.plot.pie(y='count', figsize=(5, 5), title=title,
                legend=False, ylabel='Suspect Sex', autopct='%.2f%%')
    plt.show()


def get_victim_sex_plot(df, title):
    df.set_index('Victim Sex', inplace=True)
    df.plot.pie(y='count', figsize=(5, 5), title=title,
                legend=False, ylabel='Victim Sex', autopct='%.2f%%')
    plt.show()


def get_victim_sex(df, title):
    victim_sex = df.filter(col('Victim Sex') != 'U')

    sexual_crimes = victim_sex.filter(col('Crime Category').contains(
        'SEX') | col('Crime Category').contains('RAPE')).groupBy('Victim Sex').count()

    other_crimes = victim_sex.filter(~col('Crime Category').contains(
        'SEX') & ~col('Crime Category').contains('RAPE')).groupBy('Victim Sex').count()

    get_victim_sex_plot(sexual_crimes.toPandas(),
                        'Sexual Crimes - ' + title)
    get_victim_sex_plot(other_crimes.toPandas(), 'Other Crimes - ' + title)


def main():
    spark = SparkSession.builder.appName('crimes').getOrCreate()

    df_ny = spark.read.csv('data/dfny.csv', header=True)
    df_ch = spark.read.csv('data/dfch.csv', header=True)
    df_la = spark.read.csv('data/dfla.csv', header=True)

    # MOST COMMON CRIMES AND LOCATIONS - NEW YORK, CHICAGO, LOS ANGELES, ALL

    get_most_common_crimes(df_ny, 'New York')
    get_most_common_crimes(df_ch, 'Chicago')
    get_most_common_crimes(df_la, 'Los Angeles')

    get_most_common_locations(df_ny, 'New York')
    get_most_common_locations(df_ch, 'Chicago')
    get_most_common_locations(df_la, 'Los Angeles')

    cols = ['Crime Category', 'Location Type']

    df_ny_ch = df_ny.select(*cols).union(df_ch.select(*cols))
    df_all = df_ny_ch.union(df_la.select(*cols))
    get_most_common_crimes(df_all, 'All')
    get_most_common_locations(df_all, 'All')

    # VICTIM AGE - NEW YORK, LOS ANGELES, BOTH

    cols_age = ['Victim Age Group']
    ny_age = df_ny.select(*cols_age).withColumn('City', lit('New York'))
    la_age = df_la.select(*cols_age).withColumn('City', lit('Los Angeles'))

    age_data = get_victim_age(ny_age, la_age, 'Victim Age Group')
    sns.barplot(x='Victim Age Group', y='count', data=age_data.toPandas())
    plt.title('Victim Age Group - All')
    plt.show()

    age_data_all = get_victim_age(
        ny_age, la_age, ['Victim Age Group', 'City'])

    sns.barplot(x='City', y='count', data=age_data_all.toPandas(),
                hue='Victim Age Group')
    plt.title('Victim Age Group - NY & LA')
    plt.show()

    # SUSPECT SEX - NEW YORK - SEXUAL AND OTHER CRIMES

    suspect_sex_ny = df_ny.filter(col('Suspect Sex') != 'U')
    sexual_crimes = suspect_sex_ny.filter(col('Crime Category').contains(
        'SEX') | col('Crime Category').contains('RAPE')).groupBy('Suspect Sex').count()

    other_crimes = suspect_sex_ny.filter(~col('Crime Category').contains(
        'SEX') & ~col('Crime Category').contains('RAPE')).groupBy('Suspect Sex').count()

    get_suspect_sex_plot(sexual_crimes.toPandas(), 'Sexual Crimes - New York')
    get_suspect_sex_plot(other_crimes.toPandas(), 'Other Crimes - New York')

    # VICTIM SEX - NEW YORK, LOS ANGELES, BOTH - SEXUAL AND OTHER CRIMES

    get_victim_sex(df_ny, 'New York')
    get_victim_sex(df_la, 'Los Angeles')

    cols = ['Crime Category', 'Victim Sex']
    df_ny_la = df_ny.select(*cols).union(df_la.select(*cols))
    get_victim_sex(df_ny_la, 'All')


if __name__ == "__main__":
    main()
