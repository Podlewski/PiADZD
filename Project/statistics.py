from mpl_toolkits.axes_grid1.axes_divider import make_axes_area_auto_adjustable
import matplotlib.pyplot as plt
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, lit, col
import seaborn as sns
from statistics_parser import ArgumentParser


sns.set_theme(style='darkgrid')


class Plotter:

    def __init__(self, show_plot, save_plot, plot_dpi):
        self.show_plot = show_plot
        self.save_plot = save_plot
        self.dpi = plot_dpi
        self.change_img_ratio()
        plt.rcParams['font.family'] = 'DejaVu Sans'
        plt.rcParams['legend.fontsize'] = 'large'
        plt.rcParams['figure.figsize'] = [9,5]
        if not os.path.exists('img'):
            os.makedirs('img')


    def save_show_plot(self, plot_title):
        plt.tight_layout()
        
        if self.save_plot == True:
            filename = 'img/' + plot_title.replace(' ', '_')
            plt.savefig(filename, dpi=self.dpi)
        
        if self.show_plot == True:
            plt.show()

        plt.clf()


    def barplot(self, plot_ax, plot_title, long_label=False):
        ax = plot_ax
        plt.title(plot_title)
        ax.set_yticklabels(ax.get_ymajorticklabels(), fontsize=8)

        if long_label == True:
            make_axes_area_auto_adjustable(ax)

        self.save_show_plot(plot_title)


    def get_most_common_crimes(self, df, city):
        crimes = df.groupBy('Crime Category').count().withColumnRenamed('count', 'Number of crimes').sort(
            desc('Number of crimes')).limit(10)

        ax = sns.barplot(y='Crime Category', x='Number of crimes',
                        data=crimes.toPandas())
        title = 'Most common NIBRS crimes - ' + city
        self.barplot(ax, title, long_label=True)

    def get_most_common_local_crimes(self, df, city):
        crimes = df.groupBy('Crime Description').count().withColumnRenamed('count', 'Number of crimes').sort(
            desc('Number of crimes')).limit(10)

        ax = sns.barplot(y='Crime Description', x='Number of crimes',
                        data=crimes.toPandas())
        title = 'Most common local crimes - ' + city
        self.barplot(ax, title, long_label=True)


    def get_most_common_locations(self, df, city):
        locations = df.groupBy('Location Type').count().withColumnRenamed('count', 'Number of crimes').sort(
            desc('Number of crimes')).limit(10)

        ax = sns.barplot(y='Location Type', x='Number of crimes',
                        data=locations.toPandas())
        title = 'Most common crime locations - ' + city
        self.barplot(ax, title, long_label=True)


    def get_victim_age(self, df1, df2, cols):
        df = df1.union(df2).groupBy(cols).count().withColumnRenamed(
            'count', 'Number of crimes').replace('<18', '0-18')
        df = df.filter(col('Victim Age Group') !=
                    'UNKNOWN').orderBy('Victim Age Group')
        return df.replace('0-18', '<18')


    def get_suspect_sex_plot(self, df, plot_title):
        df.set_index('Suspect Sex', inplace=True)
        df.plot.pie(y='count', figsize=(5, 5), title=plot_title,
                    legend=False, ylabel='Suspect Sex', autopct='%.2f%%')
        
        self.save_show_plot(plot_title)


    def get_victim_sex_plot(self, df, plot_title):
        df.set_index('Victim Sex', inplace=True)
        df.plot.pie(y='count', figsize=(5, 5), title=plot_title,
                    legend=False, ylabel='Victim Sex', autopct='%.2f%%')
        
        self.save_show_plot(plot_title)


    def get_victim_sex(self, df, title):
        victim_sex = df.filter(col('Victim Sex') != 'U')

        sexual_crimes = victim_sex.filter(col('Crime Category').contains(
            'SEX') | col('Crime Category').contains('RAPE')).groupBy('Victim Sex').count()

        other_crimes = victim_sex.filter(~col('Crime Category').contains(
            'SEX') & ~col('Crime Category').contains('RAPE')).groupBy('Victim Sex').count()

        self.get_victim_sex_plot(sexual_crimes.toPandas(),
                            'Sexual Crimes - ' + title)
        self.get_victim_sex_plot(other_crimes.toPandas(), 'Other Crimes - ' + title)


def main(args):
    spark = SparkSession.builder.appName('crimes').getOrCreate()

    plotter = Plotter(args.show_plot, args.save_plot, args.plot_dpi)

    df_ny = spark.read.csv('data/dfny.csv', header=True)
    df_ch = spark.read.csv('data/dfch.csv', header=True)
    df_la = spark.read.csv('data/dfla.csv', header=True)

    # MOST COMMON CRIMES AND LOCATIONS - NEW YORK, CHICAGO, LOS ANGELES, ALL

    plotter.get_most_common_crimes(df_ny, 'New York')
    plotter.get_most_common_crimes(df_ch, 'Chicago')
    plotter.get_most_common_crimes(df_la, 'Los Angeles')

    plotter.get_most_common_local_crimes(df_ny, 'New York')
    plotter.get_most_common_local_crimes(df_ch, 'Chicago')
    plotter.get_most_common_local_crimes(df_la, 'Los Angeles')

    plotter.get_most_common_locations(df_ny, 'New York')
    plotter.get_most_common_locations(df_ch, 'Chicago')
    plotter.get_most_common_locations(df_la, 'Los Angeles')

    cols = ['Crime Category', 'Location Type']

    df_ny_ch = df_ny.select(*cols).union(df_ch.select(*cols))
    df_all = df_ny_ch.union(df_la.select(*cols))
    plotter.get_most_common_crimes(df_all, 'All')
    plotter.get_most_common_locations(df_all, 'All')

    # VICTIM AGE - NEW YORK, LOS ANGELES, BOTH

    cols_age = ['Victim Age Group']
    ny_age = df_ny.select(*cols_age).withColumn('City', lit('New York'))
    la_age = df_la.select(*cols_age).withColumn('City', lit('Los Angeles'))

    age_data = plotter.get_victim_age(ny_age, la_age, 'Victim Age Group')
    sns.barplot(x='Victim Age Group', y='Number of crimes',
                data=age_data.toPandas())
    plot_title = 'Victim Age Group - All'
    plt.title(plot_title)
    plotter.save_show_plot(plot_title)

    age_data_all = plotter.get_victim_age(
        ny_age, la_age, ['Victim Age Group', 'City'])

    sns.barplot(x='City', y='Number of crimes', data=age_data_all.toPandas(),
                hue='Victim Age Group')

    plot_title = 'Victim Age Group - NY & LA'
    plt.title(plot_title)
    plotter.save_show_plot(plot_title)

    # SUSPECT SEX - NEW YORK - SEXUAL AND OTHER CRIMES

    suspect_sex_ny = df_ny.filter(col('Suspect Sex') != 'U')
    sexual_crimes = suspect_sex_ny.filter(col('Crime Category').contains(
        'SEX') | col('Crime Category').contains('RAPE')).groupBy('Suspect Sex').count()

    other_crimes = suspect_sex_ny.filter(~col('Crime Category').contains(
        'SEX') & ~col('Crime Category').contains('RAPE')).groupBy('Suspect Sex').count()

    plotter.get_suspect_sex_plot(sexual_crimes.toPandas(), 'Sexual Crimes - New York')
    plotter.get_suspect_sex_plot(other_crimes.toPandas(), 'Other Crimes - New York')

    # VICTIM SEX - NEW YORK, LOS ANGELES, BOTH - SEXUAL AND OTHER CRIMES

    plotter.get_victim_sex(df_ny, 'New York')
    plotter.get_victim_sex(df_la, 'Los Angeles')

    cols = ['Crime Category', 'Victim Sex']
    df_ny_la = df_ny.select(*cols).union(df_la.select(*cols))
    plotter.get_victim_sex(df_ny_la, 'All')


if __name__ == '__main__':
    argument_parser = ArgumentParser()
    args = argument_parser.get_args()
    main(args)
