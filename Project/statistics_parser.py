import argparse


class ArgumentParser:
    args = None

    def __init__(self):
        self.parser = argparse.ArgumentParser(
            prog='PiADZD - Project', formatter_class=argparse.RawTextHelpFormatter,
            description='Politechnika Łódzka (Lodz University of Technology)\
                         \nPiADZD - Project\
                         \n\nAuthors:\
                         \n  Aleksandra Kowalczyk\t234073\
                         \n  Zbigniew Nowacki\t234102\
                         \n  Karol Podlewski\t234106')

        self.parser.add_argument('--show', dest='show_plot', 
                                 action='store_const', const=True, default=False,
                                 help='Show plots')

        self.parser.add_argument('-d', metavar='D', dest='plot_dpi', type=int,
                                 choices=[100, 150, 200, 250, 300],
                                 default=0, help='Save plots with D dpi')

        self.args = self.parser.parse_args()

    def get_filename(self):
        return self.args.filename

    def get_args(self):
        if (self.args.plot_dpi > 0):
            self.args.save_plot = True
        else:
            self.args.save_plot = False
        return self.args
