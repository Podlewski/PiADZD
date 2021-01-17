import pandas


def clean_age_groups(row):
    return row if row in ['<18', '18-24', '25-44', '45-64', '65+', 'UNKNOWN'] else 'UNKNOWN'


def assign_age_groups(row):
    if row < 0:
        return 'UNKNOWN'
    elif row < 18:
        return '<18'
    elif row < 25:
        return '18-24'
    elif row < 45:
        return '25-44'
    elif row < 65:
        return '45-64'
    else:
        return '65+'


def assign_race(row):
    if row == 'W':
        return 'WHITE'
    elif row == 'B':
        return 'BLACK'
    elif row == 'H':
        return 'HISPANIC'
    elif row == 'I':
        return 'AMERICAN INDIAN/ALASKAN NATIVE'
    elif row == 'O':
        return 'OTHER'
    elif row in ['A', 'C', 'D', 'F', 'G', 'J', 'K', 'L', 'P', 'S', 'U', 'V', 'Z']:
        return 'ASIAN / PACIFIC ISLANDER'
    else:
        return 'UNKNOWN'


def merge_hispanic(row):
    if row in ['WHITE HISPANIC', 'BLACK HISPANIC']:
        return 'HISPANIC'
    else:
        return row


def simplify_arrest_status(row):
    if row == 'Adult Arrest' or row == 'Juv Arrest':
        return True
    else:
        return False


def reduce_nibris_code_chichago(row):
    if row == '01A':
        return 0
    elif row == '01B':
        return 1
    elif row == '04A' or row == '04B':
        return 4
    elif row == '08A' or row == '08B':
        return 8
    else:
        return int(row)


def translate_nibric_codes(row):
    if row == 0:
        return 'MANSLAUGHTER'
    elif row == 1:
        return 'INVOLUNTARY MANSLAUGHTER'
    elif row == 2:
        return 'SEXUAL ASSAULT'
    elif row == 3:
        return 'ROBBERY'
    elif row == 4:
        return 'AGGRAVATED ASSAULT'
    elif row == 5:
        return 'BURGLARY'
    elif row == 6:
        return 'THEFT'
    elif row == 7:
        return 'MOTOR VEHICLE THEFT'
    elif row == 8:
        return 'SIMPLE ASSAULT'
    elif row == 9:
        return 'ARSON'
    elif row == 10:
        return 'FORGERY & COUNTERFEITING'
    elif row == 11:
        return 'FRAUD'
    elif row == 12:
        return 'EMBEZZLEMENT'
    elif row == 13:
        return 'STOLEN PROPERTY'
    elif row == 14:
        return 'VANDALISM'
    elif row == 15:
        return 'WEAPONS VIOLATION'
    elif row == 16:
        return 'PROSTITUTION'
    elif row == 17:
        return 'SEXUAL ABUSE'
    elif row == 18:
        return 'DRUG ABUSE'
    elif row == 19:
        return 'GAMBLING'
    elif row == 20:
        return 'OFFENSES AGAINST FAMILY'
    elif row == 22:
        return 'LIQUOR LICENSE'
    elif row == 24:
        return 'DISORDERLY CONDUCT'
    elif row == 26:
        return 'OTHER'
    else:
        return 'OTHER'
