def assign_nibris_code_ny(row):
    if row == 101:   # MURDER & NON-NEGL. MANSLAUGHTER
        return 0
    elif row == 102: # HOMICIDE-NEGLIGENT-VEHICLE
        return 1
    elif row == 103: # HOMICIDE-NEGLIGENT,UNCLASSIFIE
        return 1
    elif row == 104: # RAPE
        return 2
    elif row == 105: # ROBBERY
        return 3
    elif row == 106: # FELONY ASSAULT
        return 4
    elif row == 107: # BURGLARY
        return 5
    elif row == 109: # GRAND LARCENY
        return 6
    elif row == 110: # GRAND LARCENY OF MOTOR VEHICLE
        return 7
    elif row == 111: # POSSESSION OF STOLEN PROPERTY
        return 13
    elif row == 112: # THEFT-FRAUD
        return 11
    elif row == 113: # FORGERY
        return 10
    elif row == 114: # ARSON
        return 9
    elif row == 115: # PROSTITUTION & RELATED OFFENSES
        return 16
    elif row == 116: # FELONY SEX CRIMES
        return 17
    elif row == 116: # SEX CRIMES
        return 17
    elif row == 117: # DANGEROUS DRUGS
        return 18
    elif row == 118: # DANGEROUS WEAPONS
        return 15
    elif row == 119: # INTOXICATED/IMPAIRED DRIVING
        return 26
    elif row == 120: # CHILD ABANDONMENT/NON SUPPORT
        return 20
    elif row == 120: # ENDAN WELFARE INCOMP
        return 26
    elif row == 121: # CRIMINAL MISCHIEF & RELATED OF
        return 14
    elif row == 122: # GAMBLING
        return 19
    elif row == 123: # ABORTION
        return 26
    elif row == 124: # KIDNAPPING & RELATED OFFENSES
        return 26
    elif row == 125: # NYS LAWS-UNCLASSIFIED FELONY
        return 26
    elif row == 125: # OTHER STATE LAWS (NON PENAL LA
        return 26
    elif row == 126: # MISCELLANEOUS PENAL LAW
        return 26
    elif row == 230: # JOSTLING
        return 8
    elif row == 231: # BURGLARS TOOLS
        return 5
    elif row == 232: # POSSESSION OF STOLEN PROPERTY
        return 13
    elif row == 233: # SEX CRIMES
        return 17
    elif row == 234: # PROSTITUTION & RELATED OFFENSES
        return 16
    elif row == 235: # DANGEROUS DRUGS
        return 18
    elif row == 236: # DANGEROUS WEAPONS
        return 15
    elif row == 237: # ESCAPE 3
        return 24
    elif row == 238: # FRAUDULENT ACCOSTING
        return 11
    elif row == 340: # FRAUDS
        return 11
    elif row == 341: # PETIT LARCENY
        return 6
    elif row == 342: # PETIT LARCENY OF MOTOR VEHICLE
        return 7
    elif row == 343: # OTHER OFFENSES RELATED TO THEF
        return 6
    elif row == 343: # THEFT OF SERVICES
        return 6
    elif row == 344: # ASSAULT 3 & RELATED OFFENSES
        return 8
    elif row == 345: # ENDAN WELFARE INCOMP
        return 26
    elif row == 345: # OFFENSES RELATED TO CHILDREN
        return 20
    elif row == 346: # ALCOHOLIC BEVERAGE CONTROL LAW
        return 26
    elif row == 347: # INTOXICATED & IMPAIRED DRIVING
        return 26
    elif row == 348: # VEHICLE AND TRAFFIC LAWS
        return 26
    elif row == 349: # DISRUPTION OF A RELIGIOUS SERV
        return 24
    elif row == 350: # GAMBLING
        return 19
    elif row == 351: # CRIMINAL MISCHIEF & RELATED OF
        return 14
    elif row == 352: # CRIMINAL TRESPASS
        return 26
    elif row == 353: # UNAUTHORIZED USE OF A VEHICLE
        return 26
    elif row == 354: # ANTICIPATORY OFFENSES
        return 24
    elif row == 355: # OFFENSES AGAINST THE PERSON
        return 26
    elif row == 356: # PROSTITUTION & RELATED OFFENSES
        return 16
    elif row == 357: # FORTUNE TELLING
        return 26
    elif row == 358: # OFFENSES INVOLVING FRAUD
        return 11
    elif row == 359: # OFFENSES AGAINST PUBLIC ADMINI
        return 26
    elif row == 360: # LOITERING FOR DRUG PURPOSES
        return 18
    elif row == 361: # OFF. AGNST PUB ORD SENSBLTY &
        return 26
    elif row == 362: # OFFENSES AGAINST MARRIAGE UNCL
        return 26
    elif row == 363: # OFFENSES AGAINST PUBLIC SAFETY
        return 26
    elif row == 364: # AGRICULTURE & MRKTS LAW-UNCLASSIFIED
        return 11
    elif row == 364: # OTHER STATE LAWS (NON PENAL LA
        return 26
    elif row == 364: # OTHER STATE LAWS (NON PENAL LAW)
        return 26
    elif row == 365: # ADMINISTRATIVE CODE
        return 26
    elif row == 366: # NEW YORK CITY HEALTH CODE
        return 26
    elif row == 455: # UNLAWFUL POSS. WEAP. ON SCHOOL
        return 15
    elif row == 460: # LOITERING/DEVIATE SEX
        return 17
    elif row == 571: # LOITERING/GAMBLING (CARDS, DIC
        return 19
    elif row == 572: # DISORDERLY CONDUCT
        return 24
    elif row == 577: # UNDER THE INFLUENCE OF DRUGS
        return 18
    elif row == 578: # HARRASSMENT 2
        return 26
    elif row == 672: # LOITERING
        return 26
    elif row == 675: # ADMINISTRATIVE CODE
        return 26
    elif row == 676: # NEW YORK CITY HEALTH CODE
        return 26
    elif row == 677: # NYS LAWS-UNCLASSIFIED VIOLATION # OTHER STATE LAWS
        return 26
    elif row == 678: # MISCELLANEOUS PENAL LAW
        return 26
    elif row == 685: # ADMINISTRATIVE CODES
        return 26
    elif row == 881: # OTHER TRAFFIC INFRACTION
        return 26
    else:
        return 26