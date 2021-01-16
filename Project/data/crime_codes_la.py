def assign_nibris_code_la(row):
    if row == 110:   # CRIMINAL HOMICIDE
        return 0
    elif row == 113: # MANSLAUGHTER, NEGLIGENT
        return 1
    elif row == 121: # RAPE, FORCIBLE
        return 2
    elif row == 122: # RAPE, ATTEMPTED
        return 2
    elif row == 210: # ROBBERY
        return 3
    elif row == 220: # ATTEMPTED ROBBERY
        return 3
    elif row == 230: # ASSAULT WITH DEADLY WEAPON, AGGRAVATED ASSAULT
        return 4
    elif row == 231: # ASSAULT WITH DEADLY WEAPON ON POLICE OFFICER
        return 4
    elif row == 235: # CHILD ABUSE (PHYSICAL) - AGGRAVATED ASSAULT
        return 20
    elif row == 236: # INTIMATE PARTNER - AGGRAVATED ASSAULT
        return 4
    elif row == 237: # CHILD NEGLECT (SEE 300 W.I.C.)
        return 20
    elif row == 250: # SHOTS FIRED AT MOVING VEHICLE, TRAIN OR AIRCRAFT
        return 4
    elif row == 251: # SHOTS FIRED AT INHABITED DWELLING
        return 4
    elif row == 310: # BURGLARY
        return 5
    elif row == 320: # BURGLARY, ATTEMPTED
        return 5
    elif row == 330: # BURGLARY FROM VEHICLE
        return 5
    elif row == 331: # THEFT FROM MOTOR VEHICLE - GRAND ($400 AND OVER)
        return 6
    elif row == 341: # THEFT-GRAND ($950.01 & OVER)EXCPT,GUNS,FOWL,LIVESTK,PROD
        return 6
    elif row == 343: # SHOPLIFTING-GRAND THEFT ($950.01 & OVER)
        return 6
    elif row == 345: # DISHONEST EMPLOYEE - GRAND THEFT
        return 6
    elif row == 347: # GRAND THEFT / INSURANCE FRAUD
        return 6
    elif row == 349: # GRAND THEFT / AUTO REPAIR
        return 6
    elif row == 350: # THEFT, PERSON
        return 6
    elif row == 351: # PURSE SNATCHING
        return 6
    elif row == 352: # PICKPOCKET
        return 6
    elif row == 353: # DRUNK ROLL
        return 6
    elif row == 354: # THEFT OF IDENTITY
        return 6
    elif row == 410: # BURGLARY FROM VEHICLE, ATTEMPTED
        return 5
    elif row == 420: # THEFT FROM MOTOR VEHICLE - PETTY ($950 & UNDER)
        return 6
    elif row == 421: # THEFT FROM MOTOR VEHICLE - ATTEMPT
        return 6
    elif row == 432: # BLOCKING DOOR INDUCTION CENTER
        return 24
    elif row == 433: # DRIVING WITHOUT OWNER CONSENT (DWOC)
        return 26
    elif row == 434: # FALSE IMPRISONMENT
        return 26
    elif row == 435: # LYNCHING
        return 0
    elif row == 436: # LYNCHING - ATTEMPTED
        return 26
    elif row == 437: # RESISTING ARREST
        return 26
    elif row == 438: # RECKLESS DRIVING
        return 24
    elif row == 439: # FALSE POLICE REPORT
        return 26
    elif row == 440: # THEFT PLAIN - PETTY ($950 & UNDER)
        return 6
    elif row == 441: # THEFT PLAIN - ATTEMPT
        return 6
    elif row == 442: # SHOPLIFTING - PETTY THEFT ($950 & UNDER)
        return 6
    elif row == 443: # SHOPLIFTING - ATTEMPT
        return 6
    elif row == 444: # DISHONEST EMPLOYEE - PETTY THEFT
        return 6
    elif row == 445: # DISHONEST EMPLOYEE ATTEMPTED THEFT
        return 6
    elif row == 446: # PETTY THEFT - AUTO REPAIR
        return 6
    elif row == 450: # THEFT FROM PERSON - ATTEMPT
        return 6
    elif row == 451: # PURSE SNATCHING - ATTEMPT
        return 6
    elif row == 452: # PICKPOCKET, ATTEMPT
        return 6
    elif row == 453: # DRUNK ROLL - ATTEMPT
        return 6
    elif row == 470: # TILL TAP - GRAND THEFT ($950.01 & OVER)
        return 6
    elif row == 471: # TILL TAP - PETTY ($950 & UNDER)
        return 6
    elif row == 472: # TILL TAP - ATTEMPT
        return 6
    elif row == 473: # THEFT, COIN MACHINE - GRAND ($950.01 & OVER)
        return 6
    elif row == 474: # THEFT, COIN MACHINE - PETTY ($950 & UNDER)
        return 6
    elif row == 475: # THEFT, COIN MACHINE - ATTEMPT
        return 6
    elif row == 480: # BIKE - STOLEN
        return 7
    elif row == 485: # BIKE - ATTEMPTED STOLEN
        return 7
    elif row == 487: # BOAT - STOLEN
        return 7
    elif row == 510: # VEHICLE - STOLEN
        return 7
    elif row == 520: # VEHICLE - ATTEMPT STOLEN
        return 7
    elif row == 522: # VEHICLE - MOTORIZED SCOOTERS, BICYCLES, AND WHEELCHAIRS
        return 7
    elif row == 622: # BATTERY ON A FIREFIGHTER
        return 8
    elif row == 623: # BATTERY POLICE (SIMPLE)
        return 8
    elif row == 624: # BATTERY - SIMPLE ASSAULT
        return 8
    elif row == 625: # OTHER ASSAULT
        return 8
    elif row == 626: # INTIMATE PARTNER - SIMPLE ASSAULT
        return 8
    elif row == 627: # CHILD ABUSE (PHYSICAL) - SIMPLE ASSAULT
        return 8
    elif row == 647: # THROWING OBJECT AT MOVING VEHICLE
        return 8
    elif row == 648: # ARSON
        return 9
    elif row == 649: # DOCUMENT FORGERY / STOLEN FELONY
        return 10
    elif row == 651: # DOCUMENT WORTHLESS ($200.01 & OVER)
        return 10
    elif row == 652: # DOCUMENT WORTHLESS ($200 & UNDER)
        return 10
    elif row == 653: # CREDIT CARDS, FRAUD USE ($950.01 & OVER)
        return 11
    elif row == 654: # CREDIT CARDS, FRAUD USE ($950 & UNDER
        return 11
    elif row == 660: # COUNTERFEIT
        return 10
    elif row == 661: # UNAUTHORIZED COMPUTER ACCESS
        return 13
    elif row == 662: # BUNCO, GRAND THEFT
        return 6
    elif row == 664: # BUNCO, PETTY THEFT
        return 6
    elif row == 666: # BUNCO, ATTEMPT
        return 6
    elif row == 668: # EMBEZZLEMENT, GRAND THEFT ($950.01 & OVER)
        return 12
    elif row == 670: # EMBEZZLEMENT, PETTY THEFT ($950 & UNDER)
        return 12
    elif row == 740: # VANDALISM - FELONY ($400 & OVER, ALL CHURCH VANDALISMS)
        return 14
    elif row == 745: # VANDALISM - MISDEAMEANOR ($399 OR UNDER)
        return 14
    elif row == 753: # DISCHARGE FIREARMS/SHOTS FIRED
        return 4
    elif row == 755: # BOMB SCARE
        return 4
    elif row == 756: # WEAPONS POSSESSION/BOMBING
        return 15
    elif row == 760: # LEWD/LASCIVIOUS ACTS WITH CHILD
        return 20
    elif row == 761: # BRANDISH WEAPON
        return 15
    elif row == 762: # LEWD CONDUCT
        return 24
    elif row == 763: # STALKING
        return 24
    elif row == 805: # PIMPING
        return 16
    elif row == 806: # PANDERING
        return 16
    elif row == 810: # SEX,UNLAWFUL(INC MUTUAL CONSENT, PENETRATION W/ FRGN OBJ
        return 17
    elif row == 812: # CRM AGNST CHLD (13 OR UNDER) (14-15 & SUSP 10 YRS OLDER)
        return 20
    elif row == 813: # CHILD ANNOYING (17YRS & UNDER)
        return 20
    elif row == 814: # CHILD PORNOGRAPHY
        return 16
    elif row == 815: # SEXUAL PENETRATION W/FOREIGN OBJECT
        return 17
    elif row == 820: # ORAL COPULATION
        return 17
    elif row == 821: # SODOMY/SEXUAL CONTACT B/W PENIS OF ONE PERS TO ANUS OTH
        return 17
    elif row == 822: # HUMAN TRAFFICKING - COMMERCIAL SEX ACTS
        return 17
    elif row == 830: # INCEST (SEXUAL ACTS BETWEEN BLOOD RELATIVES)
        return 17
    elif row == 840: # BEASTIALITY, CRIME AGAINST NATURE SEXUAL ASSLT WITH ANIM
        return 17
    elif row == 845: # SEX OFFENDER REGISTRANT OUT OF COMPLIANCE
        return 17
    elif row == 850: # INDECENT EXPOSURE
        return 17
    elif row == 860: # BATTERY WITH SEXUAL CONTACT
        return 17
    elif row == 865: # DRUGS, TO A MINOR
        return 18
    elif row == 870: # CHILD ABANDONMENT
        return 20
    elif row == 880: # DISRUPT SCHOOL
        return 24
    elif row == 882: # INCITING A RIOT
        return 24
    elif row == 884: # FAILURE TO DISPERSE
        return 24
    elif row == 886: # DISTURBING THE PEACE
        return 24
    elif row == 888: # TRESPASSING
        return 5
    elif row == 890: # FAILURE TO YIELD
        return 26
    elif row == 900: # VIOLATION OF COURT ORDER
        return 24
    elif row == 901: # VIOLATION OF RESTRAINING ORDER
        return 24
    elif row == 902: # VIOLATION OF TEMPORARY RESTRAINING ORDER
        return 24
    elif row == 903: # CONTEMPT OF COURT
        return 24
    elif row == 905: # FIREARMS TEMPORARY RESTRAINING ORDER (TEMP FIREARMS RO)
        return 26
    elif row == 906: # FIREARMS RESTRAINING ORDER (FIREARMS RO)
        return 26
    elif row == 910: # KIDNAPPING
        return 26
    elif row == 920: # KIDNAPPING - GRAND ATTEMPT
        return 26
    elif row == 921: # HUMAN TRAFFICKING - INVOLUNTARY SERVITUDE
        return 26
    elif row == 922: # CHILD STEALING
        return 26
    elif row == 924: # TELEPHONE PROPERTY - DAMAGE
        return 14
    elif row == 926: # TRAIN WRECKING
        return 26
    elif row == 928: # THREATENING PHONE CALLS/LETTERS
        return 26
    elif row == 930: # CRIMINAL THREATS - NO WEAPON DISPLAYED
        return 8
    elif row == 931: # REPLICA FIREARMS(SALE,DISPLAY,MANUFACTURE OR DISTRIBUTE)
        return 11
    elif row == 932: # PEEPING TOM
        return 24
    elif row == 933: # PROWLER
        return 5
    elif row == 940: # EXTORTION
        return 26
    elif row == 942: # BRIBERY
        return 26
    elif row == 943: # CRUELTY TO ANIMALS
        return 26
    elif row == 944: # CONSPIRACY
        return 26
    elif row == 946: # OTHER MISCELLANEOUS CRIME
        return 26
    elif row == 948: # BIGAMY
        return 17
    elif row == 949: # ILLEGAL DUMPING
        return 11
    elif row == 950: # DEFRAUDING INNKEEPER/THEFT OF SERVICES, OVER $400
        return 10
    elif row == 951: # DEFRAUDING INNKEEPER/THEFT OF SERVICES, $400 & UNDER
        return 10
    elif row == 952: # ABORTION/ILLEGAL
        return 26
    elif row == 954: # CONTRIBUTING
        return 26
    elif row == 956: # LETTERS, LEWD  -  TELEPHONE CALLS, LEWD
        return 26
    else:
        return 26
