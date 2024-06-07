import pandas as pd


def prepare_nonmoto_data(data):
    data = data[['Report Number', 'Injury Severity']]
    data.columns = ['ReportNumber', 'InjurySeverity']
    data = data.astype(str)
    return data


def classify_injury(injury):
    words = injury.lower().split(' ')
    if 'fatal' in words:
        return 'Fatal'
    elif 'no' in words:
        return 'No injury'
    elif 'injury' in words:
        return 'Injury'
    else:
        return 'No injury'


def transform_nonmoto_data(data):
    nonmoto = data.copy()

    nonmoto['InjurySeverity'] = nonmoto['InjurySeverity'].apply(classify_injury)

    nonmoto['Fatal'] = nonmoto['InjurySeverity'].apply(lambda x: 1 if x == 'Fatal' else 0)
    nonmoto['Injury'] = nonmoto['InjurySeverity'].apply(lambda x: 1 if x == 'Injury' else 0)

    nonmoto_agg = nonmoto.groupby('ReportNumber').agg(
        NonMotoristTotal=pd.NamedAgg('InjurySeverity', 'count'),
        NonMotoristInjury=pd.NamedAgg('Injury', 'sum'),
        NonMotoristFatal=pd.NamedAgg('Fatal', 'sum')).reset_index()
    return nonmoto_agg


def nonmoto_pipeline(raw_data):
    nonmoto = prepare_nonmoto_data(raw_data)
    nonmoto_agg = transform_nonmoto_data(nonmoto)
    return nonmoto_agg
