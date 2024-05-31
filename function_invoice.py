import pandas as pd
import numpy as np
from functools import reduce

pd.set_option('display.float_format', lambda x: '%.2f' % x)

#funcion para las sumatorias
def merge_and_sum(df1, df2, key, value_col):
    merged_df = pd.merge(df1, df2, on=key)
    return merged_df.groupby('id_service')[value_col].sum().reset_index()

#funcion para calcular el EE segun sea el caso
def calculate_EE(row):
    if row['sum_injection'] <= row['sum_consumption']:
        EE1 = row['sum_injection']
        EE2 = 0
    else:
        EE1 = row['sum_consumption']
        EE2 = row['sum_injection'] - row['sum_consumption']
    return pd.Series([EE1, EE2])

def calculate_invoice(records, consumption, services, tariffs, injection, xmData):
    # Energía Activa (EA)
    sum_consumption = merge_and_sum(records, consumption, 'id_record', 'value')
    services_with_tariffs = pd.merge(services, tariffs, on=['id_market', 'voltage_level']).drop_duplicates(subset=['id_service'])
    consumption_service_tariffs = pd.merge(sum_consumption, services_with_tariffs, on='id_service')
    consumption_service_tariffs['EA'] = consumption_service_tariffs['value'] * consumption_service_tariffs['CU']

    # Comercialización de Excedentes de Energía (EC)
    sum_injection = merge_and_sum(records, injection, 'id_record', 'value')
    injection_service_tariffs = pd.merge(sum_injection, services_with_tariffs, on='id_service')
    injection_service_tariffs['EC'] = injection_service_tariffs['value'] * injection_service_tariffs['C']

    # Calculo EE1 y EE2
    sum_consumption.rename(columns={'value': 'sum_consumption'}, inplace=True)
    sum_injection.rename(columns={'value': 'sum_injection'}, inplace=True)
    EE_sum = pd.merge(sum_consumption, sum_injection, on='id_service')
    EE_sum[['EE1', 'EE2']] = EE_sum.apply(calculate_EE, axis=1)

    accumulated_EE = pd.merge(injection_service_tariffs, EE_sum, on='id_service')
    accumulated_EE['EE1'] = accumulated_EE['EE1'] * -accumulated_EE['CU']

    # Calculo EE2 con tarifas horarias
    df_xm_hourly = pd.DataFrame(xmData)
    df_xm_hourly['record_timestamp'] = pd.to_datetime(df_xm_hourly['record_timestamp'])
    records['record_timestamp'] = pd.to_datetime(records['record_timestamp'])

    df_full = records.merge(consumption, on='id_record').merge(services, on='id_service').merge(tariffs, on=['id_market', 'voltage_level', 'cdi'], how='left')
    df_full = df_full.merge(df_xm_hourly, on='record_timestamp', how='left', suffixes=('', '_hourly'))
    df_full['Hora'] = df_full['record_timestamp'].dt.hour + 1

    def calcular_tarifa_EE2_total(df, cantidad_EE2):
        exceso_inyeccion = cantidad_EE2
        tarifa_EE2_total = 0
        for _, row in df.iterrows():
            if exceso_inyeccion <= 0:
                break
            consumo_hora, inyeccion_hora, tarifa = row['value'], row['inyeccion'], row['value_hourly']
            exceso_hora = min(inyeccion_hora - consumo_hora, exceso_inyeccion)
            tarifa_EE2_total += exceso_hora * tarifa
            exceso_inyeccion -= exceso_hora
        return tarifa_EE2_total

    df_sumatorias = EE_sum.copy()
    resultados = []

    for _, row in df_sumatorias.iterrows():
        id_service, cantidad_EE2 = row['id_service'], row['EE2']
        df_servicio = df_full[df_full['id_service'] == id_service].copy()
        df_servicio['inyeccion'] = df_servicio['value'] + 10  # Ajustar según los datos reales
        tarifa_EE2_total = calcular_tarifa_EE2_total(df_servicio, cantidad_EE2)
        resultados.append({'id_service': id_service, 'EE2': tarifa_EE2_total})

    df_resultados = pd.DataFrame(resultados)

    # Crear DataFrame final
    EA = consumption_service_tariffs[['id_service', 'EA']]
    EC = injection_service_tariffs[['id_service', 'EC']]
    EE1 = accumulated_EE[['id_service', 'EE1']]
    EE2 = df_resultados[['id_service', 'EE2']]

    invoice = reduce(lambda left, right: pd.merge(left, right, on='id_service'), [EA, EC, EE1, EE2])
    return invoice

#funcion para ignorar los valores cdi cuando el voltaje es 2 y 3
def validate_cdi(row):
    return np.nan if row['voltage_level'] in [2, 3] else row['cdi']