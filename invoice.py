import pandas as pd
from function_invoice import calculate_invoice,validate_cdi


pd.set_option('display.float_format', lambda x: '%.2f' % x)

#carga de dataframes
consumption = pd.read_csv("./data/consumption.csv")
injection = pd.read_csv("./data/injection.csv")    
tariffs = pd.read_csv("./data/tariffs.csv") 
#opcional, elimino las columnas para que no me estorben en el analisis  
tariffs = tariffs.drop(columns=['G','T','D','R','P']) 
services = pd.read_csv("./data/services.csv")
records = pd.read_csv("./data/records.csv")    
xmData= pd.read_csv("./data/xm_data_hourly_per_agent.csv")  

# Función para validar y actualizar el nivel de voltaje -> Cabe aclara que si el voltaje_level del service es 2 o 3, el cdi no importa.'


services['cdi'] = services.apply(validate_cdi, axis=1)





invoice = calculate_invoice(records, consumption, services, tariffs, injection,xmData)
print(invoice)