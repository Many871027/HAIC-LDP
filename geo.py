                                                                                                                                                                
import pandas as pd                                                                                                                                         
                                                                                                                                                            
# Para Fase 1                                                                                                                                               
df_geo = pd.read_csv("raw_data/fase_1/olist_geolocation_dataset.csv")                                                                                       
df_geo_agg = df_geo.groupby("geolocation_zip_code_prefix").agg({                                                                                            
    "geolocation_lat": "mean",                                                                                                                              
    "geolocation_lng": "mean",                                                                                                                              
    "geolocation_city": "first",                                                                                                                            
    "geolocation_state": "first"                                                                                                                            
}).reset_index()                                                                                                                                            
df_geo_agg.to_csv("raw_data/fase_1/olist_geolocation_dataset.csv", index=False)                                                                             
                                                                                                                                                            
# Para Fase 2 (opcional, en tu máquina local)                                                                                                               
df_geo2 = pd.read_csv("raw_data/fase_2/olist_geolocation_dataset.csv")                                                                                      
df_geo_agg2 = df_geo2.groupby("geolocation_zip_code_prefix").agg({                                                                                          
    "geolocation_lat": "mean",                                                                                                                              
    "geolocation_lng": "mean",                                                                                                                              
    "geolocation_city": "first",                                                                                                                            
    "geolocation_state": "first"                                                                                                                            
}).reset_index()                                                                                                                                            
df_geo_agg2.to_csv("raw_data/fase_2/olist_geolocation_dataset.csv", index=False)                                                                            
                                                                                                                                                            
print("¡Reducción completada con éxito!")                                                                                                                   
                                            