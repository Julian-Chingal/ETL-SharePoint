from .base_processor import BaseProcessor
from typing import List, Dict, Any
import pandas as pd
import traceback

class IdcePaisDestinoProcessor(BaseProcessor):
    def get_table_name(self) -> str:
        return "idce_pais_destino_inversion"
    
    def get_file_patterns(self) -> List[str]:
        return ["IDCE por país destino"]
    
    def get_key_columns(self) -> List[str]:
        return ["cod_pais", "serie", "fecha"]

    def get_read_params(self) -> Dict[str, Any]:
        return {
            'header': None, 
            'skipfooter': 1,
            'sheet_name': 'Series de datos',  
        }
    
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforma datos de IED de formato pivot a formato largo
        """
        try:
            dates = df.iloc[1, 2:].tolist()  # Columnas desde la tercera (índice 2)

            df.columns = ['COD País', 'Serie'] + dates

            # Eliminar las primeras tres filas (filas 0, 1 y 2) ya que contienen metadatos
            df = df.iloc[3:].reset_index(drop=True)

            # Limpiar 'COD País' y 'Serie' para eliminar espacios en blanco o caracteres extraños
            df['COD País'] = df['COD País'].astype(str).str.strip()
            df['Serie'] = df['Serie'].astype(str).str.strip()

            # Realizar el melt para transformar a formato largo
            id_vars = ['COD País', 'Serie']
            value_vars = dates  # Las fechas como columnas de valores

            df_melted = pd.melt(
                df,
                id_vars=id_vars,
                value_vars=value_vars,
                var_name='fecha',
                value_name='valor'
            )

            # Renombrar columnas al formato deseado
            df_melted = df_melted.rename(columns={'COD País': 'cod_pais', 'Serie': 'serie'})

            # Limpiar nuevamente 'cod_pais' y 'serie' después del melt
            df_melted['cod_pais'] = df_melted['cod_pais'].astype(str).str.strip()
            df_melted['serie'] = df_melted['serie'].astype(str).str.strip()

            # Convertir 'fecha' a formato datetime
            df_melted['fecha'] = pd.to_datetime(df_melted['fecha'], format='%d/%m/%Y')

            # Convertir la columna 'valor' a numérico, convirtiendo valores no numéricos a NaN
            df_melted['valor'] = pd.to_numeric(df_melted['valor'], errors='coerce')

            # Redondear la columna 'valor' a 2 decimales
            df_melted['valor'] = df_melted['valor'].round(2)

            # Eliminar duplicados basándose en las columnas clave
            df_melted = df_melted.drop_duplicates(subset=['cod_pais', 'serie', 'fecha'], keep='first')

            # Ordenar por las columnas clave
            df_melted = df_melted.sort_values(by=['cod_pais', 'serie', 'fecha'])

            # Reiniciar el índice
            df_melted = df_melted.reset_index(drop=True)

            print(f"Filas después de procesar: {len(df_melted)}")
            return df_melted

        except Exception as e:
            print(f"Error en transform_data: {str(e)}")
            print(traceback.format_exc())
            raise