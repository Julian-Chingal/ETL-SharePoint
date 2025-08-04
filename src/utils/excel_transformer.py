from typing import Dict, Optional
from io import BytesIO
import pandas as pd
import logging

class ExcelTransformer:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def read_excel_file(self, file_data: BytesIO, file_name: str, **kwargs) -> Optional[pd.DataFrame]:
        """
        Lee un archivo Excel con parámetros personalizables
        
        Args:
            file_data: Contenido del archivo en BytesIO
            file_name: Nombre del archivo
            **kwargs: Parámetros adicionales para pd.read_excel
        
        Returns:
            DataFrame o None si hay error
        """
        try:
            if file_name.endswith('.xlsx') or file_name.endswith('.xls'):
                df = pd.read_excel(file_data, **kwargs)
            elif file_name.endswith('.xlsb'):
                df = pd.read_excel(file_data, engine='pyxlsb', **kwargs)
            elif file_name.endswith('.csv'):
                df = pd.read_csv(file_data, **kwargs)
            else:
                self.logger.warning(f"Tipo de archivo no soportado: {file_name}")
                return None
            
            self.logger.info(f"Archivo {file_name} leído correctamente. Shape: {df.shape}")
            return df
            
        except Exception as e:
            self.logger.error(f"Error leyendo archivo {file_name}: {str(e)}")
            return None
    
    def clean_basic_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpieza básica aplicable a cualquier DataFrame
        """
        try:
            # Eliminar filas completamente vacías
            df_clean = df.dropna(how='all')
            
            # Eliminar columnas completamente vacías
            df_clean = df_clean.dropna(axis=1, how='all')
            
            # Eliminar espacios en blanco de las columnas de texto
            for col in df_clean.select_dtypes(include=['object']).columns:
                df_clean[col] = df_clean[col].astype(str).str.strip()
            
            return df_clean
            
        except Exception as e:
            self.logger.error(f"Error en limpieza básica: {str(e)}")
            return df
    
    def process_file(self, file_info: Dict, **read_kwargs) -> Optional[pd.DataFrame]:
        """
        Procesa un archivo aplicando transformaciones básicas
        
        Args:
            file_info: Diccionario con información del archivo
            **read_kwargs: Parámetros para lectura del archivo
        
        Returns:
            DataFrame procesado o None
        """
        try:
            # Leer archivo
            df = self.read_excel_file(
                file_info['data'], 
                file_info['name'], 
                **read_kwargs
            )
            
            if df is None:
                return None
            
            # Aplicar limpieza básica
            df_clean = self.clean_basic_data(df)
            
            return df_clean
            
        except Exception as e:
            self.logger.error(f"Error procesando archivo {file_info.get('name', 'unknown')}: {str(e)}")
            return None