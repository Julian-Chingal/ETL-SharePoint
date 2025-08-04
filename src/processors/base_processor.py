from abc import ABC, abstractmethod
from typing import List, Dict, Any
import pandas as pd

class BaseProcessor(ABC):
    def __init__(self, extractor, transformer, loader, logger):
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader
        self.logger = logger
        
    @abstractmethod
    def get_table_name(self) -> str:
        """Retorna el nombre de la tabla en la base de datos"""
        pass

    @abstractmethod
    def get_key_columns(self) -> List[str]:
        """Retorna el nombre de las columnas clave primaria"""
        pass

    @abstractmethod
    def get_file_patterns(self) -> List[str]:
        """Retorna patrones de archivos a procesar"""
        pass
    
    @abstractmethod
    def get_read_params(self) -> Dict[str, Any]:
        """Retorna parámetros específicos para leer archivos"""
        pass
    
    @abstractmethod
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transformaciones específicas del tipo de datos"""
        pass
    
    def process_folder(self, folder_path: str) -> bool:
        """Proceso ETL completo para la carpeta"""
        try:
            self.logger.info(f"Iniciando procesamiento de carpeta: {folder_path}")
            
            # 1. Extraer archivos
            files = self.extract_files(folder_path)
            if not files:
                self.logger.info(f"No hay archivos para procesar en {folder_path}")
                return True
            
            # 2. Transformar datos
            transformed_data = self.transform_files(files)
            if transformed_data.empty:
                self.logger.warning(f"No hay datos válidos en {folder_path}")
                return True
            
            # 3. Cargar a base de datos (sobrescribir)
            success = self.load_data(transformed_data)
            
            if success:
                self.logger.info(f"Carpeta {folder_path} procesada exitosamente")
            else:
                self.logger.error(f"Error cargando datos de {folder_path}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error procesando {folder_path}: {str(e)}")
            return False
    
    def extract_files(self, folder_path: str) -> List[Dict]:
        """Extrae archivos de la carpeta según patrones específicos"""
        try:
            self.logger.info(f"Extrayendo archivos de: {folder_path}")
            
            all_files = self.extractor.list_files(folder_path)
            self.logger.info(f"Archivos encontrados en total: {len(all_files)} - {all_files}")
            
            patterns = self.get_file_patterns()
            self.logger.info(f"Patrones de búsqueda: {patterns}")
            
            matching_files = []
            for file_name in all_files:
                matches = any(pattern.lower() in file_name.lower() for pattern in patterns)
                self.logger.info(f"Archivo: {file_name} - Coincide: {matches}")
                
                if matches:
                    self.logger.info(f"Extrayendo archivo: {file_name}")
                    
                    file_data = self.extractor.download_file(folder_path, file_name)
                    if file_data:
                        matching_files.append({
                            'name': file_name,
                            'path': folder_path,
                            'data': file_data
                        })
                        self.logger.info(f"Archivo {file_name} extraído exitosamente")
                    else:
                        self.logger.error(f"Error descargando archivo {file_name}")
            
            self.logger.info(f"Archivos extraídos: {len(matching_files)}")
            return matching_files
            
        except Exception as e:
            self.logger.error(f"Error extrayendo archivos de {folder_path}: {str(e)}")
            return []
    
    def transform_files(self, files: List[Dict]) -> pd.DataFrame:
        """Transforma todos los archivos y los consolida"""
        all_dataframes = []
        read_params = self.get_read_params()
        
        for file_info in files:
            try:
                # Usar transformer genérico con parámetros específicos
                df = self.transformer.process_file(file_info, **read_params)
                
                if df is not None and not df.empty:
                    # Aplicar transformación específica del dominio
                    df_transformed = self.transform_data(df)
                    
                    if not df_transformed.empty:
                        all_dataframes.append(df_transformed)
                        self.logger.info(f"Archivo {file_info['name']} transformado: {len(df_transformed)} filas")
                    
            except Exception as e:
                self.logger.error(f"Error transformando {file_info['name']}: {str(e)}")
        
        if all_dataframes:
            result_df = pd.concat(all_dataframes, ignore_index=True)
            self.logger.info(f"Total datos consolidados: {len(result_df)} filas")
            return result_df
        else:
            return pd.DataFrame()
    
    def load_data(self, df: pd.DataFrame) -> bool:
        """Carga datos a la base de datos evitando duplicados"""
        try:
            table_name = self.get_table_name()
            
            # Si el processor define columnas clave, úsalas
            if hasattr(self, 'get_key_columns'):
                key_columns = self.get_key_columns()
                return self.loader.insert_new_data(table_name, df, key_columns)
            else:
                # Usar todas las columnas excepto metadatos como clave
                key_columns = [col for col in df.columns if col not in ['fecha_actualizacion', 'id']]
                return self.loader.insert_new_data(table_name, df, key_columns)
                
        except Exception as e:
            self.logger.error(f"Error cargando datos: {str(e)}")
            return False