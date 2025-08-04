from .base_processor import BaseProcessor
from typing import Dict, Any, List
import pandas as pd
import traceback

class ComercioServiciosProcessor(BaseProcessor):
    def get_table_name(self) -> str:
        return "emces_servicios"

    def get_file_patterns(self) -> List[str]:
        return ["DANE", "Datos_EMCES", "xlsx"]
    
    def get_key_columns(self) -> List[str]:
        return [
            'flujo_comercial', 
            'periodo_mes', 
            'codigo', 
            'pais', 
            'departamento'
        ]
    
    def get_read_params(self) -> Dict[str, Any]:
        """Parámetros específicos para leer archivos de comercio de servicios"""
        return {
            'header': 6,  # Encabezado en fila 7 (índice 6)
            'skipfooter': 0,  # Sin filas al final a ignorar
        }

    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transformación específica para comercio de servicios
        - Mapear columnas a estructura de base de datos
        - Limpiar y validar datos específicos del dominio
        """
        try:
            df_clean = df.copy()
            
            # DEBUG: Mostrar columnas originales
            self.logger.info(f"Columnas originales del archivo: {list(df_clean.columns)}")
            print(f"Columnas originales: {list(df_clean.columns)}")
            
            # Eliminar filas donde la primera columna esté vacía (texto al final)
            if not df_clean.empty:
                first_col = df_clean.columns[0]
                df_clean = df_clean.dropna(subset=[first_col])
            
            # Mapear nombres de columnas a estructura de base de datos
            column_mapping = {
                'FLUJO_COMERCIAL': 'flujo_comercial',
                'PERIODO_MES': 'periodo_mes', 
                'CÓDIGO': 'codigo',
                'CODIGO': 'codigo',  # Sin tilde
                'DESCRIPCION_CABPS': 'descripcion_cabps',
                'PAÍS': 'pais',
                'PAIS': 'pais',  # Sin tilde
                'NOMBRE_PAÍS': 'nombre_pais',
                'NOMBRE_PAIS': 'nombre_pais',  # Sin tilde
                'DEPARTAMENTO': 'departamento',
                'NOMBRE_DEPARTAMENTO': 'nombre_departamento',
                'TOTAL_EN_MILES_DE_DOLARES': 'total_miles_dolares'
            }
            
            # DEBUG: Mostrar qué mapeos existen
            existing_mappings = {k: v for k, v in column_mapping.items() if k in df_clean.columns}
            self.logger.info(f"Mapeos encontrados: {existing_mappings}")
            print(f"Mapeos encontrados: {existing_mappings}")
            
            # Si no hay mapeos exactos, intentar mapeo flexible
            if not existing_mappings:
                self.logger.warning("No se encontraron mapeos exactos. Intentando mapeo flexible...")
                existing_mappings = self._create_flexible_mapping(df_clean.columns)
                self.logger.info(f"Mapeo flexible creado: {existing_mappings}")
            
            # Renombrar columnas
            if existing_mappings:
                df_clean = df_clean.rename(columns=existing_mappings)
            
            # Filtrar solo las columnas requeridas que existen
            required_columns = [
                'flujo_comercial', 'periodo_mes', 'codigo', 'descripcion_cabps',
                'pais', 'nombre_pais', 'departamento', 'nombre_departamento', 
                'total_miles_dolares'
            ]
            existing_columns = [col for col in required_columns if col in df_clean.columns]
            
            self.logger.info(f"Columnas disponibles después del mapeo: {existing_columns}")
            print(f"Columnas finales disponibles: {existing_columns}")
            
            if not existing_columns:
                self.logger.error("No se encontraron columnas válidas después del mapeo")
                self.logger.error(f"Columnas en DataFrame: {list(df_clean.columns)}")
                return pd.DataFrame()
            
            df_clean = df_clean[existing_columns]
            
            # Validaciones específicas del dominio
            df_clean = self._validate_comercio_servicios_data(df_clean)

            self.logger.info(f"Transformación de comercio servicios: {df_clean.shape[0]} filas, {df_clean.shape[1]} columnas")
            print(f"Transformación final: {df_clean.shape[0]} filas, {df_clean.shape[1]} columnas")
            
            # Agregar metadatos
            df_clean['fecha_actualizacion'] = pd.Timestamp.now()
            
            self.logger.info(f"Datos de comercio servicios transformados: {len(df_clean)} filas")
            return df_clean
            
        except Exception as e:
            self.logger.error(f"Error en transform_data para comercio servicios: {str(e)}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return pd.DataFrame()
    
    def _create_flexible_mapping(self, columns) -> Dict[str, str]:
        """Crea mapeo flexible basado en contenido de las columnas"""
        flexible_mapping = {}
        
        for col in columns:
            col_upper = str(col).upper().strip()
            
            if 'FLUJO' in col_upper or 'COMERCIAL' in col_upper:
                flexible_mapping[col] = 'flujo_comercial'
            elif 'PERIODO' in col_upper or 'MES' in col_upper:
                flexible_mapping[col] = 'periodo_mes'
            elif 'CÓDIGO' in col_upper or 'CODIGO' in col_upper:
                flexible_mapping[col] = 'codigo'
            elif 'DESCRIPCION' in col_upper or 'CABPS' in col_upper:
                flexible_mapping[col] = 'descripcion_cabps'
            elif ('PAÍS' in col_upper or 'PAIS' in col_upper) and 'NOMBRE' not in col_upper:
                flexible_mapping[col] = 'pais'
            elif 'NOMBRE' in col_upper and ('PAÍS' in col_upper or 'PAIS' in col_upper):
                flexible_mapping[col] = 'nombre_pais'
            elif 'DEPARTAMENTO' in col_upper and 'NOMBRE' not in col_upper:
                flexible_mapping[col] = 'departamento'
            elif 'NOMBRE' in col_upper and 'DEPARTAMENTO' in col_upper:
                flexible_mapping[col] = 'nombre_departamento'
            elif 'TOTAL' in col_upper or 'MILES' in col_upper or 'DOLARES' in col_upper:
                flexible_mapping[col] = 'total_miles_dolares'
        
        return flexible_mapping
    
    def _validate_comercio_servicios_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validaciones específicas para datos de comercio de servicios"""
        try:
            # Limpiar valores numéricos
            if 'total_miles_dolares' in df.columns:
                # Reemplazar comas por puntos si es necesario
                df['total_miles_dolares'] = df['total_miles_dolares'].astype(str).str.replace(',', '.')
                df['total_miles_dolares'] = pd.to_numeric(
                    df['total_miles_dolares'], 
                    errors='coerce'
                )
                # Eliminar filas con valores nulos en total_miles_dolares
                original_count = len(df)
                df = df.dropna(subset=['total_miles_dolares'])
                filtered_count = len(df)
                if filtered_count < original_count:
                    self.logger.info(f"Filtradas {original_count - filtered_count} filas por valores nulos en total_miles_dolares")
            
            # Validar códigos
            if 'codigo' in df.columns:
                df['codigo'] = df['codigo'].astype(str).str.strip()
                # Eliminar filas con códigos vacíos
                original_count = len(df)
                df = df[df['codigo'] != '']
                df = df[df['codigo'] != 'nan']
                df = df[df['codigo'] != 'None']
                filtered_count = len(df)
                if filtered_count < original_count:
                    self.logger.info(f"Filtradas {original_count - filtered_count} filas por códigos vacíos")
            
            # Limpiar texto
            text_columns = ['flujo_comercial', 'descripcion_cabps', 'nombre_pais', 'nombre_departamento']
            for col in text_columns:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.strip()
                    df[col] = df[col].str.upper()  # Normalizar a mayúsculas
            
            # Validar flujo comercial (debe ser Exportación o Importación)
            if 'flujo_comercial' in df.columns:
                valid_flows = ['EXPORTACIONES', 'IMPORTACIONES', 'EXPORTACIÓN', 'IMPORTACIÓN', 'EXPORTACION', 'IMPORTACION']
                original_count = len(df)
                df = df[df['flujo_comercial'].isin(valid_flows)]
                filtered_count = len(df)
                if filtered_count < original_count:
                    self.logger.info(f"Filtradas {original_count - filtered_count} filas por flujo comercial inválido")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error en validación de datos: {str(e)}")
            return df
