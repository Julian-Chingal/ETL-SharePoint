from .base_processor import BaseProcessor
from typing import Dict, Any, List
import pandas as pd
import traceback

class TurismoVisitantesPaisProcessor(BaseProcessor): 
    def get_table_name(self) -> str:
        return "visitantes_pais_residencia_turismo"

    def get_key_columns(self) -> List[str]:
        return ["anio", "mes", "pais", "continente_omt"]

    def get_file_patterns(self) -> List[str]:
        return ["OEE", "AV", "ESTADISTICAS", "TURISMO", "xlsx"]

    def get_read_params(self) -> Dict[str, Any]:
        return {
            'header': 10,
            'skipfooter': 5,
            'sheet_name': 'Visitantes Pais de Residencia',
        }

    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            df_clean = df.copy()

            # Debug: Mostrar columnas originales
            self.logger.info(f"Columnas originales del archivo: {list(df_clean.columns)}")
            column_mapping = {
                'Año': 'anio',
                'Mes': 'mes',
                'País': 'pais',
                'Pais': 'pais',  # Sin tilde
                'Continente OMT': 'continente_omt',
                'Viajeros': 'viajeros'
            }

            required_columns = {
                'anio',
                'mes',
                'pais', 
                'continente_omt',
                'viajeros'
            }

            # Mapear columnas
            existing_mappings = {k: v for k, v in column_mapping.items() if k in df_clean.columns}
            self.logger.info(f"Mapeos encontrados: {existing_mappings}")

            if existing_mappings:
                df_clean = df_clean.rename(columns=existing_mappings)

            # Columnas finales requeridas
            existing_columns = [col for col in required_columns if col in df_clean.columns]
            self.logger.info(f"Columnas existentes después del procesamiento: {existing_columns}")

            if not existing_columns:
                self.logger.error("No se encontraron columnas requeridas después del mapeo.")
                return pd.DataFrame()
            
            # Filtrar solo las columnas finales
            df_clean = df_clean[existing_columns]

            # Aplicar validaciones
            df_clean = self._validate_data(df_clean)
            self.logger.info(f"Datos de turismo transformados: {len(df_clean)} filas")
            return df_clean
        except Exception as e:
            self.logger.error(f"Error en transform_data para codigo paises: {str(e)}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return pd.DataFrame()
    
    def _validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validaciones específicas para datos de turismo"""
        try:
            df_clean = df.copy()
            
                        # 1. Validar y limpiar año
            if 'anio' in df_clean.columns:
                self.logger.info(f"Valores únicos de año antes de limpieza: {sorted(df_clean['anio'].dropna().unique())}")
                
                # Convertir a numérico
                df_clean['anio'] = pd.to_numeric(df_clean['anio'], errors='coerce')
                
                # Eliminar filas con años nulos o fuera de rango
                initial_count = len(df_clean)
                df_clean = df_clean.dropna(subset=['anio'])
                df_clean = df_clean[(df_clean['anio'] >= 2000) & (df_clean['anio'] <= 2030)]
                
                if len(df_clean) < initial_count:
                    removed = initial_count - len(df_clean)
                    self.logger.info(f"Eliminadas {removed} filas con años inválidos")
            
            # 2. Validar y normalizar mes
            if 'mes' in df_clean.columns:
                self.logger.info(f"Valores únicos de mes antes de limpieza: {sorted(df_clean['mes'].dropna().unique())}")
                
                # Limpiar valores de mes
                df_clean['mes'] = df_clean['mes'].astype(str).str.strip().str.lower()
                
                # Diccionario para normalizar meses
                meses_normalizacion = {
                    'enero': 'enero', 'febrero': 'febrero', 'marzo': 'marzo', 'abril': 'abril',
                    'mayo': 'mayo', 'junio': 'junio', 'julio': 'julio', 'agosto': 'agosto',
                    'septiembre': 'septiembre', 'octubre': 'octubre', 'noviembre': 'noviembre', 'diciembre': 'diciembre',
                    # Versiones abreviadas
                    'ene': 'enero', 'feb': 'febrero', 'mar': 'marzo', 'abr': 'abril',
                    'may': 'mayo', 'jun': 'junio', 'jul': 'julio', 'ago': 'agosto',
                    'sep': 'septiembre', 'oct': 'octubre', 'nov': 'noviembre', 'dic': 'diciembre'
                }
                
                # Aplicar normalización
                df_clean['mes'] = df_clean['mes'].map(meses_normalizacion).fillna(df_clean['mes'])
                
                # Eliminar filas con meses no válidos
                meses_validos = list(set(meses_normalizacion.values()))
                initial_count = len(df_clean)
                df_clean = df_clean[df_clean['mes'].isin(meses_validos)]
                
                if len(df_clean) < initial_count:
                    removed = initial_count - len(df_clean)
                    self.logger.info(f"Eliminadas {removed} filas con meses inválidos")
            
            # 3. Validar campo de viajeros (debe ser numérico)
            if 'viajeros' in df_clean.columns:
                # Convertir a numérico, forzando errores a NaN
                df_clean['viajeros'] = pd.to_numeric(df_clean['viajeros'], errors='coerce')
                
                # Eliminar filas con viajeros nulos o negativos
                initial_count = len(df_clean)
                df_clean = df_clean.dropna(subset=['viajeros'])
                df_clean = df_clean[df_clean['viajeros'] >= 0]
                final_count = len(df_clean)
                
                if final_count < initial_count:
                    removed = initial_count - final_count
                    self.logger.info(f"Eliminadas {removed} filas con valores de viajeros inválidos")
            
            # 5. Validar continente OMT
            if 'continente_omt' in df_clean.columns:
                # Normalizar nombres de continentes
                df_clean['continente_omt'] = df_clean['continente_omt'].astype(str).str.strip().str.title()
                
                # Eliminar continentes vacíos
                df_clean = df_clean[df_clean['continente_omt'] != '']
                df_clean = df_clean[df_clean['continente_omt'] != 'Nan']

            # 6. Normalizar nombres de países
            if 'pais' in df_clean.columns:
                df_clean['pais'] = df_clean['pais'].astype(str).str.strip()
                df_clean['pais'] = df_clean['pais'].str.title()  # Capitalizar apropiadamente
                
                # Eliminar países vacíos
                df_clean = df_clean[df_clean['pais'] != '']
                df_clean = df_clean[df_clean['pais'] != 'Nan']
            
            self.logger.info(f"Datos de turismo validados: {len(df_clean)} filas")
            return df_clean
            
        except Exception as e:
            self.logger.error(f"Error en validación de datos de turismo: {str(e)}")
            return df