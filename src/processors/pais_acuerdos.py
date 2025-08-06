from .base_processor import BaseProcessor
from typing import Dict, Any, List
import pandas as pd
import traceback

class PaisAcuerdosProcessor(BaseProcessor):
    def get_table_name(self) -> str:
        return "codigo_pais_acuerdos"
    
    def get_file_patterns(self) -> List[str]:
        return ["Código", "País", "Acuerdos"]

    def get_key_columns(self) -> List[str]:
        return ["codigo_pais"]
    
    def get_read_params(self) -> Dict[str, Any]:
        return { 
            'header': 1,  
            'skipfooter': 0,
        }

    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        try: 
            df_clean = df.copy()

            # Debbug: Mostrar columnas originales
            self.logger.info(f"Columnas originales del archivo: {list(df_clean.columns)}")
            column_mapping = { # Mapa de columnas a estructura de base de datos
                'Cod. Pais': 'codigo_pais',
                'País': 'pais',
                'Pais': 'pais', # Sin tilde
                'Grupos DIE': 'grupos_die',
                'AP': 'ap',
                'AEC': 'aec',
                'ACUERDOS': 'acuerdos',
                'ALADI': 'aladi',
                'CELAC': 'celac',
            }

            required_columns = { # Lista de columnas requeridas
                'codigo_pais', 
                'pais', 
                'grupos_die', 
                'ap', 
                'aec', 
                'acuerdos', 
                'aladi', 
                'celac'
            }

             # DEBUG: Mostrar qué mapeos existen
            existing_mappings = {k: v for k, v in column_mapping.items() if k in df_clean.columns}
            self.logger.info(f"Mapeos encontrados: {existing_mappings}")

            if existing_mappings:
                df_clean = df_clean.rename(columns=existing_mappings)

            existing_columns = [col for col in required_columns if col in df_clean.columns]
            self.logger.info(f"Columnas existentes después del mapeo: {existing_columns}")

            if not existing_columns:
                self.logger.error("No se encontraron columnas requeridas después del mapeo.")
                return pd.DataFrame()
            
            df_clean = df_clean[existing_columns]

            # Validacion 
            df_clean = self._validate_data(df_clean)
            self.logger.info(f"Datos transformados: {len(df_clean)}")
            return df_clean
        except Exception as e:
            self.logger.error(f"Error en transform_data para codigo paises: {str(e)}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return pd.DataFrame()
    
    def _validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            df_clean = df.copy()
            
            # 1. Normalizar código de país (eliminar ceros a la izquierda para coincidir con la base de datos)
            if 'codigo_pais' in df_clean.columns:
                df_clean['codigo_pais'] = df_clean['codigo_pais'].astype(str).str.strip().str.lstrip('0')

                df_clean['codigo_pais'] = df_clean['codigo_pais'].replace('', '0')
            
            # 2. Normalizar nombre de país
            if 'pais' in df_clean.columns:
                df_clean['pais'] = df_clean['pais'].astype(str).str.strip().str.title()
                df_clean = df_clean[df_clean['pais'] != '']
                df_clean = df_clean[df_clean['pais'] != 'Nan']

            # 3. Manejar registros con codigo_pais = '0'
            if 'codigo_pais' in df_clean.columns:
                # Priorizar el registro con pais = 'Mundo' para codigo_pais = '0'
                mundo_record = df_clean[(df_clean['codigo_pais'] == '0') & (df_clean['pais'] == 'Mundo')]
                if not mundo_record.empty:
                    df_clean = df_clean[df_clean['codigo_pais'] != '0']
                    df_clean = pd.concat([df_clean, mundo_record], ignore_index=True)
            
            # 4. Normalizar columnas de grupos y acuerdos
            agreement_columns = ['grupos_die', 'ap', 'aec', 'acuerdos', 'aladi', 'celac']
            for col in agreement_columns:
                if col in df_clean.columns:
                    df_clean[col] = df_clean[col].astype(str).str.strip()
            
            # 5. Validar que no haya duplicados por código de país
            if 'codigo_pais' in df_clean.columns:
                initial_count = len(df_clean)
                df_clean = df_clean.drop_duplicates(subset=['codigo_pais'], keep='first')
                final_count = len(df_clean)
                if final_count < initial_count:
                    duplicates_removed = initial_count - final_count
                    self.logger.warning(f"Eliminados {duplicates_removed} códigos de país duplicados")
            
            return df_clean   
        except Exception as e:
            self.logger.error(f"Error en validación de datos de países: {str(e)}")
            return df