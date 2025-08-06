from .base_processor import BaseProcessor
from typing import Dict, Any, List
import pandas as pd
import traceback
import re

class ComercioBienesExportacionesProcessor(BaseProcessor):
    def get_table_name(self) -> str:
        return "oee_ma_exportaciones_bienes"
    
    def get_file_patterns(self) -> List[str]:
        return ["OEE MA Exportaciones"]
    
    def get_key_columns(self) -> List[str]:
        return ["nandina", "partida", "cod_pais", "deporig", "metrica", "anio", "periodo"]

    def get_read_params(self) -> Dict[str, Any]:
        return {
            'header': 0, 
            'skipfooter': 0,
            'sheet_name': 1
        }

    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            ## Definir las columnas fijas según el Excel
            excel_columns = [
                'NANDINA', 'partida', 'TECNOLOGÍA', 'CLAS MIN', 'DES CLAS MIN',
                'GRUPOS CLAS MIN', 'MINEROS/NO MINEROS', 'CUCI_AGREGADO',
                'PAIS', 'PAÍS', 'GRUPO1', 'GRUPO2', 'GRUPO3', 'DEPORIG',
                'DEPARTAMENTO', 'región'
            ]

            # Verificar que las columnas fijas existen
            missing_fixed = [col for col in excel_columns if col not in df.columns]
            if missing_fixed:
                self.logger.warning(f"Columnas fijas faltantes: {missing_fixed}")
                return pd.DataFrame()

            # Columnas dinámicas (FOBDO20, KNETO23EMZ, etc.)
            value_columns = [col for col in df.columns if col not in excel_columns]
            if not value_columns:
                self.logger.warning("No se encontraron columnas de valor para dinamizar.")
                return pd.DataFrame()

            self.logger.info(f"Columnas originales del archivo: {list(df.columns)}")
            self.logger.info(f"Columnas dinámicas encontradas: {value_columns}")
            self.logger.info(f"Columnas fijas encontradas: {excel_columns}")

            # Transformar a formato largo usando melt
            df_melted = df.melt(
                id_vars=excel_columns,
                value_vars=value_columns,
                var_name='metrica_periodo',
                value_name='valor'
            )

            # Convertir 'valor' a numérico
            df_melted['valor'] = pd.to_numeric(df_melted['valor'], errors='coerce').round(2)

            # Separar 'metrica_periodo' en 'metrica', 'anio' y 'periodo'
            def parse_metrica_periodo(col_name):
                match = re.match(r'^(FOBDO|KNETO)(\d{2})(\w*)$', col_name)
                if match:
                    metrica, year_suffix, periodo = match.groups()
                    anio = f'20{year_suffix}'
                    periodo = periodo if periodo else 'ANUAL'
                    return metrica, anio, periodo
                return None, None, None

            # Aplicar la función para crear nuevas columnas
            df_melted[['metrica', 'anio', 'periodo']] = pd.DataFrame(
                df_melted['metrica_periodo'].apply(parse_metrica_periodo).tolist(),
                index=df_melted.index
            )

            # Verificar si hay filas con métricas no procesadas correctamente
            invalid_rows = df_melted[df_melted['metrica'].isna()]
            if not invalid_rows.empty:
                self.logger.warning(f"Filas con métricas no procesadas:\n{invalid_rows[['metrica_periodo']]}")

            # Eliminar la columna temporal y filas con métricas no válidas
            df_melted = df_melted.drop(columns=['metrica_periodo'])
            df_melted = df_melted.dropna(subset=['metrica', 'anio', 'periodo'])

            # Renombrar columnas para coincidir con la base de datos
            df_melted = df_melted.rename(columns={
                'NANDINA': 'nandina',
                'TECNOLOGÍA': 'tecnologia',
                'CLAS MIN': 'clas_min',
                'DES CLAS MIN': 'des_clas_min',
                'GRUPOS CLAS MIN': 'grupos_clas_min',
                'MINEROS/NO MINEROS': 'mineros_no_mineros',
                'CUCI_AGREGADO': 'cuci_agregado',
                'PAIS': 'cod_pais',
                'PAÍS': 'pais',
                'GRUPO1': 'grupo1',
                'GRUPO2': 'grupo2',
                'GRUPO3': 'grupo3',
                'DEPORIG': 'deporig',
                'DEPARTAMENTO': 'departamento',
                'región': 'region'
            })

            # Limpiar las columnas para eliminar espacios en blanco
            columns_to_clean = [
                'nandina', 'partida', 'tecnologia', 'clas_min', 'des_clas_min',
                'grupos_clas_min', 'mineros_no_mineros', 'cuci_agregado',
                'cod_pais', 'pais', 'grupo1', 'grupo2', 'grupo3', 'deporig',
                'departamento', 'region', 'metrica', 'anio', 'periodo'
            ]
            for col in columns_to_clean:
                if col in df_melted.columns and df_melted[col].dtype == 'object':
                    df_melted[col] = df_melted[col].astype(str).str.strip()

            # Verificar duplicados
            key_columns = self.get_key_columns()
            duplicates = df_melted[df_melted.duplicated(subset=key_columns, keep=False)]
            if not duplicates.empty:
                self.logger.warning(f"Duplicados encontrados en el DataFrame:\n{duplicates[key_columns]}")
            else:
                self.logger.info("No se encontraron duplicados en el DataFrame.")

            # Eliminar duplicados
            df_melted = df_melted.drop_duplicates(subset=key_columns, keep='first')

            # Ordenar por columnas clave
            df_melted = df_melted.sort_values(by=key_columns)

            # Reiniciar índice
            df_melted = df_melted.reset_index(drop=True)

            self.logger.info(f"Filas después de procesar: {len(df_melted)}")
            return df_melted
        except Exception as e:
            self.logger.error(f"Error transforming data: {e}")
            self.logger.error(traceback.format_exc())
            return pd.DataFrame()