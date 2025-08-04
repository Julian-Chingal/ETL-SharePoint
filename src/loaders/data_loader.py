from mysql.connector import connect, Error
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional, TypedDict, List, Dict
import pandas as pd
import logging
import pandas as pd
import logging

class DBConfig(TypedDict):
    host: str
    user: str
    password: str
    port: int
    database: str

class DataLoader:
    def __init__(self, config: DBConfig):
        self.host = config['host']
        self.port = config['port']
        self.username = config['user']
        self.password = config['password']
        self.database = config['database']
        self.connection = None
        self.engine = None
        self.logger = logging.getLogger(__name__)
        
        self._create_connection()
        self._create_engine()

    def _create_connection(self):
        """Crea la conexión MySQL"""
        try:
            self.connection = connect(
                host=self.host,
                user=self.username,
                password=self.password,
                database=self.database,
                port=self.port,
                autocommit=True
            )
            if self.connection.is_connected():
                self.logger.info("Conexión MySQL creada exitosamente")
        except Error as e:
            self.logger.error(f"Error al conectar con MySQL: {str(e)}")
            self.connection = None

    def _create_engine(self):
        """Crea el engine"""
        try:
            connection_string = (
                f"mysql+pymysql://{self.username}:"
                f"{self.password}@"
                f"{self.host}:"
                f"{self.port}/"
                f"{self.database}"
            )
            
            self.engine = create_engine(
                connection_string,
                pool_pre_ping=True,
                pool_recycle=3600
            )
            
            # Probar conexión
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            self.logger.info("Engine SQLAlchemy creado exitosamente")
            
        except Exception as e:
            self.logger.error(f"Error creando engine SQLAlchemy: {str(e)}")
            self.engine = None

    def table_exists(self, table_name: str) -> bool:
        """Verifica si una tabla existe"""
        try:
            if self.engine is None:
                return False
            
            inspector = inspect(self.engine)
            return table_name in inspector.get_table_names()
            
        except Exception as e:
            self.logger.error(f"Error verificando tabla {table_name}: {str(e)}")
            return False

    def get_existing_data(self, table_name: str, key_columns: List[str] = None) -> Optional[pd.DataFrame]:
        """
        Obtiene datos existentes de una tabla
        
        Args:
            table_name (str): Nombre de la tabla
            key_columns (List[str]): Columnas que forman la clave única
            
        Returns:
            pd.DataFrame: Datos existentes o None
        """
        try:
            if not self.table_exists(table_name):
                self.logger.info(f"Tabla {table_name} no existe")
                return None
            
            if key_columns:
                # Solo obtener las columnas clave para comparación
                columns_str = ", ".join([f"`{col}`" for col in key_columns])
                query = f"SELECT {columns_str} FROM `{table_name}`"
            else:
                query = f"SELECT * FROM `{table_name}`"
            
            existing_data = pd.read_sql(query, self.engine)
            self.logger.info(f"Datos existentes en {table_name}: {len(existing_data)} filas")
            return existing_data
            
        except Exception as e:
            self.logger.error(f"Error obteniendo datos existentes de {table_name}: {str(e)}")
            return None

    def identify_new_records(self, new_data: pd.DataFrame, existing_data: pd.DataFrame, 
                            key_columns: List[str]) -> pd.DataFrame:
        """
        Identifica registros nuevos comparando con datos existentes
        
        Args:
            new_data (pd.DataFrame): Nuevos datos a insertar
            existing_data (pd.DataFrame): Datos ya existentes en la tabla
            key_columns (List[str]): Columnas que forman la clave única
            
        Returns:
            pd.DataFrame: Solo los registros nuevos
        """
        try:
            if existing_data is None or existing_data.empty:
                self.logger.info("No hay datos existentes, todos los registros son nuevos")
                return new_data
            
            # Validar que las columnas clave existen en ambos DataFrames
            missing_in_new = [col for col in key_columns if col not in new_data.columns]
            missing_in_existing = [col for col in key_columns if col not in existing_data.columns]
            
            if missing_in_new:
                self.logger.error(f"Columnas clave faltantes en nuevos datos: {missing_in_new}")
                return pd.DataFrame()
            
            if missing_in_existing:
                self.logger.warning(f"Columnas clave faltantes en datos existentes: {missing_in_existing}")
                return new_data
            
            # Crear clave compuesta para comparación
            new_data_keys = new_data[key_columns].copy()
            existing_data_keys = existing_data[key_columns].copy()
            
            # Convertir a string para comparación
            new_data_temp = new_data.copy()
            existing_data_temp = existing_data.copy()
            
            new_data_temp['_temp_key'] = new_data_keys.apply(
                lambda row: '|'.join(row.astype(str)), axis=1
            )
            existing_data_temp['_temp_key'] = existing_data_keys.apply(
                lambda row: '|'.join(row.astype(str)), axis=1
            )
            
            # Filtrar solo registros nuevos
            new_records = new_data_temp[~new_data_temp['_temp_key'].isin(existing_data_temp['_temp_key'])].copy()
            
            # Eliminar columna temporal
            new_records = new_records.drop('_temp_key', axis=1)
            
            self.logger.info(f"Registros nuevos identificados: {len(new_records)} de {len(new_data)}")
            
            if len(new_records) < len(new_data):
                duplicates = len(new_data) - len(new_records)
                self.logger.info(f"Registros duplicados omitidos: {duplicates}")
            
            return new_records
            
        except Exception as e:
            self.logger.error(f"Error identificando registros nuevos: {str(e)}")
            return pd.DataFrame()

    def insert_new_data(self, table_name: str, df: pd.DataFrame, 
                       key_columns: List[str] = None) -> bool:
        """
        Inserta solo datos nuevos, evitando duplicados
        
        Args:
            table_name (str): Nombre de la tabla
            df (pd.DataFrame): Datos a insertar
            key_columns (List[str]): Columnas que forman la clave única
            
        Returns:
            bool: True si fue exitoso
        """
        try:
            if self.engine is None:
                self.logger.error("Engine SQLAlchemy no está disponible")
                return False
            
            if df.empty:
                self.logger.warning("DataFrame vacío, no hay datos para insertar")
                return True
            
            # Si no se especifican columnas clave, usar todas las columnas excepto metadatos
            if key_columns is None:
                key_columns = [col for col in df.columns if col not in ['fecha_actualizacion', 'id']]
            
            self.logger.info(f"Procesando {len(df)} registros para tabla {table_name}")
            self.logger.info(f"Columnas clave para duplicados: {key_columns}")
            
            # Obtener datos existentes
            existing_data = self.get_existing_data(table_name, key_columns)
            
            # Identificar solo registros nuevos
            new_records = self.identify_new_records(df, existing_data, key_columns)
            
            if new_records.empty:
                self.logger.info("No hay registros nuevos para insertar")
                return True
            
            # Insertar solo registros nuevos
            new_records.to_sql(
                name=table_name,
                con=self.engine,
                if_exists='append',
                index=False,
                chunksize=1000,
                method='multi'
            )
            
            self.logger.info(f"Insertados {len(new_records)} registros nuevos en {table_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error insertando datos nuevos en {table_name}: {str(e)}")
            return False

    def overwrite_table(self, table_name: str, df: pd.DataFrame, 
                       key_columns: List[str] = None) -> bool:
        """
        Método principal para tu ETL - inserta solo datos nuevos
        
        Args:
            table_name (str): Nombre de la tabla
            df (pd.DataFrame): Datos a procesar
            key_columns (List[str]): Columnas que forman la clave única
            
        Returns:
            bool: True si fue exitoso
        """
        # Para tu caso de uso, "overwrite" realmente significa "insertar solo nuevos"
        return self.insert_new_data(table_name, df, key_columns)

    def get_table_info(self, table_name: str) -> Optional[Dict]:
        """Obtiene información sobre una tabla"""
        try:
            if not self.table_exists(table_name):
                return None
            
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) as count FROM `{table_name}`"))
                row_count = result.fetchone()[0]
                
                inspector = inspect(self.engine)
                columns = inspector.get_columns(table_name)
                
                return {
                    'table_name': table_name,
                    'row_count': row_count,
                    'columns': [col['name'] for col in columns]
                }
                
        except Exception as e:
            self.logger.error(f"Error obteniendo información de tabla {table_name}: {str(e)}")
            return None

    def close_connections(self):
        """Cierra todas las conexiones"""
        try:
            if self.connection and self.connection.is_connected():
                self.connection.close()
                self.logger.info("Conexión MySQL cerrada")
            
            if self.engine:
                self.engine.dispose()
                self.logger.info("Engine SQLAlchemy cerrado")
                
        except Exception as e:
            self.logger.error(f"Error cerrando conexiones: {str(e)}")

    def __del__(self):
        """Destructor para cerrar conexiones"""
        self.close_connections()