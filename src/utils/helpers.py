from datetime import datetime
from typing import Dict, Any
import logging
import os

def setup_logging(log_level: str = 'INFO', log_file: str = None) -> logging.Logger:
    """
    Configura el sistema de logging
    
    Args:
        log_level (str): Nivel de logging
        log_file (str): Archivo de log (opcional)
    
    Returns:
        logging.Logger: Logger configurado
    """
    # Crear directorio de logs si no existe
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    # Configurar formato
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Configurar logger principal
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Limpiar handlers existentes
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Handler para consola
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, log_level.upper()))
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Handler para archivo si se especifica
    if log_file:
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(getattr(logging, log_level.upper()))
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

def validate_config(config: Dict[str, Any]) -> bool:
    """
    Valida la configuración del ETL
    
    Args:
        config (Dict): Diccionario de configuración
    
    Returns:
        bool: True si la configuración es válida
    """
    required_keys = [
        'SHAREPOINT_SITE_URL',
        'SHAREPOINT_USERNAME', 
        'SHAREPOINT_PASSWORD'
    ]
    
    for key in required_keys:
        if not config.get(key):
            print(f"Error: {key} no está configurado")
            return False
    
    return True

def create_directories(paths: list):
    """
    Crea directorios necesarios para el ETL
    
    Args:
        paths (list): Lista de rutas a crear
    """
    for path in paths:
        os.makedirs(path, exist_ok=True)
        print(f"Directorio creado/verificado: {path}")

def get_file_metadata(file_path: str) -> Dict[str, Any]:
    """
    Obtiene metadatos de un archivo
    
    Args:
        file_path (str): Ruta del archivo
    
    Returns:
        Dict: Metadatos del archivo
    """
    try:
        stat = os.stat(file_path)
        return {
            'size': stat.st_size,
            'modified': datetime.fromtimestamp(stat.st_mtime),
            'created': datetime.fromtimestamp(stat.st_ctime),
            'name': os.path.basename(file_path),
            'path': file_path
        }
    except Exception as e:
        return {'error': str(e)}

def format_bytes(bytes_value: int) -> str:
    """
    Formatea bytes en unidades legibles
    
    Args:
        bytes_value (int): Valor en bytes
    
    Returns:
        str: Valor formateado
    """
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_value < 1024.0:
            return f"{bytes_value:.1f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.1f} TB"

def log_etl_step(step_name: str, status: str, details: str = ""):
    """
    Registra un paso del ETL
    
    Args:
        step_name (str): Nombre del paso
        status (str): Estado (SUCCESS, ERROR, INFO)
        details (str): Detalles adicionales
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message = f"[{timestamp}] {step_name}: {status}"
    if details:
        message += f" - {details}"
    
    logger = logging.getLogger(__name__)
    
    if status == "SUCCESS":
        logger.info(message)
    elif status == "ERROR":
        logger.error(message)
    else:
        logger.info(message)

def get_file_extension(file_name: str) -> str:
    """
    Get the file extension from a file name.
    """
    return file_name.split('.')[-1] if '.' in file_name else ''

def read_excel_file(file_path:str) -> str:
    """Reads an Excel file and returns its content."""
    import pandas as pd
    return pd.read_excel(file_path)

def save_to_csv(data, output_path: str):
    """Saves the given data to a CSV file."""
    data.to_csv(output_path, index=False)
    
def log_message(message, log_file='etl.log'):
    """Logs a message to the specified log file."""
    with open(log_file, 'a') as f:
        f.write(f"{message}\n")