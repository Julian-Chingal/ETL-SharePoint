import sys
from utils.helpers import setup_logging, validate_config, log_etl_step
from etl_manager import ETLManager
from config.settings import *

def main():
    """
    Función principal del ETL para extraer, transformar y cargar datos de SharePoint
    """
    # Configurar logging
    logger = setup_logging(LOGGING_LEVEL, LOGGING_FILE)
    
    try:
        # Validar configuración
        config = {
            'SHAREPOINT_SITE_URL': SHAREPOINT_SITE_URL,
            'SHAREPOINT_USERNAME': SHAREPOINT_USERNAME,
            'SHAREPOINT_PASSWORD': SHAREPOINT_PASSWORD,
            'DATABASE_CONFIG': {
                'host': DATABASE_HOST,
                'database': DATABASE_NAME,
                'port': DATABASE_PORT,
                'user': DATABASE_USER,
                'password': DATABASE_PASSWORD
            }
        }
        
        if not validate_config(config):
            return False
        
        etl_manager = ETLManager(config)
        results = etl_manager.process_all_folders(SHAREPOINT_BASE_FOLDER)


        successful = sum(1 for success in results.values() if success)
        total = len(results)
        
        logger.info(f"Procesamiento completado: {successful}/{total} carpetas exitosas")
        

        

        return True
        
    except Exception as e:
        log_etl_step("ETL_PROCESS", "ERROR", f"Error general: {str(e)}")
        logger.error(f"Error en el proceso ETL: {str(e)}", exc_info=True)
        return False

if __name__ == "__main__":
    # Ejecutar pruebas de conexión primero
    # test_connections()
    
    # Ejecutar ETL principal
    success = main()
    
    if success:
        print("ETL completado exitosamente. Revisar logs para detalles.")
        sys.exit(0)
    else:
        print("ETL falló. Revisar logs para detalles del error.")
        sys.exit(1)
    