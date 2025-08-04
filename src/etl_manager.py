import logging
from typing import Dict
from extractors.sharepoint_extractor import SharePointExtractor
from utils.excel_transformer import ExcelTransformer
from loaders.data_loader import DataLoader
from processors import ProcessorFactory

class ETLManager: 
    def __init__(self, config: Dict):
        print('iniciando el etl')
        self.config = config
        self.extractor = SharePointExtractor(
            config['SHAREPOINT_SITE_URL'],
            config['SHAREPOINT_USERNAME'],
            config['SHAREPOINT_PASSWORD']
        )
        self.transformer = ExcelTransformer()
        self.loader = DataLoader(config['DATABASE_CONFIG'])
        self.logger = logging.getLogger(__name__)

    def process_all_folders(self, base_folder: str) -> Dict[str, bool]:
        """Procesa todas las carpetas encontradas"""
        results = {}
        
        # Obtener estructura de carpetas
        folder_details = self.extractor.get_folder_details(base_folder)
        if not folder_details:
            self.logger.error("No se pudo obtener la estructura de carpetas")
            return results
        
        # Procesar cada subcarpeta
        for subfolder in folder_details.subfolders:
            folder_name = subfolder.name
            folder_path = subfolder.path
            
            self.logger.info(f"Procesando carpeta: {folder_name}")
            
            # Obtener procesador específico
            processor_class = ProcessorFactory.get_processor(folder_name)
            if not processor_class:
                self.logger.warning(f"No hay procesador para {folder_name}, saltando...")
                continue
            
            # Crear instancia del procesador
            processor = processor_class(
                self.extractor, 
                self.transformer, 
                self.loader, 
                self.logger
            )
            
            # Ejecutar proceso ETL
            success = processor.process_folder(folder_path)
            results[folder_name] = success
            
            if success:
                self.logger.info(f"✓ {folder_name} procesado exitosamente")
            else:
                self.logger.error(f"✗ Error procesando {folder_name}")
        
        return results
    
    def process_single_folder(self, folder_name: str, folder_path: str) -> bool:
        """Procesa una carpeta específica"""
        processor_class = ProcessorFactory.get_processor(folder_name)
        if not processor_class:
            self.logger.error(f"No hay procesador para {folder_name}")
            return False
        
        processor = processor_class(
            self.extractor, 
            self.transformer, 
            self.loader, 
            self.logger
        )
        
        return processor.process_folder(folder_path)