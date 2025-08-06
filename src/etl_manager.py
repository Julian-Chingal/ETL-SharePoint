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
            
            # Obtener procesador(es) específico(s)
            processor_classes = ProcessorFactory.get_processor(folder_name)
            if not processor_classes:
                self.logger.warning(f"No hay procesador para {folder_name}, saltando...")
                continue
            
            # Manejar múltiples procesadores
            if isinstance(processor_classes, list):
                folder_success = True
                for processor_class in processor_classes:
                    success = self._process_with_single_processor(processor_class, folder_path, folder_name)
                    if not success:
                        folder_success = False
                results[folder_name] = folder_success
            else:
                # Procesador único
                success = self._process_with_single_processor(processor_classes, folder_path, folder_name)
                results[folder_name] = success
            
            if results[folder_name]:
                self.logger.info(f"✓ {folder_name} procesado exitosamente")
            else:
                self.logger.error(f"✗ Error procesando {folder_name}")
        
        return results
    
    def _process_with_single_processor(self, processor_class, folder_path: str, folder_name: str) -> bool:
        """Procesa una carpeta con un procesador específico"""
        try:
            # Crear instancia del procesador
            processor = processor_class(
                self.extractor, 
                self.transformer, 
                self.loader, 
                self.logger
            )
            
            processor_name = processor_class.__name__
            self.logger.info(f"Ejecutando {processor_name} para {folder_name}")
            
            # Ejecutar proceso ETL
            success = processor.process_folder(folder_path)
            
            if success:
                self.logger.info(f"✓ {processor_name} completado exitosamente")
            else:
                self.logger.error(f"✗ Error en {processor_name}")
                
            return success
            
        except Exception as e:
            self.logger.error(f"Error ejecutando procesador {processor_class.__name__}: {str(e)}")
            return False
    
    def process_single_folder(self, folder_name: str, folder_path: str) -> bool:
        """Procesa una carpeta específica"""
        processor_classes = ProcessorFactory.get_processor(folder_name)
        if not processor_classes:
            self.logger.error(f"No hay procesador para {folder_name}")
            return False
        
        # Manejar múltiples procesadores
        if isinstance(processor_classes, list):
            overall_success = True
            for processor_class in processor_classes:
                success = self._process_with_single_processor(processor_class, folder_path, folder_name)
                if not success:
                    overall_success = False
            return overall_success
        else:
            # Procesador único
            return self._process_with_single_processor(processor_classes, folder_path, folder_name)