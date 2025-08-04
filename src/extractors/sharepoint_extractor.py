import os
import logging
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from dataclasses import dataclass
from typing import List, Optional
from io import BytesIO
import logging
import os

@dataclass
class FolderInfo:
    name: str
    path: str
    parent_path: str
    level: str
    subfolders: List['FolderInfo'] = None
    excel_files: List[str] = None
    total_files: int = 0

class SharePointExtractor:
    def __init__(self, site_url, username, password):
        self.site_url = site_url
        self.username = username
        self.password = password
        self.ctx = None
        self.logger = logging.getLogger(__name__)
        self.folder_structure = {}

    def connect(self):
        """
        Método para conectar a SharePoint
        """
        try:
            ctx_auth = AuthenticationContext(self.site_url)
            
            if ctx_auth.acquire_token_for_user(self.username, self.password):
                self.ctx = ClientContext(self.site_url, ctx_auth)
                web = self.ctx.web
                self.ctx.load(web)
                self.ctx.execute_query()
                self.logger.info("Conexión exitosa a SharePoint")
                self.logger.info(f"Conectado a sitio: {web.properties['Title']}")
                return True
            else: 
                self.logger.error("Error al adquirir token de usuario")
                return False
        except Exception as e:
            self.logger.error(f"Error al conectar con SharePoint: {str(e)}")
            return False

    def _get_folder_url(self, folder_path):
        """Construir URL completa de la carpeta según el tipo de sitio"""
        if not folder_path:
            return "Shared Documents"
        
        if "/personal/" in self.site_url:
            return f"Documents/{folder_path}"
        else:
            return f"Shared Documents/{folder_path}"
        
    def get_folder_details(self, folder_path: str = '') -> Optional[FolderInfo]:
        try:
            if not self.ctx:
                if not self.connect():
                    return None
            
            folder_url = self._get_folder_url(folder_path)
            self.logger.log(logging.INFO, f"Obteniendo detalles de la carpeta: {folder_url}")
            
            # Obtener carpeta
            folder = self.ctx.web.get_folder_by_server_relative_url(folder_url)
            subfolders = folder.folders
            self.ctx.load(subfolders)
            self.ctx.execute_query()

            # Procesar subcarpetas recursivamente
            subfolder_infos = []
            for subfolder in subfolders:
                subfolder_path = f"{folder_path}/{subfolder.properties['Name']}" if folder_path else subfolder.properties['Name']
                subfolder_info = self.get_folder_details(subfolder_path)  # Llamada recursiva
                if subfolder_info:
                    subfolder_infos.append(subfolder_info)

            folder_info = FolderInfo(
                name=os.path.basename(folder_path) if folder_path else 'Root',
                path=folder_path,
                parent_path=os.path.dirname(folder_path) if folder_path else '',
                level=folder_path.count('/') if folder_path else 0,
                subfolders=subfolder_infos,  # Lista de objetos FolderInfo
                excel_files=[],
                total_files=0
            )
          
            return folder_info
        except Exception as e:
                self.logger.error(f"Error al obtener detalles de la carpeta: {str(e)}")
                return None
    def list_files(self, folder_path: str = '') -> List[str]:
        """
        Lista los archivos en una carpeta específica de SharePoint
        
        Args:
            folder_path (str): Ruta de la carpeta en SharePoint
            
        Returns:
            List[str]: Lista de nombres de archivos
        """
        try:
            if not self.ctx:
                if not self.connect():
                    return []
            
            # Usar el método helper para construir la URL correcta
            folder_url = self._get_folder_url(folder_path)
            self.logger.info(f"Listando archivos en: {folder_url}")
            
            try:
                # Obtener la carpeta
                folder = self.ctx.web.get_folder_by_server_relative_url(folder_url)
                files = folder.files
                self.ctx.load(files)
                self.ctx.execute_query()
                
                file_names = [file.properties['Name'] for file in files]
                self.logger.info(f"Archivos encontrados: {len(file_names)} - {file_names}")
                
                return file_names
                
            except Exception as e:
                self.logger.warning(f"Error accediendo a carpeta {folder_url}: {str(e)}")
                # Intentar método alternativo
                doc_lib = self.ctx.web.default_document_library()
                folder = doc_lib.get_folder_by_server_relative_url(folder_url)
                files = folder.files
                self.ctx.load(files)
                self.ctx.execute_query()
                
                file_names = [file.properties['Name'] for file in files]
                self.logger.info(f"Archivos encontrados (método alternativo): {len(file_names)} - {file_names}")
                
                return file_names
                
        except Exception as e:
            self.logger.error(f"Error al listar archivos en {folder_path}: {str(e)}")
            return []
        
    def download_file(self, folder_path: str, file_name: str) -> Optional[BytesIO]:
        """
        Descarga un archivo específico desde SharePoint
        
        Args:
            folder_path (str): Ruta de la carpeta
            file_name (str): Nombre del archivo
            
        Returns:
            BytesIO: Contenido del archivo en memoria
        """
        try:
            if not self.ctx:
                if not self.connect():
                    return None
            
            # Construir URL completa del archivo
            folder_url = self._get_folder_url(folder_path)
            file_url = f"{folder_url}/{file_name}"
            
            self.logger.info(f"Descargando archivo: {file_url}")
            
            try:
                # Obtener referencia al archivo
                file_obj = self.ctx.web.get_file_by_server_relative_url(file_url)
                self.ctx.load(file_obj)
                self.ctx.execute_query()
                
                # Descargar contenido
                response = file_obj.read()
                self.ctx.execute_query()
                
                # Manejar diferentes tipos de respuesta
                if isinstance(response, bytes):
                    content_bytes = response
                elif hasattr(response, 'value'):
                    content_bytes = response.value
                elif hasattr(response, 'content'):
                    content_bytes = response.content
                else:
                    # Intentar convertir a bytes
                    content_bytes = bytes(response)
                
                if not content_bytes:
                    self.logger.error(f"Contenido vacío para archivo {file_name}")
                    return None
                
                file_stream = BytesIO(content_bytes)
                self.logger.info(f"Archivo {file_name} descargado: {len(content_bytes)} bytes")
                
                print(file_stream)
                return file_stream
                
            except Exception as e:
                self.logger.error(f"Error descargando archivo {file_name}: {str(e)}")
                return None
            
        except Exception as e:
            self.logger.error(f"Error general descargando archivo {file_name}: {str(e)}")
            return None
        
    def list_folders(self, folder_path: str = '') -> List[str]:
        """
        Lista las carpetas en una ruta específica de SharePoint
        Args:
            folder_path (str): Ruta de la carpeta en SharePoint
        Returns:
            List[str]: Lista de nombres de carpetas
        """
        try:
            if not self.ctx:
                if not self.connect():
                    return []
                
            folder_url = self._get_folder_url(folder_path)
            self.logger.info(f"Listando carpetas en: {folder_url}")
            
            try:
                folder = self.ctx.web.get_folder_by_server_relative_url(folder_url)
                sub_folders = folder.folders
                self.ctx.load(sub_folders)
                self.ctx.execute_query()
            except Exception as e:
                self.logger.warning(f"Error al obtener carpeta: {str(e)}")
                doc_lib = self.ctx.web.default_document_library()
                folder = doc_lib.get_folder_by_server_relative_url(folder_url)
                sub_folders = folder.folders
                self.ctx.load(sub_folders)
                self.ctx.execute_query()
            
            folder_names = []
            for sub_folder in sub_folders:
                folder_names.append(sub_folder.properties['Name'])
            
            self.logger.info(f"Carpetas encontradas: {len(folder_names)} - {folder_names}")
            
            return folder_names
            
        except Exception as e:
            self.logger.error(f"Error al listar carpetas en SharePoint: {str(e)}")
            return []