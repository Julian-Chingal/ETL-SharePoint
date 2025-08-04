from .comercio_servicios_processor import ComercioServiciosProcessor

class ProcessorFactory:
    @staticmethod
    def get_processor(folder_name: str):
        processors = {
            # '1-Comercio-Bienes': IndustriaProcessor,
            '2-Comercio-Servicios': ComercioServiciosProcessor,
            # '3-Industria': IndustriaProcessor,
            # '4-Servicios': ServiciosProcessor,
            # 'Ajustes': TransporteProcessor,
        }

        return processors.get(folder_name, None)