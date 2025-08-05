from .comercio_servicios_processor import ComercioServiciosProcessor
from .pais_acuerdos import PaisAcuerdosProcessor
from .turismo_processor import TurismoProcessor

class ProcessorFactory:
    @staticmethod
    def get_processor(folder_name: str):
        processors = {
            # '1-Comercio-Bienes': IndustriaProcessor,
            # '2-Comercio-Servicios': ComercioServiciosProcessor,
            # '3-Industria': IndustriaProcessor,
            '4-Turismo': TurismoProcessor,
            'Ajustes': PaisAcuerdosProcessor,
        }

        return processors.get(folder_name, None)