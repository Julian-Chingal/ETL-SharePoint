from .comercio_servicios_processor import ComercioServiciosProcessor
from .pais_acuerdos import PaisAcuerdosProcessor
from .turismo_processor_salida_colombianos import TurismoSalidaColombianosProcessor
from .turismo_processor_visitantes_pais import TurismoVisitantesPaisProcessor

class ProcessorFactory:
    @staticmethod
    def get_processor(folder_name: str):
        processors = {
            # '1-Comercio-Bienes': IndustriaProcessor,
            # '2-Comercio-Servicios': ComercioServiciosProcessor,
            # '3-Industria': IndustriaProcessor,
            # '4-Turismo': TurismoSalidaColombianosProcessor,
            '4-Turismo': TurismoVisitantesPaisProcessor,
            'Ajustes': PaisAcuerdosProcessor,
        }

        return processors.get(folder_name, None)