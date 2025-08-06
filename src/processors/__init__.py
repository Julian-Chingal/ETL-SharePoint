from .comercio_bienes_exportaciones_processor import ComercioBienesExportacionesProcessor
from .comercio_servicios_processor import ComercioServiciosProcessor
from .inversion_ied_pais_origen import IedPaisOrigenProcessor
from .inversion_idce_pais_destino import IdcePaisDestinoProcessor
from .turismo_processor_salida_colombianos import TurismoSalidaColombianosProcessor
from .turismo_processor_visitantes_pais import TurismoVisitantesPaisProcessor
from .pais_acuerdos import PaisAcuerdosProcessor

class ProcessorFactory:
    @staticmethod
    def get_processor(folder_name: str):
        processors = {
            # '1-Comercio-Bienes': ComercioBienesExportacionesProcessor,
            # '2-Comercio-Servicios': ComercioServiciosProcessor,
            '3-Inversion': [IedPaisOrigenProcessor, IdcePaisDestinoProcessor],
            # '4-Turismo': TurismoSalidaColombianosProcessor,
            # '4-Turismo': TurismoVisitantesPaisProcessor,
            'Ajustes': PaisAcuerdosProcessor,
        }

        return processors.get(folder_name, None)