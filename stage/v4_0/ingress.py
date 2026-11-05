from typing import ClassVar

from stage.v3_0 import ingress


class Ingress(ingress.Ingress):
    """ v3_0's Ingress, waiting far longer for missing data while Cascadia's telemetry is incomplete. """
    data_grace_s: ClassVar[float] = 1000.0
