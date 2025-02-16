class _IRISOREF(object):
    def __init__(self, oref):
        self._oref = oref

    def __str__(self) -> str:
        return self._oref

    def __repr__(self) -> str:
        return f"<IRISOREF ${self._oref}>"
