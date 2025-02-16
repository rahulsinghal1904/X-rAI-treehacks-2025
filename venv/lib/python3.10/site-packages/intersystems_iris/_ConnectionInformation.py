import intersystems_iris._Constant


class _ConnectionInformation(object):
    def __init__(self):
        self.protocol_version = intersystems_iris._Constant._Constant.PROTOCOL_VERSION
        self._is_unicode = True
        self._compact_double = False
        self._locale = "latin-1"
        self._delimited_ids = 0
        self._server_version = ""
        self._server_version_major = 0
        self._server_version_minor = 0
        self._iris_install_dir = ""
        self._server_job_number = -1

    def _set_server_locale(self, locale):
        self._locale = self._map_server_locale(locale)

    def _parse_server_version(self, server_version):
        split_1 = server_version.split("|")
        self._server_version = split_1[0]
        if len(split_1) > 1:
            self._iris_install_dir = split_1[1]
        if self._server_version.find("Version") > 0:
            version = server_version[server_version.find("Version") + 8 :]
            self._server_version_major = version.split(".")[0]
            self._server_version_minor = version.split(".")[1]
        return

    @staticmethod
    def _map_server_locale(locale):
        # we need to map IRIS locale literals to Python locale literals
        _locales = {
            "LATIN1": "latin_1",
            "LATIN2": "iso8859_2",
            "LATINC": "iso8859_5",
            "LATINA": "iso8859_6",
            "LATING": "iso8859_7",
            "LATINH": "iso8859_8",
            "LATINT": "iso8859_11",
            "LATIN9": "iso8859_15",
            "CP1250": "cp1250",
            "CP1251": "cp1251",
            "CP1252": "cp1252",
            "CP1253": "cp1253",
            "CP1255": "cp1255",
            "CP1256": "cp1256",
            "CP1257": "cp1257",
            "CP874": "cp874",
            "UNICODE": "utf-8",
        }
        return _locales[locale.upper()] if locale.upper() in _locales else locale

    def __repr__(self) -> str:
        return f"<{self._server_version}>"
