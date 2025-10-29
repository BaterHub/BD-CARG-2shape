"""
BDgeoDB2shapeQGIS.py — PyQGIS port (prototype)

Converts CARG-coded File Geodatabase (.gdb) layers to standardized shapefiles using QGIS Processing.

Run inside QGIS (Python console or as a Processing script). For standalone usage,
initialize a QgsApplication and Processing framework before calling run().
"""

from qgis.core import (
    QgsVectorLayer,
    QgsProject,
    QgsProcessingFeedback,
    QgsFeatureRequest,
    QgsField,
    QgsFields,
    QgsVectorDataProvider,
    edit,
)
from qgis.PyQt.QtCore import QVariant
import processing
import os
import time


class CARGProcessorQGISGDB:
    def __init__(self, input_gdb):
        self.input_gdb = input_gdb
        self.workspace = os.path.dirname(os.path.abspath(input_gdb)) or os.getcwd()
        self.domini_path = os.path.join(self.workspace, "domini")
        self.output_dir = os.path.join(self.workspace, "output")
        self.feedback = QgsProcessingFeedback()

        self._sublayers = None
        self.foglio = None
        self._domain_cache = {}

        # Minimal feature configuration (extend as needed)
        self.feature_configs = {
            # Geomorfologia
            "ST010Point": {
                "search": ["ST010Point", "main.ST010Point", "main/ST010Point"],
                "output": "geomorfologia_punti.shp",
                "field_map": {
                    "Pun_Gmo": "Pun_Gmo",
                    "Tipo": "Tipo_Gmrf",
                    "Tipologia": "Tipologia",
                    "Stato": "Stato",
                    "Direzio": "Direzione",
                },
                "domains": {
                    "Tipo_Gmrf": ("d_10_tipo.dbf", "Tipo_G_txt"),
                    "Tipologia": ("d_tipologia.dbf", "Tipol_txt"),
                    "Stato": ("d_stato.dbf", "Stato_txt"),
                },
            },
            "ST011Polygon": {
                "search": ["ST011Polygon", "main.ST011Polygon", "main/ST011Polygon"],
                "output": "geomorfologia_poligoni.shp",
                "field_map": {
                    "Pol_Gmo": "Pol_Gmo",
                    "Tipo": "Tipo_Gmrf",
                    "Tipologia": "Tipologia",
                    "Stato": "Stato",
                    "Direzio": "Direzione",
                },
                "domains": {
                    "Tipo_Gmrf": ("d_11_tipo.dbf", "Tipo_G_txt"),
                    "Tipologia": ("d_tipologia.dbf", "Tipol_txt"),
                    "Stato": ("d_stato.dbf", "Stato_txt"),
                },
            },
            "ST012Polyline": {
                "search": ["ST012Polyline", "main.ST012Polyline", "main/ST012Polyline"],
                "output": "geomorfologia_linee.shp",
                "field_map": {
                    "Lin_Gmo": "Lin_Gmo",
                    "Label": "Label",
                    "Tipo": "Tipo_Gmrf",
                    "Tipologia": "Tipologia",
                    "Stato": "Stato",
                },
                "domains": {
                    "Tipo_Gmrf": ("d_12_tipo.dbf", "Tipo_G_txt"),
                    "Tipologia": ("d_tipologia.dbf", "Tipol_txt"),
                    "Stato": ("d_stato.dbf", "Stato_txt"),
                },
            },
            # Risorse/Prospezioni
            "ST013Point": {
                "search": ["ST013Point", "main.ST013Point", "main/ST013Point"],
                "output": "risorse_prospezioni.shp",
                "field_map": {
                    "Num_Ris": "Num_Ris",
                    "Label1": "Label1",
                    "Label2": "Label2",
                    "Label3": "Label3",
                    "TIPO": "Tipo",
                },
                "domains": {
                    "Tipo": ("d_13_tipo.dbf", "Tipo_txt"),
                },
            },
            # Geologia linee
            "ST018Polyline": {
                "search": ["ST018Polyline", "main.ST018Polyline", "main/ST018Polyline"],
                "output": "geologia_linee.shp",
                "field_map": {
                    "Tipo": "Tipo_geo",
                    "Tipologia": "Tipologia",
                    "Contorno": "Contorno",
                    "Affiora": "Affiora",
                    "Direzio": "Direzione",
                },
                "domains": {
                    "Tipo_geo": ("d_st018_line.dbf", "Tipo_g_txt"),
                    "Tipologia": ("d_tipologia.dbf", "Tipol_txt"),
                    "Contorno": ("d_st018_contorno.dbf", "Cont_txt"),
                    "Affiora": ("d_st018_affiora.dbf", "Affior_txt"),
                },
                "extra_fixed": {"Fase_txt": "non applicabile/non classificabile"},
            },
            # Geologia poligoni (mappature avanzate da estendere se necessario)
            "ST018Polygon": {
                "search": ["ST018Polygon", "main.ST018Polygon", "main/ST018Polygon"],
                "output": "geologia_poligoni.shp",
                "field_map": {
                    "Pol_Uc": "Pol_Uc",
                    "Uc_Lege": "Uc_Lege",
                    "Direzio": "Direzione",
                },
                "domains": {},
            },
            # Geologia punti
            "ST019Point": {
                "search": ["ST019Point", "main.ST019Point", "main/ST019Point"],
                "output": "geologia_punti.shp",
                "field_map": {
                    "Num_Oss": "Num_Oss",
                    "Quota": "Quota",
                    "Inclina": "Inclinaz",
                    "Immersio": "Immersione",
                    "Direzio": "Direzione",
                    "Tipo": "Tipo_geo",
                    "Tipologia": "Tipologia",
                    "Fase": "Fase",
                    "Verso": "Verso",
                    "Asimmetria": "Asimmetria",
                },
                "domains": {
                    "Tipo_geo": ("d_19_tipo.dbf", "Tipo_g_txt"),
                    "Tipologia": ("d_tipologia.dbf", "Tipol_txt"),
                    "Fase": ("d_fase.dbf", "Fase_txt"),
                    "Verso": ("d_verso.dbf", "Verso_txt"),
                    "Asimmetria": ("d_asimmetria.dbf", "Asimm_txt"),
                },
            },
            # Pieghe (verranno appese alle linee geologiche)
            "ST021Polyline": {
                "search": ["ST021Polyline", "main.ST021Polyline", "main/ST021Polyline"],
                "output": "geologia_linee_pieghe.shp",
                "field_map": {
                    "Tipo": "Tipo_geo",
                    "Tipologia": "Tipologia",
                    "Fase": "Fase",
                    "Direzio": "Direzione",
                },
                "domains": {
                    "Tipo_geo": ("d_st021.dbf", "Tipo_g_txt"),
                    "Tipologia": ("d_tipologia.dbf", "Tipol_txt"),
                    "Fase": ("d_fase.dbf", "Fase_txt"),
                },
                "extra_fixed": {"Affior_txt": "non applicabile", "Cont_txt": "no"},
            },
        }

    # ------------------------ helpers ------------------------
    def ensure_output_dir(self):
        desired = self.output_dir
        try:
            if not os.path.exists(desired):
                os.makedirs(desired)
            test = os.path.join(desired, f".write_test_{int(time.time())}")
            with open(test, "w", encoding="utf-8") as f:
                f.write("ok")
            os.remove(test)
        except Exception:
            ts = time.strftime("%Y%m%d_%H%M%S")
            fallback = os.path.join(self.workspace, f"output_{ts}")
            os.makedirs(fallback, exist_ok=True)
            self.output_dir = fallback

    def open_layer(self, layer_name):
        # Try different layername syntaxes for FileGDB
        candidates = [
            f"{self.input_gdb}|layername={layer_name}",
        ]
        # Also try swapping separators
        if "/" in layer_name:
            candidates.append(f"{self.input_gdb}|layername={layer_name.replace('/', '.')}" )
        if "." in layer_name:
            candidates.append(f"{self.input_gdb}|layername={layer_name.replace('.', '/')}" )

        for uri in candidates:
            vl = QgsVectorLayer(uri, layer_name, "ogr")
            if vl and vl.isValid():
                return vl
        return None

    def get_foglio(self):
        # Iterate known layers until FoglioGeologico is found
        for key, cfg in self.feature_configs.items():
            for pat in cfg['search']:
                vl = self.open_layer(pat)
                if not vl:
                    continue
                if vl.fields().indexOf("FoglioGeologico") != -1:
                    for f in vl.getFeatures(QgsFeatureRequest().setLimit(50)):
                        val = f["FoglioGeologico"]
                        if val not in (None, ""):
                            self.foglio = str(val).strip()
                            return self.foglio
        raise RuntimeError("FoglioGeologico not found in any layer")

    def load_domain(self, filename, code_field="CODE", desc_field_like="DESC"):
        key = (filename, code_field, desc_field_like)
        if key in self._domain_cache:
            return self._domain_cache[key]
        path = os.path.join(self.domini_path, filename)
        vl = QgsVectorLayer(path, filename, "ogr")
        if not vl or not vl.isValid():
            return {}
        # Find desc field by pattern
        desc_field = None
        for fld in vl.fields():
            if desc_field_like.upper() in fld.name().upper():
                desc_field = fld.name()
                break
        if not desc_field or vl.fields().indexOf(code_field) == -1:
            return {}
        m = {}
        for f in vl.getFeatures():
            c = f[code_field]
            d = f[desc_field]
            if c is None or d is None:
                continue
            m[str(c).strip()] = str(d).strip()
            if isinstance(c, (int, float)):
                m[c] = str(d).strip()
        self._domain_cache[key] = m
        return m

    def add_or_ensure_field(self, layer, name, qvariant_type=QVariant.String, length=254):
        if layer.fields().indexOf(name) == -1:
            dp = layer.dataProvider()
            fld = QgsField(name, qvariant_type, len=length if qvariant_type == QVariant.String else 0)
            dp.addAttributes([fld])
            layer.updateFields()

    def apply_domain_mapping_edit(self, layer, source_field, target_field, code_map):
        if not code_map:
            return
        self.add_or_ensure_field(layer, target_field, QVariant.String, 254)
        with edit(layer):
            for f in layer.getFeatures():
                src = f[source_field]
                val = ""
                if src is not None:
                    if src in code_map:
                        val = code_map[src]
                    else:
                        sval = str(src).strip()
                        val = code_map.get(sval, "")
                layer.changeAttributeValue(f.id(), layer.fields().indexOf(target_field), val)

    # ------------------------ processing ------------------------
    def refactor_and_save(self, input_layer, field_map, extra_fixed=None, output_name=None):
        fields_mapping = []
        for src, dst in field_map.items():
            if input_layer.fields().indexOf(src) == -1:
                fields_mapping.append({
                    'expression': f"''",
                    'length': 254,
                    'name': dst,
                    'precision': 0,
                    'type': 10,
                })
            else:
                fields_mapping.append({
                    'expression': f"attribute('{src}')",
                    'length': 254,
                    'name': dst,
                    'precision': 0,
                    'type': 10,
                })
        extra_fixed = extra_fixed or {}
        for k, v in extra_fixed.items():
            fields_mapping.append({
                'expression': f"'{v}'",
                'length': 254,
                'name': k,
                'precision': 0,
                'type': 10,
            })
        if 'Foglio' not in [m['name'] for m in fields_mapping]:
            fields_mapping.append({
                'expression': f"'{self.foglio or ''}'",
                'length': 254,
                'name': 'Foglio',
                'precision': 0,
                'type': 10,
            })
        params = {
            'INPUT': input_layer,
            'FIELDS_MAPPING': fields_mapping,
            'OUTPUT': 'TEMPORARY_OUTPUT',
        }
        refactored = processing.run('qgis:refactorfields', params, feedback=self.feedback)['OUTPUT']
        out_path = os.path.join(self.output_dir, output_name)
        processing.run('native:savefeatures', {
            'INPUT': refactored,
            'OUTPUT': out_path,
        }, feedback=self.feedback)
        return out_path

    def process_config(self, cfg_key, cfg):
        layer_name = None
        for pat in cfg['search']:
            if self.open_layer(pat):
                layer_name = pat
                break
        if not layer_name:
            return False
        vl = self.open_layer(layer_name)
        if not vl:
            return False
        out_path = self.refactor_and_save(vl, cfg['field_map'], cfg.get('extra_fixed'), cfg['output'])
        for src_field, (domain_file, target_text_field) in cfg.get('domains', {}).items():
            code_map = self.load_domain(domain_file)
            out_layer = QgsVectorLayer(out_path, os.path.basename(out_path), 'ogr')
            if out_layer and out_layer.isValid():
                mapped_src = cfg['field_map'].get(src_field, src_field)
                self.apply_domain_mapping_edit(out_layer, mapped_src, target_text_field, code_map)
                processing.run('native:savefeatures', {
                    'INPUT': out_layer,
                    'OUTPUT': out_path,
                }, feedback=self.feedback)
        return True

    def append_pieghe_into_geologia_linee(self):
        dest = os.path.join(self.output_dir, 'geologia_linee.shp')
        src = os.path.join(self.output_dir, 'geologia_linee_pieghe.shp')
        if not (os.path.exists(dest) and os.path.exists(src)):
            return
        processing.run('native:append', {
            'INPUT': [src],
            'TARGET': dest,
            'FIELD_MAPPING': [],
            'USE_FIELD_MAPPING': False,
        }, feedback=self.feedback)
        try:
            os.remove(src)
        except Exception:
            pass

    # ------------------------ entrypoint ------------------------
    def run(self):
        self.ensure_output_dir()
        self.get_foglio()
        processed = 0
        for key, cfg in self.feature_configs.items():
            if self.process_config(key, cfg):
                processed += 1
        self.append_pieghe_into_geologia_linee()
        return processed


def run(input_gdb):
    proc = CARGProcessorQGISGDB(input_gdb)
    return proc.run()

