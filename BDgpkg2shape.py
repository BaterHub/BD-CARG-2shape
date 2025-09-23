from __future__ import print_function

"""
CARG Data Conversion Script
Converts geological data from CARG coded GeoPackage to shapefiles

                ██████╗ ██████╗ 
                ██╔══██╗██╔══██╗
                ██████╔╝██████╔╝
                ██╔═══╝ ██╔═══╝ 
                ██║     ██║     
                ╚═╝     ╚═╝     
   /\                                       /\  
  /  \         BDgpkg2shape v1.0           /  \  
 /____\ patrizio.petricca@isprambiente.it /____\  

"""

import arcpy
import os
import shutil
import codecs
import csv
import re
import sys
import time
from collections import defaultdict

# Reload and set encoding for Python 2 compatibility
reload(sys)
sys.setdefaultencoding('utf8')

# Configure ArcPy environment
arcpy.env.overwriteOutput = True

class CARGProcessor:
    """
    Optimized CARG data processor for converting GeoPackage layers to shapefiles
    """
    
    def __init__(self, input_gpkg):
        self.input_gpkg = input_gpkg
        
        # Setup workspace paths
        self.input_dir = os.path.dirname(input_gpkg)
        self.workspace = self.input_dir if self.input_dir else os.getcwd()
        self.workspace_shape = os.path.join(self.workspace, "shape")
        self.workspace_output = os.path.join(self.workspace, "output")
        self.domini_path = os.path.join(self.workspace, "domini")

        # Initialize cache variables
        self._available_layers = None
        self._domain_cache = {}
        self.input_sr = None
        
        # Feature class configurations
        self.feature_configs = self._get_feature_configs()
        
        # Field standardization configurations
        self.field_standards = self._get_field_standards()

    def safe_string_conversion(self, value):
        """Single robust string conversion function"""
        if value is None:
            return ""
        
        try:
            return str(value).strip()
        except UnicodeDecodeError:
            try:
                if isinstance(value, str):
                    return value.decode('utf-8', errors='replace').strip()
                else:
                    return unicode(value).encode('utf-8', errors='replace').decode('utf-8').strip()
            except:
                try:
                    return str(value).encode('ascii', errors='ignore').decode('ascii').strip()
                except:
                    return "ENCODING_ERROR"
        except Exception:
            return "CONVERSION_ERROR"

    def get_foglio_from_geopackage(self):
        """Extract FoglioGeologico value from any layer in the geopackage"""
        available_layers = self.get_available_layers()
        
        for layer_name in available_layers:
            try:
                layer_path = os.path.join(self.input_gpkg, layer_name)
                
                # Check if FoglioGeologico field exists
                fields = [f.name.upper() for f in arcpy.ListFields(layer_path)]
                if "FOGLIOGEOLOGICO" in fields:
                    # Get the first non-null value
                    with arcpy.da.SearchCursor(layer_path, ["FoglioGeologico"]) as cursor:
                        for row in cursor:
                            if row[0] is not None:
                                self.foglio = str(row[0]).strip()
                                arcpy.AddMessage("Found FoglioGeologico: {} in layer {}".format(
                                    self.foglio, layer_name))
                                return self.foglio
            except Exception as e:
                arcpy.AddWarning("Error reading FoglioGeologico from {}: {}".format(
                    layer_name, str(e)))
                continue
        
        raise ValueError("FoglioGeologico field not found or empty in any layer")

    def _get_field_standards(self):
        """Define field standardization configurations for each output shapefile"""
        return {
            "geologia_punti.shp": {
                "field_order": ["FID", "Shape", "Num_Oss", "Quota", "Foglio", "Tipo_Geo", 
                               "Inclinaz", "Asimmetria", "Fase", "Immersione", "Verso", 
                               "Direzione", "Tipologia"],
                "field_mappings": {
                    "Tipo_g_txt": "Tipo_Geo",
                    "Asimm_txt": "Asimmetria",
                    "Fase_txt": "Fase",
                    "Verso_txt": "Verso",
                    "Tipol_txt": "Tipologia"
                }
            },
            "geologia_linee.shp": {
                "field_order": ["FID", "Shape", "Foglio", "Fase", "Affiora", "Tipo_Geo", 
                                "Contorno", "Tipologia", "Direzione"],
                "field_mappings": {
                    "Affior_txt": "Affiora",
                    "Tipo_g_txt": "Tipo_Geo",
                    "Cont_txt": "Contorno",
                    "Tipol_txt": "Tipologia",
                    "Fase_txt": "Fase"
                }
            },
            "geologia_linee_pieghe.shp": {
                "field_order": ["FID", "Shape", "Foglio", "Fase", "Affiora", "Tipo_Geo", 
                                "Contorno", "Tipologia", "Direzione"],
                "field_mappings": {
                    "Affior_txt": "Affiora",
                    "Tipo_g_txt": "Tipo_Geo", 
                    "Cont_txt": "Contorno",
                    "Tipol_txt": "Tipologia",
                    "Fase_txt": "Fase"
                }
            },
            "geologia_poligoni.shp": {
                "field_order": ["FID", "Shape", "Pol_Uc", "Uc_Lege", 
                                "Foglio", "Tipo_UQ", "Stato_UQ", "ETA_Super", "ETA_Infer", 
                                "Tipo_UG", "Tessitura", "Sigla1", "Sigla_UG", "Nome", 
                                "Legenda", "Sommerso", "Direzione"],
                "field_mappings": {
                    "ETA_super": "ETA_Super",
                    "ETA_infer": "ETA_Infer",
                    "tipo_ug": "Tipo_UG",
                    "Sigla_ug": "Sigla_UG",
                    "Sommerso_": "Sommerso"
                }
            },
            "geomorfologia_punti.shp": {
                "field_order": ["FID", "Shape", "Pun_Gmo", "Foglio", "Tipo_Gmrf", "Stato", 
                               "Tipologia", "Direzione"],
                "field_mappings": {
                    "Tipo_G_txt": "Tipo_Gmrf",
                    "Stato_txt": "Stato",
                    "Tipol_txt": "Tipologia"
                }
            },
            "geomorfologia_linee.shp": {
                "field_order": ["FID", "Shape", "Lin_Gmo", "Label", "Foglio", 
                                "Tipo_Gmrf", "Stato", "Tipologia"],
                "field_mappings": {
                    "Tipo_G_txt": "Tipo_Gmrf",
                    "Stato_txt": "Stato",
                    "Tipol_txt": "Tipologia"
                }
            },
            "geomorfologia_poligoni.shp": {
                "field_order": ["FID", "Shape", "Pol_Gmo", "Foglio", 
                                "Tipo_Gmrf", "Stato", "Tipologia", "Direzione"],
                "field_mappings": {
                    "Tipo_G_txt": "Tipo_Gmrf",
                    "Stato_txt": "Stato",
                    "Tipol_txt": "Tipologia"
                }
            },
            "risorse_prospezioni.shp": {
                "field_order": ["FID", "Shape", "Num_Ris", "Label1", "Label2", "Label3", 
                               "Foglio", "Tipo"],
                "field_mappings": {
                    "Tipo_txt": "Tipo"
                }
            }
        }

    def _get_feature_configs(self):
        """Return optimized feature class configurations"""
        return {
            "ST010Point": {
                "search_patterns": ["ST010Point", "main.ST010Point"],
                "fields": [
                    {"old": "Tipo", "new": "Tipo_Gmrf"},
                    {"old": "Tipologia", "new": "Tipologia"},
                    {"old": "Stato", "new": "Stato"},
                    {"old": "Pun_Gmo", "new": "Pun_Gmo"},
                    {"old": "Direzio", "new": "Direzione"}
                ],
                "domains": [
                    {"field": "Tipo_G_txt", "source": "Tipo_Gmrf", "domain": "d_10_tipo.dbf"},
                    {"field": "Tipol_txt", "source": "Tipologia", "domain": "d_tipologia.dbf"},
                    {"field": "Stato_txt", "source": "Stato", "domain": "d_stato.dbf"}
                ],
                "output_name": "geomorfologia_punti.shp",
                "keep_fields": ["Pun_Gmo", "Foglio", "Tipo_G_txt", "Stato_txt", "Tipol_txt", "Direzione"]
            },
            "ST011Polygon": {
                "search_patterns": ["ST011Polygon", "main.ST011Polygon"],
                "fields": [
                    {"old": "Tipo", "new": "Tipo_Gmrf"},
                    {"old": "Tipologia", "new": "Tipologia"},
                    {"old": "Stato", "new": "Stato"},
                    {"old": "Pol_Gmo", "new": "Pol_Gmo"},
                    {"old": "Direzio", "new": "Direzione"}
                ],
                "domains": [
                    {"field": "Tipo_G_txt", "source": "Tipo_Gmrf", "domain": "d_11_tipo.dbf"},
                    {"field": "Tipol_txt", "source": "Tipologia", "domain": "d_tipologia.dbf"},
                    {"field": "Stato_txt", "source": "Stato", "domain": "d_stato.dbf"}
                ],
                "output_name": "geomorfologia_poligoni.shp",
                "keep_fields": ["Pol_Gmo", "Foglio", "Tipo_G_txt", "Stato_txt", "Tipol_txt", "Direzione"]
            },
            "ST012Polyline": {
                "search_patterns": ["ST012Polyline", "main.ST012Polyline"],
                "fields": [
                    {"old": "Tipo", "new": "Tipo_Gmrf"},
                    {"old": "Tipologia", "new": "Tipologia"},
                    {"old": "Stato", "new": "Stato"},
                    {"old": "Lin_Gmo", "new": "Lin_Gmo"},
                    {"old": "Label", "new": "Label"} 
                ],
                "domains": [
                    {"field": "Tipo_G_txt", "source": "Tipo_Gmrf", "domain": "d_12_tipo.dbf"},
                    {"field": "Tipol_txt", "source": "Tipologia", "domain": "d_tipologia.dbf"},
                    {"field": "Stato_txt", "source": "Stato", "domain": "d_stato.dbf"}
                ],
                "output_name": "geomorfologia_linee.shp",
                "keep_fields": ["Lin_Gmo", "Label", "Foglio", "Tipo_G_txt", "Stato_txt", "Tipol_txt"]
            },
            "ST013Point": {
                "search_patterns": ["ST013Point", "main.ST013Point"],
                "fields": [
                    {"old": "Num_Ris", "new": "Num_Ris", "sources": ["Num_Ris", "NUMERORIS"]},
                    {"old": "Label1", "new": "Label1", "sources": ["Label1"]},
                    {"old": "Label2", "new": "Label2", "sources": ["Label2"]},
                    {"old": "Label3", "new": "Label3", "sources": ["Label3"]},
                    {"old": "TIPO", "new": "Tipo", "sources": ["TIPO", "Tipo"]}
                ],
                "domains": [
                    {"field": "Tipo_txt", "source": "Tipo", "domain": "d_13_tipo.dbf"}
                ],
                "output_name": "risorse_prospezioni.shp",
                "keep_fields": ["Num_Ris", "Label1", "Label2", "Label3", "Foglio", "Tipo_txt"]
            },
            "ST018Polyline": {
                "search_patterns": ["ST018Polyline", "main.ST018Polyline"],
                "fields": [
                    {"old": "Tipo", "new": "Tipo_geo"},
                    {"old": "Tipologia", "new": "Tipologia"},
                    {"old": "Contorno", "new": "Contorno"},
                    {"old": "Affiora", "new": "Affiora"},
                    {"old": "Direzio", "new": "Direzione"}
                ],
                "domains": [
                    {"field": "Tipo_g_txt", "source": "Tipo_geo", "domain": "d_st018_line.dbf"},
                    {"field": "Tipol_txt", "source": "Tipologia", "domain": "d_tipologia.dbf"},
                    {"field": "Cont_txt", "source": "Contorno", "domain": "d_st018_contorno.dbf"},
                    {"field": "Affior_txt", "source": "Affiora", "domain": "d_st018_affiora.dbf"}
                ],
                "output_name": "geologia_linee.shp",
                "keep_fields": ["Foglio", "Affior_txt", "Tipo_g_txt", "Cont_txt", "Tipol_txt", "Direzione", "Fase_txt"],
                "special_processing": "geology_lines"
            },
            "ST018Polygon": {
                "search_patterns": ["ST018Polygon", "main.ST018Polygon"],
                "fields": [
                    {"old": "UQ_CAR", "new": "Tipo_UQ"},
                    {"old": "UQ_CAR", "new": "Stato_UQ"},
                    {"old": "UC_LEGE", "new": "ETA_super"},
                    {"old": "UC_LEGE", "new": "ETA_infer"},
                    {"old": "UC_LEGE", "new": "tipo_ug"},
                    {"old": "ID_TESS", "new": "Tessitura"},
                    {"old": "SOMMERSO", "new": "Sommerso_"},
                    {"old": "UC_LEGE", "new": "Sigla1"},
                    {"old": "UC_LEGE", "new": "Sigla_ug"},
                    {"old": "UC_LEGE", "new": "Nome"},
                    {"old": "UC_LEGE", "new": "Legenda"},
                    {"old": "Pol_Uc", "new": "Pol_Uc"},
                    {"old": "Uc_Lege", "new": "Uc_Lege"},
                    {"old": "Direzio", "new": "Direzione"}
                ],
                "domains": [],
                "output_name": "geologia_poligoni.shp",
                "special_processing": "geology_polygons",
                "keep_fields": ["Pol_Uc", "Uc_Lege", "Foglio", "Direzione", "Tipo_UQ", "Stato_UQ", 
                              "ETA_super", "ETA_infer", "tipo_ug", "Tessitura", "Sigla1", "Sigla_ug", 
                              "Nome", "Legenda", "Sommerso_"]
            },
            "ST019Point": {
                "search_patterns": ["ST019Point", "main.ST019Point"],
                "fields": [
                    {"old": "Tipo", "new": "Tipo_geo"},
                    {"old": "Tipologia", "new": "Tipologia"},
                    {"old": "Fase", "new": "Fase"},
                    {"old": "Verso", "new": "Verso"},
                    {"old": "Asimmetria", "new": "Asimmetria"},
                    {"old": "Num_Oss", "new": "Num_Oss"},
                    {"old": "Quota", "new": "Quota"},        
                    {"old": "Inclina", "new": "Inclinaz"},
                    {"old": "Immersio", "new": "Immersione"},
                    {"old": "Direzio", "new": "Direzione"}           
                ],
                "domains": [
                    {"field": "Tipo_g_txt", "source": "Tipo_geo", "domain": "d_19_tipo.dbf"},
                    {"field": "Tipol_txt", "source": "Tipologia", "domain": "d_tipologia.dbf"},
                    {"field": "Fase_txt", "source": "Fase", "domain": "d_fase.dbf"},
                    {"field": "Verso_txt", "source": "Verso", "domain": "d_verso.dbf"},
                    {"field": "Asimm_txt", "source": "Asimmetria", "domain": "d_asimmetria.dbf"}
                ],
                "output_name": "geologia_punti.shp",
                "keep_fields": ["Num_Oss", "Quota", "Foglio", "Tipo_g_txt", "Inclinaz", "Asimm_txt", 
                              "Fase_txt", "Immersione", "Verso_txt", "Direzione", "Tipol_txt"]
            },
            "ST021Polyline": {
                "search_patterns": ["ST021Polyline", "main.ST021Polyline"],
                "fields": [
                    {"old": "Tipo", "new": "Tipo_geo"},
                    {"old": "Tipologia", "new": "Tipologia"},
                    {"old": "Fase", "new": "Fase"},
                    {"old": "Direzio", "new": "Direzione"}
                ],
                "domains": [
                    {"field": "Tipo_g_txt", "source": "Tipo_geo", "domain": "d_st021.dbf"},
                    {"field": "Tipol_txt", "source": "Tipologia", "domain": "d_tipologia.dbf"},
                    {"field": "Fase_txt", "source": "Fase", "domain": "d_fase.dbf"}
                ],
                "output_name": "geologia_linee_pieghe.shp",
                "keep_fields": ["Foglio", "Fase_txt", "Affior_txt", "Tipo_g_txt", "Cont_txt", "Tipol_txt", "Direzione"],
                "special_processing": "geology_lines_pieghe"
            }
        }

    def handle_direzione_field_for_geology_polygons(self, shapefile):
            """
            Manage Direzione field for geologia_poligoni:
            - Copy 'Direzio' values to 'Direzione'
            - Remove 'Direzio'
            """
            arcpy.AddMessage("Handling Direzione field for geologia_poligoni...")
            
            try:
                # Ottieni i campi esistenti
                existing_fields = {f.name.upper(): f.name for f in arcpy.ListFields(shapefile)}
                
                # Trova i campi Direzio (case-insensitive)
                direzio_field = None
                for field_key, field_name in existing_fields.items():
                    if field_key in ["DIREZIO", "DIREZIONE"]:
                        if "DIREZIO" in field_key:
                            direzio_field = field_name
                            break
                
                if not direzio_field:
                    arcpy.AddMessage("'Direzio' not found in geologia_poligoni")
                    return
                
                # Add 'Direzione' if doesn't exist
                if "DIREZIONE" not in existing_fields:
                    # check anc copy 'Direzio' type
                    direzio_field_obj = None
                    for f in arcpy.ListFields(shapefile):
                        if f.name == direzio_field:
                            direzio_field_obj = f
                            break
                    
                    if direzio_field_obj:
                        field_length = getattr(direzio_field_obj, 'length', 50) if direzio_field_obj.type == 'String' else None
                        arcpy.AddField_management(shapefile, "Direzione", direzio_field_obj.type, field_length=field_length)
                        arcpy.AddMessage("Campo Direzione aggiunto")
                    else:
                        # Fallback: add as text
                        arcpy.AddField_management(shapefile, "Direzione", "TEXT", field_length=50)
                        arcpy.AddMessage("'Direzione' field created (type TEXT as default)")
                
                # Copy i values from 'Direzio' to 'Direzione'
                with arcpy.da.UpdateCursor(shapefile, [direzio_field, "Direzione"]) as cursor:
                    for row in cursor:
                        direzio_val = row[0]
                        direzione_val = direzio_val if direzio_val is not None else ""
                        cursor.updateRow([direzio_val, direzione_val])
                
                arcpy.AddMessage("Values copied from {} to Direzione".format(direzio_field))
                
                # Remove 'Direzio' field
                try:
                    arcpy.DeleteField_management(shapefile, [direzio_field])
                    arcpy.AddMessage("Field {} rremoved".format(direzio_field))
                except Exception as e:
                    arcpy.AddWarning("Impossible removing field {}: {}".format(direzio_field, str(e)))
            
            except Exception as e:
                arcpy.AddError("Error in modifing field 'Direzione': {}".format(str(e)))

    def standardize_field_names_and_order(self, shapefile_path):
        """
        Standardize field names and order according to specifications
        """
        shapefile_name = os.path.basename(shapefile_path)
        
        if shapefile_name not in self.field_standards:
            arcpy.AddMessage("Standardization not defined for {}".format(shapefile_name))
            return
        
        config = self.field_standards[shapefile_name]
        field_mappings_dict = config.get("field_mappings", {})
        target_order = config.get("field_order", [])
        
        # Remove OBJECTID from field order if present
        if "OBJECTID" in target_order:
            target_order.remove("OBJECTID")
        
        arcpy.AddMessage("Standardizing fields for {}...".format(shapefile_name))
        
        try:
            # Get existing field info (excluding OBJECTID)
            existing_fields_info = {f.name: f for f in arcpy.ListFields(shapefile_path) 
                                if f.type not in ['OID']}
            
            # Create temporary file for standardization
            workspace_dir = os.path.dirname(shapefile_path)
            temp_name = "temp_standardized_" + os.path.basename(shapefile_path)
            temp_path = os.path.join(workspace_dir, temp_name)
            
            # Remove temp file if exists
            if arcpy.Exists(temp_path):
                arcpy.Delete_management(temp_path)
            
            # Build field mappings for conversion (excluding OBJECTID)
            field_mappings = arcpy.FieldMappings()
            processed_fields = set()
            
            # First pass: add fields in desired order
            for target_field_name in target_order:
                # Skip system fields
                if target_field_name in ["FID", "Shape"]:
                    continue
                    
                # Find source field
                source_field_name = None
                
                # Check if there's an inverse mapping for this target field
                for old_name, new_name in field_mappings_dict.items():
                    if new_name == target_field_name and old_name in existing_fields_info:
                        source_field_name = old_name
                        break
                
                # If not found through mapping, search directly
                if not source_field_name and target_field_name in existing_fields_info:
                    source_field_name = target_field_name
                
                if source_field_name and source_field_name in existing_fields_info:
                    field_info = existing_fields_info[source_field_name]
                    
                    # Create field mapping
                    field_map = arcpy.FieldMap()
                    field_map.addInputField(shapefile_path, source_field_name)
                    
                    # Configure output field with desired name
                    out_field = field_map.outputField
                    out_field.name = target_field_name
                    out_field.aliasName = target_field_name
                    field_map.outputField = out_field
                    
                    field_mappings.addFieldMap(field_map)
                    processed_fields.add(source_field_name)
                    
                    arcpy.AddMessage("  Mapping: {} -> {}".format(source_field_name, target_field_name))
                
                else:
                    # Create empty field if doesn't exist
                    field_type, field_length = self._get_field_type_from_name(target_field_name)
                    
                    # Add field temporarily to original file
                    if target_field_name not in existing_fields_info:
                        arcpy.AddField_management(shapefile_path, target_field_name, field_type, field_length=field_length)
                        
                        # Update field info
                        existing_fields_info = {f.name: f for f in arcpy.ListFields(shapefile_path) 
                                            if f.type not in ['OID']}
                    
                    # Now add the mapping
                    if target_field_name in existing_fields_info:
                        field_map = arcpy.FieldMap()
                        field_map.addInputField(shapefile_path, target_field_name)
                        field_mappings.addFieldMap(field_map)
                        processed_fields.add(target_field_name)
                        arcpy.AddMessage("  Added new field: {}".format(target_field_name))
            
            # Second pass: add remaining fields (excluding OBJECTID)
            for field_name, field_info in existing_fields_info.items():
                if (field_name not in processed_fields and 
                    field_info.type not in ['OID', 'Geometry']):
                    
                    field_map = arcpy.FieldMap()
                    field_map.addInputField(shapefile_path, field_name)
                    field_mappings.addFieldMap(field_map)
                    processed_fields.add(field_name)
            
            # Execute conversion with field mappings
            if field_mappings.fieldCount > 0:
                arcpy.FeatureClassToFeatureClass_conversion(
                    shapefile_path, 
                    workspace_dir,
                    os.path.splitext(temp_name)[0],
                    field_mapping=field_mappings
                )
                
                # Replace original file
                arcpy.Delete_management(shapefile_path)
                arcpy.Rename_management(temp_path, shapefile_path)
                
                arcpy.AddMessage("Field standardization completed successfully for {}".format(shapefile_name))
                arcpy.AddMessage("="*60)
            
        except Exception as e:
            # Cleanup on error
            if arcpy.Exists(temp_path):
                try:
                    arcpy.Delete_management(temp_path)
                except:
                    pass
            
            arcpy.AddWarning("  Field standardization failed for {}: {}".format(shapefile_name, str(e)))

    def _get_field_type_from_name(self, field_name):
        """Determine appropriate field type based on field name"""
        numeric_patterns = ["Area", "Perimeter", "Length", "Quota", "Inclinaz", "Immersione", "Direzione"]
        integer_patterns = ["Num_", "FID"]
        
        field_name_upper = field_name.upper()
        
        # Numeric fields (double)
        for pattern in numeric_patterns:
            if pattern.upper() in field_name_upper:
                return "DOUBLE", None
        
        # Integer fields
        for pattern in integer_patterns:
            if pattern.upper() in field_name_upper:
                return "LONG", None
        
        # Default: text field
        return "TEXT", 255

    def validate_inputs(self):
        """Validate input parameters with enhanced checks"""
        if not self.input_gpkg:
            raise ValueError("Input GeoPackage path is required")
            
        if not os.path.exists(self.input_gpkg):
            raise ValueError("Input GeoPackage path does not exist: {}".format(self.input_gpkg))
        
        if not self.input_gpkg.lower().endswith('.gpkg'):
            raise ValueError("Input file must be a GeoPackage (.gpkg)")
        
        # Extract foglio value from geopackage
        self.get_foglio_from_geopackage()

        if not self.foglio or not self.foglio.strip():
            raise ValueError("FoglioGeologico field is empty or not found in geopackage")

    def cleanup_resources(self):
        """Minimal resource cleanup"""
        try:
            arcpy.env.workspace = ""
            arcpy.ClearWorkspaceCache_management()
        except Exception:
            pass

    def safe_remove_directory(self, directory_path, max_attempts=2):
        """Optimized directory removal with reduced attempts"""
        if not os.path.exists(directory_path):
            return True
        
        for attempt in range(max_attempts):
            try:
                self.cleanup_resources()
                
                # Remove read-only attributes recursively
                for root, dirs, files in os.walk(directory_path):
                    for file in files:
                        try:
                            file_path = os.path.join(root, file)
                            os.chmod(file_path, 0o777)
                        except:
                            pass
                
                shutil.rmtree(directory_path)
                arcpy.AddMessage("Successfully removed directory: {}".format(directory_path))
                return True
                
            except Exception as e:
                if attempt < max_attempts - 1:
                    arcpy.AddWarning("Attempt {} failed to remove {}: {}".format(
                        attempt + 1, directory_path, str(e)))
                else:
                    arcpy.AddError("Failed to remove directory after {} attempts: {}".format(
                        max_attempts, directory_path))
        
        return False

    def setup_workspace(self):
        """Setup workspace directories"""
        self.cleanup_resources()
        
        directories = [self.workspace_shape, self.workspace_output]
        
        for folder in directories:
            if not self.safe_remove_directory(folder):
                raise RuntimeError("Cannot proceed due to directory cleanup failure: {}".format(folder))
            
            try:
                os.makedirs(folder)
                arcpy.AddMessage("Created directory: {}".format(folder))
            except Exception as e:
                raise RuntimeError("Failed to create directory {}: {}".format(folder, str(e)))

    def get_available_layers(self):
        """Enhanced layer discovery with caching"""
        if self._available_layers is not None:
            return self._available_layers
            
        available_layers = set()
        
        try:
            # Set workspace and get feature classes
            original_workspace = arcpy.env.workspace
            arcpy.env.workspace = self.input_gpkg
            
            feature_classes = arcpy.ListFeatureClasses()
            if feature_classes:
                available_layers.update(feature_classes)
            
            # Get additional layers from describe
            try:
                desc = arcpy.Describe(self.input_gpkg)
                if hasattr(desc, 'children'):
                    for child in desc.children:
                        if hasattr(child, 'name') and hasattr(child, 'dataType'):
                            if child.dataType in ['FeatureClass', 'Table']:
                                available_layers.add(child.name)
            except Exception:
                pass
            
            # Restore workspace
            arcpy.env.workspace = original_workspace
            
        except Exception as e:
            arcpy.AddWarning("Error getting available layers: {}".format(str(e)))
        
        self._available_layers = list(available_layers)
        return self._available_layers

    def find_layer_by_pattern(self, available_layers, patterns):
        """Optimized pattern matching with case-insensitive search"""
        # Create a lookup dictionary for faster searching
        layer_lookup = {layer.lower(): layer for layer in available_layers}
        
        for pattern in patterns:
            pattern_lower = pattern.lower()
            
            # Exact match first (fastest)
            if pattern_lower in layer_lookup:
                return layer_lookup[pattern_lower]
            
            # Partial match
            for layer_key, layer_name in layer_lookup.items():
                if pattern_lower in layer_key:
                    return layer_name
        
        return None

    def sanitize_shapefile_name(self, name):
        """Enhanced shapefile name sanitization"""
        # Remove or replace invalid characters
        name = re.sub(r'[^a-zA-Z0-9_]', '_', str(name))
        
        # Ensure doesn't start with number
        if name and name[0].isdigit():
            name = "shp_" + name
        
        # Truncate to valid length
        return name[:31] if name else "unnamed"

    def load_domain_mappings(self, domain_file, code_field="CODE", desc_field_pattern="DESC", is_gpkg_table=False):
        """Optimized domain mapping loader with UTF-8 error handling"""
        if is_gpkg_table:
            domain_table_path = os.path.join(self.input_gpkg, domain_file)
        else:
            domain_table_path = os.path.join(self.domini_path, domain_file)
        
        if not arcpy.Exists(domain_table_path):
            arcpy.AddWarning("Domain table not found: {}".format(domain_table_path))
            return {}
        
        try:
            # Get field information efficiently
            fields = arcpy.ListFields(domain_table_path)
            field_names = [f.name for f in fields]
            
            # Find description field
            desc_field = None
            for field_name in field_names:
                if desc_field_pattern.upper() in field_name.upper():
                    desc_field = field_name
                    break
            
            if not desc_field:
                arcpy.AddWarning("Description field matching '{}' not found in {}".format(
                    desc_field_pattern, domain_file))
                return {}
            
            # Validate required fields exist
            if code_field not in field_names:
                arcpy.AddWarning("Code field '{}' not found in {}".format(code_field, domain_file))
                return {}
            
            # Build mapping dictionary efficiently with UTF-8 error handling
            code_map = {}
            cursor_fields = [code_field, desc_field]
            
            with arcpy.da.SearchCursor(domain_table_path, cursor_fields) as cursor:
                for row in cursor:
                    try:
                        code_val, desc_val = row
                        
                        if code_val is not None and desc_val is not None:
                            # Handle UTF-8 encoding issues
                            try:
                                desc_clean = str(desc_val).strip()
                            except UnicodeDecodeError:
                                # Try different encodings or replace problematic characters
                                try:
                                    if isinstance(desc_val, str):
                                        desc_clean = desc_val.decode('utf-8', errors='replace').strip()
                                    else:
                                        desc_clean = unicode(desc_val).encode('utf-8', errors='replace').decode('utf-8').strip()
                                except:
                                    desc_clean = str(desc_val).encode('ascii', errors='ignore').decode('ascii').strip()
                            
                            # Create multiple key mappings for different data types
                            keys_to_map = []
                            try:
                                keys_to_map.append(str(code_val).strip())
                            except UnicodeDecodeError:
                                keys_to_map.append(str(code_val).encode('ascii', errors='ignore').decode('ascii').strip())
                            
                            try:
                                # Try numeric conversions
                                if str(code_val).replace('.', '').replace('-', '').isdigit():
                                    float_val = float(str(code_val))
                                    int_val = int(float_val)
                                    keys_to_map.extend([code_val, float_val, int_val])
                            except (ValueError, TypeError, UnicodeDecodeError):
                                keys_to_map.append(code_val)
                            
                            # Map all variants to the same description
                            for key in keys_to_map:
                                code_map[key] = desc_clean
                                
                    except Exception as row_error:
                        # Skip problematic rows but continue processing
                        arcpy.AddWarning("Skipping problematic row in {}: {}".format(domain_file, str(row_error)))
                        continue
            
            unique_mappings = len(set(code_map.values()))
            arcpy.AddMessage("Loaded {} unique mappings from {}".format(unique_mappings, domain_file))
            return code_map
            
        except Exception as e:
            arcpy.AddWarning("Error reading domain {}: {}".format(domain_table_path, str(e)))
            return {}

    def apply_domain_mapping(self, shapefile, field_name, source_field, code_map):
        """Optimized domain mapping application with batch processing"""
        if not code_map:
            arcpy.AddWarning("No domain mappings available for {}".format(field_name))
            return
        
        # Create field if it doesn't exist
        existing_fields = {f.name.upper(): f.name for f in arcpy.ListFields(shapefile)}
        if field_name.upper() not in existing_fields:
            arcpy.AddField_management(shapefile, field_name, "TEXT", field_length=255)
        
        # Apply mapping with optimized cursor
        stats = {"mapped": 0, "total": 0, "unmapped_samples": set()}
        
        try:
            with arcpy.da.UpdateCursor(shapefile, [source_field, field_name]) as cursor:
                for row in cursor:
                    src_val, _ = row
                    stats["total"] += 1
                    mapped_value = ""
                    
                    if src_val is not None:
                        # Try direct lookup first (fastest)
                        if src_val in code_map:
                            mapped_value = code_map[src_val]
                            stats["mapped"] += 1
                        else:
                            # Try string conversion
                            str_val = str(src_val).strip()
                            if str_val in code_map:
                                mapped_value = code_map[str_val]
                                stats["mapped"] += 1
                            else:
                                # Try numeric conversions
                                try:
                                    float_val = float(str_val)
                                    if float_val in code_map:
                                        mapped_value = code_map[float_val]
                                        stats["mapped"] += 1
                                    elif int(float_val) in code_map:
                                        mapped_value = code_map[int(float_val)]
                                        stats["mapped"] += 1
                                    else:
                                        self._add_unmapped_sample(stats["unmapped_samples"], src_val)
                                except (ValueError, TypeError):
                                    self._add_unmapped_sample(stats["unmapped_samples"], src_val)
                    
                    cursor.updateRow([src_val, mapped_value])
            
            # Report results
            success_rate = (stats["mapped"] / stats["total"] * 100) if stats["total"] > 0 else 0
            arcpy.AddMessage("Mapping '{}': {}/{} ({:.1f}% success)".format(
                field_name, stats["mapped"], stats["total"], success_rate))
            
            if stats["unmapped_samples"]:
                sample_str = ", ".join(list(stats["unmapped_samples"])[:5])
                arcpy.AddMessage("  Sample unmapped values: {}".format(sample_str))
                
        except Exception as e:
            arcpy.AddError("Error applying domain mapping for {}: {}".format(field_name, str(e)))

    def _add_unmapped_sample(self, unmapped_samples, src_val):
        """Helper to add unmapped sample with limit"""
        if len(unmapped_samples) < 5:
            unmapped_samples.add("'{}' ({})".format(src_val, type(src_val).__name__))

    def process_field_mapping(self, shapefile, field_config, existing_fields_dict):
        """Optimized field mapping with enhanced source field detection"""
        old_name = field_config.get("old")
        new_name = field_config.get("new", old_name)
        source_fields = field_config.get("sources", [old_name] if old_name else [])
        
        # Find the first available source field (case-insensitive)
        found_source = None
        for source in source_fields:
            source_upper = source.upper()
            if source_upper in existing_fields_dict:
                found_source = existing_fields_dict[source_upper]
                break
        
        if not found_source:
            arcpy.AddWarning("No source field found for '{}' in: {}".format(new_name, source_fields))
            return False
        
        # Handle field mapping efficiently
        if new_name.upper() != found_source.upper():
            try:
                # Get source field properties
                source_field_obj = None
                for f in arcpy.ListFields(shapefile):
                    if f.name.upper() == found_source.upper():
                        source_field_obj = f
                        break
                
                if not source_field_obj:
                    arcpy.AddWarning("Source field object not found for {}".format(found_source))
                    return False
                
                # Add new field with appropriate properties
                field_length = getattr(source_field_obj, 'length', 50)
                arcpy.AddField_management(shapefile, new_name, source_field_obj.type, field_length=field_length)
                
                # Copy values using field calculator
                expression = "!{}!".format(found_source)
                arcpy.CalculateField_management(shapefile, new_name, expression, "PYTHON_9.3")
                
                arcpy.AddMessage("Mapped {} -> {}".format(found_source, new_name))
                
            except Exception as e:
                arcpy.AddWarning("Error mapping field {} -> {}: {}".format(found_source, new_name, str(e)))
                return False
        else:
            arcpy.AddMessage("Field {} already exists with correct name".format(new_name))
        
        return True

    def process_sommerso_field_optimized(self, output_shapefile_ETRF):
        """Optimized processing of SOMMERSO field with batch operations"""
        arcpy.AddMessage("Processing SOMMERSO field...")
        
        # Get existing fields efficiently
        existing_fields = {f.name.upper(): f.name for f in arcpy.ListFields(output_shapefile_ETRF)}
        
        # Find SOMMERSO field (case-insensitive)
        sommerso_field = None
        for field_key, field_name in existing_fields.items():
            if "SOMMERSO" in field_key and "SOMMERSO_" not in field_key:
                sommerso_field = field_name
                break
        
        if not sommerso_field:
            arcpy.AddWarning("SOMMERSO field not found in shapefile")
            # Add empty field
            arcpy.AddField_management(output_shapefile_ETRF, "Sommerso_", "TEXT", field_length=10)
            return
        
        # Add Sommerso_ field if it doesn't exist
        if "SOMMERSO_" not in existing_fields:
            arcpy.AddField_management(output_shapefile_ETRF, "Sommerso_", "TEXT", field_length=10)
        
        # Batch process values
        try:
            with arcpy.da.UpdateCursor(output_shapefile_ETRF, [sommerso_field, "Sommerso_"]) as cursor:
                for row in cursor:
                    sommerso_val = row[0]
                    
                    # Convert to string and check value
                    if sommerso_val in [1, "1", "1.0"]:
                        new_val = "SI"
                    elif sommerso_val in [2, "2", "2.0"]:
                        new_val = "NO"
                    else:
                        new_val = ""
                    
                    cursor.updateRow([sommerso_val, new_val])
                    
            arcpy.AddMessage("SOMMERSO field processed successfully")
            
        except Exception as e:
            arcpy.AddWarning("Error processing SOMMERSO field: {}".format(str(e)))

    def process_geology_lines_standard(self, shapefile):
        """
        Processing for geologia_linee (ST018Polyline):
        - Add field 'Fase_txt' with value "non applicabile/non classificabile"
        """
        arcpy.AddMessage("Processing geologia_linee with special handling...")
        
        try:
            # Aggiungi campo Fase_txt se non esiste
            existing_fields = {f.name.upper(): f.name for f in arcpy.ListFields(shapefile)}
            
            if "FASE_TXT" not in existing_fields:
                arcpy.AddField_management(shapefile, "Fase_txt", "TEXT", field_length=255)
                arcpy.AddMessage("'Fase_txt' created")
            
            # Imposta tutti i valori di Fase_txt a "non applicabile"
            with arcpy.da.UpdateCursor(shapefile, ["Fase_txt"]) as cursor:
                for row in cursor:
                    cursor.updateRow(["non applicabile/non classificabile"])
            
            arcpy.AddMessage("'Fase_txt' compiled with 'non applicabile/non classificabile'")
            return True
            
        except Exception as e:
            arcpy.AddError("Error in processing geologia_linee: {}".format(str(e)))
            return False
        
    def process_geology_lines_pieghe(self, shapefile):
        """
        Processing for geologia_linee_pieghe (ST021Polyline):
        - same fields as geologia_linee.shp
        - Affiora: "non applicabile" 
        - Contorno: "no"
        - Other fields mapped trough domains
        """
        arcpy.AddMessage("Processing geologia_linee_pieghe with standardized fields...")
        
        try:
            # Obtain existing foelds
            existing_fields = {f.name.upper(): f.name for f in arcpy.ListFields(shapefile)}
            existing_field_names_upper = [f.upper() for f in existing_fields.values()]
            
            # Add required fields
            fields_to_add = {
                "Affior_txt": ("TEXT", 255),
                "Cont_txt": ("TEXT", 255)
            }
            
            for field_name, (field_type, length) in fields_to_add.items():
                if field_name.upper() not in existing_fields:
                    arcpy.AddField_management(shapefile, field_name, field_type, field_length=length)
                    arcpy.AddMessage("Campo {} aggiunto".format(field_name))
                    existing_fields = {f.name.upper(): f.name for f in arcpy.ListFields(shapefile)}
                    existing_field_names_upper = [f.upper() for f in existing_fields.values()]
            
            # set and populate fixed-string fields
            update_fields = []
            field_values = []
            
            if "AFFIOR_TXT" in existing_field_names_upper:
                update_fields.append("Affior_txt")
                field_values.append("non applicabile")
            
            if "CONT_TXT" in existing_field_names_upper:
                update_fields.append("Cont_txt") 
                field_values.append("no")
            
            # update fields if required
            if update_fields:
                with arcpy.da.UpdateCursor(shapefile, update_fields) as cursor:
                    for row in cursor:
                        cursor.updateRow(field_values)
                
                arcpy.AddMessage("Field compiled: Affior_txt = 'non applicabile', Cont_txt = 'no'")
            else:
                arcpy.AddMessage("Field compilation not required")
            
            arcpy.AddMessage("geologia_linee_pieghe processing OK")
            return True
            
        except Exception as e:
            arcpy.AddError("geologia_linee_pieghe processing ERROR: {}".format(str(e)))
            import traceback
            arcpy.AddError(traceback.format_exc())
            return False

    def process_geology_polygons_optimized(self, output_shapefile_ETRF):
        """Highly optimized processing for ST018Polygon with batch operations and caching"""
        arcpy.AddMessage("Starting optimized processing for geologia_poligoni.shp...")

        # Define auxiliary table paths
        auxiliary_tables = {
            "t1000": os.path.join(self.input_gpkg, "main.T0180801000"),
            "t2000": os.path.join(self.input_gpkg, "main.T0180802000"),
            "t3000": os.path.join(self.input_gpkg, "main.T0180803000")
        }

        # Verify all auxiliary tables exist
        missing_tables = [name for name, path in auxiliary_tables.items() if not arcpy.Exists(path)]
        if missing_tables:
            arcpy.AddError("Missing auxiliary tables: {}".format(missing_tables))
            return False

        # Add required fields in batch
        self._add_geology_fields(output_shapefile_ETRF)

        # Process SOMMERSO field directly from shapefile
        self.process_sommerso_field_optimized(output_shapefile_ETRF)

        # DEBUG: Analizza i problemi UTF-8 prima del processing
        self.debug_utf8_issues_in_auxiliary_tables()

        # Process auxiliary tables with caching
        table_data = self.create_safe_auxiliary_table_loader()
        
        # Load domain mappings once and cache
        domain_mappings = self._load_geology_domain_mappings()

        # Apply all mappings in batch operations
        self._apply_geology_mappings_batch(output_shapefile_ETRF, table_data, domain_mappings)

        # Clean up fields
        self._cleanup_geology_fields(output_shapefile_ETRF)
        self.handle_direzione_field_for_geology_polygons(output_shapefile_ETRF)

        arcpy.AddMessage("Optimized processing for geologia_poligoni.shp completed.")
        return True

    def _add_geology_fields(self, shapefile):
        """Add all required fields for geology polygons in batch"""
        fields_to_add = {
            "Tipo_UQ": ("TEXT", 255), "TempTIPO": ("TEXT", 50), 
            "Stato_UQ": ("TEXT", 255), "TempSTATO": ("TEXT", 50),
            "ETA_super": ("TEXT", 255), "TMP_super": ("TEXT", 50), 
            "ETA_infer": ("TEXT", 255), "TMP_infer": ("TEXT", 50),
            "tipo_ug": ("TEXT", 255), "Temp_UnGeo": ("TEXT", 50), 
            "Tessitura": ("TEXT", 255), "TempTESS": ("TEXT", 50),
            "Sommerso_": ("TEXT", 10), "Sigla1": ("TEXT", 255), 
            "Sigla_ug": ("TEXT", 255), "Nome": ("TEXT", 255), 
            "Legenda": ("TEXT", 255)
        }
        
        existing_fields = {f.name for f in arcpy.ListFields(shapefile)}
        
        for field_name, (field_type, length) in fields_to_add.items():
            if field_name not in existing_fields:
                arcpy.AddField_management(shapefile, field_name, field_type, field_length=length)

    def debug_utf8_issues_in_auxiliary_tables(self):
        """
        Funzione di debug per identificare record problematici nelle tabelle ausiliarie
        VERSIONE CORRETTA con nomi campi reali
        """
        arcpy.AddMessage("=== DEBUG UTF-8 ISSUES ===")
        
        auxiliary_tables = {
            "T0180801000": {
                "path": os.path.join(self.input_gpkg, "main.T0180801000"),
                "fields": ["OBJECTID", "Uq_Car", "Tipo", "Stato"]
            },
            "T0180802000": {
                "path": os.path.join(self.input_gpkg, "main.T0180802000"), 
                "fields": ["OBJECTID", "Uc_Lege", "Eta_Sup", "Eta_Inf", "S1_Tipo", "Sigla1", "Sigla_Carta", "Nome", "Legenda"]
            },
            "T0180803000": {
                "path": os.path.join(self.input_gpkg, "main.T0180803000"),
                "fields": ["OBJECTID", "Id_Tess", "Tessitura"]
            }
        }
        
        for table_name, config in auxiliary_tables.items():
            table_path = config["path"]
            fields = config["fields"]
            
            if not arcpy.Exists(table_path):
                arcpy.AddMessage("Table {} not found, skipping...".format(table_name))
                continue
                
            arcpy.AddMessage("\n--- Debugging table: {} ---".format(table_name))
            
            try:
                # First test: count number of records
                total_count = arcpy.GetCount_management(table_path)
                arcpy.AddMessage("Total records in {}: {}".format(table_name, str(total_count)))
                
                # Obtain available records
                available_fields = [f.name for f in arcpy.ListFields(table_path)]
                test_fields = [f for f in fields if f in available_fields]
                
                arcpy.AddMessage("Available fields: {}".format(available_fields))
                arcpy.AddMessage("Testing fields: {}".format(test_fields))
                
                # Test all fields
                problematic_records = []
                processed_count = 0
                
                with arcpy.da.SearchCursor(table_path, test_fields) as cursor:
                    for row in cursor:
                        processed_count += 1
                        record_id = row[0] if len(row) > 0 else processed_count
                        
                        # Test fields for records
                        for i, field_name in enumerate(test_fields):
                            if i < len(row):
                                field_value = row[i]
                                
                                try:
                                    # Test conversion to string with UTF-8 encoding
                                    if field_value is not None:
                                        str_value = self.safe_string_conversion(field_value)
                                        
                                        # Test encoding/decoding
                                        try:
                                            str_value.encode('utf-8').decode('utf-8')
                                        except UnicodeDecodeError as encode_error:
                                            problematic_records.append({
                                                "record_id": record_id,
                                                "field": field_name,
                                                "error": "UTF-8 encode/decode error: {}".format(str(encode_error)),
                                                "value_type": type(field_value).__name__,
                                                "value_preview": repr(str_value)[:100]
                                            })
                                            continue
                                        
                                except Exception as field_error:
                                    problematic_records.append({
                                        "record_id": record_id,
                                        "field": field_name,
                                        "error": "General error: {}".format(str(field_error)),
                                        "value_type": type(field_value).__name__,
                                        "value_preview": "Could not preview"
                                    })
                        
                        # Progress report each 50 record
                        if processed_count % 50 == 0:
                            arcpy.AddMessage("Processed {} records...".format(processed_count))
                            
                arcpy.AddMessage("Completed processing {} records from {}".format(processed_count, table_name))
                
                # Report record with problems
                if problematic_records:
                    arcpy.AddMessage("\n*** PROBLEMATIC RECORDS FOUND IN {} ***".format(table_name))
                    for i, record in enumerate(problematic_records):
                        arcpy.AddMessage("Problem #{}: Record ID/FID: {}, Field: '{}', Error: {}".format(
                            i+1, record["record_id"], record["field"], record["error"]))
                        arcpy.AddMessage("  Value type: {}, Preview: {}".format(
                            record["value_type"], record["value_preview"]))
                        if i >= 10:  # List first 10 problems
                            arcpy.AddMessage("  ... and {} more problems".format(len(problematic_records) - 10))
                            break
                else:
                    arcpy.AddMessage("No UTF-8 problems found in {}".format(table_name))
                    
            except Exception as table_error:
                arcpy.AddError("Error debugging table {}: {}".format(table_name, str(table_error)))
                import traceback
                arcpy.AddError(traceback.format_exc())
        
        arcpy.AddMessage("=== END DEBUG UTF-8 ISSUES ===\n")

    def create_safe_auxiliary_table_loader(self):
        """
        load auxiliary tables with correct fields name
        """
        arcpy.AddMessage("Loading auxiliary table data with enhanced UTF-8 handling...")
        
        auxiliary_tables = {
            "t1000": os.path.join(self.input_gpkg, "main.T0180801000"),
            "t2000": os.path.join(self.input_gpkg, "main.T0180802000"),
            "t3000": os.path.join(self.input_gpkg, "main.T0180803000")
        }
        
        table_data = {}
        
        # Tables field names configuration
        table_configs = {
            "t1000": {
                "fields": ["Uq_Car", "Tipo", "Stato"],
                "key_field": "Uq_Car"
            },
            "t2000": {
                "fields": ["Uc_Lege", "Eta_Sup", "Eta_Inf", "S1_Tipo", "Sigla1", "Sigla_Carta", "Nome", "Legenda"],
                "key_field": "Uc_Lege"
            },
            "t3000": {
                "fields": ["Id_Tess", "Tessitura"],
                "key_field": "Id_Tess"
            }
        }
        
        for table_key, table_path in auxiliary_tables.items():
            if not arcpy.Exists(table_path):
                arcpy.AddWarning("Table {} not found".format(table_key))
                table_data[table_key] = {}
                continue
                
            arcpy.AddMessage("Loading {} with safe UTF-8 handling...".format(table_key))
            table_data[table_key] = {}
            config = table_configs[table_key]
            
            processed_count = 0
            error_count = 0
            
            try:
                with arcpy.da.SearchCursor(table_path, ["OBJECTID"] + config["fields"]) as cursor:
                    for row in cursor:
                        processed_count += 1
                        record_id = row[0]
                        
                        try:
                            # Safe convertion of values
                            safe_values = {}
                            
                            for i, field_name in enumerate(config["fields"]):
                                field_index = i + 1  # +1 perchÃ© abbiamo OBJECTID come primo campo
                                if field_index < len(row):
                                    raw_value = row[field_index]
                                    safe_value = self.safe_string_conversion(raw_value)
                                    safe_values[field_name] = safe_value
                            
                            # Use key field for indexing
                            key_value = safe_values.get(config["key_field"], "")
                            if key_value:
                                if table_key == "t1000":
                                    table_data[table_key][key_value] = {
                                        "TIPO": safe_values.get("Tipo", ""),
                                        "STATO": safe_values.get("Stato", "")
                                    }
                                elif table_key == "t2000":
                                    table_data[table_key][key_value] = {
                                        "ETA_SUP": safe_values.get("Eta_Sup", ""),
                                        "ETA_INF": safe_values.get("Eta_Inf", ""),
                                        "S1_TIPO": safe_values.get("S1_Tipo", ""),
                                        "SIGLA1": safe_values.get("Sigla1", ""),
                                        "SIGLA_CARTA": safe_values.get("Sigla_Carta", ""),
                                        "NOME": safe_values.get("Nome", ""),
                                        "LEGENDA": safe_values.get("Legenda", "")
                                    }
                                elif table_key == "t3000":
                                    table_data[table_key][key_value] = safe_values.get("Tessitura", "")
                            
                        except Exception as row_error:
                            error_count += 1
                            arcpy.AddWarning("Error processing record {} in {}: {}".format(
                                record_id, table_key, str(row_error)))
                            continue
            
            except Exception as table_error:
                arcpy.AddError("Error processing table {}: {}".format(table_key, str(table_error)))
                table_data[table_key] = {}
            
            arcpy.AddMessage("Loaded {} records from {} ({} errors)".format(
                processed_count - error_count, table_key, error_count))
        
        return table_data

    def _load_geology_domain_mappings(self):
        """Load all geology-related domain mappings at once"""
        domain_files = {
            "tipo": "d_1000_tipo.dbf",
            "stato": "d_stato.dbf",
            "eta": "d_t2000_eta.dbf",
            "sigla_tipo": "d_2000_SiglaTipo.dbf",
            "tessitura": "d_t3000.dbf"
        }
        
        domain_mappings = {}
        for key, filename in domain_files.items():
            domain_mappings[key] = self.load_domain_mappings(filename, is_gpkg_table=False)
            
        return domain_mappings

    def _apply_geology_mappings_batch(self, shapefile, table_data, domain_mappings):
        """Apply all geology mappings in optimized batch operations"""
        
        # Process T1000 mappings (UQ_CAR -> TIPO, STATO)
        arcpy.AddMessage("Applying T1000 mappings...")
        cursor_fields = ["UQ_CAR", "TempTIPO", "Tipo_UQ", "TempSTATO", "Stato_UQ"]
        with arcpy.da.UpdateCursor(shapefile, cursor_fields) as cursor:
            for row in cursor:
                uq_car_val = str(row[0]).strip() if row[0] is not None else ""
                data = table_data["t1000"].get(uq_car_val, {})
                
                # Set temp values and domain-mapped values
                temp_tipo = data.get("TIPO", "")
                temp_stato = data.get("STATO", "")
                
                row[1] = temp_tipo  # TempTIPO
                row[2] = domain_mappings["tipo"].get(temp_tipo, "")  # Tipo_UQ
                row[3] = temp_stato  # TempSTATO
                row[4] = domain_mappings["stato"].get(temp_stato, "")  # Stato_UQ
                
                cursor.updateRow(row)

        # Process T2000 mappings (UC_LEGE -> multiple fields)
        arcpy.AddMessage("Applying T2000 mappings...")
        cursor_fields = ["UC_LEGE", "TMP_super", "ETA_super", "TMP_infer", "ETA_infer", 
                        "Temp_UnGeo", "tipo_ug", "Sigla1", "Sigla_ug", "Nome", "Legenda"]
        with arcpy.da.UpdateCursor(shapefile, cursor_fields) as cursor:
            for row in cursor:
                uc_lege_val = str(row[0]).strip() if row[0] is not None else ""
                data = table_data["t2000"].get(uc_lege_val, {})

                # Map all fields at once
                row[1] = data.get("ETA_SUP", "")  # TMP_super
                row[2] = domain_mappings["eta"].get(data.get("ETA_SUP", ""), "")  # ETA_super
                row[3] = data.get("ETA_INF", "")  # TMP_infer
                row[4] = domain_mappings["eta"].get(data.get("ETA_INF", ""), "")  # ETA_infer
                row[5] = data.get("S1_TIPO", "")  # Temp_UnGeo
                row[6] = domain_mappings["sigla_tipo"].get(data.get("S1_TIPO", ""), "")  # tipo_ug
                row[7] = data.get("SIGLA1", "")  # Sigla1
                row[8] = data.get("SIGLA_CARTA", "")  # Sigla_ug
                row[9] = data.get("NOME", "")  # Nome
                row[10] = data.get("LEGENDA", "")  # Legenda
                
                cursor.updateRow(row)

        # Process T3000 mappings (ID_TESS -> Tessitura)
        arcpy.AddMessage("Applying T3000 mappings...")
        cursor_fields = ["ID_TESS", "TempTESS", "Tessitura"]
        with arcpy.da.UpdateCursor(shapefile, cursor_fields) as cursor:
            for row in cursor:
                id_tess_val = str(row[0]).strip() if row[0] is not None else ""
                tessitura_val = table_data["t3000"].get(id_tess_val, "")
                
                row[1] = tessitura_val  # TempTESS
                row[2] = domain_mappings["tessitura"].get(tessitura_val, "")  # Tessitura
                
                cursor.updateRow(row)

    def _cleanup_geology_fields(self, shapefile):
        """Clean up temporary fields used in geology processing"""
        temp_fields = [
            "TIPO", "TIPOLOGIA", "ID_LIMITE", "ID_ELEST", "CONTORNO", "AFFIORA", 
            "PUN_GMO", "STATO", "ST018_", "ST018_ID", "UQ_CAR", "ID_TESS", 
            "ID_AMB", "SOMMERSO", "TempTESS", "TempTIPO", "TempSTATO", 
            "TMP_super", "TMP_infer", "Temp_UnGeo"
        ]
        
        existing_fields = [f.name for f in arcpy.ListFields(shapefile)]
        fields_to_delete = [f for f in temp_fields if f in existing_fields]

        if fields_to_delete:
            try:
                arcpy.DeleteField_management(shapefile, fields_to_delete)
                arcpy.AddMessage("Deleted {} temporary fields".format(len(fields_to_delete)))
            except Exception as e:
                arcpy.AddWarning("Could not delete some temporary fields: {}".format(str(e)))

    def diagnose_geopackage_quality(self):
        """
        Check layers main.ST011Polygon and main.ST018Polygon,
        verify topology and save a report in a CSV file.
        Include Pol_Uc/Pol_Gmo field trough join over FEATURE_ID.
        """
        arcpy.AddMessage("="*60)
        arcpy.AddMessage("GEOPACKAGE QUALITY DIAGNOSIS")
        arcpy.AddMessage("="*60)

        # Layer target
        target_layers = ["main.ST011Polygon", "main.ST018Polygon"]
        available_layers = self.get_available_layers()

        # create CSV 
        report_csv = os.path.join(self.workspace, "F" + self.foglio + "_geometry_issues.csv")
        with codecs.open(report_csv, "w", "utf-8") as csvfile:
            writer = csv.writer(csvfile)
            # Remove "tmp" fields
            writer.writerow(["Pol_ID", "Layer", "Problem"])

            for layer_name in target_layers:
                if layer_name not in available_layers:
                    arcpy.AddWarning("Layer {} not found in GeoPackage".format(layer_name))
                    continue

                arcpy.AddMessage("\n--- Checking polygon layer: {} ---".format(layer_name))
                try:
                    # gpkg
                    layer_path = os.path.join(self.input_gpkg, layer_name)

                    # Export tmp files
                    temp_shp = os.path.join(self.workspace_shape, layer_name.replace(".", "_") + "_check.shp")
                    if arcpy.Exists(temp_shp):
                        arcpy.Delete_management(temp_shp)

                    arcpy.FeatureClassToFeatureClass_conversion(
                        layer_path, self.workspace_shape, os.path.basename(temp_shp))

                    # Table "tmp" of errors
                    result_table = os.path.join(self.workspace, "checkgeom_" + layer_name.replace(".", "_") + ".dbf")
                    if arcpy.Exists(result_table):
                        arcpy.Delete_management(result_table)

                    arcpy.CheckGeometry_management(temp_shp, result_table)

                    count = int(arcpy.GetCount_management(result_table)[0])
                    if count == 0:
                        arcpy.AddMessage("  ✓ Geometries OK")
                        continue
                    else:
                        arcpy.AddWarning("  ⚠ Found {} geometry problems".format(count))

                    # Read errors
                    fields = [f.name for f in arcpy.ListFields(result_table)]
                    fid_field = "FEATURE_ID" if "FEATURE_ID" in fields else (
                        "FID" if "FID" in fields else ("OID" if "OID" in fields else "OBJECTID"))
                    problem_field = "PROBLEM" if "PROBLEM" in fields else fields[1]
                    detail_field = None
                    for candidate in ["DESCRIPTION", "MESSAGE", "CHECK"]:
                        if candidate in fields:
                            detail_field = candidate
                            break

                    # Verify Pol_Uc or Pol_Gmo
                    temp_shp_fields = [f.name for f in arcpy.ListFields(temp_shp)]
                    pol_field = None
                    for field_name in temp_shp_fields:
                        if field_name.upper() in ["POL_UC", "POL_GMO"]:
                            pol_field = field_name
                            break
                    if not pol_field:
                        for field_name in temp_shp_fields:
                            if "POL" in field_name.upper() and ("UC" in field_name.upper() or "GMO" in field_name.upper()):
                                pol_field = field_name
                                break

                    if not pol_field:
                        arcpy.AddWarning("Fields Pol_Uc/Pol_Gmo not found in {}".format(layer_name))
                        pol_field = None

                    # Maps FEATURE_ID -> Pol_Uc/Pol_Gmo
                    pol_map = {}
                    if pol_field:
                        key_field = fid_field if fid_field in temp_shp_fields else "FID"
                        with arcpy.da.SearchCursor(temp_shp, [key_field, pol_field]) as pol_cursor:
                            for row in pol_cursor:
                                pol_map[row[0]] = row[1]

                    # Write CSV
                    cursor_fields = [fid_field, problem_field]
                    if detail_field:
                        cursor_fields.append(detail_field)

                    with arcpy.da.SearchCursor(result_table, cursor_fields) as cursor:
                        for row in cursor:
                            fid_val = row[0]
                            problem_val = row[1]
                            pol_val = pol_map.get(fid_val, "N/A") if pol_field else "N/A"

                            # Scrivi solo Pol_ID, Layer e Problem (senza tmp_shp_FID)
                            writer.writerow([pol_val, layer_name, problem_val])

                except Exception as e:
                    arcpy.AddWarning("Error checking {}: {}".format(layer_name, str(e)))

                finally:
                    # Pulizia shapefile temporaneo e tabella
                    try:
                        if arcpy.Exists(temp_shp):
                            arcpy.Delete_management(temp_shp)
                        if arcpy.Exists(result_table):
                            arcpy.Delete_management(result_table)
                    except Exception as cleanup_error:
                        arcpy.AddWarning("Problems while cleaning: {}".format(str(cleanup_error)))

        arcpy.AddMessage("="*60)
        arcpy.AddMessage("END GEOPACKAGE DIAGNOSIS")
        arcpy.AddMessage("Report saved in: {}".format(report_csv))
        arcpy.AddMessage("="*60)

    def process_feature_class_optimized(self, input_fc, fc_name, config):
        """Unified and optimized feature class processing with streamlined workflow"""
        arcpy.AddMessage("Processing {} -> {}...".format(fc_name, config["output_name"]))
        
        # Validate input
        if not arcpy.Exists(input_fc):
            arcpy.AddMessage("Feature class {} not found".format(fc_name))
            return False
        
        try:
            count = arcpy.GetCount_management(input_fc)
            arcpy.AddMessage("Processing {} with {} features".format(fc_name, str(count)))
        except Exception as e:
            arcpy.AddWarning("Could not get feature count for {}: {}".format(fc_name, str(e)))

        # Define file paths
        temp_files = self._get_temp_file_paths(fc_name)
        
        try:
            # Step 1: Convert to shapefile with projection
            self._convert_and_keep_projection(input_fc, temp_files)
            
            # Step 2: Process fields and domains
            special_processing = config.get("special_processing")
            
            if special_processing == "geology_polygons":
                if not self.process_geology_polygons_optimized(temp_files["shapefile"]):
                    raise RuntimeError("Geology polygon processing failed")
            elif special_processing == "geology_lines":
                # Prima applica il processing standard
                self._process_standard_fields_and_domains(temp_files["shapefile"], config)
                # Poi applica il processing speciale
                if not self.process_geology_lines_standard(temp_files["shapefile"]):
                    raise RuntimeError("Geology lines standard processing failed")
            elif special_processing == "geology_lines_pieghe":
                # Prima applica il processing standard (inclusi i domini)
                self._process_standard_fields_and_domains(temp_files["shapefile"], config)
                # Poi applica il processing speciale
                if not self.process_geology_lines_pieghe(temp_files["shapefile"]):
                    raise RuntimeError("Geology lines pieghe processing failed")
            else:
                self._process_standard_fields_and_domains(temp_files["shapefile"], config)
            
            # Step 3: Add Foglio field
            self._add_foglio_field(temp_files["shapefile"])
            
            # Step 4: Clean up fields based on configuration
            self._cleanup_output_fields(temp_files["shapefile"], config)
            
            # Step 5: Save final output
            final_output = os.path.join(self.workspace_output, config["output_name"])
            arcpy.FeatureClassToFeatureClass_conversion(
                temp_files["shapefile"], 
                self.workspace_output, 
                os.path.splitext(config["output_name"])[0]
            )
            
            # Step 6: Standardize field names and order (NUOVA FUNZIONE)
            self.standardize_field_names_and_order(final_output)
            
            arcpy.AddMessage("Successfully processed {} -> {}".format(fc_name, config["output_name"]))
            arcpy.AddMessage("="*60)
            return True
            
        except Exception as e:
            arcpy.AddError("Error processing {}: {}".format(fc_name, str(e)))
            return False
        
        finally:
            # Clean up temporary files
            self._cleanup_temp_files(temp_files)

    def _get_temp_file_paths(self, fc_name):
        """Generate temporary file paths for processing"""
        base_name = self.sanitize_shapefile_name(fc_name)
        return {
            "shapefile": os.path.join(self.workspace_shape, base_name + ".shp"),
        }

    def _convert_and_keep_projection(self, input_fc, temp_files):
        """Convert feature class to shapefile maintaining original projection"""
        # Clean existing files
        if arcpy.Exists(temp_files["shapefile"]):
            arcpy.Delete_management(temp_files["shapefile"])
        
        # Convert to shapefile mantenendo la proiezione originale
        base_name = os.path.splitext(os.path.basename(temp_files["shapefile"]))[0]
        arcpy.FeatureClassToFeatureClass_conversion(
            input_fc, self.workspace_shape, base_name + ".shp"
        )
        
        # Load and store reference system (SR from the first processed layer)
        if self.input_sr is None:
            desc = arcpy.Describe(temp_files["shapefile"])
            self.input_sr = desc.spatialReference
            arcpy.AddMessage("Using input projection: {}".format(self.input_sr.name))

    def _process_standard_fields_and_domains(self, shapefile, config):
        """Process standard field mappings and domain applications"""
        # Get existing fields for mapping
        existing_fields_dict = {f.name.upper(): f.name for f in arcpy.ListFields(shapefile)}
        
        # Process field mappings
        for field_config in config.get("fields", []):
            self.process_field_mapping(shapefile, field_config, existing_fields_dict)
        
        # Process domain mappings if domini folder exists
        if os.path.exists(self.domini_path):
            for domain_info in config.get("domains", []):
                code_map = self.load_domain_mappings(domain_info["domain"], is_gpkg_table=False)
                self.apply_domain_mapping(
                    shapefile, domain_info["field"], 
                    domain_info["source"], code_map
                )

    def _add_foglio_field(self, shapefile):
        """Add and populate Foglio field from geopackage FoglioGeologico"""
        existing_fields = {f.name.upper(): f.name for f in arcpy.ListFields(shapefile)}
        
        if "FOGLIO" not in existing_fields:
            arcpy.AddField_management(shapefile, "Foglio", "TEXT", field_length=50)
        
        # Use the foglio value extracted from geopackage
        arcpy.CalculateField_management(shapefile, "Foglio", "'{}'".format(self.foglio), "PYTHON_9.3")

    def _cleanup_output_fields(self, shapefile, config):
        """Clean up output fields based on configuration - versione sicura"""
        # Get fields to keep from configuration
        fields_to_keep = set(config.get("keep_fields", []))
        
        # Add system fields that should never be deleted
        system_fields = {"FID", "Shape"}
        fields_to_keep.update(system_fields)
        
        # Add "Foglio" if exist
        existing_field_names = [f.name for f in arcpy.ListFields(shapefile)]
        if "Foglio" in existing_field_names:
            fields_to_keep.add("Foglio")
        
        # Get current fields
        existing_fields = []
        for f in arcpy.ListFields(shapefile):
            if f.type not in ['OID', 'Geometry']:
                existing_fields.append(f.name)
        
        # Determine fields to delete
        fields_to_delete = [f for f in existing_fields if f not in fields_to_keep]
        
        # SECURITY CHECK: at least one attribute field
        remaining_data_fields = [f for f in existing_fields if f not in fields_to_delete and f not in system_fields]
        
        if len(remaining_data_fields) == 0:
            arcpy.AddWarning("Cannot delete all data fields - shapefile must have at least one attribute field")
            
            # Keep essential fields
            essential_fields = []
            
            # Search essential field with priority order 
            priority_fields = ["Foglio", "Tipo_g_txt", "Tipo_G_txt", "Tipol_txt", "Fase_txt", 
                            "Tipo_geo", "Tipologia", "Fase", "Label", "Num_Oss", "Pun_Gmo", 
                            "Lin_Gmo", "Pol_Gmo", "Num_Ris"]
            
            for priority_field in priority_fields:
                if priority_field in fields_to_delete:
                    essential_fields.append(priority_field)
                    fields_to_delete.remove(priority_field)
                    break
            
            if essential_fields:
                arcpy.AddMessage("Keeping essential field(s) to avoid empty shapefile: {}".format(essential_fields))
            else:
                # If not priority fields, keep the first available field
                if fields_to_delete:
                    keep_field = fields_to_delete[0]
                    fields_to_delete.remove(keep_field)
                    arcpy.AddMessage("Keeping field '{}' to avoid empty shapefile".format(keep_field))
        
        # Remove unnecessary fields
        if fields_to_delete:
            try:
                # Log deleting fields
                arcpy.AddMessage("Attempting to delete fields: {}".format(fields_to_delete))
                
                arcpy.DeleteField_management(shapefile, fields_to_delete)
                arcpy.AddMessage("Successfully cleaned up {} unused fields".format(len(fields_to_delete)))
                
            except Exception as e:
                arcpy.AddWarning("Could not delete some fields: {}".format(str(e)))
                
                arcpy.AddMessage("Attempting individual field deletion...")
                deleted_count = 0
                
                for field_name in fields_to_delete:
                    try:
                        arcpy.DeleteField_management(shapefile, [field_name])
                        deleted_count += 1
                    except Exception as field_error:
                        arcpy.AddWarning("Could not delete field '{}': {}".format(field_name, str(field_error)))
                
                if deleted_count > 0:
                    arcpy.AddMessage("Successfully deleted {} fields individually".format(deleted_count))
        else:
            arcpy.AddMessage("No fields to delete - all fields are marked as essential")

        # Check final shapefile
        final_fields = [f.name for f in arcpy.ListFields(shapefile) if f.type not in ['OID', 'Geometry']]
        arcpy.AddMessage("Final field count (excluding system fields): {}".format(len(final_fields)))
        
        if len(final_fields) == 0:
            arcpy.AddError("CRITICAL: Shapefile has no data fields remaining!")
            raise RuntimeError("Shapefile cleanup resulted in no data fields")
        
        arcpy.AddMessage("Remaining fields: {}".format(final_fields))

    def _cleanup_temp_files(self, temp_files):
        """Clean up temporary files"""
        for temp_file in temp_files.values():
            if temp_file and arcpy.Exists(temp_file):
                try:
                    arcpy.Delete_management(temp_file)
                except Exception:
                    pass

    def combine_geology_lines_optimized(self):
        """
        Append geologia_linee_pieghe to geologia_linee
        """
        arcpy.AddMessage("="*60)
        arcpy.AddMessage("APPENDING GEOLOGY LINES")
        arcpy.AddMessage("="*60)
        
        dest_file = os.path.join(self.workspace_output, "geologia_linee.shp")
        source_file = os.path.join(self.workspace_output, "geologia_linee_pieghe.shp")
        
        if not arcpy.Exists(dest_file):
            arcpy.AddWarning("Destination file not found: {}".format(dest_file))
            return
                
        if not arcpy.Exists(source_file):
            arcpy.AddWarning("Source file not found: {}".format(source_file))
            return
        
        try:
            # Verify that both files have the same structure
            dest_fields = [f.name for f in arcpy.ListFields(dest_file)]
            source_fields = [f.name for f in arcpy.ListFields(source_file)]
            
            arcpy.AddMessage("Destination fields: {}".format(dest_fields))
            arcpy.AddMessage("Source fields: {}".format(source_fields))
            
            # Count records
            dest_count_before = int(arcpy.GetCount_management(dest_file)[0])
            source_count = int(arcpy.GetCount_management(source_file)[0])
            
            arcpy.AddMessage("Features before append: {} (dest) + {} (source)".format(
                dest_count_before, source_count))
            
            # Append
            arcpy.AddMessage("Executing append operation...")
            arcpy.Append_management(source_file, dest_file, "NO_TEST")
            
            # Verify
            dest_count_after = int(arcpy.GetCount_management(dest_file)[0])
            arcpy.AddMessage("Features after append: {} (dest)".format(dest_count_after))
            
            if dest_count_after == dest_count_before + source_count:
                arcpy.AddMessage("✓ Append operation successful")
                
                # Eras source file
                arcpy.Delete_management(source_file)
                arcpy.AddMessage("✓ Source file deleted: geologia_linee_pieghe.shp")
            else:
                arcpy.AddWarning("⚠ Append operation may have issues")
                arcpy.AddWarning("Expected {} features, got {}".format(
                    dest_count_before + source_count, dest_count_after))
                
                # Hold source file in case of errors
                arcpy.AddMessage("Source file kept for manual inspection")
            
            # Standardize merged file
            self.standardize_field_names_and_order(dest_file)
            arcpy.AddMessage("✓ Final standardization applied")
            
        except Exception as e:
            arcpy.AddError("Failed to combine geology lines: {}".format(str(e)))
            import traceback
            arcpy.AddError(traceback.format_exc())

    def _add_compatible_field(self, shapefile, field_obj):
        """Add a field with compatible type and length"""
        field_type_map = {
            'String': 'TEXT',
            'Integer': 'LONG', 
            'SmallInteger': 'SHORT',
            'Double': 'DOUBLE',
            'Single': 'FLOAT'
        }
        
        arcpy_type = field_type_map.get(field_obj.type, 'TEXT')
        field_length = getattr(field_obj, 'length', 255) if arcpy_type == 'TEXT' else None
        
        arcpy.AddField_management(shapefile, field_obj.name, arcpy_type, field_length=field_length)

    def _set_default_values(self, shapefile, default_values):
        """Set default values for specified fields"""
        existing_fields = [f.name for f in arcpy.ListFields(shapefile)]
        update_fields = [field for field in default_values.keys() if field in existing_fields]
        
        if not update_fields:
            return
            
        with arcpy.da.UpdateCursor(shapefile, update_fields) as cursor:
            for row in cursor:
                new_row = []
                for i, field_name in enumerate(update_fields):
                    current_val = row[i] if i < len(row) else None
                    if current_val and str(current_val).strip():
                        new_row.append(current_val)
                    else:
                        new_row.append(default_values[field_name])
                cursor.updateRow(new_row)

    def process_all_optimized(self):
        """Main optimized processing function with enhanced error handling and performance"""
        start_time = time.time()
        
        try:
            # Setup workspace
            self.setup_workspace()

            # Validate inputs
            self.validate_inputs()
            self.diagnose_geopackage_quality()  # comment to skip gpkg quality check

            # Log the extracted foglio value
            arcpy.AddMessage("Extracted FoglioGeologico: {}".format(self.foglio))
            
            # Check domini folder
            if not os.path.exists(self.domini_path):
                arcpy.AddWarning("Domini folder not found - domain mappings will be skipped")
            
            # Get available layers with caching
            available_layers = self.get_available_layers()
            arcpy.AddMessage("Found {} layers in GeoPackage".format(len(available_layers)))
            arcpy.AddMessage("="*60)
            
            if not available_layers:
                raise RuntimeError("No layers found in input GeoPackage!")
            
            # Process all feature classes
            processed_count = 0
            failed_count = 0
            
            for fc_name, config in self.feature_configs.items():
                try:
                    # Find layer
                    found_layer = self.find_layer_by_pattern(available_layers, config["search_patterns"])
                    
                    if not found_layer:
                        arcpy.AddWarning("Layer not found for {}".format(fc_name))
                        failed_count += 1
                        continue
                    
                    # Build input path
                    input_fc = os.path.join(self.input_gpkg, found_layer)
                    
                    # Process the feature class
                    if self.process_feature_class_optimized(input_fc, fc_name, config):
                        processed_count += 1
                    else:
                        failed_count += 1
                        
                except Exception as e:
                    arcpy.AddError("Error processing {}: {}".format(fc_name, str(e)))
                    failed_count += 1
            
            # APPEND geology lines
            arcpy.AddMessage("="*60)
            arcpy.AddMessage("FINAL PROCESSING: COMBINING GEOLOGY LINES")
            arcpy.AddMessage("="*60)
            self.combine_geology_lines_optimized()
            
            # Apply field standardization to all remaining files
            self._standardize_all_output_files()
            
            # Final cleanup
            self.final_cleanup_optimized()
            
            # Report results
            processing_time = time.time() - start_time
            arcpy.AddMessage("Processing completed in {:.1f} seconds!".format(processing_time))
            arcpy.AddMessage("Successfully processed: {} | Failed: {}".format(processed_count, failed_count))
            
            # List output files
            self._report_output_files()
            
        except Exception as e:
            arcpy.AddError("Script failed: {}".format(str(e)))
            import traceback
            arcpy.AddError(traceback.format_exc())
            raise

    def _standardize_all_output_files(self):
        """Apply field standardization to all output files"""
        output_files = [f for f in os.listdir(self.workspace_output) if f.endswith('.shp')]
        
        for filename in output_files:
            filepath = os.path.join(self.workspace_output, filename)
            if arcpy.Exists(filepath):
                arcpy.AddMessage("Applying final standardization to {}...".format(filename))
                self.standardize_field_names_and_order(filepath)

    def _standardize_combined_files(self):
        """Apply field standardization to files that were combined"""
        combined_files = ["geologia_linee.shp"]
        
        for filename in combined_files:
            filepath = os.path.join(self.workspace_output, filename)
            if arcpy.Exists(filepath):
                arcpy.AddMessage("Applying post-combination field standardization to {}...".format(filename))
                self.standardize_field_names_and_order(filepath)

    def _report_output_files(self):
        """Report generated output files"""
        if os.path.exists(self.workspace_output):
            output_files = [f for f in os.listdir(self.workspace_output) if f.endswith('.shp')]
            if output_files:
                arcpy.AddMessage("Generated {} output files:".format(len(output_files)))
                for f in sorted(output_files):
                    try:
                        count = arcpy.GetCount_management(os.path.join(self.workspace_output, f))
                        arcpy.AddMessage("  {} ({} features)".format(f, str(count)))
                    except:
                        arcpy.AddMessage("  {}".format(f))
            
            if "geologia_linee_pieghe.shp" not in output_files:
                arcpy.AddMessage("  geologia_linee_pieghe.shp was merged into geologia_linee.shp")

    def final_cleanup_optimized(self):
        """Optimized final cleanup with batch operations"""
        try:
            # Remove unwanted files
            unwanted_files = ["st017_punti.shp"]
            for unwanted in unwanted_files:
                unwanted_path = os.path.join(self.workspace_output, unwanted)
                if arcpy.Exists(unwanted_path):
                    arcpy.Delete_management(unwanted_path)
            
            # Verify final outputs
            self._verify_final_outputs_optimized()
            
            # Clean up temporary directories
            temp_dirs = [self.workspace_shape]
            for temp_dir in temp_dirs:
                if os.path.exists(temp_dir):
                    self.safe_remove_directory(temp_dir)
            
            arcpy.AddMessage("Final cleanup completed")
            
        except Exception as e:
            arcpy.AddWarning("Error during final cleanup: {}".format(str(e)))

    def _verify_final_outputs_optimized(self):
        """Optimized verification of final output structure"""
        expected_files = {
            "geomorfologia_punti.shp": ["Pun_Gmo", "Foglio", "Tipo_Gmrf"],
            "geomorfologia_poligoni.shp": ["Pol_Gmo", "Foglio", "Tipo_Gmrf"],
            "geomorfologia_linee.shp": ["Lin_Gmo", "Foglio", "Tipo_Gmrf"],
            "risorse_prospezioni.shp": ["Num_Ris", "Foglio", "Tipo"],
            "geologia_linee.shp": ["Foglio", "Fase", "Affiora", "Tipo_Geo", "Contorno", "Tipologia", "Direzione"],
            "geologia_poligoni.shp": ["Pol_Uc", "Foglio", "Tipo_UQ"],
            "geologia_punti.shp": ["Num_Oss", "Foglio", "Tipo_Geo"]
        }
        
        arcpy.AddMessage("\n=== OUTPUT VERIFICATION ===")
        
        for filename, critical_fields in expected_files.items():
            shapefile_path = os.path.join(self.workspace_output, filename)
            
            if arcpy.Exists(shapefile_path):
                try:
                    count = arcpy.GetCount_management(shapefile_path)
                    actual_fields = [f.name for f in arcpy.ListFields(shapefile_path)]
                    missing_fields = [f for f in critical_fields if f not in actual_fields]
                    
                    if missing_fields:
                        arcpy.AddWarning("⚠ {} - Missing fields: {}".format(filename, missing_fields))
                    else:
                        arcpy.AddMessage("✓ {} - {} features, all critical fields present".format(
                            filename, str(count)))
                        
                except Exception as e:
                    arcpy.AddError("✗ {} - Verification error: {}".format(filename, str(e)))
            else:
                if filename != "geologia_linee_pieghe.shp":
                    arcpy.AddWarning("✗ {} - File not created".format(filename))
                else:
                    arcpy.AddMessage("✓ {} - Correctly merged into geologia_linee.shp".format(filename))
        
        arcpy.AddMessage("=== END VERIFICATION ===\n")


def main():
    """Main execution function with enhanced error handling"""
    try:
        # Get and validate parameters
        input_gpkg = arcpy.GetParameterAsText(0)
     
        if not input_gpkg or not input_gpkg.strip():
            arcpy.AddError("Input GeoPackage parameter is required")
            sys.exit(1)
        
        # Log start information
        arcpy.AddMessage("="*60)
        arcpy.AddMessage("CARG DATA CONVERSION - OPTIMIZED VERSION")
        arcpy.AddMessage("="*60)
        arcpy.AddMessage("Input GeoPackage: {}".format(input_gpkg))
        arcpy.AddMessage("Start time: {}".format(time.strftime("%Y-%m-%d %H:%M:%S")))
        arcpy.AddMessage("="*60)
        
        # Create processor and run
        processor = CARGProcessor(input_gpkg)
        processor.process_all_optimized()
        
        arcpy.AddMessage("="*60)
        arcpy.AddMessage("CARG DATA CONVERSION COMPLETED SUCCESSFULLY!")
        arcpy.AddMessage("End time: {}".format(time.strftime("%Y-%m-%d %H:%M:%S")))
        arcpy.AddMessage("="*60)
        
    except Exception as e:
        arcpy.AddError("="*60)
        arcpy.AddError("FATAL ERROR IN CARG CONVERSION")
        arcpy.AddError("="*60)
        arcpy.AddError("Error: {}".format(str(e)))
        
        # Add detailed traceback for debugging
        import traceback
        arcpy.AddError("Detailed traceback:")
        for line in traceback.format_exc().split('\n'):
            if line.strip():
                arcpy.AddError("  {}".format(line))
        
        arcpy.AddError("="*60)
        sys.exit(1)


if __name__ == "__main__":
    main()
    