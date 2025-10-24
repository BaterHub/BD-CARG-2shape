"""
CARG Data Conversion Script for ArcGIS Pro
Converts geological data from CARG coded GeoDB to shapefiles

   ██████╗ ██████╗ 
   ██╔══██╗██╔══██╗
   ██████╔╝██████╔╝
   ██╔═══╝ ██╔═══╝ 
   ██║     ██║     
   ╚═╝     ╚═╝     
 BDgeoDB2shape v2.0 - ArcGIS Pro

"""

import arcpy
import os
import shutil
import csv
import re
import sys
import time
from collections import defaultdict

# Configure ArcPy environment
arcpy.env.overwriteOutput = True
arcpy.env.parallelProcessingFactor = "100%"

class CARGProcessor:
    """
    Optimized CARG data processor for converting GeoDB layers to shapefiles
    Compatible with ArcGIS Pro and Python 3
    """
    
    def __init__(self, input_gdb):
        self.input_gdb = input_gdb
        
        # Setup workspace paths
        self.input_dir = os.path.dirname(input_gdb)
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

    class DualLogger:
        """Logger che scrive sia su file che su stdout"""
        def __init__(self, original_stdout, file_handler):
            self.original_stdout = original_stdout
            self.file_handler = file_handler
        
        def write(self, message):
            if message.strip():  # Evita righe vuote
                self.original_stdout.write(message)
                self.file_handler.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {message}")
        
        def flush(self):
            self.original_stdout.flush()
            self.file_handler.flush()

    def setup_file_logging(self):
        """Setup dual logging to file and arcpy"""
        self.log_file = os.path.join(self.workspace, f"F{self.foglio}_processing.log")
        
        # Crea un handler per il file
        self.file_handler = open(self.log_file, 'w', encoding='utf-8')
        
        # Redirigi stdout per catturare anche i print
        self.original_stdout = sys.stdout
        sys.stdout = self.DualLogger(self.original_stdout, self.file_handler)

    def close_file_logging(self):
        """Close file logging"""
        if hasattr(self, 'file_handler') and self.file_handler:
            sys.stdout = self.original_stdout
            self.file_handler.close()

    def safe_string_conversion(self, value):
        """Robust string conversion function for Python 3"""
        if value is None:
            return ""
        
        try:
            return str(value).strip()
        except Exception:
            try:
                # Handle any remaining encoding issues
                return str(value).encode('utf-8', errors='replace').decode('utf-8').strip()
            except Exception:
                return "CONVERSION_ERROR"

    def get_foglio_from_geodatabase(self):
        """Extract FoglioGeologico value from any layer in the geodatabase"""
        available_layers_dict = self.get_available_layers()
        
        # Search through all datasets and layers
        for dataset_name, layer_list in available_layers_dict.items():
            if dataset_name == "tables":
                continue
                
            for layer_name in layer_list:
                try:
                    # Build complete layer path
                    if dataset_name == "root":
                        layer_path = os.path.join(self.input_gdb, layer_name)
                    else:
                        layer_path = os.path.join(self.input_gdb, dataset_name, layer_name)
                    
                    # Check if FoglioGeologico field exists
                    fields = [f.name.upper() for f in arcpy.ListFields(layer_path)]
                    if "FOGLIOGEOLOGICO" in fields:
                        # Get the first non-null value
                        with arcpy.da.SearchCursor(layer_path, ["FoglioGeologico"]) as cursor:
                            for row in cursor:
                                if row[0] is not None:
                                    self.foglio = str(row[0]).strip()
                                    arcpy.AddMessage("Found FoglioGeologico: {} in layer {}/{}".format(
                                        self.foglio, dataset_name, layer_name))
                                    return self.foglio
                except Exception as e:
                    arcpy.AddWarning("Error reading FoglioGeologico from {}/{}: {}".format(
                        dataset_name, layer_name, str(e)))
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
                "search_patterns": ["ST010Point"],
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
                "search_patterns": ["ST011Polygon"],
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
                "search_patterns": ["ST012Polyline"],
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
                "search_patterns": ["ST013Point"],
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
                "search_patterns": ["ST018Polyline"],
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
                "search_patterns": ["ST018Polygon"],
                "fields": [
                    {"old": "Pol_Uc", "new": "Pol_Uc"},
                    {"old": "Uc_Lege", "new": "Uc_Lege"},  
                    {"old": "Direzio", "new": "Direzione"}
                ],
                "domains": [],
                "output_name": "geologia_poligoni.shp",
                "special_processing": "geology_polygons",
                "keep_fields": ["Pol_Uc", "Uc_Lege", "Foglio", "Direzione", "Tipo_UQ", "Stato_UQ", 
                              "ETA_Super", "ETA_Infer", "Tipo_UG", "Tessitura", "Sigla1", "Sigla_UG", 
                              "Nome", "Legenda", "Sommerso_"]
            },
            "ST019Point": {
                "search_patterns": ["ST019Point"],
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
                "search_patterns": ["ST021Polyline"],
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

    def handle_direzione_field_for_geology_polygons_fixed(self, shapefile):
        """
        Manage Direzione field without overwriting with zeros - FIXED VERSION
        """
        arcpy.AddMessage("Handling Direzione field for geologia_poligoni...")
        
        try:
            # Get existing fields
            existing_fields = {f.name.upper(): f.name for f in arcpy.ListFields(shapefile)}
            
            # Find Direzio field (case-insensitive)
            direzio_field = None
            direzione_field = None
            
            for field_key, field_name in existing_fields.items():
                if field_key == "DIREZIO":
                    direzio_field = field_name
                elif field_key == "DIREZIONE":
                    direzione_field = field_name
            
            arcpy.AddMessage("Found fields - Direzio: {}, Direzione: {}".format(direzio_field, direzione_field))
            
            # If we already have Direzione field, we're done
            if direzione_field and not direzio_field:
                arcpy.AddMessage("Direzione field already exists and properly named")
                return
            
            # If we have both fields, remove Direzio
            if direzio_field and direzione_field:
                try:
                    arcpy.DeleteField_management(shapefile, [direzio_field])
                    arcpy.AddMessage("Removed duplicate field {}".format(direzio_field))
                    return
                except Exception as e:
                    arcpy.AddWarning("Could not remove duplicate field {}: {}".format(direzio_field, str(e)))
                    return
            
            # If we only have Direzio, rename it to Direzione
            if direzio_field and not direzione_field:
                try:
                    # First try to add the new field
                    field_type = "DOUBLE"  # Default type
                    field_length = None
                    
                    # Get field properties from Direzio
                    for f in arcpy.ListFields(shapefile):
                        if f.name == direzio_field:
                            if f.type == 'String':
                                field_type = "TEXT"
                                field_length = f.length
                            else:
                                field_type = "DOUBLE"
                            break
                    
                    # Add new Direzione field
                    arcpy.AddField_management(shapefile, "Direzione", field_type, field_length=field_length)
                    arcpy.AddMessage("Added Direzione field with type {}".format(field_type))
                    
                    # Copy values from Direzio to Direzione
                    copied_count = 0
                    null_count = 0
                    
                    with arcpy.da.UpdateCursor(shapefile, [direzio_field, "Direzione"]) as cursor:
                        for row in cursor:
                            direzio_val = row[0]
                            
                            if direzio_val is not None:
                                # Copy the original value
                                cursor.updateRow([direzio_val, direzio_val])
                                copied_count += 1
                            else:
                                # Set appropriate null value
                                if field_type == "TEXT":
                                    cursor.updateRow([direzio_val, ""])
                                else:
                                    cursor.updateRow([direzio_val, None])
                                null_count += 1
                    
                    arcpy.AddMessage("Copied {} values, {} were null".format(copied_count, null_count))
                    
                    # Remove original Direzio field
                    try:
                        arcpy.DeleteField_management(shapefile, [direzio_field])
                        arcpy.AddMessage("Removed original field {}".format(direzio_field))
                    except Exception as e:
                        arcpy.AddWarning("Could not remove field {}: {}".format(direzio_field, str(e)))
                    
                    return
                        
                except Exception as e:
                    arcpy.AddError("Error in Direzione field handling: {}".format(str(e)))
                    import traceback
                    arcpy.AddError(traceback.format_exc())
                    return
            
            # If no direction field found, create empty one
            elif not direzio_field and not direzione_field:
                arcpy.AddField_management(shapefile, "Direzione", "DOUBLE")
                arcpy.AddMessage("Created empty Direzione field")
            
        except Exception as e:
            arcpy.AddError("Error in Direzione field handling: {}".format(str(e)))
            import traceback
            arcpy.AddError(traceback.format_exc())

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
            # Helper to exclude computed geometry metrics and system-like fields
            def _is_excluded_field(name):
                n = name.upper()
                if n in ("OBJECTID",):
                    return True
                if n.startswith("SHAPE_") and n != "SHAPE":
                    return True
                return False

            # Get existing field info (excluding OBJECTID and excluded names)
            existing_fields_info = {}
            for f in arcpy.ListFields(shapefile_path):
                if f.type in ['OID']:
                    continue
                if _is_excluded_field(f.name):
                    continue
                existing_fields_info[f.name] = f
            
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
                        existing_fields_info = {}
                        for f in arcpy.ListFields(shapefile_path):
                            if f.type in ['OID']:
                                continue
                            if _is_excluded_field(f.name):
                                continue
                            existing_fields_info[f.name] = f
                    
                    # Now add the mapping
                    if target_field_name in existing_fields_info:
                        field_map = arcpy.FieldMap()
                        field_map.addInputField(shapefile_path, target_field_name)
                        field_mappings.addFieldMap(field_map)
                        processed_fields.add(target_field_name)
                        arcpy.AddMessage("  Added new field: {}".format(target_field_name))
            
            # Second pass: add remaining fields (excluding OBJECTID and excluded)
            for field_name, field_info in existing_fields_info.items():
                if (field_name not in processed_fields and 
                    field_info.type not in ['OID', 'Geometry']):
                    if _is_excluded_field(field_name):
                        continue
                    
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
        numeric_patterns = ["Area", "Perimeter", "Length", "Quota", "Inclinaz", "Immersione"]
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
        return "TEXT", 254

    def validate_inputs(self):
        """Validate input parameters with enhanced checks for File Geodatabase"""
        if not self.input_gdb:
            raise ValueError("Input File Geodatabase path is required")
            
        if not os.path.exists(self.input_gdb):
            raise ValueError("Input File Geodatabase path does not exist: {}".format(self.input_gdb))
        
        if not self.input_gdb.lower().endswith('.gdb'):
            raise ValueError("Input file must be a File Geodatabase (.gdb)")
        
        # Extract foglio value from geodatabase
        self.get_foglio_from_geodatabase()

        if not self.foglio or not self.foglio.strip():
            raise ValueError("FoglioGeologico field is empty or not found in geodatabase")

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

        # Create temporary File Geodatabase to avoid shapefile field-type limits in intermediate steps
        scratch_folder = getattr(arcpy.env, 'scratchFolder', None) or self.workspace
        self.temp_gdb = os.path.join(scratch_folder, "carg_temp.gdb")
        try:
            if arcpy.Exists(self.temp_gdb):
                arcpy.Delete_management(self.temp_gdb)
            arcpy.CreateFileGDB_management(scratch_folder, os.path.basename(self.temp_gdb))
            arcpy.AddMessage("Created temporary GDB: {}".format(self.temp_gdb))
        except Exception as e:
            raise RuntimeError("Failed to create temporary GDB {}: {}".format(self.temp_gdb, str(e)))

    def get_available_layers(self):
        """Enhanced layer discovery for GDB with nested structure"""
        if self._available_layers is not None:
            return self._available_layers
            
        available_layers = {}
        
        try:
            # Set workspace
            original_workspace = arcpy.env.workspace
            arcpy.env.workspace = self.input_gdb
            
            # Get feature datasets
            feature_datasets = arcpy.ListDatasets("", "Feature")
            
            if feature_datasets:
                for dataset in feature_datasets:
                    available_layers[dataset] = []
                    
                    # Get feature classes
                    arcpy.env.workspace = os.path.join(self.input_gdb, dataset)
                    feature_classes = arcpy.ListFeatureClasses()
                    
                    if feature_classes:
                        for fc in feature_classes:
                            available_layers[dataset].append(fc)
            
            # Reset workspace
            arcpy.env.workspace = self.input_gdb
            
            # Get feature classes at root level
            root_fcs = arcpy.ListFeatureClasses()
            if root_fcs:
                available_layers["root"] = root_fcs
                
            # Get tables for auxiliary domains (T0180801000, etc.)
            tables = arcpy.ListTables()
            if tables:
                available_layers["tables"] = tables
            
            arcpy.env.workspace = original_workspace
            
        except Exception as e:
            arcpy.AddWarning("Error getting available layers: {}".format(str(e)))
        
        self._available_layers = available_layers
        return self._available_layers

    def find_layer_by_pattern(self, available_layers_dict, patterns):
        """Pattern matching for GDB layers"""
        
        for pattern in patterns:
            pattern_lower = pattern.lower()
            
            for dataset_name, layer_list in available_layers_dict.items():
                if dataset_name == "tables":
                    continue
                    
                for layer_name in layer_list:
                    layer_lower = layer_name.lower()
                    
                    # Exact match
                    if pattern_lower == layer_lower:
                        return os.path.join(dataset_name, layer_name) if dataset_name != "root" else layer_name
                    
                    # Partial match
                    if pattern_lower in layer_lower:
                        return os.path.join(dataset_name, layer_name) if dataset_name != "root" else layer_name
        
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
        """Optimized domain mapping loader with UTF-8 error handling and caching"""
        cache_key = (domain_file, code_field, desc_field_pattern, is_gpkg_table)
        if cache_key in self._domain_cache:
            return self._domain_cache[cache_key]
        if is_gpkg_table:
            domain_table_path = os.path.join(self.input_gdb, domain_file)
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
                            desc_clean = str(desc_val).strip()
                            
                            # Create multiple key mappings for different data types
                            keys_to_map = [str(code_val).strip()]
                            
                            try:
                                # Try numeric conversions
                                if str(code_val).replace('.', '').replace('-', '').isdigit():
                                    float_val = float(str(code_val))
                                    int_val = int(float_val)
                                    keys_to_map.extend([code_val, float_val, int_val])
                            except (ValueError, TypeError):
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
            self._domain_cache[cache_key] = code_map
            return code_map
            
        except Exception as e:
            arcpy.AddWarning("Error reading domain {}: {}".format(domain_table_path, str(e)))
            self._domain_cache[cache_key] = {}
            return {}

    def apply_domain_mapping(self, shapefile, field_name, source_field, code_map):
        """Optimized domain mapping application with batch processing"""
        if not code_map:
            arcpy.AddWarning("No domain mappings available for {}".format(field_name))
            return
        
        # Create field if it doesn't exist
        existing_fields = {f.name.upper(): f.name for f in arcpy.ListFields(shapefile)}
        if field_name.upper() not in existing_fields:
            arcpy.AddField_management(shapefile, field_name, "TEXT", field_length=254)
        
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
                
                # Copy values using field calculator - Updated for Python 3
                expression = "!{}!".format(found_source)
                arcpy.CalculateField_management(shapefile, new_name, expression, "PYTHON3")
                
                arcpy.AddMessage("Mapped {} -> {}".format(found_source, new_name))
                
            except Exception as e:
                arcpy.AddWarning("Error mapping field {} -> {}: {}".format(found_source, new_name, str(e)))
                return False
        else:
            arcpy.AddMessage("Field {} already exists with correct name".format(new_name))
        
        return True
    
    def process_sommerso_field_optimized(self, output_shapefile_ETRF):
        """Processing SOMMERSO field"""
        arcpy.AddMessage("Processing SOMMERSO field with enhanced logic...")
        
        try:
            # Get all fields
            existing_fields = {f.name.upper(): f.name for f in arcpy.ListFields(output_shapefile_ETRF)}
            arcpy.AddMessage("Available fields: {}".format(list(existing_fields.keys())))
            
            # Get SOMMERSO field (case-insensitive)
            sommerso_field = None
            for field_key, field_name in existing_fields.items():
                if "SOMMERSO" in field_key and "SOMMERSO_" not in field_key:
                    sommerso_field = field_name
                    arcpy.AddMessage("Found SOMMERSO: {}".format(sommerso_field))
                    break
            
            if not sommerso_field:
                arcpy.AddWarning("SOMMERSO field not found within available fields")
                # Add empty field
                if "SOMMERSO_" not in existing_fields:
                    arcpy.AddField_management(output_shapefile_ETRF, "Sommerso_", "TEXT", field_length=10)
                    arcpy.AddMessage("Empty Sommerso_ field created")
                return
            
            # Add Sommerso_ if not exist
            if "SOMMERSO_" not in existing_fields:
                arcpy.AddField_management(output_shapefile_ETRF, "Sommerso_", "TEXT", field_length=10)
                arcpy.AddMessage("Sommerso_ field created")
            
            # Processing of values
            stats = {"si": 0, "no": 0, "altro": 0, "null": 0}
            
            with arcpy.da.UpdateCursor(output_shapefile_ETRF, [sommerso_field, "Sommerso_"]) as cursor:
                for i, row in enumerate(cursor):
                    sommerso_val = row[0]
                    
                    if sommerso_val is None:
                        new_val = ""
                        stats["null"] += 1
                    else:
                        # Convert to string
                        str_val = str(sommerso_val).strip()
                        
                        # Parse values
                        if str_val in ["1", "1.0", "SI", "si", "S", "s"]:
                            new_val = "SI"
                            stats["si"] += 1
                        elif str_val in ["2", "2.0", "NO", "no", "N", "n"]:
                            new_val = "NO" 
                            stats["no"] += 1
                        else:
                            new_val = ""
                            stats["altro"] += 1
                            if i < 5:  # Log first few problematic values
                                arcpy.AddMessage("SOMMERSO field type not recognized: '{}' (type: {})".format(
                                    str_val, type(sommerso_val).__name__))
                    
                    cursor.updateRow([sommerso_val, new_val])
                    
                    # Progress reporting
                    if (i + 1) % 1000 == 0:
                        arcpy.AddMessage("Processed {} records...".format(i + 1))
            
            # Report statistics
            arcpy.AddMessage("Results of conversion for SOMMERSO:")
            arcpy.AddMessage("  SI: {}".format(stats["si"]))
            arcpy.AddMessage("  NO: {}".format(stats["no"]))
            arcpy.AddMessage("  Other/NR: {}".format(stats["altro"]))
            arcpy.AddMessage("  Null: {}".format(stats["null"]))
            
            arcpy.AddMessage("SOMMERSO field processed")
            
        except Exception as e:
            arcpy.AddError("Error in processing SOMMERSO field: {}".format(str(e)))
            import traceback
            arcpy.AddError(traceback.format_exc())

    def process_geology_lines_standard(self, shapefile):
        """
        Processing for geologia_linee (ST018Polyline):
        - Add field 'Fase_txt' with value "non applicabile/non classificabile"
        """
        arcpy.AddMessage("Processing geologia_linee with special handling...")
        
        try:
            # Add Fase_txt field if not exists
            existing_fields = {f.name.upper(): f.name for f in arcpy.ListFields(shapefile)}
            
            if "FASE_TXT" not in existing_fields:
                arcpy.AddField_management(shapefile, "Fase_txt", "TEXT", field_length=254)
                arcpy.AddMessage("'Fase_txt' created")
            
            # Set all Fase_txt values to "non applicabile"
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
        - Other fields mapped through domains
        """
        arcpy.AddMessage("Processing geologia_linee_pieghe with standardized fields...")
        
        try:
            # Get existing fields
            existing_fields = {f.name.upper(): f.name for f in arcpy.ListFields(shapefile)}
            existing_field_names_upper = [f.upper() for f in existing_fields.values()]
            
            # Add required fields
            fields_to_add = {
                "Affior_txt": ("TEXT", 254),
                "Cont_txt": ("TEXT", 254)
            }
            
            for field_name, (field_type, length) in fields_to_add.items():
                if field_name.upper() not in existing_fields:
                    arcpy.AddField_management(shapefile, field_name, field_type, field_length=length)
                    arcpy.AddMessage("Field {} added".format(field_name))
                    existing_fields = {f.name.upper(): f.name for f in arcpy.ListFields(shapefile)}
                    existing_field_names_upper = [f.upper() for f in existing_fields.values()]
            
            # Set and populate fixed-string fields
            update_fields = []
            field_values = []
            
            if "AFFIOR_TXT" in existing_field_names_upper:
                update_fields.append("Affior_txt")
                field_values.append("non applicabile")
            
            if "CONT_TXT" in existing_field_names_upper:
                update_fields.append("Cont_txt") 
                field_values.append("no")
            
            # Update fields if required
            if update_fields:
                with arcpy.da.UpdateCursor(shapefile, update_fields) as cursor:
                    for row in cursor:
                        cursor.updateRow(field_values)
                
                arcpy.AddMessage("Fields compiled: Affior_txt = 'non applicabile', Cont_txt = 'no'")
            else:
                arcpy.AddMessage("Field compilation not required")
            
            arcpy.AddMessage("geologia_linee_pieghe processing OK")
            return True
            
        except Exception as e:
            arcpy.AddError("geologia_linee_pieghe processing ERROR: {}".format(str(e)))
            import traceback
            arcpy.AddError(traceback.format_exc())
            return False

    def process_geology_polygons_final_fix(self, output_shapefile_ETRF):
        """Final comprehensive fix for geology polygons with proper debugging"""
        arcpy.AddMessage("Starting FINAL FIX for geologia_poligoni processing...")
        
        try:
            # STEP 1: Debug current state
            arcpy.AddMessage("=== DEBUGGING CURRENT STATE ===")
            current_fields = [f.name for f in arcpy.ListFields(output_shapefile_ETRF)]
            arcpy.AddMessage("Current shapefile fields: {}".format(current_fields))
            
            # STEP 2: Enhanced table discovery
            auxiliary_tables = self.enhanced_auxiliary_table_discovery_gdb_fixed()
            
            if not auxiliary_tables:
                arcpy.AddError("No auxiliary tables found - cannot proceed")
                return False
            
            # STEP 3: Debug T2000 mapping issues specifically
            self.debug_t2000_mapping_issues_fixed(output_shapefile_ETRF, auxiliary_tables)
            
            # STEP 4: Add required fields
            self._add_geology_fields_comprehensive(output_shapefile_ETRF)
            
            # STEP 5: Load table data with enhanced debugging
            table_data = self._load_auxiliary_table_data_gdb_fixed(auxiliary_tables)
            
            # STEP 6: Load domain mappings
            domain_mappings = self._load_geology_domain_mappings_safe()
            
            # STEP 7: Apply mappings with corrected functions
            if table_data.get("t1000"):
                self._apply_t1000_mappings_gdb_fixed(output_shapefile_ETRF, table_data["t1000"], domain_mappings)
            
            if table_data.get("t2000"):
                self._apply_t2000_mappings_gdb_corrected(output_shapefile_ETRF, table_data["t2000"], domain_mappings)
            
            if table_data.get("t3000"):
                self._apply_t3000_mappings_gdb(output_shapefile_ETRF, table_data["t3000"], domain_mappings)
            
            # STEP 8: Handle Direzione field CORRECTLY (without overwriting with zeros)
            self.handle_direzione_field_for_geology_polygons_fixed(output_shapefile_ETRF)
            
            # STEP 9: Process SOMMERSO field
            self.process_sommerso_field_optimized(output_shapefile_ETRF)
            
            # STEP 10: Final verification
            self._verify_geology_fields_populated(output_shapefile_ETRF)
            
            arcpy.AddMessage("Final fix for geology polygons completed successfully")
            return True
            
        except Exception as e:
            arcpy.AddError("Critical error in final geology polygon fix: {}".format(str(e)))
            import traceback
            arcpy.AddError(traceback.format_exc())
            return False

################################################################################################ ricontrollare da qui in poi

    def debug_t2000_mapping_issues_fixed(self, shapefile, auxiliary_tables):
        """Debug T2000 mapping with case-insensitive field matching"""
        arcpy.AddMessage("=== DEBUGGING T2000 MAPPING ISSUES (FIXED) ===")
        
        # Check if T2000 table exists and has data
        if "t2000" not in auxiliary_tables:
            arcpy.AddError("T2000 table not found in auxiliary_tables")
            return
        
        t2000_path = auxiliary_tables["t2000"]
        arcpy.AddMessage("T2000 table path: {}".format(t2000_path))
        
        if not arcpy.Exists(t2000_path):
            arcpy.AddError("T2000 table does not exist at path: {}".format(t2000_path))
            return
        
        # Check T2000 table structure
        t2000_fields = [f.name for f in arcpy.ListFields(t2000_path)]
        arcpy.AddMessage("T2000 table fields: {}".format(t2000_fields))
        
        # Check shapefile structure for UC_LEGE values
        shapefile_fields = [f.name for f in arcpy.ListFields(shapefile)]
        arcpy.AddMessage("Shapefile fields: {}".format(shapefile_fields))
        
        # Case-insensitive search for UC_LEGE field
        uc_lege_field_in_shapefile = None
        for field in shapefile_fields:
            if field.upper() == "UC_LEGE":
                uc_lege_field_in_shapefile = field
                break
        
        if uc_lege_field_in_shapefile:
            arcpy.AddMessage("Found UC_LEGE field in shapefile as: {}".format(uc_lege_field_in_shapefile))
            
            # Sample UC_LEGE values from shapefile
            uc_lege_samples = []
            with arcpy.da.SearchCursor(shapefile, [uc_lege_field_in_shapefile]) as cursor:
                for i, row in enumerate(cursor):
                    if i >= 10:  # First 10 samples
                        break
                    uc_lege_samples.append(str(row[0]) if row[0] is not None else "NULL")
            arcpy.AddMessage("Sample UC_LEGE values from shapefile: {}".format(uc_lege_samples))
        else:
            arcpy.AddError("UC_LEGE field not found in shapefile (case-insensitive)")
            return
        
        # Sample UC_LEGE values from T2000 table
        t2000_samples = []
        uc_lege_field_in_table = None
        
        # Find UC_LEGE field in T2000 (case insensitive)
        for field in t2000_fields:
            if field.upper() in ["UC_LEGE", "UCLEGE", "UC_LEGENDA"]:
                uc_lege_field_in_table = field
                break
        
        if uc_lege_field_in_table:
            arcpy.AddMessage("Found UC_LEGE field in T2000 as: {}".format(uc_lege_field_in_table))
            with arcpy.da.SearchCursor(t2000_path, [uc_lege_field_in_table]) as cursor:
                for i, row in enumerate(cursor):
                    if i >= 10:  # First 10 samples
                        break
                    t2000_samples.append(str(row[0]) if row[0] is not None else "NULL")
            arcpy.AddMessage("Sample UC_LEGE values from T2000: {}".format(t2000_samples))
        else:
            arcpy.AddError("UC_LEGE field not found in T2000 table")
            return
        
        # Check for matches
        shapefile_set = set(uc_lege_samples)
        t2000_set = set(t2000_samples)
        matches = shapefile_set.intersection(t2000_set)
        arcpy.AddMessage("Matching UC_LEGE values: {}".format(list(matches)))
        
        arcpy.AddMessage("=== END T2000 DEBUG (FIXED) ===")

    def enhanced_auxiliary_table_discovery_gdb_fixed(self):
        """Enhanced table discovery with more thorough search patterns"""
        arcpy.AddMessage("=== ENHANCED AUXILIARY TABLE DISCOVERY (FIXED) ===")
        
        # More comprehensive search patterns
        table_patterns = {
            "t1000": {
                "names": ["T0180801000", "ST018PolygonT0180801000", "T1000", "ST018_T1000", "1000"],
                "required_fields": ["UQ_CAR", "TIPO", "STATO"],
                "key_field": "UQ_CAR"
            },
            "t2000": {
                "names": ["T0180802000", "ST018PolygonT0180802000", "T2000", "ST018_T2000", "2000"],
                "required_fields": ["UC_LEGE", "ETA_SUP", "ETA_INF", "S1_TIPO"],
                "key_field": "UC_LEGE"
            },
            "t3000": {
                "names": ["T0180803000", "ST018PolygonT0180803000", "T3000", "ST018_T3000", "3000"],
                "required_fields": ["ID_TESS", "TESSITURA"],
                "key_field": "ID_TESS"
            }
        }
        
        available_layers = self.get_available_layers()
        found_tables = {}
        
        # Build comprehensive search locations
        search_locations = []
        
        # 1. All tables (root level)
        if "tables" in available_layers:
            for table in available_layers["tables"]:
                search_locations.append(("table", table, os.path.join(self.input_gdb, table)))
        
        # 2. All feature classes in all datasets
        for dataset_name, layer_list in available_layers.items():
            if dataset_name not in ["tables", "root"]:
                for item_name in layer_list:
                    full_path = os.path.join(self.input_gdb, dataset_name, item_name)
                    search_locations.append((dataset_name, item_name, full_path))
        
        # 3. Root level feature classes  
        if "root" in available_layers:
            for item_name in available_layers["root"]:
                full_path = os.path.join(self.input_gdb, item_name)
                search_locations.append(("root", item_name, full_path))
        
        arcpy.AddMessage("Searching in {} locations...".format(len(search_locations)))
        
        # Search for each table type
        for table_key, config in table_patterns.items():
            arcpy.AddMessage("Searching for {} table...".format(table_key))
            found = False
            
            for location_type, item_name, full_path in search_locations:
                if found:
                    break
                    
                if not arcpy.Exists(full_path):
                    continue
                
                # Check name patterns (more flexible matching)
                name_match = False
                item_name_upper = item_name.upper()
                
                for pattern in config["names"]:
                    pattern_upper = pattern.upper()
                    if (pattern_upper in item_name_upper or 
                        item_name_upper.endswith(pattern_upper) or
                        any(p in item_name_upper for p in pattern_upper.split('_'))):
                        name_match = True
                        break
                
                if not name_match:
                    continue
                
                # Verify required fields exist
                try:
                    available_fields = [f.name.upper() for f in arcpy.ListFields(full_path)]
                    required_found = 0
                    
                    for req_field in config["required_fields"]:
                        if req_field.upper() in available_fields:
                            required_found += 1
                    
                    # Accept if we have at least the key field + one other
                    if required_found >= 2:
                        found_tables[table_key] = full_path
                        found = True
                        arcpy.AddMessage("✓ Found {} at: {} ({}/{} required fields)".format(
                            table_key, full_path, required_found, len(config["required_fields"])))
                        break
                        
                except Exception as e:
                    arcpy.AddWarning("Error checking fields in {}: {}".format(full_path, str(e)))
                    continue
            
            if not found:
                arcpy.AddWarning("Table {} not found with required fields".format(table_key))
        
        return found_tables

    def _add_geology_fields_comprehensive(self, shapefile):
        """
        Add all necessary fields
        """
        fields_to_add = {
            # Main fields
            "Tipo_UQ": ("TEXT", 254),
            "TempTIPO": ("TEXT", 50), 
            "Stato_UQ": ("TEXT", 254),
            "TempSTATO": ("TEXT", 50),
            "ETA_Super": ("TEXT", 254),
            "ETA_Infer": ("TEXT", 254),
            "TMP_super": ("TEXT", 50), 
            "TMP_infer": ("TEXT", 50),
            "Tipo_UG": ("TEXT", 254),
            "Temp_UnGeo": ("TEXT", 50), 
            "Tessitura": ("TEXT", 254),
            "TempTESS": ("TEXT", 50),
            "Sommerso_": ("TEXT", 10),
            
            # Extra fields
            "Sigla1": ("TEXT", 254),
            "Sigla_UG": ("TEXT", 254),
            "Nome": ("TEXT", 254),
            "Legenda": ("TEXT", 254)
        }
        
        existing_fields = {f.name for f in arcpy.ListFields(shapefile)}
        added_count = 0
        
        for field_name, (field_type, length) in fields_to_add.items():
            if field_name not in existing_fields:
                try:
                    arcpy.AddField_management(shapefile, field_name, field_type, field_length=length)
                    added_count += 1
                except Exception as e:
                    arcpy.AddWarning("Could not add field {}: {}".format(field_name, str(e)))
        
        arcpy.AddMessage("Added {} new fields to geology polygons".format(added_count))

    def _process_with_auxiliary_tables_gdb(self, shapefile, auxiliary_tables):
        """
        Processingfor Geodatabase with auxiliary tables
        """
        arcpy.AddMessage("Processing with GDB auxiliary tables...")
        
        try:
            # Load data
            table_data = self._load_auxiliary_table_data_gdb_fixed(auxiliary_tables)
            
            # Load domains
            domain_mappings = self._load_geology_domain_mappings_safe()
            
            # Apply mappings
            self._apply_geology_mappings_comprehensive_gdb(shapefile, table_data, domain_mappings)
            
            return True
            
        except Exception as e:
            arcpy.AddError("Error in GDB auxiliary table processing: {}".format(str(e)))
            return False

    def _process_with_fallback(self, shapefile, partial_tables):
        """
        Processing di fallback if auxiliary tables are incomplete
        """
        arcpy.AddMessage("Using fallback processing...")
        
        try:
            # Process
            if partial_tables:
                arcpy.AddMessage("Processing with {} partial tables...".format(len(partial_tables)))
                table_data = self._load_auxiliary_table_data_gdb_fixed(partial_tables)
                domain_mappings = self._load_geology_domain_mappings_safe()
                self._apply_geology_mappings_comprehensive_gdb(shapefile, table_data, domain_mappings)
            
            # Set default values
            default_values = {
                "Tipo_UQ": "Non classificato",
                "Stato_UQ": "Non definito", 
                "ETA_Super": "",
                "ETA_Infer": "",
                "Tipo_UG": "",
                "Tessitura": "",
                "Sigla1": "",
                "Sigla_UG": "",
                "Nome": "",
                "Legenda": ""
            }
            
            self._set_default_values_safe(shapefile, default_values)
            arcpy.AddMessage("Applied default values for missing data")
            
            return True
            
        except Exception as e:
            arcpy.AddError("Error in fallback processing: {}".format(str(e)))
            return False

    def _load_auxiliary_table_data_gdb_fixed(self, auxiliary_tables):
        """Load table data with case-insensitive field handling"""
        table_data = {"t1000": {}, "t2000": {}, "t3000": {}}
        
        table_configs = {
            "t1000": {
                "fields": ["UQ_CAR", "TIPO", "STATO"],
                "key_field": "UQ_CAR"
            },
            "t2000": {
                "fields": ["UC_LEGE", "ETA_SUP", "ETA_INF", "S1_TIPO", "SIGLA1", "SIGLA_CARTA", "NOME", "LEGENDA"],
                "key_field": "UC_LEGE"
            },
            "t3000": {
                "fields": ["ID_TESS", "TESSITURA"],
                "key_field": "ID_TESS"
            }
        }
        
        for table_key, table_path in auxiliary_tables.items():
            if table_key not in table_configs:
                continue
                
            config = table_configs[table_key]
            arcpy.AddMessage("Loading {} from {}...".format(table_key, table_path))
            
            try:
                if not arcpy.Exists(table_path):
                    arcpy.AddWarning("Table {} does not exist".format(table_path))
                    continue
                
                # Get available fields with case-insensitive matching
                all_fields = [f.name for f in arcpy.ListFields(table_path)]
                all_fields_dict = {f.name.upper(): f.name for f in arcpy.ListFields(table_path)}
                
                # Map required fields to actual field names (case-insensitive)
                fields_to_read = []
                actual_field_names = []
                
                for required_field in config["fields"]:
                    if required_field.upper() in all_fields_dict:
                        actual_field_name = all_fields_dict[required_field.upper()]
                        fields_to_read.append(actual_field_name)
                        actual_field_names.append(required_field)  # Keep standardized name for mapping
                    else:
                        arcpy.AddWarning("Required field '{}' not found in {}".format(required_field, table_key))
                
                if not fields_to_read:
                    arcpy.AddWarning("No readable fields found in {}".format(table_key))
                    continue
                
                arcpy.AddMessage("Reading fields from {}: {}".format(table_key, fields_to_read))
                
                # Load data
                record_count = 0
                key_field_name = None
                key_field_standard_name = None
                
                # Find actual key field name and its standard name
                for i, standard_name in enumerate(actual_field_names):
                    if standard_name.upper() == config["key_field"].upper():
                        key_field_name = fields_to_read[i]
                        key_field_standard_name = standard_name
                        break
                
                if not key_field_name:
                    arcpy.AddError("Key field '{}' not found in {}".format(config["key_field"], table_key))
                    continue
                
                with arcpy.da.SearchCursor(table_path, fields_to_read) as cursor:
                    for row in cursor:
                        try:
                            record_count += 1
                            
                            # Get key value
                            key_index = fields_to_read.index(key_field_name)
                            key_value = self.safe_string_conversion(row[key_index])
                            
                            if not key_value:
                                continue
                            
                            # Build data dictionary
                            if table_key == "t3000":
                                # Simple structure for T3000
                                tessitura_index = None
                                for i, standard_name in enumerate(actual_field_names):
                                    if standard_name.upper() == "TESSITURA":
                                        tessitura_index = i
                                        break
                                
                                if tessitura_index is not None and tessitura_index < len(row):
                                    table_data[table_key][key_value] = self.safe_string_conversion(row[tessitura_index])
                            else:
                                # Complex structure for T1000 and T2000
                                values_dict = {}
                                for i, standard_name in enumerate(actual_field_names):
                                    if i < len(row):
                                        values_dict[standard_name] = self.safe_string_conversion(row[i])
                                
                                table_data[table_key][key_value] = values_dict
                            
                            # Debug first few records
                            if record_count <= 3:
                                arcpy.AddMessage("  Record {}: key='{}', data={}".format(
                                    record_count, key_value, 
                                    table_data[table_key][key_value] if key_value in table_data[table_key] else "ERROR"))
                            
                        except Exception as row_error:
                            arcpy.AddWarning("Error processing record {} in {}: {}".format(record_count, table_key, str(row_error)))
                            continue
                
                arcpy.AddMessage("Successfully loaded {} records from {}".format(len(table_data[table_key]), table_key))
                
            except Exception as table_error:
                arcpy.AddError("Error loading table {}: {}".format(table_key, str(table_error)))
                import traceback
                arcpy.AddError(traceback.format_exc())
        
        return table_data

    def _verify_geology_fields_populated(self, shapefile):
        """Verify that key geology fields were populated"""
        arcpy.AddMessage("=== VERIFYING GEOLOGY FIELD POPULATION ===")
        
        fields_to_check = ["Tipo_UQ", "Stato_UQ", "ETA_Super", "ETA_Infer", "Tipo_UG", "Sigla_UG"]
        existing_fields = [f.name for f in arcpy.ListFields(shapefile)]
        
        for field_name in fields_to_check:
            if field_name in existing_fields:
                # Count non-empty values
                non_empty_count = 0
                total_count = 0
                sample_values = []
                
                with arcpy.da.SearchCursor(shapefile, [field_name]) as cursor:
                    for row in cursor:
                        total_count += 1
                        value = row[0]
                        if value and str(value).strip() and str(value).strip().lower() not in ["", "none", "null"]:
                            non_empty_count += 1
                            if len(sample_values) < 3:
                                sample_values.append(str(value)[:50])  # First 50 chars
                
                if non_empty_count > 0:
                    percentage = (non_empty_count / total_count * 100) if total_count > 0 else 0
                    arcpy.AddMessage("✓ {}: {}/{} populated ({:.1f}%) - samples: {}".format(
                        field_name, non_empty_count, total_count, percentage, sample_values))
                else:
                    arcpy.AddWarning("✗ {}: 0/{} populated - FIELD IS EMPTY!".format(field_name, total_count))
            else:
                arcpy.AddWarning("✗ Field '{}' not found in shapefile".format(field_name))
        
        arcpy.AddMessage("=== END FIELD VERIFICATION ===")

    def _load_geology_domain_mappings_safe(self):
        """
        Carica domini per geology con gestione errori
        """
        domain_files = {
            "tipo": "d_1000_tipo.dbf",
            "stato": "d_stato.dbf",
            "eta": "d_t2000_eta.dbf",
            "sigla_tipo": "d_2000_SiglaTipo.dbf",
            "tessitura": "d_t3000.dbf"
        }
        
        domain_mappings = {}
        for key, filename in domain_files.items():
            try:
                domain_mappings[key] = self.load_domain_mappings(filename, is_gpkg_table=False)
                if not domain_mappings[key]:
                    arcpy.AddWarning("No mappings loaded for domain {}".format(key))
            except Exception as e:
                arcpy.AddWarning("Error loading domain {}: {}".format(key, str(e)))
                domain_mappings[key] = {}
        
        return domain_mappings

    def _apply_geology_mappings_comprehensive_gdb(self, shapefile, table_data, domain_mappings):
        """
        Applica mappature complete per GDB
        """
        try:
            # Mappatura T1000 (UQ_CAR -> TIPO, STATO)
            if table_data.get("t1000"):
                arcpy.AddMessage("Applying T1000 mappings for GDB...")
                self._apply_t1000_mappings_gdb(shapefile, table_data["t1000"], domain_mappings)

            # Mappatura T2000 (UC_LEGE -> multiple fields)
            if table_data.get("t2000"):
                arcpy.AddMessage("Applying T2000 mappings for GDB...")
                self._apply_t2000_mappings_gdb_corrected(shapefile, table_data["t2000"], domain_mappings)

            # Mappatura T3000 (ID_TESS -> Tessitura)
            if table_data.get("t3000"):
                arcpy.AddMessage("Applying T3000 mappings for GDB...")
                self._apply_t3000_mappings_gdb(shapefile, table_data["t3000"], domain_mappings)

        except Exception as e:
            arcpy.AddError("Error applying geology mappings for GDB: {}".format(str(e)))
            raise

    def _apply_t1000_mappings_gdb_fixed(self, shapefile, t1000_data, domain_mappings):
        """Fixed T1000 mapping with better error handling and debugging"""
        try:
            arcpy.AddMessage("Starting T1000 mapping with {} records in lookup table".format(len(t1000_data)))
            
            # Debug: show sample data
            if t1000_data:
                sample_keys = list(t1000_data.keys())[:3]
                arcpy.AddMessage("Sample T1000 keys: {}".format(sample_keys))
                for key in sample_keys:
                    arcpy.AddMessage("  Key '{}' -> {}".format(key, t1000_data[key]))
            
            # Verify fields exist in shapefile
            existing_fields = [f.name.upper() for f in arcpy.ListFields(shapefile)]
            required_fields = ["UQ_CAR", "TIPO_UQ", "STATO_UQ"]
            
            for field in required_fields:
                if field not in existing_fields:
                    arcpy.AddWarning("Required field '{}' not found in shapefile".format(field))
                    return
            
            updated_count = 0
            not_found_count = 0
            sample_not_found = []
            
            with arcpy.da.UpdateCursor(shapefile, ["UQ_CAR", "Tipo_UQ", "Stato_UQ"]) as cursor:
                for row in cursor:
                    uq_car_val = self.safe_string_conversion(row[0])
                    
                    if uq_car_val in t1000_data:
                        data = t1000_data[uq_car_val]
                        
                        # Get raw values
                        tipo_raw = data.get("TIPO", "")
                        stato_raw = data.get("STATO", "")
                        
                        # Apply domain mappings with fallback
                        tipo_mapped = domain_mappings.get("tipo", {}).get(tipo_raw, tipo_raw) if tipo_raw else "Non classificato"
                        stato_mapped = domain_mappings.get("stato", {}).get(stato_raw, stato_raw) if stato_raw else "Non definito"
                        
                        # Update row
                        new_row = [uq_car_val, tipo_mapped, stato_mapped]
                        cursor.updateRow(new_row)
                        updated_count += 1
                        
                        if updated_count <= 5:  # Debug first 5 updates
                            arcpy.AddMessage("  Updated UQ_CAR '{}': Tipo='{}'->'{}', Stato='{}'->'{}'"
                                .format(uq_car_val, tipo_raw, tipo_mapped, stato_raw, stato_mapped))
                    else:
                        not_found_count += 1
                        if len(sample_not_found) < 5:
                            sample_not_found.append(uq_car_val)
            
            arcpy.AddMessage("T1000 mapping results: {} updated, {} not found in lookup".format(updated_count, not_found_count))
            if sample_not_found:
                arcpy.AddMessage("Sample UQ_CAR values not found: {}".format(sample_not_found))
                            
        except Exception as e:
            arcpy.AddError("Error in T1000 mapping: {}".format(str(e)))
            import traceback
            arcpy.AddError(traceback.format_exc())

    def _apply_t2000_mappings_gdb_corrected(self, shapefile, t2000_data, domain_mappings):
        """Corrected T2000 mapping with proper field name handling"""
        try:
            arcpy.AddMessage("Starting CORRECTED T2000 mapping (v3 - FIXED)...")
            arcpy.AddMessage("T2000 data contains {} records".format(len(t2000_data)))
            
            if not t2000_data:
                arcpy.AddWarning("T2000 data is empty - skipping T2000 mapping")
                return
            
            # Show sample T2000 data
            sample_keys = list(t2000_data.keys())[:3]
            for key in sample_keys:
                arcpy.AddMessage("Sample T2000 key '{}' -> {}".format(key, t2000_data[key]))
            
            # FIXED: Use the actual field names created in the shapefile
            # These match the field names from _get_feature_configs() for ST018Polygon
            field_mappings = {
                "ETA_super": "ETA_SUP",     # Match the actual field name created
                "ETA_infer": "ETA_INF",     # Match the actual field name created  
                "tipo_ug": "S1_TIPO",       # Match the actual field name created
                "Sigla1": "SIGLA1",
                "Sigla_ug": "SIGLA_CARTA",  # Match the actual field name created
                "Nome": "NOME",
                "Legenda": "LEGENDA"
            }
            
            # Case-insensitive field matching for shapefile
            existing_fields = {f.name.upper(): f.name for f in arcpy.ListFields(shapefile)}
            available_mappings = {}
            
            # Find UC_LEGE field (case-insensitive)
            uc_lege_field = None
            if "UC_LEGE" in existing_fields:
                uc_lege_field = existing_fields["UC_LEGE"]
                arcpy.AddMessage("Found UC_LEGE field as: {}".format(uc_lege_field))
            else:
                arcpy.AddError("UC_LEGE field not found in shapefile")
                return
            
            # Map target fields to their actual names in shapefile (case-insensitive)
            for target_field, source_field in field_mappings.items():
                # Try exact match first
                if target_field in existing_fields.values():
                    available_mappings[target_field] = source_field
                    arcpy.AddMessage("Will map {} <- {}".format(target_field, source_field))
                else:
                    # Try case-insensitive match
                    target_upper = target_field.upper()
                    if target_upper in existing_fields:
                        actual_field_name = existing_fields[target_upper]
                        available_mappings[actual_field_name] = source_field
                        arcpy.AddMessage("Will map {} <- {} (case-insensitive)".format(actual_field_name, source_field))
                    else:
                        arcpy.AddWarning("Target field {} not found in shapefile".format(target_field))
            
            if not available_mappings:
                arcpy.AddWarning("No T2000 target fields found in shapefile")
                return
            
            # Prepare cursor fields: UC_LEGE + all target fields
            cursor_fields = [uc_lege_field] + list(available_mappings.keys())
            arcpy.AddMessage("Using cursor fields: {}".format(cursor_fields))
            
            # Apply mappings
            updated_count = 0
            not_found_count = 0
            empty_uc_lege_count = 0
            debug_sample_count = 0
            
            with arcpy.da.UpdateCursor(shapefile, cursor_fields) as cursor:
                for row in cursor:
                    uc_lege_val = self.safe_string_conversion(row[0])
                    
                    if not uc_lege_val:
                        empty_uc_lege_count += 1
                        continue
                    
                    if uc_lege_val in t2000_data:
                        data = t2000_data[uc_lege_val]
                        new_row = list(row)
                        row_updated = False
                        
                        # Map each field
                        for i, (target_field, source_field) in enumerate(available_mappings.items(), 1):  # Start at index 1 (after UC_LEGE)
                            raw_value = data.get(source_field, "")
                            
                            # FIXED: Apply domain mapping based on field type with proper field name matching
                            mapped_value = raw_value  # Default to raw value
                            
                            target_field_upper = target_field.upper()
                            if target_field_upper in ["ETA_SUPER", "ETA_INFER"]:
                                # Apply ETA domain mapping
                                if raw_value and raw_value in domain_mappings.get("eta", {}):
                                    mapped_value = domain_mappings["eta"][raw_value]
                                elif raw_value:
                                    # Try string conversion for domain lookup
                                    str_raw = str(raw_value).strip()
                                    mapped_value = domain_mappings.get("eta", {}).get(str_raw, raw_value)
                            elif target_field_upper in ["TIPO_UG"]:
                                # Apply SIGLA_TIPO domain mapping  
                                if raw_value and raw_value in domain_mappings.get("sigla_tipo", {}):
                                    mapped_value = domain_mappings["sigla_tipo"][raw_value]
                                elif raw_value:
                                    str_raw = str(raw_value).strip()
                                    mapped_value = domain_mappings.get("sigla_tipo", {}).get(str_raw, raw_value)
                            # For other fields (Sigla1, Sigla_ug, Nome, Legenda), use raw value
                            
                            new_row[i] = mapped_value
                            row_updated = True
                            
                            # Debug first few updates
                            if debug_sample_count < 5:
                                arcpy.AddMessage("  Row {}: {} '{}' -> '{}' -> '{}'".format(
                                    debug_sample_count + 1, target_field, raw_value, 
                                    "domain_mapped" if mapped_value != raw_value else "direct",
                                    mapped_value[:50]))  # Limit output length
                        
                        if row_updated:
                            cursor.updateRow(new_row)
                            updated_count += 1
                            debug_sample_count += 1
                    else:
                        not_found_count += 1
            
            arcpy.AddMessage("T2000 mapping results:")
            arcpy.AddMessage("  Updated: {}".format(updated_count))
            arcpy.AddMessage("  Not found in T2000: {}".format(not_found_count))
            arcpy.AddMessage("  Empty UC_LEGE: {}".format(empty_uc_lege_count))
            
            # Verification step - check if fields were actually populated
            self._verify_t2000_field_population(shapefile, list(available_mappings.keys()))
            
        except Exception as e:
            arcpy.AddError("Error in corrected T2000 mapping v3: {}".format(str(e)))
            import traceback
            arcpy.AddError(traceback.format_exc())

    def _get_feature_configs_fixed_st018polygon(self):
        """Fixed ST018Polygon configuration - this should replace the ST018Polygon section in your _get_feature_configs method"""
        return {
            "ST018Polygon": {
                "search_patterns": ["ST018Polygon"],
                "fields": [
                    # These are the actual fields that should be created/renamed
                    {"old": "Pol_Uc", "new": "Pol_Uc"},
                    {"old": "Uc_Lege", "new": "Uc_Lege"},  
                    {"old": "Direzio", "new": "Direzione"},
                    # Don't create these fields here - they'll be created in auxiliary table processing
                    # They get populated from T1000, T2000, T3000 tables via UC_LEGE/UQ_CAR lookups
                ],
                "domains": [],  # Domains are handled via auxiliary tables
                "output_name": "geologia_poligoni.shp",
                "special_processing": "geology_polygons",
                "keep_fields": ["Pol_Uc", "Uc_Lege", "Foglio", "Direzione", "Tipo_UQ", "Stato_UQ", 
                            "ETA_Super", "ETA_Infer", "Tipo_UG", "Tessitura", "Sigla1", "Sigla_UG", 
                            "Nome", "Legenda", "Sommerso_"]
            }
        }

    def _verify_t2000_field_population(self, shapefile, field_list):
        """Verify that T2000 fields were actually populated"""
        arcpy.AddMessage("=== VERIFYING T2000 FIELD POPULATION ===")
        
        for field_name in field_list:
            try:
                non_empty_count = 0
                total_count = 0
                sample_values = []
                
                with arcpy.da.SearchCursor(shapefile, [field_name]) as cursor:
                    for row in cursor:
                        total_count += 1
                        value = row[0]
                        if value and str(value).strip() and str(value).strip().lower() not in ["", "none", "null"]:
                            non_empty_count += 1
                            if len(sample_values) < 3:
                                sample_values.append(str(value)[:30])  # First 30 chars
                
                if non_empty_count > 0:
                    percentage = (non_empty_count / total_count * 100) if total_count > 0 else 0
                    arcpy.AddMessage("  {}: {}/{} populated ({:.1f}%) - samples: {}".format(
                        field_name, non_empty_count, total_count, percentage, sample_values))
                else:
                    arcpy.AddWarning("  {}: 0/{} populated - FIELD IS EMPTY!".format(field_name, total_count))
                    
            except Exception as e:
                arcpy.AddWarning("  Error verifying {}: {}".format(field_name, str(e)))
        
        arcpy.AddMessage("=== END T2000 VERIFICATION ===")

    def _apply_t3000_mappings_gdb(self, shapefile, t3000_data, domain_mappings):
        """Apply T3000 mapping"""
        try:
            # {id: tessitura}
            if not isinstance(t3000_data, dict):
                arcpy.AddWarning("T3000 data is not in correct format")
                return
                
            # Verify existing fields
            existing_fields = [f.name.upper() for f in arcpy.ListFields(shapefile)]
            
            cursor_fields = []
            if "ID_TESS" in existing_fields:
                cursor_fields.append("ID_TESS")
            if "TESSITURA" in existing_fields:
                cursor_fields.append("Tessitura")
            
            if not cursor_fields:
                arcpy.AddWarning("No T3000 mapping fields found in shapefile")
                return
            
            with arcpy.da.UpdateCursor(shapefile, cursor_fields) as cursor:
                updated_count = 0
                for row in cursor:
                    id_tess_val = self.safe_string_conversion(row[0]) if len(row) > 0 else ""
                    
                    if id_tess_val in t3000_data:
                        tessitura_val = t3000_data[id_tess_val]
                        
                        new_row = list(row)
                        
                        if len(cursor_fields) > 1 and "Tessitura" in cursor_fields:
                            tessitura_index = cursor_fields.index("Tessitura")
                            if tessitura_index < len(new_row):
                                new_row[tessitura_index] = domain_mappings.get("tessitura", {}).get(tessitura_val, tessitura_val)
                        
                        cursor.updateRow(new_row)
                        updated_count += 1
                        
            arcpy.AddMessage("T3000 mapping: updated {} records".format(updated_count))
                
        except Exception as e:
            arcpy.AddError("Error in T3000 mapping: {}".format(str(e)))

    def debug_table_loading(self, auxiliary_tables):
        """Debug function to see what's actually loaded from tables"""
        arcpy.AddMessage("=== DEBUG TABLE LOADING ===")
        
        for table_key, table_path in auxiliary_tables.items():
            arcpy.AddMessage("Table: {} -> {}".format(table_key, table_path))
            
            if not arcpy.Exists(table_path):
                arcpy.AddMessage("  Table does not exist!")
                continue
                
            try:
                count = arcpy.GetCount_management(table_path)
                arcpy.AddMessage("  Record count: {}".format(count))
                
                fields = [f.name for f in arcpy.ListFields(table_path)]
                arcpy.AddMessage("  Fields: {}".format(fields))
                
                # Show first 3 records
                with arcpy.da.SearchCursor(table_path, fields[:5]) as cursor:  # Prime 5 colonne
                    arcpy.AddMessage("  First 3 records:")
                    for i, row in enumerate(cursor):
                        if i >= 3:
                            break
                        arcpy.AddMessage("    Record {}: {}".format(i+1, row))
                        
            except Exception as e:
                arcpy.AddMessage("  Error reading table: {}".format(str(e)))
        
        arcpy.AddMessage("=== END DEBUG ===")

    def _cleanup_geology_fields_safe(self, shapefile):
        """
        clean tmp fields
        """
        temp_fields = [
            "TIPO", "TIPOLOGIA", "ID_LIMITE", "ID_ELEST", "CONTORNO", "AFFIORA", 
            "PUN_GMO", "STATO", "ST018_", "ST018_ID", "UQ_CAR", "ID_TESS", 
            "ID_AMB", "SOMMERSO", "TempTESS", "TempTIPO", "TempSTATO", 
            "TMP_super", "TMP_infer", "Temp_UnGeo"
        ]
        
        existing_fields = [f.name for f in arcpy.ListFields(shapefile)]
        fields_to_delete = []
        
        for temp_field in temp_fields:
            # search similar fields name
            for existing_field in existing_fields:
                if (temp_field.upper() == existing_field.upper() or
                    temp_field.upper() in existing_field.upper()):
                    if existing_field not in fields_to_delete:
                        fields_to_delete.append(existing_field)
        
        if fields_to_delete:
            try:
                arcpy.DeleteField_management(shapefile, fields_to_delete)
                arcpy.AddMessage("Deleted {} temporary fields".format(len(fields_to_delete)))
            except Exception as e:
                arcpy.AddWarning("Could not delete some temporary fields: {}".format(str(e)))
                deleted_count = 0
                for field in fields_to_delete:
                    try:
                        arcpy.DeleteField_management(shapefile, [field])
                        deleted_count += 1
                    except:
                        pass
                arcpy.AddMessage("Deleted {} fields individually".format(deleted_count))

    def _set_default_values_safe(self, shapefile, default_values):
        """
        set default values
        """
        existing_fields = [f.name for f in arcpy.ListFields(shapefile)]
        update_fields = [field for field in default_values.keys() if field in existing_fields]
        
        if not update_fields:
            return
        
        try:
            updated_count = 0
            with arcpy.da.UpdateCursor(shapefile, update_fields) as cursor:
                for row in cursor:
                    new_row = []
                    row_updated = False
                    
                    for i, field_name in enumerate(update_fields):
                        current_val = row[i] if i < len(row) else None
                        
                        # if empty, null or "None" value, use default value
                        if (current_val is None or 
                            str(current_val).strip() == "" or 
                            str(current_val).strip().lower() == "none"):
                            new_row.append(default_values[field_name])
                            row_updated = True
                        else:
                            new_row.append(current_val)
                    
                    if row_updated:
                        cursor.updateRow(new_row)
                        updated_count += 1
            
            arcpy.AddMessage("Updated {} records with default values".format(updated_count))
            
        except Exception as e:
            arcpy.AddWarning("Error setting default values: {}".format(str(e)))

    def debug_gdb_complete_content(self):
        """
        Debug of GDB
        """
        arcpy.AddMessage("\n=== COMPLETE GDB CONTENT ANALYSIS ===")
        
        try:
            original_workspace = arcpy.env.workspace
            arcpy.env.workspace = self.input_gdb
            
            # List of datasets
            datasets = arcpy.ListDatasets("", "All")
            if datasets:
                arcpy.AddMessage("Found {} datasets:".format(len(datasets)))
                for ds in datasets:
                    arcpy.AddMessage("  Dataset: {} (type: {})".format(ds, arcpy.Describe(ds).datasetType))
                    
                    # Contenuts of dataset
                    arcpy.env.workspace = os.path.join(self.input_gdb, ds)
                    fcs = arcpy.ListFeatureClasses()
                    tables = arcpy.ListTables()
                    
                    if fcs:
                        for fc in fcs:
                            try:
                                count = arcpy.GetCount_management(fc)
                                fields = [f.name for f in arcpy.ListFields(fc)][:3]  # first 3 columns
                                arcpy.AddMessage("    FC: {} ({} records, fields: {})".format(fc, count, fields))
                            except:
                                arcpy.AddMessage("    FC: {} (error reading details)".format(fc))
                    
                    if tables:
                        for table in tables:
                            try:
                                count = arcpy.GetCount_management(table)
                                fields = [f.name for f in arcpy.ListFields(table)][:3]
                                arcpy.AddMessage("    Table: {} ({} records, fields: {})".format(table, count, fields))
                            except:
                                arcpy.AddMessage("    Table: {} (error reading details)".format(table))
            
            # Reset workspace
            arcpy.env.workspace = self.input_gdb
            
            # Root level
            root_fcs = arcpy.ListFeatureClasses()
            root_tables = arcpy.ListTables()
            
            if root_fcs:
                arcpy.AddMessage("Root Feature Classes ({})".format(len(root_fcs)))
                for fc in root_fcs:
                    try:
                        count = arcpy.GetCount_management(fc)
                        arcpy.AddMessage("  FC: {} ({} records)".format(fc, count))
                    except:
                        arcpy.AddMessage("  FC: {}".format(fc))
            
            if root_tables:
                arcpy.AddMessage("Root Tables ({}):".format(len(root_tables)))
                for table in root_tables:
                    try:
                        fields = [f.name for f in arcpy.ListFields(table)][:5]
                        count = arcpy.GetCount_management(table)
                        arcpy.AddMessage("  Table: {} ({} records, fields: {})".format(table, count, fields))
                    except:
                        arcpy.AddMessage("  Table: {}".format(table))
            
            arcpy.env.workspace = original_workspace
            arcpy.AddMessage("=== END GDB ANALYSIS ===\n")
            
        except Exception as e:
            arcpy.AddError("Error in GDB content debug: {}".format(str(e)))   

    def create_safe_auxiliary_table_loader(self):
        """
        load auxiliary tables from GDB
        """
        arcpy.AddMessage("Loading auxiliary table data with enhanced UTF-8 handling...")
        
        auxiliary_tables = {
            "t1000": os.path.join(self.input_gdb, "ST018", "ST018PolygonT0180801000"),
            "t2000": os.path.join(self.input_gdb, "ST018", "ST018PolygonT0180802000"), 
            "t3000": os.path.join(self.input_gdb, "ST018", "ST018PolygonT0180803000")
        }
        
        # Verify alternative: tables at root level
        available_layers = self.get_available_layers()
        tables = available_layers.get("tables", [])
        
        for table_key, table_path in auxiliary_tables.items():
            if not arcpy.Exists(table_path):
                # Search in root or other dataset
                table_name_variants = [
                    "T0180801000", "T0180802000", "T0180803000"
                ][["t1000", "t2000", "t3000"].index(table_key)]
                
                for table in tables:
                    if table_name_variants in table:
                        auxiliary_tables[table_key] = os.path.join(self.input_gdb, table)
                        break
        
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
                row[2] = domain_mappings["eta"].get(data.get("ETA_SUP", ""), "")  # ETA_Super
                row[3] = data.get("ETA_INF", "")  # TMP_infer
                row[4] = domain_mappings["eta"].get(data.get("ETA_INF", ""), "")  # ETA_Infer
                row[5] = data.get("S1_TIPO", "")  # Temp_UnGeo
                row[6] = domain_mappings["sigla_tipo"].get(data.get("S1_TIPO", ""), "")  # Tipo_UG
                row[7] = data.get("SIGLA1", "")  # Sigla1
                row[8] = data.get("SIGLA_CARTA", "")  # Sigla_UG
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

    def diagnose_geodatabase_quality(self):
        """
        Check layers ST011Polygon and ST018Polygon,
        verify topology and save a report in a CSV file.
        Include Pol_Uc/Pol_Gmo field through join over FEATURE_ID.
        """
        arcpy.AddMessage("="*60)
        arcpy.AddMessage("GEODATABASE QUALITY DIAGNOSIS")
        arcpy.AddMessage("="*60)

        # Layer target
        target_layers = ["ST011Polygon", "ST018Polygon"]
        available_layers_dict = self.get_available_layers()

        # create CSV 
        report_csv = os.path.join(self.workspace, "F" + self.foglio + "_geometry_issues.csv")
        
        with open(report_csv, "w", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            # Remove "tmp" fields
            writer.writerow(["Pol_ID", "Layer", "Problem"])

            for layer_name in target_layers:
                layer_found = False
                layer_path = None
                
                # Search layer in all datasets
                for dataset_name, layer_list in available_layers_dict.items():
                    if dataset_name == "tables":
                        continue
                        
                    if layer_name in layer_list:
                        layer_found = True
                        # Build complete path
                        if dataset_name == "root":
                            layer_path = os.path.join(self.input_gdb, layer_name)
                        else:
                            layer_path = os.path.join(self.input_gdb, dataset_name, layer_name)
                        break
                
                if not layer_found:
                    arcpy.AddWarning("Layer {} not found in GeoDB".format(layer_name))
                    continue

                arcpy.AddMessage("\n--- Checking polygon layer: {} ---".format(layer_name))
                
                try:
                    # Export to temporary shapefile for analysis
                    temp_shp = os.path.join(self.workspace_shape, layer_name.replace(".", "_") + "_check.shp")
                    if arcpy.Exists(temp_shp):
                        arcpy.Delete_management(temp_shp)

                    arcpy.FeatureClassToFeatureClass_conversion(
                        layer_path, self.workspace_shape, os.path.basename(temp_shp))

                    # Create temporary table for geometry errors
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

                    # Read error details
                    fields = [f.name for f in arcpy.ListFields(result_table)]
                    fid_field = "FEATURE_ID" if "FEATURE_ID" in fields else (
                        "FID" if "FID" in fields else ("OID" if "OID" in fields else "OBJECTID"))
                    problem_field = "PROBLEM" if "PROBLEM" in fields else fields[1]
                    detail_field = None
                    for candidate in ["DESCRIPTION", "MESSAGE", "CHECK"]:
                        if candidate in fields:
                            detail_field = candidate
                            break

                    # Find Pol_Uc or Pol_Gmo field
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

                    # Create mapping FEATURE_ID -> Pol_Uc/Pol_Gmo
                    pol_map = {}
                    if pol_field:
                        # Find the correct key field in the shapefile
                        key_field = None
                        for field_name in temp_shp_fields:
                            if field_name.upper() in ["OBJECTID", "FID", "OID"]:
                                key_field = field_name
                                break
                        
                        if key_field:
                            with arcpy.da.SearchCursor(temp_shp, [key_field, pol_field]) as pol_cursor:
                                for row in pol_cursor:
                                    pol_map[row[0]] = row[1]
                        else:
                            arcpy.AddWarning("Could not find key field for mapping in {}".format(layer_name))

                    # Write to CSV
                    cursor_fields = [fid_field, problem_field]
                    if detail_field:
                        cursor_fields.append(detail_field)

                    with arcpy.da.SearchCursor(result_table, cursor_fields) as cursor:
                        for row in cursor:
                            fid_val = row[0]
                            problem_val = row[1]
                            detail_val = row[2] if detail_field and len(row) > 2 else ""
                            
                            # Combine problem and detail for better description
                            full_problem = problem_val
                            if detail_val:
                                full_problem = "{} - {}".format(problem_val, detail_val)
                            
                            pol_val = pol_map.get(fid_val, "N/A") if pol_field else "N/A"

                            # Write only Pol_ID, Layer and Problem (without tmp_shp_FID)
                            writer.writerow([pol_val, layer_name, full_problem])

                    arcpy.AddMessage("  ✓ Written {} problems to report".format(count))

                except Exception as e:
                    arcpy.AddWarning("Error checking {}: {}".format(layer_name, str(e)))
                    
                finally:
                    # Cleanup temporary files
                    try:
                        if arcpy.Exists(temp_shp):
                            arcpy.Delete_management(temp_shp)
                        if arcpy.Exists(result_table):
                            arcpy.Delete_management(result_table)
                    except Exception as cleanup_error:
                        arcpy.AddWarning("Problems while cleaning: {}".format(str(cleanup_error)))

        arcpy.AddMessage("="*60)
        arcpy.AddMessage("END GEODATABASE DIAGNOSIS")
        arcpy.AddMessage("Report saved in: {}".format(report_csv))
        arcpy.AddMessage("="*60)

    def process_feature_class_optimized(self, input_fc, fc_name, config):
        """Unified and optimized feature class processing with streamlined workflow for GDB"""
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
                if not self.process_geology_polygons_final_fix(temp_files["shapefile"]):
                    raise RuntimeError("Geology polygon processing failed")
            elif special_processing == "geology_lines":
                # Apply standard processing
                self._process_standard_fields_and_domains(temp_files["shapefile"], config)
                # Then apply special processing
                if not self.process_geology_lines_standard(temp_files["shapefile"]):
                    raise RuntimeError("Geology lines standard processing failed")
            elif special_processing == "geology_lines_pieghe":
                # Apply standard processing
                self._process_standard_fields_and_domains(temp_files["shapefile"], config)
                # Then apply special processing
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
            
            # Step 6: Standardize field names and order
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
        """Generate temporary paths for processing inside a GDB (avoids shp limits)"""
        base_name = self.sanitize_shapefile_name(fc_name)
        return {
            "shapefile": os.path.join(self.temp_gdb, base_name),
        }

    def _convert_and_keep_projection(self, input_fc, temp_files):
        """Convert input feature class into temp GDB maintaining projection (no shp yet)"""
        # Clean existing temp
        if arcpy.Exists(temp_files["shapefile"]):
            arcpy.Delete_management(temp_files["shapefile"])
        
        out_name = os.path.basename(temp_files["shapefile"])  # FC name in GDB
        arcpy.FeatureClassToFeatureClass_conversion(
            input_fc, self.temp_gdb, out_name
        )
        
        # Load and store reference system (SR from the first processed layer)
        if self.input_sr is None:
            desc = arcpy.Describe(temp_files["shapefile"])
            self.input_sr = desc.spatialReference
            arcpy.AddMessage("---> Using input projection: {} <---".format(self.input_sr.name))

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
        """Add and populate Foglio field with domain mapping from d_foglio.dbf"""
        existing_fields = {f.name.upper(): f.name for f in arcpy.ListFields(shapefile)}
        
        if "FOGLIO" not in existing_fields:
            arcpy.AddField_management(shapefile, "Foglio", "TEXT", field_length=254)
        
        # Load domain mappings for foglio
        foglio_domain = self.load_domain_mappings("d_foglio.dbf", code_field="N1", desc_field_pattern="N2", is_gpkg_table=False)
        
        if not foglio_domain:
            arcpy.AddWarning("Domain mapping for Foglio not found in d_foglio.dbf - using raw values")
            # Fallback to original behavior
            arcpy.CalculateField_management(shapefile, "Foglio", "'{}'".format(self.foglio), "PYTHON3")
            return
        
        # Apply domain mapping to convert foglio value
        mapped_foglio = foglio_domain.get(self.foglio, self.foglio)  # Use original if not found in domain
        
        # Log the mapping result
        if mapped_foglio != self.foglio:
            arcpy.AddMessage("Foglio mapped: '{}' -> '{}'".format(self.foglio, mapped_foglio))
        else:
            arcpy.AddMessage("Foglio value '{}' not found in domain, using original value".format(self.foglio))
        
        # Set the mapped value
        arcpy.CalculateField_management(shapefile, "Foglio", "'{}'".format(mapped_foglio), "PYTHON3")

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

        # If working inside a GDB feature class, avoid trying to delete mandatory geometry fields
        is_gdb_fc = not shapefile.lower().endswith('.shp')
        if is_gdb_fc:
            for mandatory in ("Shape_Length", "Shape_Area"):
                if mandatory in fields_to_delete:
                    fields_to_delete.remove(mandatory)
        
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
        
        # INIZIALIZZA PRIMA I WORKSPACE E VALIDA
        try:
            # Setup workspace
            self.setup_workspace()

            # Validate inputs - QUESTA È LA FUNZIONE CHE IMPOSTA self.foglio
            self.validate_inputs()
            
            # ORA PUOI CREARE IL FILE LOG con il foglio estratto
            log_file = os.path.join(self.workspace, f"F{self.foglio}_processing.log")
            file_handler = open(log_file, 'w', encoding='utf-8')
            
            def log_to_file(message):
                """Helper function to write to log file"""
                timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
                file_handler.write(f"{timestamp} - {message}\n")
                file_handler.flush()
            
            log_to_file("CARG Data Conversion Started")
            log_to_file(f"Input File Geodatabase: {self.input_gdb}")
            log_to_file(f"Extracted FoglioGeologico: {self.foglio}")
            
            # Continua con il resto del processing...
            self.diagnose_geodatabase_quality()  # comment to skip GDB quality check
            log_to_file("Geodatabase quality diagnosis completed")

            # Log the extracted foglio value
            arcpy.AddMessage("Extracted FoglioGeologico: {}".format(self.foglio))
            log_to_file("Extracted FoglioGeologico: {}".format(self.foglio))
            
            # Check domini folder
            if not os.path.exists(self.domini_path):
                arcpy.AddWarning("Domini folder not found - domain mappings will be skipped")
                log_to_file("WARNING: Domini folder not found - domain mappings will be skipped")
            
            # Get available layers with caching
            available_layers = self.get_available_layers()
            arcpy.AddMessage("Found {} layers in GeoDB".format(len(available_layers)))
            log_to_file("Found {} layers in GeoDB".format(len(available_layers)))
            arcpy.AddMessage("="*60)
            log_to_file("="*60)
            
            if not available_layers:
                log_to_file("ERROR: No layers found in input File Geodatabase!")
                raise RuntimeError("No layers found in input File Geodatabase!")
            
            # Process all feature classes
            processed_count = 0
            failed_count = 0
            
            for fc_name, config in self.feature_configs.items():
                try:
                    # Find layer
                    found_layer = self.find_layer_by_pattern(available_layers, config["search_patterns"])
                    
                    if not found_layer:
                        arcpy.AddWarning("Layer not found for {}".format(fc_name))
                        log_to_file("WARNING: Layer not found for {}".format(fc_name))
                        failed_count += 1
                        continue
                    
                    # Build input path
                    input_fc = os.path.join(self.input_gdb, found_layer)
                    
                    # Process the feature class
                    arcpy.AddMessage("Processing {} -> {}...".format(fc_name, config["output_name"]))
                    log_to_file("Processing {} -> {}...".format(fc_name, config["output_name"]))
                    
                    if self.process_feature_class_optimized(input_fc, fc_name, config):
                        processed_count += 1
                        log_to_file("SUCCESS: Processed {}".format(fc_name))
                    else:
                        failed_count += 1
                        log_to_file("FAILED: Processing {}".format(fc_name))
                        
                except Exception as e:
                    arcpy.AddError("Error processing {}: {}".format(fc_name, str(e)))
                    log_to_file("ERROR processing {}: {}".format(fc_name, str(e)))
                    failed_count += 1
            
            # APPEND geology lines
            arcpy.AddMessage("="*60)
            log_to_file("="*60)
            arcpy.AddMessage("FINAL PROCESSING: COMBINING GEOLOGY LINES")
            log_to_file("FINAL PROCESSING: COMBINING GEOLOGY LINES")
            arcpy.AddMessage("="*60)
            log_to_file("="*60)
            self.combine_geology_lines_optimized()
            log_to_file("Geology lines combination completed")
            
            # Field standardization already applied per-layer and post-append; skipping global pass to save time
            
            # Final cleanup
            self.final_cleanup_optimized()
            log_to_file("Final cleanup completed")
            
            # Report results
            processing_time = time.time() - start_time
            arcpy.AddMessage("Processing completed in {:.1f} seconds!".format(processing_time))
            arcpy.AddMessage("Successfully processed: {} | Failed: {}".format(processed_count, failed_count))
            
            # Log dei risultati finali
            log_to_file("Processing completed in {:.1f} seconds!".format(processing_time))
            log_to_file("Successfully processed: {} | Failed: {}".format(processed_count, failed_count))
            log_to_file("SCRIPT COMPLETED SUCCESSFULLY")
            
            # List output files
            self._report_output_files()
            
        except Exception as e:
            # Se c'è un errore prima della creazione del file_handler, crealo ora
            if 'file_handler' not in locals():
                try:
                    # Prova a creare il file log con un nome generico se self.foglio non è disponibile
                    log_file = os.path.join(self.workspace, "processing_error.log")
                    file_handler = open(log_file, 'w', encoding='utf-8')
                    
                    def log_to_file(message):
                        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
                        file_handler.write(f"{timestamp} - {message}\n")
                        file_handler.flush()
                    
                    log_to_file(f"ERROR before foglio extraction: {str(e)}")
                except:
                    pass  # Se non riesce a creare il log, continua comunque
            
            arcpy.AddError("Script failed: {}".format(str(e)))
            import traceback
            arcpy.AddError(traceback.format_exc())
            
            # Se file_handler esiste, logga l'errore
            if 'file_handler' in locals():
                log_to_file("SCRIPT FAILED: {}".format(str(e)))
                log_to_file("TRACEBACK: {}".format(traceback.format_exc()))
            
            raise
        
        finally:
            # CHIUDI IL FILE HANDLER se esiste
            if 'file_handler' in locals():
                file_handler.close()

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
    """Main execution function with enhanced error handling for File Geodatabase"""
    try:
        # Get and validate parameters
        input_gdb = arcpy.GetParameterAsText(0)
     
        if not input_gdb or not input_gdb.strip():
            arcpy.AddError("Input File Geodatabase parameter is required")
            sys.exit(1)
        
        # Log start information
        arcpy.AddMessage("="*60)
        arcpy.AddMessage("CARG DATA CONVERSION - OPTIMIZED VERSION FOR FILE GEODATABASE")
        arcpy.AddMessage("="*60)
        arcpy.AddMessage("Input File Geodatabase: {}".format(input_gdb))
        arcpy.AddMessage("Start time: {}".format(time.strftime("%Y-%m-%d %H:%M:%S")))
        arcpy.AddMessage("="*60)
        
        # Create processor and run
        processor = CARGProcessor(input_gdb)
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
       
