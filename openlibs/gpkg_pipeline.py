import os
import time
import pandas as pd
import geopandas as gpd
import fiona
from dbfread import DBF


def _load_domain_dbf(path, code_field='CODE', desc_pattern='DESC'):
    if not os.path.exists(path):
        return {}
    try:
        recs = list(DBF(path, encoding='utf-8', ignore_missing_memofile=True))
        if not recs:
            return {}
        desc_field = None
        for k in recs[0].keys():
            if desc_pattern.upper() in k.upper():
                desc_field = k
                break
        if not desc_field or code_field not in recs[0]:
            return {}
        m = {}
        for r in recs:
            c = r.get(code_field)
            d = r.get(desc_field)
            if c is None or d is None:
                continue
            d = str(d).strip()
            m[str(c).strip()] = d
            if isinstance(c, (int, float)):
                m[c] = d
        return m
    except Exception:
        return {}


def _apply_domain(df, src_col, out_col, mapping):
    if not mapping:
        df[out_col] = ''
        return df
    def _map(v):
        if pd.isna(v):
            return ''
        return mapping.get(v, mapping.get(str(v).strip(), ''))
    df[out_col] = df[src_col].apply(_map) if src_col in df.columns else ''
    return df


def _ensure_text_len(df, cols, maxlen=254):
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str).str.slice(0, maxlen)
    return df


def _save_shp(gdf, path):
    drop_cols = [c for c in gdf.columns if c.upper().startswith('SHAPE_')]
    gdf = gdf.drop(columns=drop_cols, errors='ignore')
    gdf.to_file(path, driver='ESRI Shapefile', encoding='utf-8')
    return path


def process_gpkg(gpkg_path, domini_dir=None, output_dir=None):
    workspace = os.path.dirname(os.path.abspath(gpkg_path))
    domini_dir = domini_dir or os.path.join(workspace, 'domini')
    output_dir = output_dir or os.path.join(workspace, 'output')

    try:
        os.makedirs(output_dir, exist_ok=True)
        test = os.path.join(output_dir, f'.write_test_{int(time.time())}')
        with open(test, 'w', encoding='utf-8') as f:
            f.write('ok')
        os.remove(test)
    except Exception:
        ts = time.strftime('%Y%m%d_%H%M%S')
        output_dir = os.path.join(workspace, f'output_{ts}')
        os.makedirs(output_dir, exist_ok=True)

    layers = fiona.listlayers(gpkg_path)

    # FoglioGeologico
    foglio = None
    for ln in layers:
        try:
            g = gpd.read_file(gpkg_path, layer=ln)
            if 'FoglioGeologico' in g.columns:
                v = g['FoglioGeologico'].dropna().astype(str).str.strip()
                if not v.empty:
                    foglio = v.iloc[0]
                    break
        except Exception:
            pass
    if foglio is None:
        raise RuntimeError('FoglioGeologico non trovato')
    foglio_map = _load_domain_dbf(os.path.join(domini_dir, 'd_foglio.dbf'), code_field='N1', desc_pattern='N2')
    foglio_txt = foglio_map.get(foglio, foglio)

    # Configs aligned to PRO pipeline (subset, extend as needed)
    configs = {
        'ST010Point': {
            'search': ['ST010Point','main.ST010Point'],
            'output': 'geomorfologia_punti.shp',
            'fields': {'Pun_Gmo':'Pun_Gmo','Tipo':'Tipo_Gmrf','Tipologia':'Tipologia','Stato':'Stato','Direzio':'Direzione'},
            'domains': {
                'Tipo_Gmrf': ('d_10_tipo.dbf','Tipo_G_txt'),
                'Tipologia': ('d_tipologia.dbf','Tipol_txt'),
                'Stato': ('d_stato.dbf','Stato_txt'),
            },
            'keep': ['Pun_Gmo','Foglio','Tipo_Gmrf','Stato','Tipologia','Direzione']
        },
        'ST011Polygon': {
            'search': ['ST011Polygon','main.ST011Polygon'],
            'output': 'geomorfologia_poligoni.shp',
            'fields': {'Pol_Gmo':'Pol_Gmo','Tipo':'Tipo_Gmrf','Tipologia':'Tipologia','Stato':'Stato','Direzio':'Direzione'},
            'domains': {
                'Tipo_Gmrf': ('d_11_tipo.dbf','Tipo_G_txt'),
                'Tipologia': ('d_tipologia.dbf','Tipol_txt'),
                'Stato': ('d_stato.dbf','Stato_txt'),
            },
            'keep': ['Pol_Gmo','Foglio','Tipo_Gmrf','Stato','Tipologia','Direzione']
        },
        'ST012Polyline': {
            'search': ['ST012Polyline','main.ST012Polyline'],
            'output': 'geomorfologia_linee.shp',
            'fields': {'Lin_Gmo':'Lin_Gmo','Label':'Label','Tipo':'Tipo_Gmrf','Tipologia':'Tipologia','Stato':'Stato'},
            'domains': {
                'Tipo_Gmrf': ('d_12_tipo.dbf','Tipo_G_txt'),
                'Tipologia': ('d_tipologia.dbf','Tipol_txt'),
                'Stato': ('d_stato.dbf','Stato_txt'),
            },
            'keep': ['Lin_Gmo','Label','Foglio','Tipo_Gmrf','Stato','Tipologia']
        },
        'ST013Point': {
            'search': ['ST013Point','main.ST013Point'],
            'output': 'risorse_prospezioni.shp',
            'fields': {'Num_Ris':'Num_Ris','Label1':'Label1','Label2':'Label2','Label3':'Label3','TIPO':'Tipo'},
            'domains': {'Tipo': ('d_13_tipo.dbf','Tipo_txt')},
            'keep': ['Num_Ris','Label1','Label2','Label3','Foglio','Tipo']
        },
        'ST018Polyline': {
            'search': ['ST018Polyline','main.ST018Polyline'],
            'output': 'geologia_linee.shp',
            'fields': {'Tipo':'Tipo_geo','Tipologia':'Tipologia','Contorno':'Contorno','Affiora':'Affiora','Direzio':'Direzione'},
            'domains': {'Tipo_geo': ('d_st018_line.dbf','Tipo_g_txt'),'Tipologia':('d_tipologia.dbf','Tipol_txt'),'Contorno':('d_st018_contorno.dbf','Cont_txt'),'Affiora':('d_st018_affiora.dbf','Affior_txt')},
            'extra_fixed': {'Fase_txt':'non applicabile/non classificabile'},
            'keep': ['Foglio','Fase','Affiora','Tipo_Geo','Contorno','Tipologia','Direzione']
        },
        'ST018Polygon': {
            'search': ['ST018Polygon','main.ST018Polygon'],
            'output': 'geologia_poligoni.shp',
            'fields': {'Pol_Uc':'Pol_Uc','Uc_Lege':'Uc_Lege','Direzio':'Direzione','UQ_CAR':'UQ_CAR','UC_LEGE':'UC_LEGE','ID_TESS':'ID_TESS','SOMMERSO':'SOMMERSO'},
            'domains': {},
            'keep': ['Pol_Uc','Uc_Lege','Foglio','Direzione','Tipo_UQ','Stato_UQ','ETA_Super','ETA_Infer','Tipo_UG','Tessitura','Sigla1','Sigla_UG','Nome','Legenda','Sommerso']
        },
        'ST019Point': {
            'search': ['ST019Point','main.ST019Point'],
            'output': 'geologia_punti.shp',
            'fields': {'Num_Oss':'Num_Oss','Quota':'Quota','Inclina':'Inclinaz','Immersio':'Immersione','Direzio':'Direzione','Tipo':'Tipo_geo','Tipologia':'Tipologia','Fase':'Fase','Verso':'Verso','Asimmetria':'Asimmetria'},
            'domains': {'Tipo_geo': ('d_19_tipo.dbf','Tipo_G_txt'),'Tipologia':('d_tipologia.dbf','Tipol_txt'),'Fase':('d_fase.dbf','Fase_txt'),'Verso':('d_verso.dbf','Verso_txt'),'Asimmetria':('d_asimmetria.dbf','Asimm_txt')},
            'keep': ['Num_Oss','Quota','Inclinaz','Immersione','Direzione','Tipo_Geo','Tipologia','Fase','Verso','Asimmetria','Foglio']
        },
        'ST021Polyline': {
            'search': ['ST021Polyline','main.ST021Polyline'],
            'output': 'geologia_linee_pieghe.shp',
            'fields': {'Tipo':'Tipo_geo','Tipologia':'Tipologia','Fase':'Fase','Direzio':'Direzione'},
            'domains': {'Tipo_geo': ('d_st021.dbf','Tipo_g_txt'),'Tipologia':('d_tipologia.dbf','Tipol_txt'),'Fase':('d_fase.dbf','Fase_txt')},
            'extra_fixed': {'Affior_txt':'non applicabile','Cont_txt':'no'},
            'keep': ['Foglio','Fase','Affiora','Tipo_Geo','Contorno','Tipologia','Direzione']
        },
    }

    def find_layer(name_variants):
        for nv in name_variants:
            if nv in layers:
                return nv
        for nv in name_variants:
            for L in layers:
                if nv.lower() in L.lower():
                    return L
        return None

    def _standardize_and_save(gdf, out_path, keep_fields=None, final_rename=None):
        # Ensure columns exist
        keep_fields = keep_fields or []
        for c in keep_fields:
            if c not in gdf.columns and c != 'geometry':
                gdf[c] = ''
        # Apply final renames before ordering
        if final_rename:
            gdf = gdf.rename(columns=final_rename)
        # Order columns: keep_fields first (when present)
        cols = [c for c in keep_fields if c in gdf.columns]
        # Append remaining non-geometry columns
        cols += [c for c in gdf.columns if c not in cols and c != 'geometry']
        gdf = gdf[cols + (['geometry'] if 'geometry' in gdf.columns else [])]
        gdf = _ensure_text_len(gdf, [c for c in gdf.columns if c != 'geometry'])
        _save_shp(gdf, out_path)

    def process_one(cfg):
        ln = find_layer(cfg['search'])
        if not ln:
            return False
        g = gpd.read_file(gpkg_path, layer=ln)
        out = pd.DataFrame(index=g.index)
        for src, dst in cfg['fields'].items():
            if src in g.columns:
                out[dst] = g[src]
            else:
                out[dst] = ''
        for k, v in cfg.get('extra_fixed', {}).items():
            out[k] = v
        out['Foglio'] = foglio_txt
        # Domain mappings
        for src, (domfile, target) in cfg.get('domains', {}).items():
            dom = _load_domain_dbf(os.path.join(domini_dir, domfile))
            mapped_src = cfg['fields'].get(src, src)
            if mapped_src in out.columns:
                out = _apply_domain(out, mapped_src, target, dom)
        # Final renames per output
        final_rename = {}
        out_name = cfg['output']
        if out_name == 'geomorfologia_punti.shp':
            final_rename = {'Tipo_G_txt':'Tipo_Gmrf','Tipol_txt':'Tipologia','Stato_txt':'Stato'}
        elif out_name == 'geomorfologia_linee.shp':
            final_rename = {'Tipo_G_txt':'Tipo_Gmrf','Tipol_txt':'Tipologia','Stato_txt':'Stato'}
        elif out_name == 'geomorfologia_poligoni.shp':
            final_rename = {'Tipo_G_txt':'Tipo_Gmrf','Tipol_txt':'Tipologia','Stato_txt':'Stato'}
        elif out_name == 'risorse_prospezioni.shp':
            final_rename = {'Tipo_txt':'Tipo'}
        elif out_name in ('geologia_linee.shp','geologia_linee_pieghe.shp'):
            final_rename = {'Tipo_g_txt':'Tipo_Geo','Tipol_txt':'Tipologia','Cont_txt':'Contorno','Affior_txt':'Affiora','Fase_txt':'Fase'}
        elif out_name == 'geologia_punti.shp':
            final_rename = {'Tipo_G_txt':'Tipo_Geo','Tipol_txt':'Tipologia','Fase_txt':'Fase','Verso_txt':'Verso','Asimm_txt':'Asimmetria'}
        g_out = gpd.GeoDataFrame(out, geometry=g.geometry, crs=g.crs)
        _standardize_and_save(g_out, os.path.join(output_dir, cfg['output']), keep_fields=cfg.get('keep'), final_rename=final_rename)
        return True

    processed = 0
    # First pass standard layers except ST018Polygon (special)
    for key, cfg in configs.items():
        if key == 'ST018Polygon':
            continue
        if process_one(cfg):
            processed += 1

    # Special handling for ST018Polygon
    pol_cfg = configs['ST018Polygon']
    ln = find_layer(pol_cfg['search'])
    if ln:
        gp = gpd.read_file(gpkg_path, layer=ln)
        df = pd.DataFrame(index=gp.index)
        # Base fields
        for src, dst in pol_cfg['fields'].items():
            df[dst] = gp[src] if src in gp.columns else ''
        df['Foglio'] = foglio_txt
        # Direzione from Direzio if present
        if 'Direzione' not in df.columns and 'Direzio' in gp.columns:
            df['Direzione'] = gp['Direzio']
        # SOMMERSO mapping
        if 'SOMMERSO' in gp.columns:
            def _som(v):
                if str(v) in ('1','1.0') or v == 1: return 'SI'
                if str(v) in ('2','2.0') or v == 2: return 'NO'
                return ''
            df['Sommerso_'] = gp['SOMMERSO'].apply(_som)
        else:
            df['Sommerso_'] = ''
        # Join auxiliary tables if available
        layers_set = set(layers)
        # T1000
        if 'main.T0180801000' in layers_set:
            t1000 = gpd.read_file(gpkg_path, layer='main.T0180801000')
            t1000 = t1000[['Uq_Car','Tipo','Stato']]
            df = df.join(t1000.set_index('Uq_Car'), on='UQ_CAR') if 'UQ_CAR' in df.columns else df
            # map domains
            dom_tipo = _load_domain_dbf(os.path.join(domini_dir,'d_1000_tipo.dbf'))
            dom_stato= _load_domain_dbf(os.path.join(domini_dir,'d_stato.dbf'))
            if 'Tipo' in df.columns: df = _apply_domain(df, 'Tipo', 'Tipo_UQ', dom_tipo)
            if 'Stato' in df.columns: df = _apply_domain(df, 'Stato', 'Stato_UQ', dom_stato)
        else:
            df['Tipo_UQ'] = ''
            df['Stato_UQ'] = ''
        # T2000
        if 'main.T0180802000' in layers_set:
            t2000 = gpd.read_file(gpkg_path, layer='main.T0180802000')
            t2000 = t2000[['Uc_Lege','Eta_Sup','Eta_Inf','S1_Tipo','Sigla1','Sigla_Carta','Nome','Legenda']]
            df = df.join(t2000.set_index('Uc_Lege'), on='UC_LEGE') if 'UC_LEGE' in df.columns else df
            dom_eta = _load_domain_dbf(os.path.join(domini_dir,'d_t2000_eta.dbf'))
            dom_s1  = _load_domain_dbf(os.path.join(domini_dir,'d_2000_SiglaTipo.dbf'))
            if 'Eta_Sup' in df.columns: df = _apply_domain(df, 'Eta_Sup', 'ETA_super', dom_eta)
            if 'Eta_Inf' in df.columns: df = _apply_domain(df, 'Eta_Inf', 'ETA_infer', dom_eta)
            if 'S1_Tipo' in df.columns: df = _apply_domain(df, 'S1_Tipo', 'tipo_ug', dom_s1)
            # Copy extras
            for c_src, c_dst in [('Sigla1','Sigla1'),('Sigla1','Sigla_ug'),('Nome','Nome'),('Legenda','Legenda')]:
                if c_src in df.columns: df[c_dst] = df[c_src]
        else:
            for c in ['ETA_super','ETA_infer','tipo_ug','Sigla1','Sigla_ug','Nome','Legenda']:
                if c not in df.columns: df[c] = ''
        # T3000
        if 'main.T0180803000' in layers_set:
            t3000 = gpd.read_file(gpkg_path, layer='main.T0180803000')
            t3000 = t3000[['Id_Tess','Tessitura']]
            df = df.join(t3000.set_index('Id_Tess'), on='ID_TESS') if 'ID_TESS' in df.columns else df
            # If domain for Tessitura exists, try mapping; else keep text
            # dom_tess = _load_domain_dbf(os.path.join(domini_dir,'d_t3000.dbf'))
        else:
            if 'Tessitura' not in df.columns: df['Tessitura'] = ''
        # Final renames for polygons
        rename_map = {'ETA_super':'ETA_Super','ETA_infer':'ETA_Infer','tipo_ug':'Tipo_UG','Sigla_ug':'Sigla_UG','Sommerso_':'Sommerso'}
        g_out = gpd.GeoDataFrame(df, geometry=gp.geometry, crs=gp.crs)
        _standardize_and_save(g_out, os.path.join(output_dir, pol_cfg['output']), keep_fields=pol_cfg.get('keep'), final_rename=rename_map)
        processed += 1

    # Append folds
    pieghe = os.path.join(output_dir, 'geologia_linee_pieghe.shp')
    linee = os.path.join(output_dir, 'geologia_linee.shp')
    if os.path.exists(pieghe) and os.path.exists(linee):
        gL = gpd.read_file(linee)
        gP = gpd.read_file(pieghe)
        gM = pd.concat([gL, gP], ignore_index=True)
        gM.to_file(linee, driver='ESRI Shapefile', encoding='utf-8')
        try:
            os.remove(pieghe)
        except Exception:
            pass

    return output_dir, processed
