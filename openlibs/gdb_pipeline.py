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


def process_gdb(gdb_path, domini_dir=None, output_dir=None):
    workspace = os.path.dirname(os.path.abspath(gdb_path))
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

    layers = fiona.listlayers(gdb_path)

    # FoglioGeologico
    foglio = None
    for ln in layers:
        try:
            g = gpd.read_file(gdb_path, layer=ln)
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

    configs = {
        'ST010Point': {'search':['ST010Point','main.ST010Point','main/ST010Point'], 'output':'geomorfologia_punti.shp',
                       'fields': {'Pun_Gmo':'Pun_Gmo','Tipo':'Tipo_Gmrf','Tipologia':'Tipologia','Stato':'Stato','Direzio':'Direzione'},
                       'domains': {'Tipo_Gmrf': ('d_10_tipo.dbf','Tipo_G_txt'),'Tipologia':('d_tipologia.dbf','Tipol_txt'),'Stato':('d_stato.dbf','Stato_txt')}},
        'ST011Polygon': {'search':['ST011Polygon','main.ST011Polygon','main/ST011Polygon'], 'output':'geomorfologia_poligoni.shp',
                         'fields': {'Pol_Gmo':'Pol_Gmo','Tipo':'Tipo_Gmrf','Tipologia':'Tipologia','Stato':'Stato','Direzio':'Direzione'},
                         'domains': {'Tipo_Gmrf': ('d_11_tipo.dbf','Tipo_G_txt'),'Tipologia':('d_tipologia.dbf','Tipol_txt'),'Stato':('d_stato.dbf','Stato_txt')}},
        'ST012Polyline': {'search':['ST012Polyline','main.ST012Polyline','main/ST012Polyline'], 'output':'geomorfologia_linee.shp',
                          'fields': {'Lin_Gmo':'Lin_Gmo','Label':'Label','Tipo':'Tipo_Gmrf','Tipologia':'Tipologia','Stato':'Stato'},
                          'domains': {'Tipo_Gmrf': ('d_12_tipo.dbf','Tipo_G_txt'),'Tipologia':('d_tipologia.dbf','Tipol_txt'),'Stato':('d_stato.dbf','Stato_txt')}},
        'ST013Point': {'search':['ST013Point','main.ST013Point','main/ST013Point'], 'output':'risorse_prospezioni.shp',
                       'fields': {'Num_Ris':'Num_Ris','Label1':'Label1','Label2':'Label2','Label3':'Label3','TIPO':'Tipo'},
                       'domains': {'Tipo': ('d_13_tipo.dbf','Tipo_txt')}},
        'ST018Polyline': {'search':['ST018Polyline','main.ST018Polyline','main/ST018Polyline'], 'output':'geologia_linee.shp',
                          'fields': {'Tipo':'Tipo_geo','Tipologia':'Tipologia','Contorno':'Contorno','Affiora':'Affiora','Direzio':'Direzione'},
                          'domains': {'Tipo_geo': ('d_st018_line.dbf','Tipo_g_txt'),'Tipologia':('d_tipologia.dbf','Tipol_txt'),'Contorno':('d_st018_contorno.dbf','Cont_txt'),'Affiora':('d_st018_affiora.dbf','Affior_txt')},
                          'extra_fixed': {'Fase_txt':'non applicabile/non classificabile'}},
        'ST018Polygon': {'search':['ST018Polygon','main.ST018Polygon','main/ST018Polygon'], 'output':'geologia_poligoni.shp',
                         'fields': {'Pol_Uc':'Pol_Uc','Uc_Lege':'Uc_Lege','Direzio':'Direzione'}, 'domains': {}},
        'ST019Point': {'search':['ST019Point','main.ST019Point','main/ST019Point'], 'output':'geologia_punti.shp',
                       'fields': {'Num_Oss':'Num_Oss','Quota':'Quota','Inclina':'Inclinaz','Immersio':'Immersione','Direzio':'Direzione','Tipo':'Tipo_geo','Tipologia':'Tipologia','Fase':'Fase','Verso':'Verso','Asimmetria':'Asimmetria'},
                       'domains': {'Tipo_geo': ('d_19_tipo.dbf','Tipo_G_txt'),'Tipologia':('d_tipologia.dbf','Tipol_txt'),'Fase':('d_fase.dbf','Fase_txt'),'Verso':('d_verso.dbf','Verso_txt'),'Asimmetria':('d_asimmetria.dbf','Asimm_txt')}},
        'ST021Polyline': {'search':['ST021Polyline','main.ST021Polyline','main/ST021Polyline'], 'output':'geologia_linee_pieghe.shp',
                          'fields': {'Tipo':'Tipo_geo','Tipologia':'Tipologia','Fase':'Fase','Direzio':'Direzione'},
                          'domains': {'Tipo_geo': ('d_st021.dbf','Tipo_g_txt'),'Tipologia':('d_tipologia.dbf','Tipol_txt'),'Fase':('d_fase.dbf','Fase_txt')},
                          'extra_fixed': {'Affior_txt':'non applicabile','Cont_txt':'no'}},
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

    def process_one(cfg):
        ln = find_layer(cfg['search'])
        if not ln:
            return False
        g = gpd.read_file(gdb_path, layer=ln)
        out = pd.DataFrame(index=g.index)
        for src, dst in cfg['fields'].items():
            out[dst] = g[src] if src in g.columns else ''
        for k, v in cfg.get('extra_fixed', {}).items():
            out[k] = v
        out['Foglio'] = foglio_txt
        for src, (domfile, target) in cfg.get('domains', {}).items():
            dom = _load_domain_dbf(os.path.join(domini_dir, domfile))
            mapped_src = cfg['fields'].get(src, src)
            if mapped_src in out.columns:
                out = _apply_domain(out, mapped_src, target, dom)
        g_out = gpd.GeoDataFrame(out, geometry=g.geometry, crs=g.crs)
        g_out = _ensure_text_len(g_out, [c for c in g_out.columns if c != 'geometry'])
        g_out.to_file(os.path.join(output_dir, cfg['output']), driver='ESRI Shapefile', encoding='utf-8')
        return True

    processed = 0
    for _, cfg in configs.items():
        if process_one(cfg):
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

