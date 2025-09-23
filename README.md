# CARG Data Conversion Tool - gpkg to shape

**Versione**: 1.0  
**Autore**: Patrizio Petricca (patrizio.petricca@isprambiente.it)

Un tool Python per ArcGIS che converte dati geologici da GeoPackage codificati CARG in shapefile standardizzati.

```
   /\                                       /\  
  /  \         BDgpkg2shape v1.0           /  \  
 /____\ patrizio.petricca@isprambiente.it /____\  
```

## Descrizione

Questo script converte i dati geologici dal formato GeoPackage codificato CARG (Carta Geologica d'Italia) in shapefile standardizzati, applicando le mappature dei domini e la standardizzazione dei campi secondo le specifiche CARG.

## Caratteristiche principali

- ✅ Conversione automatica da GeoPackage CARG a shapefile
- ✅ Mappatura dei domini da tabelle ausiliarie
- ✅ Standardizzazione dei nomi dei campi e ordine
- ✅ Gestione ottimizzata delle codifiche UTF-8
- ✅ Controllo qualità geometrie con report CSV
- ✅ Processing specializzato per diverse tipologie di layer
- ✅ Combinazione automatica delle linee geologiche
- ✅ Gestione robusta degli errori e cleanup automatico

## Requisiti

### Software
- **ArcGIS Desktop** 10.x o superiore con licenza ArcInfo
- **Python 2.7** (integrato con ArcGIS Desktop)
- **ArcPy** (modulo Python di ArcGIS)

### Struttura dati di input
```
workspace/
├── input.gpkg                 # GeoPackage CARG di input
└── domini/                    # Cartella con tabelle dei domini (.dbf)
    ├── d_10_tipo.dbf
    ├── d_11_tipo.dbf
    ├── d_12_tipo.dbf
    ├── d_13_tipo.dbf
    ├── d_19_tipo.dbf
    ├── d_st018_line.dbf
    ├── d_st021.dbf
    ├── d_1000_tipo.dbf
    ├── d_2000_SiglaTipo.dbf
    ├── d_t2000_eta.dbf
    ├── d_t3000.dbf
    ├── d_st018_contorno.dbf
    ├── d_st018_affiora.dbf
    ├── d_tipologia.dbf
    ├── d_stato.dbf
    ├── d_fase.dbf
    ├── d_verso.dbf
    └── d_asimmetria.dbf
```

## Layer supportati

Il tool processa automaticamente i seguenti layer CARG:

| Layer CARG | Output Shapefile | Descrizione |
|------------|------------------|-------------|
| ST010Point | geomorfologia_punti.shp | Punti geomorfologici |
| ST011Polygon | geomorfologia_poligoni.shp | Poligoni geomorfologici |
| ST012Polyline | geomorfologia_linee.shp | Linee geomorfologiche |
| ST013Point | risorse_prospezioni.shp | Punti di risorse e prospezioni |
| ST018Polyline | geologia_linee.shp | Linee geologiche |
| ST018Polygon | geologia_poligoni.shp | Poligoni geologici |
| ST019Point | geologia_punti.shp | Punti geologici |
| ST021Polyline | *merged* → geologia_linee.shp | Linee di pieghe (unite alle linee geologiche) |

## Utilizzo

### Come Script ArcGIS
1. Apri ArcGIS Desktop
2. Vai su **Geoprocessing** → **ArcToolbox**
3. Aggiungi lo script come tool personalizzato
4. Imposta il parametro:
   - **Input GeoPackage**: percorso del file .gpkg CARG

### Da Command Line
```python
import arcpy
import sys
sys.path.append('path/to/script/directory')
from BDgpkg2shape import main

# Imposta il parametro
arcpy.SetParameterAsText(0, r"C:\path\to\your\data.gpkg")

# Esegui
main()
```

### Come Modulo Python
```python
from BDgpkg2shape import CARGProcessor

# Crea il processore
processor = CARGProcessor(r"C:\path\to\your\data.gpkg")

# Esegui la conversione
processor.process_all_optimized()
```

## Output

Lo script genera i seguenti file nella cartella `output/`:

### Shapefile generati
- `geomorfologia_punti.shp` - Punti geomorfologici
- `geomorfologia_linee.shp` - Linee geomorfologiche  
- `geomorfologia_poligoni.shp` - Poligoni geomorfologici
- `geologia_punti.shp` - Punti geologici di misurazione
- `geologia_linee.shp` - Elementi geologici lineari (include pieghe)
- `geologia_poligoni.shp` - Unità geologiche poligonali
- `risorse_prospezioni.shp` - Punti di risorse e prospezioni

### Report di qualità
- `F[FOGLIO]_geometry_issues.csv` - Report degli errori geometrici rilevati

## Caratteristiche tecniche

### Mappatura dei domini
- Conversione automatica dei codici numerici in descrizioni testuali
- Gestione robusta degli errori di codifica UTF-8
- Cache per ottimizzare le performance delle mappature

### Standardizzazione campi
- Ordine dei campi conforme alle specifiche CARG
- Rinominazione automatica dei campi secondo convenzioni
- Gestione delle mappature complesse per i poligoni geologici

### Processing specializzato

#### Poligoni geologici (ST018Polygon)
- Join con tabelle ausiliarie T0180801000, T0180802000, T0180803000
- Mappatura di età, tipologie litologiche, tessiture
- Gestione campo "Sommerso" (1=SI, 2=NO)

#### Linee geologiche
- Merge automatico di ST018Polyline e ST021Polyline
- Gestione campi specializzati per pieghe e faglie
- Standardizzazione delle tipologie di contorno

### Controllo qualità
- Verifica integrità geometrica con `CheckGeometry`
- Report dettagliato degli errori per Pol_Uc/Pol_Gmo
- Validazione della struttura dati di input

## Configurazione avanzata

### Personalizzazione mappature campi
Le configurazioni sono definite nel metodo `_get_field_standards()`:

```python
"geologia_punti.shp": {
    "field_order": ["FID", "Shape", "Num_Oss", "Quota", "Foglio", ...],
    "field_mappings": {
        "Tipo_g_txt": "Tipo_Geo",
        "Asimm_txt": "Asimmetria",
        ...
    }
}
```

### Gestione errori UTF-8
Lo script include una funzione di conversione robusta per gestire problemi di codifica:

```python
def safe_string_conversion(self, value):
    # Gestione multipla di encoding con fallback
    # Supporto per Python 2.7 e caratteri speciali
```

## Log e debugging

Il tool produce log dettagliati tramite `arcpy.AddMessage()`:
- Informazioni sui layer processati
- Statistiche delle mappature dei domini  
- Report degli errori e warning
- Tempi di elaborazione per layer

### Debug UTF-8
Per diagnosticare problemi di encoding, attivare:
```python
self.debug_utf8_issues_in_auxiliary_tables()
```

## Limitazioni note

1. **Compatibilità**: Progettato per ArcGIS Desktop (Python 2.7)
2. **Proiezione**: Mantiene il sistema di riferimento originale del GeoPackage
3. **Memoria**: Caricamento in memoria delle tabelle dei domini (ottimizzato per dataset tipici CARG)
4. **Dipendenze**: Richiede presenza della cartella `domini/` con tabelle complete

## Troubleshooting

### Errori comuni

**"FoglioGeologico field not found"**
- Verificare che il GeoPackage contenga il campo FoglioGeologico in almeno un layer
- Il campo deve avere valori non nulli

**"Domain table not found"**  
- Controllare che la cartella `domini/` sia presente nella directory del GeoPackage
- Verificare la presenza di tutti i file .dbf richiesti

**"UTF-8 encoding errors"**
- Attivare la funzione di debug per identificare record problematici
- Lo script gestisce automaticamente la maggior parte dei problemi di encoding

**"Could not delete some fields"**
- Normale in alcuni contesti ArcGIS, non compromette il risultato finale
- I campi vengono processati individualmente come fallback

### Performance

Per dataset di grandi dimensioni:
- Utilizzare SSD per migliorare I/O
- Aumentare RAM disponibile per ArcGIS
- Considerare l'elaborazione per singoli fogli geologici

## Contributori

- **Patrizio Petricca** - Sviluppo iniziale e manutenzione

## Licenza

Questo progetto è sviluppato per ISPRA nell'ambito del progetto CARG (Carta Geologica d'Italia).

## Changelog

### v1.0
- Prima release pubblica
- Supporto completo layer CARG standard
- Ottimizzazioni performance e gestione UTF-8
- Report qualità geometrie
- Documentazione completa
