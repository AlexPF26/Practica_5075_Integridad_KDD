import json
import logging
from datetime import datetime
from pathlib import Path
import pandas as pd

# Raíz del proyecto (un nivel por encima de scripts/)
BASE_DIR = Path(__file__).resolve().parent.parent

STAGING_DIR = BASE_DIR / "staging" / "verified"
TAMPERED_DIR = BASE_DIR / "quarantine" / "tampered"
QUARANTINE_DIR = BASE_DIR / "quarantine"
PROCESSED_DIR = BASE_DIR / "processed"
LOGS_DIR = BASE_DIR / "logs"
REPORTS_DIR = BASE_DIR / "reports"
SAMPLES_DIR = BASE_DIR / "output_samples"

LOG_PATH = LOGS_DIR / "ejecucion_pipeline.log"
INFORME_PATH = REPORTS_DIR / "INFORME_CALIDAD_KDD.md"
MANIFEST_QUARANTINE_PATH = SAMPLES_DIR / "quarantine_manifest.txt"
METRICAS_JSON_PATH = SAMPLES_DIR / "metricas_perfilado.json"

COLUMNAS_OBLIGATORIAS = {"id_transaccion", "timestamp", "importe_eur", "unidades"}


def configurar_logger() -> logging.Logger:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("pipeline_kdd")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] [FASE-2/3] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Escribe en logs/ejecucion_pipeline.log en modo append para conservar lo registrado en Fase 1
    file_handler = logging.FileHandler(LOG_PATH, mode="a", encoding="utf-8")
    file_handler.setFormatter(formatter)

    # Muestra también la traza en terminal
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


def ejecutar_fase_2(logger: logging.Logger) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    ficheros_verificados = sorted(STAGING_DIR.glob("*.csv"))
    ficheros_tampered = sorted(TAMPERED_DIR.glob("*.csv"))

    if not ficheros_verificados:
        logger.error(f"No hay ficheros verificados en {STAGING_DIR}")
        raise FileNotFoundError(f"No se encontraron ficheros en {STAGING_DIR}")

    dataframes = []
    for fichero in ficheros_verificados:
        df_temp = pd.read_csv(fichero)
        faltantes = COLUMNAS_OBLIGATORIAS - set(df_temp.columns)
        if faltantes:
            logger.error(f"Schema drift en {fichero.name}: faltan {faltantes}")
            raise ValueError(f"Schema drift en {fichero.name}: faltan {faltantes}")
        df_temp["fichero_origen"] = fichero.name
        dataframes.append(df_temp)

    df_bruto = pd.concat(dataframes, ignore_index=True)
    total_inicial = len(df_bruto)
    sin_nulos_bruto = int(df_bruto.dropna().shape[0])
    logger.info(
        f"Iniciando saneamiento sobre {len(ficheros_verificados)} ficheros ({total_inicial} registros brutos)"
    )

    rechazados = []

    def segregar(df_actual: pd.DataFrame, mascara_error: pd.Series, motivo: str) -> pd.DataFrame:
        invalidos = df_actual[mascara_error].copy()
        if not invalidos.empty:
            invalidos["motivo_rechazo"] = motivo
            rechazados.append(invalidos)
            logger.warning(f"Segregados {len(invalidos)} registros a cuarentena -> Motivo: {motivo}")
        return df_actual[~mascara_error].copy()

    df = df_bruto.copy()

    # 1. Nulos en claves obligatorias y duplicados
    df = segregar(df, df["id_transaccion"].isna(), "id_transaccion nulo")
    df = segregar(df, df["importe_eur"].isna() | df["unidades"].isna(), "importe_eur o unidades nulo")
    df = segregar(
        df,
        df.duplicated(subset=["id_transaccion"], keep="first"),
        "id_transaccion duplicado",
    )

    # Imputación de variables categóricas secundarias
    if "categoria" in df.columns:
        nulos_cat = int(df["categoria"].isna().sum())
        if nulos_cat > 0:
            df["categoria"] = df["categoria"].fillna("Desconocido")
            logger.info(f"Imputados {nulos_cat} valores nulos en 'categoria' -> 'Desconocido'")

    if "metodo_pago" in df.columns:
        nulos_mp = int(df["metodo_pago"].isna().sum())
        if nulos_mp > 0:
            df["metodo_pago"] = df["metodo_pago"].fillna("No especificado")
            logger.info(f"Imputados {nulos_mp} valores nulos en 'metodo_pago' -> 'No especificado'")

    # 2. Coherencia temporal (ISO 8601 y año en curso)
    ts_parsed = pd.to_datetime(df["timestamp"], errors="coerce")
    df = segregar(df, ts_parsed.isna(), "timestamp con formato corrupto")

    ts_parsed = pd.to_datetime(df["timestamp"])
    anio_actual = datetime.now().year
    ahora = pd.Timestamp.now()
    mascara_fecha_invalida = (ts_parsed.dt.year != anio_actual) | (ts_parsed > ahora)
    df = segregar(df, mascara_fecha_invalida, f"timestamp fuera del rango valido ({anio_actual})")
    df["timestamp"] = ts_parsed.dt.strftime("%Y-%m-%d %H:%M:%S")

    # 3. Reglas de dominio de negocio
    df = segregar(df, df["importe_eur"] <= 0, "importe_eur <= 0")
    df = segregar(df, df["unidades"] < 1, "unidades < 1")
    df["unidades"] = df["unidades"].astype(int)

    # 4. Detección de outliers mediante IQR en importe_eur
    q1 = df["importe_eur"].quantile(0.25)
    q3 = df["importe_eur"].quantile(0.75)
    iqr = q3 - q1
    limite_superior = q3 + 1.5 * iqr
    df = segregar(
        df,
        df["importe_eur"] > limite_superior,
        f"outlier importe_eur (> {limite_superior:.2f} EUR)",
    )

    df_bad = pd.concat(rechazados, ignore_index=True) if rechazados else pd.DataFrame()
    ruta_bad = QUARANTINE_DIR / "bad_records.csv"
    df_bad.to_csv(ruta_bad, index=False)
    logger.info(f"Registros defectuosos exportados en: {ruta_bad} ({len(df_bad)} registros)")

    metadatos = {
        "ficheros_validados": [f.name for f in ficheros_verificados],
        "ficheros_rechazados": [f.name for f in ficheros_tampered],
        "total_inicial": total_inicial,
        "sin_nulos_bruto": sin_nulos_bruto,
        "total_cuarentena": len(df_bad),
        "limite_iqr_superior": round(float(limite_superior), 2),
    }
    return df, df_bad, metadatos


def ejecutar_fase_3(
    df_limpio: pd.DataFrame, df_bad: pd.DataFrame, meta: dict, logger: logging.Logger
) -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    SAMPLES_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Dataset KDD limpio para BI
    df_export = df_limpio.drop(columns=["fichero_origen"], errors="ignore")
    ruta_kdd = PROCESSED_DIR / "dataset_kdd_listo.csv"
    df_export.to_csv(ruta_kdd, index=False)
    logger.info(f"Dataset KDD final exportado en: {ruta_kdd} ({len(df_export)} registros)")

    # 2. Métricas ISO/IEC 25012 y exportación a output_samples/metricas_perfilado.json
    total_ficheros = len(meta["ficheros_validados"]) + len(meta["ficheros_rechazados"])
    tasa_integridad = (len(meta["ficheros_validados"]) / total_ficheros * 100) if total_ficheros else 0.0
    tasa_comp_bruta = (meta["sin_nulos_bruto"] / meta["total_inicial"] * 100) if meta["total_inicial"] else 0.0
    sin_nulos_final = int(df_export.dropna().shape[0])
    tasa_comp_final = (sin_nulos_final / len(df_export) * 100) if len(df_export) else 0.0

    serie_imp = df_export["importe_eur"]
    desglose_rechazos = (
        df_bad["motivo_rechazo"].value_counts().to_dict()
        if (not df_bad.empty and "motivo_rechazo" in df_bad.columns)
        else {}
    )

    metricas_json = {
        "norma_referencia": "ISO/IEC 25012",
        "fecha_generacion": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "integridad_criptografica": {
            "total_ficheros_lote": total_ficheros,
            "ficheros_aprobados": meta["ficheros_validados"],
            "ficheros_rechazados": meta["ficheros_rechazados"],
            "tasa_integridad_pct": round(tasa_integridad, 2),
        },
        "completitud_datos": {
            "registros_brutos_totales": meta["total_inicial"],
            "registros_brutos_sin_nulos": meta["sin_nulos_bruto"],
            "tasa_completitud_bruta_pct": round(tasa_comp_bruta, 2),
            "registros_validos_finales": len(df_export),
            "registros_segregados_cuarentena": meta["total_cuarentena"],
            "registros_finales_sin_nulos": sin_nulos_final,
            "tasa_completitud_final_pct": round(tasa_comp_final, 2),
        },
        "distribucion_importe_eur": {
            "media": round(float(serie_imp.mean()), 2),
            "mediana": round(float(serie_imp.median()), 2),
            "desviacion_estandar": round(float(serie_imp.std()), 2),
            "percentil_25": round(float(serie_imp.quantile(0.25)), 2),
            "percentil_75": round(float(serie_imp.quantile(0.75)), 2),
            "minimo": round(float(serie_imp.min()), 2),
            "maximo": round(float(serie_imp.max()), 2),
            "umbral_superior_iqr": meta["limite_iqr_superior"],
        },
        "desglose_cuarentena_por_motivo": desglose_rechazos,
    }

    METRICAS_JSON_PATH.write_text(json.dumps(metricas_json, indent=4, ensure_ascii=False), encoding="utf-8")
    logger.info(f"Metricas JSON exportadas en: {METRICAS_JSON_PATH}")

    # 3. Manifiesto de cuarentena en output_samples/quarantine_manifest.txt
    lineas_manifest = [
        "====================================================================",
        "MANIFIESTO DE CUARENTENA - PIPELINE DE CALIDAD DE DATOS (KDD)",
        f"Fecha de auditoria: {metricas_json['fecha_generacion']}",
        "====================================================================",
        "",
        "[1] FICHEROS RECHAZADOS POR INTEGRIDAD SHA-256 (quarantine/tampered/)",
    ]
    if meta["ficheros_rechazados"]:
        for fname in meta["ficheros_rechazados"]:
            lineas_manifest.append(
                f"  - {fname} -> Firma SHA-256 discrepante respecto a manifest_checksums.sha256"
            )
    else:
        lineas_manifest.append("  - Ningun fichero rechazado en Fase 1.")

    lineas_manifest.extend([
        "",
        f"[2] REGISTROS SEGREGADOS POR CALIDAD DE DATOS (quarantine/bad_records.csv) - Total: {len(df_bad)}",
    ])
    if not df_bad.empty:
        for _, row in df_bad.iterrows():
            tx_id = row["id_transaccion"] if pd.notna(row["id_transaccion"]) else "SIN_ID"
            origen = row.get("fichero_origen", "desconocido")
            motivo = row.get("motivo_rechazo", "no_especificado")
            lineas_manifest.append(f"  - [{origen}] ID: {tx_id} | Motivo: {motivo}")
    else:
        lineas_manifest.append("  - Ningun registro segregado en Fase 2.")

    MANIFEST_QUARANTINE_PATH.write_text("\n".join(lineas_manifest) + "\n", encoding="utf-8")
    logger.info(f"Manifiesto de cuarentena exportado en: {MANIFEST_QUARANTINE_PATH}")

    # 4. Informe Markdown en reports/INFORME_CALIDAD_KDD.md
    tabla_motivos = (
        "\n".join(f"| {m} | {c} |" for m, c in desglose_rechazos.items())
        if desglose_rechazos
        else "| Ninguno | 0 |"
    )

    contenido_md = f"""# Informe de Calidad de Datos y Perfilado KDD (ISO/IEC 25012)

**Fecha de ejecución:** {metricas_json['fecha_generacion']}  
**Dataset analítico generado:** `processed/dataset_kdd_listo.csv`  
**Artefactos de respaldo:** `output_samples/metricas_perfilado.json` y `output_samples/quarantine_manifest.txt`

---

## 1. Métricas de Calidad del Lote (ISO/IEC 25012)

### Integridad Criptográfica (Fase 1)
* **Ficheros recibidos en lote:** {total_ficheros}
* **Ficheros verificados (`staging/verified/`):** {len(meta['ficheros_validados'])} ({', '.join(meta['ficheros_validados'])})
* **Ficheros rechazados (`quarantine/tampered/`):** {len(meta['ficheros_rechazados'])} ({', '.join(meta['ficheros_rechazados']) if meta['ficheros_rechazados'] else 'Ninguno'})
* **Tasa de Integridad Criptográfica:** **{metricas_json['integridad_criptografica']['tasa_integridad_pct']}%**

### Completitud y Saneamiento de Registros (Fase 2)
* **Registros brutos iniciales:** {meta['total_inicial']}
* **Registros aislados en cuarentena (`quarantine/bad_records.csv`):** {meta['total_cuarentena']}
* **Registros conformes exportados a BI:** {len(df_export)}
* **Tasa de Completitud inicial:** {metricas_json['completitud_datos']['tasa_completitud_bruta_pct']}% ({meta['sin_nulos_bruto']}/{meta['total_inicial']})
* **Tasa de Completitud final:** **{metricas_json['completitud_datos']['tasa_completitud_final_pct']}%** ({sin_nulos_final}/{len(df_export)})

#### Desglose de registros segregados en cuarentena
| Motivo de rechazo | Registros afectados |
| :--- | :---: |
{tabla_motivos}

---

## 2. Distribución Estadística de `importe_eur` (Post-Limpieza)

| Estadístico | Valor (EUR) |
| :--- | :---: |
| Media aritmética | {metricas_json['distribucion_importe_eur']['media']:.2f} € |
| Mediana ($P_{{50}}$) | {metricas_json['distribucion_importe_eur']['mediana']:.2f} € |
| Desviación estándar | {metricas_json['distribucion_importe_eur']['desviacion_estandar']:.2f} € |
| Percentil 25 ($Q_1$) | {metricas_json['distribucion_importe_eur']['percentil_25']:.2f} € |
| Percentil 75 ($Q_3$) | {metricas_json['distribucion_importe_eur']['percentil_75']:.2f} € |
| Rango válido (Mín – Máx) | {metricas_json['distribucion_importe_eur']['minimo']:.2f} € – {metricas_json['distribucion_importe_eur']['maximo']:.2f} € |

---

## 3. Conclusiones Técnicas y Justificación para BI

1. **Aislamiento en cuarentena sin detención del flujo:** Los ficheros alterados (`quarantine/tampered/`) y los registros corruptos (`quarantine/bad_records.csv`) se segregan automáticamente manteniendo la trazabilidad en `output_samples/quarantine_manifest.txt`, evitando interrumpir la carga diaria al almacén de BI.
2. **Política de nulidad e imputación:** Las filas sin `id_transaccion` o sin `importe_eur` se descartaron para no comprometer la integridad contable. Los nulos en atributos descriptivos (`categoria` y `metodo_pago`) se imputaron como `'Desconocido'` y `'No especificado'`, alcanzando un **100% de completitud** sin perder ventas válidas.
3. **Coherencia temporal y de dominio:** La normalización a ISO 8601 y el descarte de fechas fuera del ejercicio vigente junto con el filtrado de `importe_eur <= 0` y `unidades < 1` eliminan anomalías de captura en TPV.
4. **Control de *outliers* mediante IQR:** El filtrado de importes superiores a $Q_3 + 1.5 \times IQR$ ({meta['limite_iqr_superior']:.2f} €) evita sesgos en métricas agregadas y modelos de minería de datos en la fase KDD.
"""
    INFORME_PATH.write_text(contenido_md, encoding="utf-8")
    logger.info(f"Informe Markdown generado en: {INFORME_PATH}")


if __name__ == "__main__":
    log = configurar_logger()
    df_saneado, df_cuarentena, metadatos = ejecutar_fase_2(log)
    ejecutar_fase_3(df_saneado, df_cuarentena, metadatos, log)