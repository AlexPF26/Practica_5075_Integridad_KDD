# Informe de Calidad de Datos y Perfilado KDD (ISO/IEC 25012)

**Fecha de ejecución:** 2026-11-12 20:26:59  
**Dataset analítico generado:** `processed/dataset_kdd_listo.csv`  
**Artefactos de respaldo:** `output_samples/metricas_perfilado.json` y `output_samples/quarantine_manifest.txt`

---

## 1. Métricas de Calidad del Lote (ISO/IEC 25012)

### Integridad Criptográfica (Fase 1)
* **Ficheros recibidos en lote:** 6
* **Ficheros verificados (`staging/verified/`):** 3 (transacciones_tienda_este.csv, transacciones_tienda_norte.csv, transacciones_tienda_sur.csv)
* **Ficheros rechazados (`quarantine/tampered/`):** 3 (transacciones_tienda_este.csv, transacciones_tienda_norte.csv, transacciones_tienda_sur.csv)
* **Tasa de Integridad Criptográfica:** **50.0%**

### Completitud y Saneamiento de Registros (Fase 2)
* **Registros brutos iniciales:** 150
* **Registros aislados en cuarentena (`quarantine/bad_records.csv`):** 14
* **Registros conformes exportados a BI:** 136
* **Tasa de Completitud inicial:** 96.67% (145/150)
* **Tasa de Completitud final:** **100.0%** (136/136)

#### Desglose de registros segregados en cuarentena
| Motivo de rechazo | Registros afectados |
| :--- | :---: |
| outlier importe_eur (> 137.88 EUR) | 4 |
| id_transaccion nulo | 2 |
| timestamp fuera del rango valido (2026) | 2 |
| importe_eur <= 0 | 2 |
| unidades < 1 | 2 |
| importe_eur o unidades nulo | 1 |
| timestamp con formato corrupto | 1 |

---

## 2. Distribución Estadística de `importe_eur` (Post-Limpieza)

| Estadístico | Valor (EUR) |
| :--- | :---: |
| Media aritmética | 71.92 € |
| Mediana ($P_{50}$) | 73.55 € |
| Desviación estándar | 24.86 € |
| Percentil 25 ($Q_1$) | 56.63 € |
| Percentil 75 ($Q_3$) | 88.08 € |
| Rango válido (Mín – Máx) | 12.50 € – 124.47 € |

---

## 3. Conclusiones Técnicas y Justificación para BI

1. **Aislamiento en cuarentena sin detención del flujo:** Los ficheros alterados (`quarantine/tampered/`) y los registros corruptos (`quarantine/bad_records.csv`) se segregan automáticamente manteniendo la trazabilidad en `output_samples/quarantine_manifest.txt`, evitando interrumpir la carga diaria al almacén de BI.
2. **Política de nulidad e imputación:** Las filas sin `id_transaccion` o sin `importe_eur` se descartaron para no comprometer la integridad contable. Los nulos en atributos descriptivos (`categoria` y `metodo_pago`) se imputaron como `'Desconocido'` y `'No especificado'`, alcanzando un **100% de completitud** sin perder ventas válidas.
3. **Coherencia temporal y de dominio:** La normalización a ISO 8601 y el descarte de fechas fuera del ejercicio vigente junto con el filtrado de `importe_eur <= 0` y `unidades < 1` eliminan anomalías de captura en TPV.
4. **Control de *outliers* mediante IQR:** El filtrado de importes superiores a $Q_3 + 1.5 	imes IQR$ (137.88 €) evita sesgos en métricas agregadas y modelos de minería de datos en la fase KDD.
