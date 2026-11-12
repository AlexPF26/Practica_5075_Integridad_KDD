import hashlib
import logging
import shutil
from pathlib import Path

# Raíz del proyecto (un nivel por encima de scripts/)
BASE_DIR = Path(__file__).resolve().parent.parent

RAW_DIR = BASE_DIR / "raw_batch_202611"
MANIFEST_PATH = RAW_DIR / "manifest_checksums.sha256"
VERIFIED_DIR = BASE_DIR / "staging" / "verified"
TAMPERED_DIR = BASE_DIR / "quarantine" / "tampered"
LOGS_DIR = BASE_DIR / "logs"
LOG_PATH = LOGS_DIR / "ejecucion_pipeline.log"


def configurar_logger() -> logging.Logger:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("integridad_sha256")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] [FASE-1] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # mode="w" inicia un log limpio al arrancar el pipeline completo
    file_handler = logging.FileHandler(LOG_PATH, mode="w", encoding="utf-8")
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


def calcular_sha256(ruta_fichero: Path) -> str:
    hasher = hashlib.sha256()
    with open(ruta_fichero, "rb") as f:
        while chunk := f.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest()


def verificar_lote(logger: logging.Logger) -> None:
    if not MANIFEST_PATH.exists():
        logger.error(f"No se encuentra el manifiesto de firmas: {MANIFEST_PATH}")
        raise FileNotFoundError(f"Manifiesto no encontrado en {MANIFEST_PATH}")

    # Crear y limpiar zonas de destino para evitar residuos de ejecuciones anteriores
    VERIFIED_DIR.mkdir(parents=True, exist_ok=True)
    TAMPERED_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"Iniciando validación criptográfica SHA-256 sobre lote: {RAW_DIR.name}")

    aprobados = 0
    rechazados = 0

    with open(MANIFEST_PATH, "r", encoding="utf-8") as manifiesto:
        for num_linea, linea in enumerate(manifiesto, start=1):
            linea = linea.strip()
            if not linea or linea.startswith("#"):
                continue

            partes = linea.split(maxsplit=1)
            if len(partes) != 2:
                logger.warning(f"Línea {num_linea} con formato inválido en manifiesto: {linea}")
                continue

            hash_esperado = partes[0].strip().lower()
            # Limpia posible '*' (modo binario de sha256sum) o rutas relativas
            nombre_fichero = Path(partes[1].strip().lstrip("*")).name
            ruta_fichero = RAW_DIR / nombre_fichero

            if not ruta_fichero.exists():
                logger.error(f"Fichero declarado en manifiesto no encontrado en origen: {nombre_fichero}")
                rechazados += 1
                continue

            hash_calculado = calcular_sha256(ruta_fichero).lower()

            if hash_calculado == hash_esperado:
                shutil.copy2(ruta_fichero, VERIFIED_DIR / nombre_fichero)
                aprobados += 1
                logger.info(
                    f"OK | {nombre_fichero} -> Firma SHA-256 válida ({hash_calculado[:12]}...). "
                    f"Enviado a {VERIFIED_DIR.relative_to(BASE_DIR)}/"
                )
            else:
                shutil.copy2(ruta_fichero, TAMPERED_DIR / nombre_fichero)
                rechazados += 1
                logger.critical(
                    f"ALERTA DE INTEGRIDAD | {nombre_fichero} -> Hash discrepante! "
                    f"Esperado: {hash_esperado[:12]}... | Calculado: {hash_calculado[:12]}... "
                    f"Aislado en {TAMPERED_DIR.relative_to(BASE_DIR)}/"
                )

    logger.info(
        f"Resumen Fase 1 -> Total evaluados: {aprobados + rechazados} | "
        f"Aprobados (verified): {aprobados} | Rechazados (tampered): {rechazados}"
    )


if __name__ == "__main__":
    log = configurar_logger()
    verificar_lote(log)