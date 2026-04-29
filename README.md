# Trabajo Final - Dimensionamiento Backhaul RSU

Simulación en ns-3 para dimensionar el enlace backhaul entre RSU y Central.

## Requisitos

- ns-3.45 (o superior)
- gnuplot (opcional, para generar gráficas PNG)
- Compilador C++ compatible con C++17

## Instalación

1. Crear enlace simbólico desde el directorio `scratch` de ns-3:

```bash
ln -s /home/USUARIO/Trabajofinal /opt/salas/ns-allinone-3.45/ns-3.45/scratch/Trabajofinal
```

2. Compilar el proyecto:

```bash
cd /opt/salas/ns-allinone-3.45/ns-3.45
./ns3 configure --enable-examples
./ns3 build scratch/Trabajofinal/fase401
```

## Ejecución

### Modo por defecto (mudo)

```bash
./ns3 run "scratch/Trabajofinal/fase401"
```

La simulación ejecuta sin salida en consola y genera 4 archivos `.plt`:
- `retardo.plt` - Retardo medio vs capacidad backhaul
- `jitter.plt` - Jitter medio vs capacidad backhaul
- `pdr.plt` - PDR (Packet Delivery Ratio) vs capacidad backhaul
- `throughput.plt` - Throughput vs capacidad backhaul

### Con salida de logs

```bash
# Nivel INFO: muestra configuración, progreso y resultados
NS_LOG="Fase401=info" ./ns3 run "scratch/Trabajofinal/fase401"

# Nivel DEBUG: muestra además cada paquete TX/RX/Drop
NS_LOG="Fase401=debug" ./ns3 run "scratch/Trabajofinal/fase401"
```

## Parámetros configurables

| Parámetro | Descripción | Valor por defecto |
|-----------|-------------|-------------------|
| `--nVehiculos` | Número de vehículos (fuentes) | 50 |
| `--tamPaqueteBytes` | Tamaño payload telemetría (bytes) | 1024 |
| `--periodoEnvio` | Periodo entre envíos (ej: 0.1s) | 0.1s |
| `--tasaMinMbps` | Capacidad mínima backhaul (Mbps) | 1.0 |
| `--tasaMaxMbps` | Capacidad máxima backhaul (Mbps) | 10.0 |
| `--tasaPasoMbps` | Paso de barrido (Mbps) | 1.0 |
| `--numMuestras` | Réplicas por punto | 5 |
| `--duracionSim` | Duración simulación (ej: 60s) | 60s |
| `--tiempoTransitorio` | Transitorio a ignorar (ej: 2s) | 2s |
| `--backhaulDelay` | Retardo base backhaul (ej: 2ms) | 2ms |

### Ejemplo personalizado

```bash
./ns3 run "scratch/Trabajofinal/fase401 --nVehiculos=80 --tamPaqueteBytes=1500 \
  --periodoEnvio=0.05s --tasaMinMbps=2 --tasaMaxMbps=20 --tasaPasoMbps=2 \
  --numMuestras=10 --duracionSim=30s"
```

## Generar gráficas PNG

Una vez generados los archivos `.plt`, ejecutar:

```bash
gnuplot retardo.plt
gnuplot jitter.plt
gnuplot pdr.plt
gnuplot throughput.plt
```

Esto genera los archivos PNG correspondientes:
- `retardo.png`
- `jitter.png`
- `pdr.png`
- `throughput.png`

## Niveles de logs disponibles

| Variable de entorno | Qué muestra |
|---------------------|-------------|
| (ninguna) | Mudo total (solo genera archivos .plt) |
| `NS_LOG="Fase401=info"` | Configuración + progreso + resultados |
| `NS_LOG="Fase401=debug"` | Info + detalles de cada paquete TX/RX/Drop |
| `NS_LOG="Fase401=function"` | Llamadas a funciones |
| `NS_LOG="Fase401=all"` | Todo combinado |

