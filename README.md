# CARLA Data Generation

Synchronous CARLA recorder for loop-closure datasets.

The main script records one front camera, one roof LiDAR, and one IMU from a single ego vehicle. It warms up sensors first, validates synchronization, then records a deterministic loop route.

## Files

```text
carla/carla_sync_camera_lidar_imu_recorder.py  Main recorder
carla/AGENT.md                                Development notes and validation history
```

Generated datasets are ignored by git via `.gitignore`.

## Requirements

- CARLA 0.9.16 server running
- Python 3.12 with CARLA PythonAPI available

The validated local Python path was:

```powershell
C:\Users\kwaks\AppData\Local\Programs\Python\Python312\python.exe
```

## Basic Run

Use an existing fixed route:

```powershell
C:\Users\kwaks\AppData\Local\Programs\Python\Python312\python.exe carla\carla_sync_camera_lidar_imu_recorder.py --motion-mode route-loop --route-file carla\validation_output\fixed_route_latest.json --out-root carla\validation_output --run-name route_loop_fixed_next
```

Plan a new route automatically:

```powershell
C:\Users\kwaks\AppData\Local\Programs\Python\Python312\python.exe carla\carla_sync_camera_lidar_imu_recorder.py --motion-mode route-loop --out-root carla\validation_output --run-name route_loop_new
```

Each route-loop run saves a reusable route file:

```text
<run_dir>/route.json
```

You can save an additional copy:

```powershell
--save-route-file carla\validation_output\fixed_route_latest.json
```

## Important Defaults

```text
world rate:              100 Hz
camera:                  20 Hz PNG
LiDAR:                   10 Hz sweeps
IMU:                     100 Hz BIN
route laps:              1.05
loop closure radius:     2 m
loop closure yaw:        45 deg
route-loop timeout:      disabled by default
warmup timeout:          10 s
route IMU mode:          synthetic
```

## Output

A run directory contains:

```text
camera/data/*.png
camera/timestamps.txt
lidar/data/*.bin
lidar/timestamps.txt
imu/data/*.bin
imu/timestamps.txt
meta.yaml
route.json
```

`meta.yaml` includes synchronization stats, route summary, and dataset validity.

## Last Validated Result

The latest fixed-route validation produced:

```text
route.closed: true
dataset_status.valid: true
camera frames: 1124
LiDAR sweeps: 562
IMU samples: 5620
missing_lidar_partials: 0
missing_imu: 0
dropped_partial_sweeps: 0
```

