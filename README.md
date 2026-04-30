# CARLA Data Generation

Synchronous CARLA recorder for loop-closure datasets.

The main script records one front camera, one roof LiDAR, and one IMU from a single ego vehicle. It warms up sensors first, validates synchronization, then records a deterministic loop route.

## Files

```text
carla/carla_sync_camera_lidar_imu_recorder.py  Main recorder
carla/carla_rotation_around_object_recorder.py Ego vehicle rounded-rectangle orbit around a target vehicle
carla/carla_dual_vehicle_tracking_recorder.py Ego/target free-motion tracking dataset recorder
carla/configs/*.json                         Curated scenario configs
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

## Scenario Runs

The two scenario scripts support `--config <json>`. Values in the config are used as defaults, and explicit CLI flags override them.

### Rotation Around Object

Use the curated Town10HD pose:

```powershell
C:\Users\kwaks\AppData\Local\Programs\Python\Python312\python.exe carla\carla_rotation_around_object_recorder.py --config carla\configs\town10_rotation_object_pos120.json --run-name rotation_town10_pos120
```

Same mode without a config:

```powershell
C:\Users\kwaks\AppData\Local\Programs\Python\Python312\python.exe carla\carla_rotation_around_object_recorder.py --no-camera --map Town10HD --target-spawn-index 0 --ego-blueprint vehicle.tesla.model3 --target-blueprint vehicle.audi.tt --orbit-half-length 16 --orbit-half-width 12 --corner-radius 4 --orbit-laps 1 --out-root carla\validation_output --run-name rotation_object_new
```

Override one config value from CLI:

```powershell
C:\Users\kwaks\AppData\Local\Programs\Python\Python312\python.exe carla\carla_rotation_around_object_recorder.py --config carla\configs\town10_rotation_object_pos120.json --orbit-laps 2 --run-name rotation_town10_2laps
```

To curate a new open area manually, pass an explicit target pose:

```powershell
C:\Users\kwaks\AppData\Local\Programs\Python\Python312\python.exe carla\carla_rotation_around_object_recorder.py --no-camera --map Town10HD --target-x 55.542 --target-y 130.460 --target-z 0.600 --target-yaw -179.680 --orbit-half-length 16 --orbit-half-width 12 --corner-radius 4 --out-root carla\validation_output --run-name rotation_manual_pose
```

### Dual Vehicle Tracking

Use the curated Town10HD area:

```powershell
C:\Users\kwaks\AppData\Local\Programs\Python\Python312\python.exe carla\carla_dual_vehicle_tracking_recorder.py --config carla\configs\town10_dual_tracking_pos137.json --run-name dual_town10_pos137
```

Same mode without a config:

```powershell
C:\Users\kwaks\AppData\Local\Programs\Python\Python312\python.exe carla\carla_dual_vehicle_tracking_recorder.py --no-camera --map Town10HD --area-spawn-index 0 --ego-blueprint vehicle.tesla.model3 --target-blueprint vehicle.audi.tt --duration 20 --area-length 60 --area-width 40 --out-root carla\validation_output --run-name dual_tracking_new
```

Override one config value from CLI:

```powershell
C:\Users\kwaks\AppData\Local\Programs\Python\Python312\python.exe carla\carla_dual_vehicle_tracking_recorder.py --config carla\configs\town10_dual_tracking_pos137.json --duration 40 --run-name dual_town10_40s
```

This mode also writes target ground truth:

```text
target_pose/data.csv
scenario_paths.json
```

By default, the dual-vehicle mode splits the configured area into separate A/B zones and pre-checks sampled trajectories against vehicle bounding radii plus `--collision-margin`. Use `--shared-area` only when you intentionally want both vehicles in the same movement region.

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
camera/timestamps.csv
lidar/data/*.bin
lidar/timestamps.csv
imu/data/*.bin
imu/timestamps.csv
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
