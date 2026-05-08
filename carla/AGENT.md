# CARLA Data Generation Agent Notes

## Target workflow

The recorder should support a fully automatic loop-closure data collection mode:

1. Attach to the running CARLA server in synchronous mode.
2. Spawn or reuse the ego vehicle at the current/selected start pose.
3. Spawn exactly one front camera, one roof LiDAR, and one IMU at base_link origin.
4. Warm up the simulator and sensors while the vehicle is held still.
5. Start recording only after sensor streams are stable and synchronized.
6. Drive a deterministic closed route that returns near the start pose.
7. Stop and finalize automatically after loop closure is observed.

The priority is timestamp stability and dataset consistency, not driving realism.

## Startup and warmup state machine

Use explicit phases instead of recording immediately:

```text
CONNECT
  -> CONFIGURE_WORLD_SYNC
  -> SPAWN_EGO_AND_SENSORS
  -> SENSOR_WARMUP
  -> SYNC_VALIDATION
  -> ARM_RECORDING
  -> ROUTE_LOOP_RECORDING
  -> LOOP_CLOSURE_FINALIZE
  -> CLEANUP
```

### SENSOR_WARMUP

During warmup:

- Keep the ego vehicle stopped with brake/handbrake.
- Tick the world at 100 Hz.
- Do not write camera, LiDAR, or IMU files.
- Fill the sensor buffers and discard all measurements before the warmup boundary.
- Minimum default warmup should remain at least 20 ticks, but stability checks matter more than a fixed count.

### SYNC_VALIDATION

Before moving the vehicle or writing files, verify:

- IMU is present every world tick for a validation window.
- LiDAR partial packets are present every world tick for at least one full sweep.
- Camera frames arrive exactly every `camera_ticks` world frames.
- LiDAR sweep boundaries are exactly every `lidar_sweep_ticks` world frames.
- No stale frames from before warmup remain in any buffer.

Suggested defaults:

- `--warmup-min-ticks 50`
- `--sync-validation-sweeps 3`
- `--sync-max-missing 0`

If validation fails, keep the vehicle stopped, clear buffers, and retry until `--warmup-timeout` expires.

## Route-loop driving mode

Add a route mode rather than relying on Traffic Manager wandering:

```text
--motion-mode route-loop
```

Recommended route construction:

1. Use the ego start waypoint from `world.get_map().get_waypoint(start_location, project_to_road=True)`.
2. Build a road graph from `world.get_map().get_topology()`.
3. Search for a closed route that starts near the current waypoint and returns near it.
4. Prefer right/straight choices consistently so the route is deterministic.
5. If graph search is too complex for a first version, sample candidate spawn points and choose the route whose final waypoint returns closest to the start after a target distance.

Recommended route parameters:

```text
--loop-target-distance 800.0
--loop-min-distance 300.0
--loop-closure-radius 5.0
--loop-closure-yaw-deg 30.0
--target-speed-kmh 25.0
--route-repeat false
```

Current implementation uses CARLA Traffic Manager with an explicit deterministic path via `traffic_manager.set_path()`. This keeps dependencies low because `BasicAgent` pulls in `numpy`, `networkx`, and `shapely`. The important distinction is that Traffic Manager must not be allowed to wander randomly; it must receive an explicit path and the recorder must own the loop-closure stop condition.

## Recording start alignment

Recording should begin only on a clean multi-sensor boundary:

- IMU: current world frame.
- Camera: `world_frame % camera_ticks == 0`.
- LiDAR: `world_frame % lidar_sweep_ticks == 0`, then begin collecting the next sweep.

After validation passes:

1. Wait for the next frame satisfying both camera and LiDAR boundary conditions.
2. Clear camera/LiDAR/IMU buffers.
3. Release brake and start route following.
4. Start writing from that boundary.

This avoids a partial first LiDAR sweep and avoids camera frames whose timestamps predate motion start.

## Loop closure stop condition

Do not stop immediately when the route begins near the start. Require all of:

- Recorded distance traveled is greater than `--loop-min-distance`.
- Current ego position is within `--loop-closure-radius` meters of the recorded start pose.
- Current yaw differs from start yaw by less than `--loop-closure-yaw-deg`.
- At least one full LiDAR sweep has been finalized after entering the closure radius.

When closure is detected:

1. Finish the current LiDAR sweep.
2. Drain camera/IMU frames up to the same world frame boundary.
3. Close timestamp files and async writers.
4. Write route summary into `meta.yaml`.

## Metadata additions

For loop-mode runs, add this section to `meta.yaml`:

```yaml
route:
  mode: "route-loop"
  target_speed_kmh: 25.0
  planned_length_m: 800.0
  recorded_distance_m: 812.3
  closure_distance_m: 2.1
  closure_yaw_error_deg: 4.6
  start_frame: 12345
  start_timestamp: 12.345
  end_frame: 93456
  end_timestamp: 823.456
  start_pose_carla: {x: 0.0, y: 0.0, z: 0.0, roll: 0.0, pitch: 0.0, yaw: 0.0}
  end_pose_carla: {x: 0.0, y: 0.0, z: 0.0, roll: 0.0, pitch: 0.0, yaw: 0.0}
```

## Failure policy

If stable synchronization cannot be achieved:

- Do not start driving.
- Do not create a misleading partial dataset.
- Print the missing frame statistics and exit non-zero unless `--warmup-retry-forever` is set.

If route planning fails:

- Do not fall back silently to Traffic Manager.
- Print the start waypoint and map name.
- Exit with a clear route-planning error.

If a sensor packet is missing during recording:

- Drop the incomplete LiDAR sweep.
- Mark the run as invalid in `meta.yaml`.
- Continue only if an explicit `--allow-dropped-sweeps` option is set.

## Handoff State For Next Session

Use this section as the first checkpoint when continuing work after context compaction or in a new chat. The user may say "continue previous work"; in that case read this file and inspect `carla_sync_camera_lidar_imu_recorder.py` before making edits.

## Requested Next Recorder Modes

The user wants two additional dataset-generation modes, but each mode should be implemented as its own Python script rather than expanding `carla_sync_camera_lidar_imu_recorder.py`.

Keep the existing synchronized camera/LiDAR/IMU recording behavior and metadata conventions where practical. These new scripts are still data-generation tools, not full perception/SLAM/tracking pipelines. Do not implement point-cloud cropping, SLAM map processing, or target tracking algorithms unless the user asks later.

### 1. Rotation Around Object Recorder

Create a separate script:

```text
carla/carla_rotation_around_object_recorder.py
```

Purpose:

- Spawn an ego vehicle A with the existing camera/LiDAR/IMU sensor rig.
- Spawn a target object B as a vehicle actor.
- Move A once around B so a later SLAM/map pipeline can isolate B's point cloud from a known region.
- Only implement the data-generation part: "A drives around B once."

Implementation requirements:

- A and B must use deterministic, explicitly configurable vehicle blueprints so repeated runs spawn the same vehicle types.
  - Suggested CLI:
    - `--ego-blueprint vehicle.tesla.model3`
    - `--target-blueprint vehicle.audi.tt`
    - Keep `--ego-filter` only as a fallback if an exact blueprint is not found.
- B should be spawned as a static or handbraked vehicle actor with physics disabled if needed for repeatability.
- A should not drive a perfect circle. Use a smooth rounded-rectangle / rounded-square path around B.
  - Four straight sides plus smooth corner arcs is preferred.
  - A should face mostly along the path tangent, with the LiDAR naturally observing B from multiple sides.
  - Suggested parameters: `--orbit-half-width`, `--orbit-half-length`, `--corner-radius`, `--orbit-laps`, `--target-speed-kmh`.
- The script must ensure there is enough open space around B before recording.
  - Prefer loading/using a map area with broad open space rather than narrow roads.
  - Candidate maps/areas should be configurable with `--map` and explicit `--target-location` / `--target-yaw`.
  - A first implementation may ship with curated defaults for a wide, low-obstacle area, but should fail clearly if the target area cannot support the requested orbit extents.
- Use kinematic transform playback for A unless physical driving is specifically needed. Deterministic path following is more important than realistic tire dynamics.
- Warm up and validate sensors before moving, as in the existing recorder.
- Save target metadata in `meta.yaml`.

Suggested metadata:

```yaml
scenario:
  mode: "rotation-around-object"
  ego_blueprint: "vehicle.tesla.model3"
  target_blueprint: "vehicle.audi.tt"
  target_pose_carla: {x: 0.0, y: 0.0, z: 0.0, roll: 0.0, pitch: 0.0, yaw: 0.0}
  orbit_half_width_m: 12.0
  orbit_half_length_m: 16.0
  corner_radius_m: 4.0
  orbit_laps: 1.0
  target_speed_kmh: 15.0
  completed: true
```

Validation expectations:

- A completes one full rounded-rectangle orbit around B.
- B remains fixed and visible to LiDAR from multiple sides.
- No missing LiDAR partials, missing IMU frames, or dropped partial sweeps.
- The dataset is marked invalid if the orbit cannot be completed or the target area clearance check fails.

### 2. Dual Vehicle Free-Motion Tracking Recorder

Create a separate script:

```text
carla/carla_dual_vehicle_tracking_recorder.py
```

Purpose:

- Spawn ego vehicle A with the existing camera/LiDAR/IMU sensor rig.
- Spawn target vehicle B nearby.
- Move A and B near each other in a broad open area so later code can use A's LiDAR to track B's position.
- Only implement data generation and ground-truth metadata. Do not implement a LiDAR tracking algorithm.

Implementation requirements:

- A and B must use deterministic, explicitly configurable vehicle blueprints.
  - Suggested CLI:
    - `--ego-blueprint vehicle.tesla.model3`
    - `--target-blueprint vehicle.audi.tt`
- The mode should not require A and B to have identical motion.
- Prefer broad, free-space motion over road-following Traffic Manager behavior.
  - Use configurable open-area bounds centered at a chosen location.
  - Use deterministic kinematic trajectories by default.
  - Suggested trajectory families: rounded rectangles, figure-eight, offset sinusoidal paths, or waypoint loops within the same open area.
- Keep A close enough for LiDAR observations of B but avoid collisions.
  - Suggested parameters: `--area-width`, `--area-length`, `--min-separation`, `--max-separation`, `--duration`, `--ego-speed-kmh`, `--target-speed-kmh`.
- Record per-frame or per-tick ground-truth target pose relative to A, in addition to the normal sensor streams.
  - Suggested output: `target_pose/timestamps.txt` or `target_pose/data.csv`.
  - Include both CARLA world pose for B and relative pose from A to B.
- Warm up and validate sensors before moving, as in the existing recorder.

Suggested metadata:

```yaml
scenario:
  mode: "dual-vehicle-free-motion-tracking"
  ego_blueprint: "vehicle.tesla.model3"
  target_blueprint: "vehicle.audi.tt"
  area_center_carla: {x: 0.0, y: 0.0, z: 0.0}
  area_width_m: 40.0
  area_length_m: 60.0
  min_separation_m: 8.0
  max_separation_m: 35.0
  ego_trajectory: "rounded-rectangle"
  target_trajectory: "figure-eight"
  completed: true
```

Validation expectations:

- A and B move simultaneously in the selected open area.
- B remains within useful LiDAR range of A for most of the run.
- A and B do not collide or leave the configured area bounds.
- Ground-truth B pose relative to A is written for downstream tracking evaluation.
- The dataset is marked invalid if separation, collision, area-bound, or sensor-sync constraints are violated.

### Shared Guidance For New Scenario Scripts

- Prefer sharing/refactoring the existing recorder's proven utilities instead of copy-pasting large blocks indefinitely.
  - Good candidates: CARLA import/path setup, blueprint configuration, sensor writers, sensor buffers, warmup/sync validation, metadata helpers, and cleanup.
  - If refactoring, keep behavior equivalent for the already-validated route-loop recorder.
- Keep synchronous CARLA ticking and integer sensor periods.
- Preserve output coordinate conventions:
  - LiDAR/IMU outputs use ROS body frame `x forward, y left, z up`.
  - CARLA y is flipped for exported sensor vectors.
- Add clear CLI examples to `README.md` after the scripts exist.
- Fail early with explicit messages when maps, spawn poses, vehicle blueprints, clearance checks, or route generation cannot satisfy the requested scenario.
- Use deterministic seeds and save all scenario parameters into `meta.yaml`.

### Current Implemented State

Main script:

```text
data-gen/carla/carla_sync_camera_lidar_imu_recorder.py
```

Additional scenario script now present:

```text
data-gen/carla/carla_rotation_around_object_recorder.py
data-gen/carla/carla_dual_vehicle_tracking_recorder.py
data-gen/carla/configs/town10_rotation_object_pos120.json
data-gen/carla/configs/town10_dual_tracking_pos137.json
```

Implemented features as of the last session:

- Synchronous CARLA recorder for one front camera, one roof LiDAR, and one IMU.
- World fixed step: 100 Hz by default.
- Camera: 20 Hz PNG, default 1280x360, 90 deg FOV.
- LiDAR: 10 Hz sweeps built from 100 Hz partial packets.
- LiDAR BIN fields: float32 row-major `x, y, z, intensity, ring, timestamp`.
- IMU: 100 Hz BIN, float64 row-major `accel_x, accel_y, accel_z, gyro_x, gyro_y, gyro_z, compass, timestamp`.
- Output coordinate convention: ROS body frame `x forward, y left, z up`; CARLA y is flipped.
- `meta.yaml` and per-sensor `timestamps.txt` are written.
- LiDAR default is tuned to `--lidar-sweep-points 65000`, so CARLA requests 650k rays/sec and typically saves about 50k returned hit points per sweep.
- `get_world()` UnicodeDecodeError handling was improved; root cause seen locally was CARLA cache ACL/write failure under `%USERPROFILE%\carlaCache`.
- Startup now has `SENSOR_WARMUP -> SYNC_VALIDATION -> ARM_RECORDING` before recording/driving.
- New `--motion-mode route-loop` exists.
- `route-loop` now plans a closed CARLA waypoint route via `Waypoint.next()` and replays it with a deterministic kinematic route player.
- The route player disables ego vehicle physics after warmup/validation and moves the actor along the planned polyline at `--target-speed-kmh`; this favors repeatable timestamped data over physical driving realism.
- `route-loop` now validates the planned route before recording; if no route returns within the configured closure radius and yaw tolerance, the run fails before arming motion.
- Random route-loop spawn selection skips spawn points that cannot produce a closed route for the current route parameters.
- Route-loop runs always save a reusable fixed route JSON at `route.json`; `--save-route-file` writes an additional copy and `--route-file` reloads it for deterministic repeated runs.
- `--route-laps` and `--route-extra-distance` can record beyond one loop. Closure is detected once, but recording continues until the requested route distance is reached.
- Kinematic route-loop IMU defaults to `--route-imu-mode synthetic`, generated from the known route trajectory. `--route-imu-mode carla` preserves the raw CARLA IMU behavior if needed.
- Route-loop defaults are tuned for fixed-route collection: no route timeout by default, `--loop-min-distance 50`, `--loop-closure-radius 2`, `--loop-closure-yaw-deg 45`, `--route-laps 1.05`, and `--route-imu-mode synthetic`.
- Warmup validation timeout now defaults to 10 seconds.
- Route metadata is appended to `meta.yaml` through `append_route_summary()`.
- Warmup/sync validation stats are appended to `meta.yaml` through `append_sync_summary()`.
- Dataset validity status is appended to `meta.yaml` through `append_dataset_status()`.

Rotation-around-object script:

- Supports `--config <json>`. Config keys use argparse destination names, e.g. `target_x`, `orbit_half_length`, `no_camera`. Explicit CLI flags override config values.
- Spawns ego vehicle A with the existing camera/LiDAR/IMU rig.
- Supports `--no-camera`, which skips the camera actor, camera listener, camera warmup validation, camera writer, and camera output directory.
- Spawns target vehicle B from deterministic `--target-blueprint`.
- Uses deterministic `--ego-blueprint` for A, with fallback filters if exact blueprints are unavailable.
- Moves A kinematically around B on a rounded-rectangle path, controlled by `--orbit-half-length`, `--orbit-half-width`, `--corner-radius`, `--orbit-direction`, `--orbit-laps`, and `--target-speed-kmh`.
- Uses the existing synchronous world tick, sensor warmup/validation, sensor writers, LiDAR packet merge, synthetic IMU style, and dataset status helpers.
- Saves `orbit_path.json` plus a `scenario:` section in `meta.yaml`.
- Performs a basic target-clearance check using B's actor bounding box and the nearest orbit radius. This does not guarantee static environment clearance; users should still choose a broad open map area or explicit target pose.
- Curated Town10HD config currently available:
  - `carla/configs/town10_rotation_object_pos120.json`
  - Pose was found by probing temporary actor spawns along the rounded-rectangle path.

Dual-vehicle tracking script:

- Supports `--config <json>`. Config keys use argparse destination names, e.g. `area_center_x`, `area_length`, `no_camera`. Explicit CLI flags override config values.
- Spawns ego vehicle A with the existing camera/LiDAR/IMU rig.
- Supports `--no-camera`, which skips the camera actor, camera listener, camera warmup validation, camera writer, and camera output directory.
- Spawns target vehicle B from deterministic `--target-blueprint`.
- Uses deterministic `--ego-blueprint` for A, with fallback filters if exact blueprints are unavailable.
- Moves A and B simultaneously with kinematic open-area trajectories.
  - By default, the configured area is split into separated A/B zones with `--zone-gap`.
  - A defaults to a rounded-rectangle path inside its zone.
  - B defaults to a figure-eight path inside its zone.
  - `--shared-area` disables zone separation when intentionally needed.
- Before recording, sampled A/B trajectories are checked against `max(--min-separation, ego_radius + target_radius + --collision-margin)`. If the sampled planned paths are too close, the script fails before recording.
- Main controls include `--area-length`, `--area-width`, `--area-spawn-index`, explicit `--area-center-x/y/z`, `--duration`, `--stationary-start-duration`, `--ego-speed-kmh`, `--target-speed-kmh`, `--min-separation`, `--max-separation`, `--zone-gap`, and `--collision-margin`.
- Writes normal camera/LiDAR/IMU streams from A.
- Writes B ground truth at every world tick to `target_pose/data.csv`.
  - Includes A world pose, B world pose, and relative B pose using position plus quaternion columns.
- Saves `scenario_paths.json` plus a `scenario:` section in `meta.yaml`.
- Marks the dataset invalid if sensor sync fails, LiDAR partials are dropped, A/B violate min/max separation, or either vehicle leaves the configured area bounds.
- Curated Town10HD config currently available:
  - `carla/configs/town10_dual_tracking_pos137.json`
  - Area was found by probing temporary actor spawns on sampled A/B trajectories.

Important functions currently present:

```text
run_sensor_warmup_and_validation()
build_tm_loop_path()
append_route_summary()
append_sync_summary()
append_dataset_status()
apply_motion_control()
apply_route_loop_kinematic()
main()
```

Important CLI options currently present:

```text
--motion-mode {static,sine,traffic-manager,route-loop}
--warmup-min-ticks
--warmup-timeout
--sync-validation-sweeps
--sync-max-missing
--target-speed-kmh
--loop-target-distance
--loop-min-distance
--loop-closure-radius
--loop-closure-yaw-deg
--loop-timeout
--loop-waypoints
--route-direction {ccw,cw}
--route-step-m
--route-search-beam
--route-search-max-factor
--route-file
--save-route-file
--route-laps
--route-extra-distance
--route-imu-mode {synthetic,carla}
--respect-traffic-lights
```

Important current defaults:

```text
--warmup-timeout 10
--loop-timeout 0        # no route-loop timeout
--loop-min-distance 50
--loop-closure-radius 2
--loop-closure-yaw-deg 45
--route-laps 1.05
--route-extra-distance 0
--route-imu-mode synthetic
```

### Verified Runs

CARLA 0.9.16 was running locally on Windows. Python used:

```text
C:\Users\kwaks\AppData\Local\Programs\Python\Python312\python.exe
```

Validated basic synchronous capture:

```text
data-gen/carla/validation_output/smoke_final_001
```

Observed:

```text
camera_png 20
lidar_bin 10
imu_bin 100
camera_timestamps 20
lidar_timestamps 10
imu_timestamps 100
lidar_points avg/min/max 51265.7 / 50543 / 51472
first_lidar_unique_partial_ts 10
first_lidar_ring_minmax 0.0 / 63.0
missing_lidar_partials 0
missing_imu 0
```

Validated route-loop warmup/validation and route arming:

```text
data-gen/carla/validation_output/route_loop_smoke_001
```

This run intentionally timed out after 5 sec and did not close the loop. It verified:

- warmup/validation passed,
- route was armed with `TrafficManager.set_path()`,
- camera/LiDAR/IMU remained synchronized,
- no missing LiDAR/IMU packets,
- route section was appended to `meta.yaml`.

Route-loop closure probe before waypoint planner improvement:

```text
data-gen/carla/validation_output/route_loop_closure_probe_001
```

This run used a short/small route probe and did not close the loop before timeout. It showed the current spawn-point path heuristic is not yet reliable for loop closure.

Validated waypoint/kinematic route-loop closure:

```text
data-gen/carla/validation_output/route_loop_kinematic_close_003
```

Observed:

```text
planned_length_m 366.617574215
planned_start_end_distance_m 5.002089500
recorded_distance_m 363.869933441
closure_distance_m 7.731132507
closure_yaw_error_deg 0.000001296
camera_png 1048
lidar_bin 524
imu_bin 5240
missing_lidar_partials 0
missing_imu 0
dropped_partial_sweeps 0
route.closed true
dataset_status.valid true
```

Validated fixed route save, reload, and more-than-one-lap recording:

```text
data-gen/carla/validation_output/route_loop_fixed_laps_001
data-gen/carla/validation_output/route_loop_fixed_load_laps_001
data-gen/carla/validation_output/fixed_route_latest.json
```

Observed for the loaded fixed-route run:

```text
route.source file
route_laps 1.05
playback_length_m 371.678436279
target_record_distance_m 390.262358093
recorded_distance_m 390.182169240
closure_distance_m 0.022900648
closure_yaw_error_deg 0.014987897
camera_png 1124
lidar_bin 562
imu_bin 5620
missing_lidar_partials 0
missing_imu 0
dropped_partial_sweeps 0
route.closed true
dataset_status.valid true
```

Synthetic IMU statistics from the loaded fixed-route run:

```text
accel_x min/max 0.000000000 / 0.000000000
accel_y min/max -0.037264860 / 4.820541292
accel_z min/max 9.810000000 / 9.810000000
gyro_z min/max -0.694157946 / 0.005366140
```

### Known Issues / Risks

- `route-loop` path generation is only a first implementation. It selects spawn points around the start pose by angular bins and asks Traffic Manager to follow them. This is deterministic-ish but not guaranteed to produce a true closed drivable route.
- The older spawn-point closure probe did not return near the start. The waypoint/kinematic route-loop has since closed successfully in a live CARLA run.
- The waypoint planner is still a bounded beam search over `Waypoint.next()` branches, so some maps/start poses may fail to find an available loop even when one exists.
- `route-loop` is now kinematic, not Traffic Manager/physics driving. This is deliberate for deterministic dataset generation.
- Raw CARLA IMU under physics-disabled transform driving had near-zero gyro and large accel spikes. Synthetic route IMU is now default and has been validated to produce bounded yaw-rate/lateral-acceleration values on the fixed-route run.
- Synthetic IMU is trajectory-derived rather than a full tire/suspension dynamics simulation. It is appropriate for deterministic loop-closure timing data, but not for physically realistic vehicle dynamics research.
- The current loop stop condition works, but it cannot trigger if route generation never returns near the start. Random route-loop spawn selection now skips known non-closing spawn points.
- `--route-ignore-lights` remains true by default, but `--respect-traffic-lights` now disables it.
- If missing packets occur during recording, the script marks the dataset invalid in `meta.yaml`, but it still continues the run. The stricter `--allow-dropped-sweeps` failure policy is not implemented yet.
- `duration` is ignored for `route-loop`; route-loop stops on route target distance by default, or `--loop-timeout` if explicitly set above zero.

### Next Work Items

Priority order for the next session:

1. If route planning fails on the target map/start pose, improve the planner.
   - Consider using `world.get_map().get_topology()` to build a lane graph.
   - Keep the pre-recording route validation behavior.

2. Implement stricter missing-packet failure policy.
   - Add explicit `--allow-dropped-sweeps` behavior.
   - Stop/exit non-zero by default if packets are missing during recording.

3. Optional: add route preview tooling.
   - A no-recording route planning/export mode would make it easier to curate fixed routes before data collection.

### Useful Commands

Basic syntax check:

```powershell
C:\Users\kwaks\AppData\Local\Programs\Python\Python312\python.exe -m py_compile data-gen\carla\carla_sync_camera_lidar_imu_recorder.py
```

Basic 1-second sync smoke:

```powershell
C:\Users\kwaks\AppData\Local\Programs\Python\Python312\python.exe data-gen\carla\carla_sync_camera_lidar_imu_recorder.py --duration 1.0 --warmup-timeout 10 --out-root data-gen\carla\validation_output --run-name smoke_next
```

Route-loop smoke without expecting closure:

```powershell
C:\Users\kwaks\AppData\Local\Programs\Python\Python312\python.exe data-gen\carla\carla_sync_camera_lidar_imu_recorder.py --motion-mode route-loop --loop-timeout 5 --loop-min-distance 100000 --warmup-timeout 10 --out-root data-gen\carla\validation_output --run-name route_loop_smoke_next
```

Fixed route-loop run using defaults:

```powershell
C:\Users\kwaks\AppData\Local\Programs\Python\Python312\python.exe data-gen\carla\carla_sync_camera_lidar_imu_recorder.py --motion-mode route-loop --route-file data-gen\carla\validation_output\fixed_route_latest.json --out-root data-gen\carla\validation_output --run-name route_loop_fixed_next
```

### User Goal To Preserve

The user wants to run one script and have it:

1. connect to the current CARLA server,
2. warm up until sensor data is stable,
3. start driving only after stable synchronization,
4. drive one loop from the current/start position,
5. synchronously save camera, LiDAR, and IMU,
6. stop/finalize automatically after loop closure.

Do not regress the timestamp stability behavior while improving route-loop driving.
