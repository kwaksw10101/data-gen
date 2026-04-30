#!/usr/bin/env python3
"""
Synchronous CARLA recorder for an ego vehicle orbiting one target vehicle.

This script is intentionally a data generator only. It spawns:
  A: ego vehicle with camera, LiDAR, and IMU
  B: target vehicle, held fixed near the center of the scene

The ego vehicle is replayed kinematically around B on a rounded-rectangle path.
It does not perform SLAM, point-cloud cropping, or object tracking.
"""

from __future__ import annotations

import argparse
import array
import json
import math
import os
import random
import shlex
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Sequence, Tuple

import carla_sync_camera_lidar_imu_recorder as rec


def make_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=None, help="Optional JSON file with default CLI values; explicit CLI args override it")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=2000)
    ap.add_argument("--timeout", type=float, default=10.0)
    ap.add_argument("--carla-root", default=None, help="CARLA root, e.g. C:/Users/.../CARLA_0.9.16")
    ap.add_argument("--map", default=None, help="Optional map name to load before recording")
    ap.add_argument("--out-root", default=os.path.join("data-gen", "carla", "output"))
    ap.add_argument("--run-name", default=None)
    ap.add_argument("--seed", type=int, default=7)

    ap.add_argument("--warmup-ticks", type=int, default=20)
    ap.add_argument("--warmup-min-ticks", type=int, default=50)
    ap.add_argument("--warmup-timeout", type=float, default=10.0)
    ap.add_argument("--sync-validation-sweeps", type=int, default=3)
    ap.add_argument("--sync-max-missing", type=int, default=0)
    ap.add_argument("--fixed-dt", type=float, default=0.01)
    ap.add_argument("--wait-timeout", type=float, default=2.0)
    ap.add_argument("--max-writer-queue", type=int, default=128)

    ap.add_argument("--camera-hz", type=float, default=20.0)
    ap.add_argument("--no-camera", action="store_true", help="Disable camera actor, camera warmup validation, and PNG output")
    ap.add_argument("--camera-width", type=int, default=1280)
    ap.add_argument("--camera-height", type=int, default=360)
    ap.add_argument("--camera-fov", type=float, default=90.0)
    ap.add_argument("--camera-color", choices=["rgb", "gray"], default="rgb")
    ap.add_argument("--camera-x", type=float, default=1.8)
    ap.add_argument("--camera-y", type=float, default=0.0)
    ap.add_argument("--camera-z", type=float, default=1.6)

    ap.add_argument("--lidar-hz", type=float, default=10.0)
    ap.add_argument("--lidar-sweep-points", type=int, default=65000)
    ap.add_argument("--lidar-pps", type=int, default=0)
    ap.add_argument("--lidar-channels", type=int, default=64)
    ap.add_argument("--lidar-lower-fov", type=float, default=-5.0)
    ap.add_argument("--lidar-upper-fov", type=float, default=15.0)
    ap.add_argument("--lidar-horizontal-fov", type=float, default=360.0)
    ap.add_argument("--lidar-range", type=float, default=100.0)
    ap.add_argument("--lidar-x", type=float, default=0.0)
    ap.add_argument("--lidar-y", type=float, default=0.0)
    ap.add_argument("--lidar-z", type=float, default=1.9)

    ap.add_argument("--imu-hz", type=float, default=100.0)
    ap.add_argument("--imu-x", type=float, default=0.0)
    ap.add_argument("--imu-y", type=float, default=0.0)
    ap.add_argument("--imu-z", type=float, default=0.0)

    ap.add_argument("--ego-blueprint", default="vehicle.tesla.model3")
    ap.add_argument("--target-blueprint", default="vehicle.audi.tt")
    ap.add_argument("--ego-filter", default="vehicle.tesla.model3", help="Fallback if --ego-blueprint is unavailable")
    ap.add_argument("--target-filter", default="vehicle.*", help="Fallback if --target-blueprint is unavailable")

    ap.add_argument("--target-x", type=float, default=None)
    ap.add_argument("--target-y", type=float, default=None)
    ap.add_argument("--target-z", type=float, default=None)
    ap.add_argument("--target-roll", type=float, default=0.0)
    ap.add_argument("--target-pitch", type=float, default=0.0)
    ap.add_argument("--target-yaw", type=float, default=None, help="Defaults to 0 for explicit target coordinates, or the selected spawn point yaw")
    ap.add_argument("--target-spawn-index", type=int, default=0)
    ap.add_argument("--spawn-z-offset", type=float, default=0.0)

    ap.add_argument("--orbit-half-length", type=float, default=16.0)
    ap.add_argument("--orbit-half-width", type=float, default=12.0)
    ap.add_argument("--corner-radius", type=float, default=4.0)
    ap.add_argument("--orbit-yaw-offset", type=float, default=0.0)
    ap.add_argument("--orbit-direction", choices=["ccw", "cw"], default="ccw")
    ap.add_argument("--orbit-laps", type=float, default=1.0)
    ap.add_argument("--target-speed-kmh", type=float, default=15.0)
    ap.add_argument("--path-step-m", type=float, default=0.5)
    ap.add_argument("--lookahead-m", type=float, default=2.0)
    ap.add_argument("--min-target-clearance", type=float, default=4.0)
    ap.add_argument("--skip-clearance-check", action="store_true")
    return ap


def parse_args_with_config() -> argparse.Namespace:
    parser = make_arg_parser()
    args = parser.parse_args()
    if not args.config:
        return args

    with open(args.config, "r", encoding="utf-8") as f:
        config = json.load(f)
    if not isinstance(config, dict):
        raise RuntimeError(f"config must be a JSON object: {args.config}")

    defaults = {}
    valid_dests = {action.dest for action in parser._actions}
    for key, value in config.items():
        dest = key.replace("-", "_")
        if dest not in valid_dests:
            raise RuntimeError(f"unknown config key {key!r} in {args.config}")
        if dest != "config":
            defaults[dest] = value

    parser.set_defaults(**defaults)
    return parser.parse_args()


def pick_exact_vehicle_blueprint(
    bp_lib: Any,
    blueprint_id: str,
    fallback_filter: str,
    role_name: str,
    rng: random.Random,
) -> Any:
    try:
        bp = bp_lib.find(blueprint_id)
    except Exception:
        candidates = list(bp_lib.filter(fallback_filter))
        if not candidates:
            candidates = list(bp_lib.filter("vehicle.*"))
        if not candidates:
            raise RuntimeError(f"no vehicle blueprint found for {blueprint_id!r} or {fallback_filter!r}")
        bp = rng.choice(candidates)
        print(f"[WARN] blueprint {blueprint_id!r} not found; using fallback {bp.id!r}")

    rec.set_attr_if_exists(bp, "role_name", role_name)
    if bp.has_attribute("color"):
        colors = list(bp.get_attribute("color").recommended_values)
        if colors:
            color_index = rng.randrange(len(colors))
            bp.set_attribute("color", colors[color_index])
    return bp


def sorted_spawn_points(world_map: Any) -> List[Any]:
    points = list(world_map.get_spawn_points())
    points.sort(
        key=lambda tf: (
            round(float(tf.location.x), 3),
            round(float(tf.location.y), 3),
            round(float(tf.location.z), 3),
            round(float(tf.rotation.yaw), 3),
        )
    )
    return points


def resolve_target_transform(carla: Any, world: Any, args: argparse.Namespace) -> Any:
    world_map = world.get_map()
    if args.target_x is not None or args.target_y is not None:
        if args.target_x is None or args.target_y is None:
            raise RuntimeError("--target-x and --target-y must be provided together")
        if args.target_z is None:
            probe = carla.Location(x=float(args.target_x), y=float(args.target_y), z=0.0)
            wp = world_map.get_waypoint(probe, project_to_road=True)
            z = float(wp.transform.location.z) if wp is not None else 0.0
        else:
            z = float(args.target_z)
        return carla.Transform(
            carla.Location(x=float(args.target_x), y=float(args.target_y), z=z + float(args.spawn_z_offset)),
            carla.Rotation(
                roll=float(args.target_roll),
                pitch=float(args.target_pitch),
                yaw=float(0.0 if args.target_yaw is None else args.target_yaw),
            ),
        )

    spawn_points = sorted_spawn_points(world_map)
    if not spawn_points:
        raise RuntimeError("current map has no spawn points; pass --target-x/--target-y/--target-z")
    idx = int(args.target_spawn_index) % len(spawn_points)
    tf = spawn_points[idx]
    return carla.Transform(
        carla.Location(
            x=float(tf.location.x),
            y=float(tf.location.y),
            z=float(tf.location.z) + float(args.spawn_z_offset),
        ),
        carla.Rotation(
            roll=float(args.target_roll),
            pitch=float(args.target_pitch),
            yaw=float(args.target_yaw if args.target_yaw is not None else tf.rotation.yaw),
        ),
    )


def rotate_xy(x: float, y: float, yaw_deg: float) -> Tuple[float, float]:
    yaw = math.radians(yaw_deg)
    c = math.cos(yaw)
    s = math.sin(yaw)
    return x * c - y * s, x * s + y * c


def build_rounded_rectangle_path(carla: Any, center: Any, yaw_deg: float, args: argparse.Namespace) -> List[Any]:
    half_l = float(args.orbit_half_length)
    half_w = float(args.orbit_half_width)
    radius = float(args.corner_radius)
    step = max(0.05, float(args.path_step_m))
    if half_l <= 0.0 or half_w <= 0.0:
        raise RuntimeError("orbit half extents must be positive")
    if radius <= 0.0 or radius >= min(half_l, half_w):
        raise RuntimeError("--corner-radius must be >0 and smaller than both orbit half extents")

    local_points: List[Tuple[float, float]] = []

    def add_local(x: float, y: float) -> None:
        if local_points:
            px, py = local_points[-1]
            if math.hypot(x - px, y - py) < 1e-8:
                return
        local_points.append((x, y))

    def add_line(start: Tuple[float, float], end: Tuple[float, float]) -> None:
        length = math.hypot(end[0] - start[0], end[1] - start[1])
        n = max(1, int(math.ceil(length / step)))
        for i in range(1, n + 1):
            a = i / n
            add_local(start[0] + (end[0] - start[0]) * a, start[1] + (end[1] - start[1]) * a)

    def add_arc(cx: float, cy: float, start_deg: float, end_deg: float) -> None:
        delta = math.radians(end_deg - start_deg)
        n = max(2, int(math.ceil(abs(delta) * radius / step)))
        for i in range(1, n + 1):
            a = math.radians(start_deg) + delta * (i / n)
            add_local(cx + radius * math.cos(a), cy + radius * math.sin(a))

    start = (half_l, -half_w + radius)
    add_local(*start)
    add_line(start, (half_l, half_w - radius))
    add_arc(half_l - radius, half_w - radius, 0.0, 90.0)
    add_line((half_l - radius, half_w), (-half_l + radius, half_w))
    add_arc(-half_l + radius, half_w - radius, 90.0, 180.0)
    add_line((-half_l, half_w - radius), (-half_l, -half_w + radius))
    add_arc(-half_l + radius, -half_w + radius, 180.0, 270.0)
    add_line((-half_l + radius, -half_w), (half_l - radius, -half_w))
    add_arc(half_l - radius, -half_w + radius, 270.0, 360.0)

    if args.orbit_direction == "cw":
        local_points = [local_points[0]] + list(reversed(local_points[1:-1])) + [local_points[0]]
    elif math.hypot(local_points[-1][0] - local_points[0][0], local_points[-1][1] - local_points[0][1]) > 1e-8:
        local_points.append(local_points[0])

    nodes = []
    for x_local, y_local in local_points:
        x_rot, y_rot = rotate_xy(x_local, y_local, yaw_deg)
        nodes.append(
            carla.Location(
                x=float(center.x) + x_rot,
                y=float(center.y) + y_rot,
                z=float(center.z),
            )
        )
    return nodes


def cumulative_lengths(nodes: Sequence[Any]) -> List[float]:
    cumulative = [0.0]
    for a, b in zip(nodes[:-1], nodes[1:]):
        cumulative.append(cumulative[-1] + float(b.distance(a)))
    return cumulative


def sample_path(nodes: Sequence[Any], cumulative: Sequence[float], distance_m: float) -> Tuple[float, float, float]:
    total = float(cumulative[-1])
    if total <= 1e-9:
        loc = nodes[0]
        return float(loc.x), float(loc.y), float(loc.z)
    d = max(0.0, min(total, distance_m))
    idx = 0
    while idx + 1 < len(cumulative) - 1 and cumulative[idx + 1] < d:
        idx += 1
    start = nodes[idx]
    end = nodes[min(idx + 1, len(nodes) - 1)]
    seg_len = max(1e-9, float(end.distance(start)))
    alpha = rec.clamp((d - cumulative[idx]) / seg_len, 0.0, 1.0)
    return (
        float(start.x) + (float(end.x) - float(start.x)) * alpha,
        float(start.y) + (float(end.y) - float(start.y)) * alpha,
        float(start.z) + (float(end.z) - float(start.z)) * alpha,
    )


def transform_at_distance(
    carla: Any,
    nodes: Sequence[Any],
    cumulative: Sequence[float],
    distance_m: float,
    lookahead_m: float,
) -> Any:
    total = float(cumulative[-1])
    d = rec.clamp(distance_m, 0.0, total)
    x, y, z = sample_path(nodes, cumulative, d)
    back_x, back_y, _ = sample_path(nodes, cumulative, max(0.0, d - lookahead_m))
    ahead_x, ahead_y, _ = sample_path(nodes, cumulative, min(total, d + lookahead_m))
    if math.hypot(ahead_x - back_x, ahead_y - back_y) < 1e-6 and len(nodes) >= 2:
        ahead_x = float(nodes[1].x)
        ahead_y = float(nodes[1].y)
        back_x = float(nodes[0].x)
        back_y = float(nodes[0].y)
    yaw = math.degrees(math.atan2(ahead_y - back_y, ahead_x - back_x))
    return carla.Transform(carla.Location(x=x, y=y, z=z), carla.Rotation(yaw=yaw))


def synthetic_imu_values(meas: Any, motion_state: Dict[str, Any]) -> array.array:
    base = motion_state.get("synthetic_imu_base", (0.0, 0.0, 9.81, 0.0, 0.0, 0.0, float(meas.compass)))
    return array.array(
        "d",
        [
            float(base[0]),
            float(base[1]),
            float(base[2]),
            float(base[3]),
            float(base[4]),
            float(base[5]),
            float(base[6]),
            float(meas.timestamp),
        ],
    )


def apply_orbit_kinematic(
    carla: Any,
    ego: Any,
    nodes: Sequence[Any],
    cumulative: Sequence[float],
    motion_state: Dict[str, Any],
    args: argparse.Namespace,
    world_dt: float,
) -> bool:
    total_length = float(cumulative[-1])
    target_distance = total_length * max(0.0, float(args.orbit_laps))
    speed_mps = max(0.1, float(args.target_speed_kmh) / 3.6)
    distance_m = min(target_distance, float(motion_state.get("distance_m", 0.0)) + speed_mps * world_dt)
    playback_distance = distance_m % total_length if total_length > 1e-9 else 0.0
    if target_distance - distance_m <= 1e-6 and abs(target_distance % total_length) <= speed_mps * world_dt + 1e-6:
        playback_distance = total_length

    tf = transform_at_distance(carla, nodes, cumulative, playback_distance, max(0.1, float(args.lookahead_m)))
    ego.set_transform(tf)
    try:
        ego.set_target_velocity(carla.Vector3D(0.0, 0.0, 0.0))
        ego.set_target_angular_velocity(carla.Vector3D(0.0, 0.0, 0.0))
    except Exception:
        pass

    yaw = float(tf.rotation.yaw)
    prev_yaw = float(motion_state.get("prev_yaw", yaw))
    if bool(motion_state.get("imu_initialized", False)):
        yaw_rate = math.radians(rec.wrap_deg(yaw - prev_yaw)) / world_dt
    else:
        yaw_rate = 0.0
        motion_state["imu_initialized"] = True
    yaw_rate = rec.clamp(yaw_rate, -2.5, 2.5)
    motion_state["synthetic_imu_base"] = (
        0.0,
        rec.clamp(speed_mps * yaw_rate, -20.0, 20.0),
        9.81,
        0.0,
        0.0,
        -yaw_rate,
        float((math.radians(90.0 - yaw)) % (2.0 * math.pi)),
    )
    motion_state["distance_m"] = distance_m
    motion_state["prev_yaw"] = yaw
    return distance_m >= target_distance - 1e-6


def check_target_clearance(target: Any, args: argparse.Namespace) -> Tuple[bool, float, float]:
    extent = target.bounding_box.extent
    target_radius = math.hypot(float(extent.x), float(extent.y))
    nearest_path_radius = min(float(args.orbit_half_length), float(args.orbit_half_width))
    clearance = nearest_path_radius - target_radius
    return clearance >= float(args.min_target_clearance), clearance, target_radius


def save_orbit_path(path: str, nodes: Sequence[Any], cumulative: Sequence[float], args: argparse.Namespace) -> None:
    data = {
        "format": "carla_rounded_rectangle_orbit_v1",
        "orbit_half_length_m": float(args.orbit_half_length),
        "orbit_half_width_m": float(args.orbit_half_width),
        "corner_radius_m": float(args.corner_radius),
        "orbit_laps": float(args.orbit_laps),
        "target_speed_kmh": float(args.target_speed_kmh),
        "path_length_m": float(cumulative[-1]),
        "nodes": [rec.location_to_dict(loc) for loc in nodes],
    }
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(data, f, indent=2)
        f.write("\n")


def run_sensor_warmup_and_validation(
    carla: Any,
    world: Any,
    ego: Any,
    args: argparse.Namespace,
    camera_buf: Any,
    lidar_buf: Any,
    imu_buf: Any,
    camera_ticks: int,
    lidar_sweep_ticks: int,
) -> Tuple[int, Dict[str, Any]]:
    if not bool(getattr(args, "no_camera", False)):
        return rec.run_sensor_warmup_and_validation(
            carla,
            world,
            ego,
            args,
            camera_buf,
            lidar_buf,
            imu_buf,
            camera_ticks,
            lidar_sweep_ticks,
        )

    min_warmup_ticks = max(int(args.warmup_ticks), int(args.warmup_min_ticks))
    validation_ticks = max(lidar_sweep_ticks * int(args.sync_validation_sweeps), 10)
    max_missing = int(args.sync_max_missing)
    deadline = None if args.warmup_timeout <= 0.0 else time.time() + float(args.warmup_timeout)
    attempt = 0

    while True:
        attempt += 1
        print(
            f"[WARMUP] attempt={attempt} camera=disabled warmup_ticks={min_warmup_ticks} "
            f"validation_ticks={validation_ticks}"
        )

        for _ in range(min_warmup_ticks):
            rec.hold_vehicle_stopped(carla, ego)
            world.tick()

        warmup_frame = int(world.get_snapshot().frame)
        lidar_buf.clear()
        imu_buf.clear()

        missing_imu = 0
        missing_lidar = 0
        first_validation_frame = None
        last_validation_frame = None

        for _ in range(validation_ticks):
            rec.hold_vehicle_stopped(carla, ego)
            world_frame = int(world.tick())
            if first_validation_frame is None:
                first_validation_frame = world_frame
            last_validation_frame = world_frame

            if imu_buf.wait(world_frame, args.wait_timeout) is None:
                missing_imu += 1
            if lidar_buf.wait(world_frame, args.wait_timeout) is None:
                missing_lidar += 1

        missing_ok = missing_imu <= max_missing and missing_lidar <= max_missing
        stats: Dict[str, Any] = {
            "attempt": attempt,
            "warmup_frame": warmup_frame,
            "first_validation_frame": first_validation_frame,
            "last_validation_frame": last_validation_frame,
            "validation_ticks": validation_ticks,
            "missing_imu": missing_imu,
            "missing_lidar": missing_lidar,
            "camera_frames": 0,
            "camera_diffs": [],
            "camera_ok": True,
            "camera_disabled": True,
            "missing_ok": missing_ok,
        }

        if missing_ok:
            for _ in range(lidar_sweep_ticks * 2 + 1):
                rec.hold_vehicle_stopped(carla, ego)
                frame = int(world.tick())
                if (frame % lidar_sweep_ticks) == 0:
                    lidar_buf.clear()
                    imu_buf.clear()
                    stats["record_start_boundary_frame"] = frame
                    print(
                        f"[WARMUP] stable. camera=disabled "
                        f"missing_lidar={missing_lidar} missing_imu={missing_imu} boundary_frame={frame}"
                    )
                    return frame, stats
            stats["alignment_failed"] = True

        print(f"[WARMUP][RETRY] camera=disabled missing_lidar={missing_lidar} missing_imu={missing_imu}")
        lidar_buf.clear()
        imu_buf.clear()

        if deadline is not None and time.time() > deadline:
            raise RuntimeError(f"sensor warmup/sync validation failed before timeout: {stats}")


def write_meta(
    path: str,
    args: argparse.Namespace,
    server_version: str,
    map_name: str,
    vehicle_id: str,
    spawn_tf: Any,
    out_dir: str,
    run_name: str,
    camera_tick: float,
    lidar_tick: float,
    imu_tick: float,
    packets_per_sweep: int,
    points_per_second: int,
) -> None:
    setattr(args, "imu_data_source", "synthetic_kinematic")
    setattr(args, "imu_noise_model", "ideal_zero_noise_bias")
    if not bool(getattr(args, "no_camera", False)):
        rec.write_meta(
            path,
            args,
            server_version,
            map_name,
            vehicle_id,
            spawn_tf,
            out_dir,
            run_name,
            camera_tick,
            lidar_tick,
            imu_tick,
            packets_per_sweep,
            points_per_second,
        )
        return

    spawn = rec.transform_to_dict(spawn_tf)
    lidar_tf_ros = rec.ros_transform_from_carla_location(args.lidar_x, args.lidar_y, args.lidar_z)
    imu_tf_ros = rec.ros_transform_from_carla_location(args.imu_x, args.imu_y, args.imu_z)
    lidar_to_imu_tf_ros = rec.relative_ros_transform(lidar_tf_ros, imu_tf_ros)
    cmd = " ".join(shlex.quote(x) for x in sys.argv)

    text = f"""# Generated by {os.path.basename(sys.argv[0])}
generated_at_local: "{datetime.now().isoformat(timespec="seconds")}"
run_name: "{run_name}"
output_dir: "{out_dir.replace(os.sep, "/")}"
command: "{cmd.replace(chr(34), chr(39))}"

carla:
  server_version: "{server_version}"
  map: "{map_name}"
  vehicle_blueprint: "{vehicle_id}"
  spawn_carla:
    x: {spawn["x"]:.9f}
    y: {spawn["y"]:.9f}
    z: {spawn["z"]:.9f}
    roll: {spawn["roll"]:.9f}
    pitch: {spawn["pitch"]:.9f}
    yaw: {spawn["yaw"]:.9f}

world:
  synchronous_mode: true
  fixed_delta_seconds: {args.fixed_dt:.9f}
  rate_hz: {1.0 / args.fixed_dt:.9f}

coordinate_convention:
  output_frame: "ROS body frame: x forward, y left, z up"
  carla_to_output_position_vector: "[x, -y, z]"
  carla_to_output_imu_angular_velocity: "[-x, y, -z]"
  note: "LiDAR packets are merged without recorder-side deskew; each point keeps its partial packet timestamp."

camera:
  enabled: false

lidar:
  name: "lidar"
  blueprint: "sensor.lidar.ray_cast"
  directory: "lidar"
  data_directory: "lidar/data"
  timestamps_file: "lidar/timestamps.csv"
  data_format: "raw float32 row-major Nx6"
  point_stride_bytes: 24
  fields: ["x", "y", "z", "intensity", "ring", "timestamp"]
  timestamp_field: "absolute CARLA elapsed_seconds of the 100Hz partial packet; constant for points in the same partial"
  rate_hz: {args.lidar_hz:.9f}
  partial_packet_rate_hz: {1.0 / lidar_tick:.9f}
  sensor_tick: {lidar_tick:.9f}
  packets_per_sweep: {packets_per_sweep}
  rotation_frequency_hz: {args.lidar_hz:.9f}
  points_per_second: {points_per_second}
  carla_requested_rays_per_sweep: {args.lidar_sweep_points}
  horizon_scan_requested: {rec.requested_horizon_scan(int(args.lidar_sweep_points), int(args.lidar_channels))}
  channels: {args.lidar_channels}
  lower_fov_deg: {args.lidar_lower_fov:.9f}
  upper_fov_deg: {args.lidar_upper_fov:.9f}
  horizontal_fov_deg: {args.lidar_horizontal_fov:.9f}
  max_range_m: {args.lidar_range:.9f}
  base_link_to_sensor_ros:
    x: {lidar_tf_ros["x"]:.9f}
    y: {lidar_tf_ros["y"]:.9f}
    z: {lidar_tf_ros["z"]:.9f}
    roll: {lidar_tf_ros["roll"]:.9f}
    pitch: {lidar_tf_ros["pitch"]:.9f}
    yaw: {lidar_tf_ros["yaw"]:.9f}
  timestamps_columns: ["seq", "sweep_end_timestamp_seconds", "sweep_end_carla_frame", "sweep_start_timestamp_seconds", "sweep_start_carla_frame"]

imu:
  name: "imu"
  blueprint: "sensor.other.imu"
  directory: "imu"
  data_directory: "imu/data"
  timestamps_file: "imu/timestamps.csv"
  data_format: "raw float64 row-major 1x8"
  sample_stride_bytes: 64
  fields: ["accel_x", "accel_y", "accel_z", "gyro_x", "gyro_y", "gyro_z", "compass", "timestamp"]
  accel_units: "m/s^2"
  gyro_units: "rad/s"
  compass_units: "rad, CARLA-provided compass"
  timestamp_field: "absolute CARLA elapsed_seconds"
  data_source: "synthetic_kinematic"
  noise_model: "ideal_zero_noise_bias"
  noise_spec:
    accel_noise_density: 0.0
    gyro_noise_density: 0.0
    accel_bias_random_walk: 0.0
    gyro_bias_random_walk: 0.0
    accel_bias_initial: [0.0, 0.0, 0.0]
    gyro_bias_initial: [0.0, 0.0, 0.0]
  rate_hz: {args.imu_hz:.9f}
  sensor_tick: {imu_tick:.9f}
  base_link_to_sensor_ros:
    x: {imu_tf_ros["x"]:.9f}
    y: {imu_tf_ros["y"]:.9f}
    z: {imu_tf_ros["z"]:.9f}
    roll: {imu_tf_ros["roll"]:.9f}
    pitch: {imu_tf_ros["pitch"]:.9f}
    yaw: {imu_tf_ros["yaw"]:.9f}
  timestamps_columns: ["seq", "timestamp_seconds", "carla_frame"]

sensor_pair:
  lidar_to_imu_ros:
    x: {lidar_to_imu_tf_ros["x"]:.9f}
    y: {lidar_to_imu_tf_ros["y"]:.9f}
    z: {lidar_to_imu_tf_ros["z"]:.9f}
    roll: {lidar_to_imu_tf_ros["roll"]:.9f}
    pitch: {lidar_to_imu_tf_ros["pitch"]:.9f}
    yaw: {lidar_to_imu_tf_ros["yaw"]:.9f}
"""
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(text)


def append_scenario_summary(path: str, summary: Dict[str, Any]) -> None:
    def pose(tf: Any) -> str:
        return (
            f"{{x: {tf.location.x:.9f}, y: {tf.location.y:.9f}, z: {tf.location.z:.9f}, "
            f"roll: {tf.rotation.roll:.9f}, pitch: {tf.rotation.pitch:.9f}, yaw: {tf.rotation.yaw:.9f}}}"
        )

    text = "\nscenario:\n"
    text += "  mode: \"rotation-around-object\"\n"
    text += f"  completed: {str(bool(summary['completed'])).lower()}\n"
    text += f"  ego_blueprint: \"{summary['ego_blueprint']}\"\n"
    text += f"  target_blueprint: \"{summary['target_blueprint']}\"\n"
    text += f"  target_pose_carla: {pose(summary['target_pose_carla'])}\n"
    text += f"  ego_start_pose_carla: {pose(summary['ego_start_pose_carla'])}\n"
    text += f"  ego_end_pose_carla: {pose(summary['ego_end_pose_carla'])}\n"
    text += f"  orbit_half_length_m: {summary['orbit_half_length_m']:.9f}\n"
    text += f"  orbit_half_width_m: {summary['orbit_half_width_m']:.9f}\n"
    text += f"  corner_radius_m: {summary['corner_radius_m']:.9f}\n"
    text += f"  orbit_laps: {summary['orbit_laps']:.9f}\n"
    text += f"  target_speed_kmh: {summary['target_speed_kmh']:.9f}\n"
    text += f"  path_length_m: {summary['path_length_m']:.9f}\n"
    text += f"  target_record_distance_m: {summary['target_record_distance_m']:.9f}\n"
    text += f"  recorded_distance_m: {summary['recorded_distance_m']:.9f}\n"
    text += f"  target_clearance_m: {summary['target_clearance_m']:.9f}\n"
    text += f"  start_frame: {summary['start_frame']}\n"
    text += f"  start_timestamp: {summary['start_timestamp']:.9f}\n"
    text += f"  end_frame: {summary['end_frame']}\n"
    text += f"  end_timestamp: {summary['end_timestamp']:.9f}\n"
    text += "  orbit_path_file: \"orbit_path.json\"\n"
    with open(path, "a", encoding="utf-8", newline="\n") as f:
        f.write(text)


def main() -> None:
    args = parse_args_with_config()
    if args.orbit_laps <= 0.0:
        raise ValueError("--orbit-laps must be positive")

    rng = random.Random(args.seed)
    carla = rec.import_carla_or_exit(args.carla_root)

    world_dt = float(args.fixed_dt)
    camera_ticks = 0 if args.no_camera else rec.ensure_integer_period_ticks("camera", args.camera_hz, world_dt)
    lidar_sweep_ticks = rec.ensure_integer_period_ticks("lidar", args.lidar_hz, world_dt)
    imu_ticks = rec.ensure_integer_period_ticks("imu", args.imu_hz, world_dt)
    if imu_ticks != 1:
        raise ValueError("For this recorder IMU must run every world tick; use --imu-hz 100 with --fixed-dt 0.01")

    camera_tick = camera_ticks * world_dt if not args.no_camera else 0.0
    lidar_tick = world_dt
    imu_tick = imu_ticks * world_dt
    points_per_second = int(args.lidar_pps) if args.lidar_pps > 0 else int(round(args.lidar_sweep_points * args.lidar_hz))

    run_name = args.run_name or datetime.now().strftime("rotation_object_%Y%m%d_%H%M%S")
    out_dir = os.path.abspath(os.path.join(args.out_root, run_name))
    lidar_dir = os.path.join(out_dir, "lidar")
    imu_dir = os.path.join(out_dir, "imu")
    dirs = [os.path.join(lidar_dir, "data"), os.path.join(imu_dir, "data")]
    if not args.no_camera:
        camera_dir = os.path.join(out_dir, "camera")
        dirs.insert(0, os.path.join(camera_dir, "data"))
    else:
        camera_dir = None
    for d in dirs:
        rec.mkdir(d)

    client = carla.Client(args.host, args.port)
    client.set_timeout(args.timeout)

    world = None
    original_settings = None
    actors: List[Any] = []
    camera_writer = None
    lidar_writer = None
    imu_writer = None
    f_cam_ts = None
    f_lidar_ts = None
    f_imu_ts = None

    camera_stamps: List[float] = []
    lidar_stamps: List[float] = []
    imu_stamps: List[float] = []
    missing_lidar = 0
    missing_imu = 0
    dropped_partial_sweeps = 0
    orbit_completed = False
    target_clearance = float("nan")
    target_radius = float("nan")
    start_frame = 0
    start_timestamp = 0.0
    last_world_frame = 0
    last_sim_timestamp = 0.0

    try:
        world = rec.get_or_load_world(client, args)
        original_settings = world.get_settings()
        settings = world.get_settings()
        settings.synchronous_mode = True
        settings.fixed_delta_seconds = world_dt
        world.apply_settings(settings)

        bp_lib = world.get_blueprint_library()
        ego_bp = pick_exact_vehicle_blueprint(bp_lib, args.ego_blueprint, args.ego_filter, "ego", rng)
        target_bp = pick_exact_vehicle_blueprint(bp_lib, args.target_blueprint, args.target_filter, "target", rng)

        target_tf = resolve_target_transform(carla, world, args)
        target = world.try_spawn_actor(target_bp, target_tf)
        if target is None:
            raise RuntimeError("failed to spawn target vehicle B at the requested target pose")
        actors.append(target)
        try:
            target.set_simulate_physics(False)
        except Exception:
            pass

        orbit_yaw = float(target_tf.rotation.yaw) + float(args.orbit_yaw_offset)
        orbit_nodes = build_rounded_rectangle_path(carla, target_tf.location, orbit_yaw, args)
        orbit_cumulative = cumulative_lengths(orbit_nodes)
        if orbit_cumulative[-1] <= 1e-6:
            raise RuntimeError("generated orbit path is empty")
        save_orbit_path(os.path.join(out_dir, "orbit_path.json"), orbit_nodes, orbit_cumulative, args)

        if not args.skip_clearance_check:
            ok, target_clearance, target_radius = check_target_clearance(target, args)
            if not ok:
                raise RuntimeError(
                    f"target clearance check failed: clearance={target_clearance:.2f}m "
                    f"target_radius={target_radius:.2f}m required={args.min_target_clearance:.2f}m"
                )
        else:
            _ok, target_clearance, target_radius = check_target_clearance(target, args)

        start_tf = transform_at_distance(carla, orbit_nodes, orbit_cumulative, 0.0, max(0.1, float(args.lookahead_m)))
        ego = world.try_spawn_actor(ego_bp, start_tf)
        if ego is None:
            raise RuntimeError("failed to spawn ego vehicle A at the orbit start pose; choose a clearer target area")
        actors.append(ego)
        try:
            ego.set_simulate_physics(True)
        except Exception:
            pass

        camera = None
        if not args.no_camera:
            camera_bp = bp_lib.find("sensor.camera.rgb")
            rec.configure_camera_blueprint(camera_bp, args.camera_width, args.camera_height, args.camera_fov, camera_tick)
            rec.set_attr_if_exists(camera_bp, "role_name", "camera")
            camera = world.spawn_actor(
                camera_bp,
                carla.Transform(carla.Location(x=args.camera_x, y=args.camera_y, z=args.camera_z), carla.Rotation()),
                attach_to=ego,
            )
            actors.append(camera)

        lidar_bp = bp_lib.find("sensor.lidar.ray_cast")
        rec.configure_lidar_blueprint(
            lidar_bp,
            args.lidar_range,
            points_per_second,
            args.lidar_channels,
            args.lidar_upper_fov,
            args.lidar_lower_fov,
            args.lidar_horizontal_fov,
            args.lidar_hz,
            lidar_tick,
        )
        rec.set_attr_if_exists(lidar_bp, "role_name", "lidar")
        lidar = world.spawn_actor(
            lidar_bp,
            carla.Transform(carla.Location(x=args.lidar_x, y=args.lidar_y, z=args.lidar_z), carla.Rotation()),
            attach_to=ego,
        )
        actors.append(lidar)

        imu_bp = bp_lib.find("sensor.other.imu")
        rec.configure_imu_blueprint(imu_bp, imu_tick)
        rec.set_attr_if_exists(imu_bp, "role_name", "imu")
        imu = world.spawn_actor(
            imu_bp,
            carla.Transform(carla.Location(x=args.imu_x, y=args.imu_y, z=args.imu_z), carla.Rotation()),
            attach_to=ego,
        )
        actors.append(imu)

        camera_buf = rec.SensorBuffer("camera", max_keep=8192) if not args.no_camera else None
        lidar_buf = rec.SensorBuffer("lidar", max_keep=8192)
        imu_buf = rec.SensorBuffer("imu", max_keep=8192)
        if camera is not None and camera_buf is not None:
            camera.listen(lambda m: camera_buf.put(m))
        lidar.listen(lambda m: lidar_buf.put(m))
        imu.listen(lambda m: imu_buf.put(m))

        meta_path = os.path.join(out_dir, "meta.yaml")
        write_meta(
            meta_path,
            args,
            client.get_server_version(),
            world.get_map().name,
            ego_bp.id,
            start_tf,
            out_dir,
            run_name,
            camera_tick,
            lidar_tick,
            imu_tick,
            lidar_sweep_ticks,
            points_per_second,
        )

        camera_writer = (
            rec.CameraWriter(os.path.join(camera_dir, "data"), args.camera_color, max_queue=args.max_writer_queue)
            if camera_dir is not None
            else None
        )
        lidar_writer = rec.LidarWriter(
            os.path.join(lidar_dir, "data"),
            args.lidar_channels,
            args.lidar_lower_fov,
            args.lidar_upper_fov,
            max_queue=args.max_writer_queue,
        )
        imu_writer = rec.ImuWriter(os.path.join(imu_dir, "data"), max_queue=args.max_writer_queue)

        f_cam_ts = (
            rec.open_csv_with_header(
                os.path.join(camera_dir, "timestamps.csv"),
                ("seq", "timestamp_seconds", "carla_frame"),
            )
            if camera_dir is not None
            else None
        )
        f_lidar_ts = rec.open_csv_with_header(
            os.path.join(lidar_dir, "timestamps.csv"),
            (
                "seq",
                "sweep_end_timestamp_seconds",
                "sweep_end_carla_frame",
                "sweep_start_timestamp_seconds",
                "sweep_start_carla_frame",
            ),
        )
        f_imu_ts = rec.open_csv_with_header(
            os.path.join(imu_dir, "timestamps.csv"),
            ("seq", "timestamp_seconds", "carla_frame"),
        )

        print(f"[RUN] output: {out_dir}")
        print(
            f"[SCENARIO] target={target_bp.id} ego={ego_bp.id} "
            f"path_length={orbit_cumulative[-1]:.1f}m laps={args.orbit_laps:.2f} "
            f"speed={args.target_speed_kmh:.1f}km/h clearance={target_clearance:.2f}m"
        )
        print(
            f"[RUN] world={1/world_dt:.1f}Hz, camera={'off' if args.no_camera else f'{args.camera_hz:.1f}Hz'}, "
            f"lidar={args.lidar_hz:.1f}Hz ({lidar_sweep_ticks} partials/sweep), imu={args.imu_hz:.1f}Hz"
        )

        warmup_frame, sync_stats = run_sensor_warmup_and_validation(
            carla,
            world,
            ego,
            args,
            camera_buf,
            lidar_buf,
            imu_buf,
            camera_ticks,
            lidar_sweep_ticks,
        )
        rec.append_sync_summary(meta_path, sync_stats)

        try:
            ego.set_simulate_physics(False)
        except Exception:
            pass
        ego.set_transform(start_tf)
        if camera_buf is not None:
            camera_buf.clear()
        lidar_buf.clear()
        imu_buf.clear()

        start_snapshot = world.get_snapshot()
        start_frame = int(start_snapshot.frame)
        start_timestamp = float(start_snapshot.timestamp.elapsed_seconds)

        seq_camera = 0
        seq_lidar = 0
        seq_imu = 0
        tick_idx = 0
        lidar_packets: List[Tuple[bytes, float, int]] = []
        last_progress_t = time.time()
        record_start_wall = time.time()
        motion_state: Dict[str, Any] = {"distance_m": 0.0}
        target_record_distance = float(orbit_cumulative[-1]) * float(args.orbit_laps)

        while True:
            orbit_completed = apply_orbit_kinematic(
                carla, ego, orbit_nodes, orbit_cumulative, motion_state, args, world_dt
            )

            world_frame = int(world.tick())
            tick_idx += 1
            last_world_frame = world_frame
            last_sim_timestamp = float(world.get_snapshot().timestamp.elapsed_seconds)

            imu_meas = imu_buf.wait(world_frame, args.wait_timeout)
            if imu_meas is None:
                missing_imu += 1
                print(f"[WARN] missing IMU frame={world_frame}")
            else:
                imu_writer.push((seq_imu, synthetic_imu_values(imu_meas, motion_state)))
                rec.write_csv_row(
                    f_imu_ts,
                    (f"{seq_imu:06d}", rec.format_float(float(imu_meas.timestamp)), int(imu_meas.frame)),
                )
                imu_stamps.append(float(imu_meas.timestamp))
                seq_imu += 1

            lidar_meas = lidar_buf.wait(world_frame, args.wait_timeout)
            if lidar_meas is None:
                missing_lidar += 1
                if lidar_packets:
                    dropped_partial_sweeps += 1
                    lidar_packets = []
                print(f"[WARN] missing LiDAR partial frame={world_frame}; dropping current partial sweep")
            else:
                lidar_packets.append((bytes(lidar_meas.raw_data), float(lidar_meas.timestamp), int(lidar_meas.frame)))
                if len(lidar_packets) == lidar_sweep_ticks:
                    start_ts = lidar_packets[0][1]
                    start_packet_frame = lidar_packets[0][2]
                    end_ts = lidar_packets[-1][1]
                    end_frame = lidar_packets[-1][2]
                    lidar_writer.push((seq_lidar, lidar_packets))
                    rec.write_csv_row(
                        f_lidar_ts,
                        (
                            f"{seq_lidar:06d}",
                            rec.format_float(end_ts),
                            end_frame,
                            rec.format_float(start_ts),
                            start_packet_frame,
                        ),
                    )
                    lidar_stamps.append(end_ts)
                    seq_lidar += 1
                    lidar_packets = []

            if camera_buf is not None and camera_writer is not None and f_cam_ts is not None:
                for cam_meas in camera_buf.pop_all_ready(world_frame, warmup_frame):
                    camera_writer.push((seq_camera, bytes(cam_meas.raw_data), int(cam_meas.width), int(cam_meas.height)))
                    rec.write_csv_row(
                        f_cam_ts,
                        (f"{seq_camera:06d}", rec.format_float(float(cam_meas.timestamp)), int(cam_meas.frame)),
                    )
                    camera_stamps.append(float(cam_meas.timestamp))
                    seq_camera += 1

            if orbit_completed and not lidar_packets:
                break

            now = time.time()
            if now - last_progress_t >= 2.0:
                sim_done = tick_idx * world_dt
                wall_elapsed = max(1e-6, now - record_start_wall)
                print(
                    f"[REC] sim={sim_done:.2f}s wall={wall_elapsed:.2f}s "
                    f"camera={'off' if args.no_camera else seq_camera} lidar={seq_lidar} imu={seq_imu} "
                    f"orbit_dist={float(motion_state.get('distance_m', 0.0)):.1f}/{target_record_distance:.1f}m"
                )
                last_progress_t = now

        time.sleep(0.05)
        if camera_buf is not None and camera_writer is not None and f_cam_ts is not None:
            for cam_meas in camera_buf.pop_all_ready(10**12, warmup_frame):
                camera_writer.push((seq_camera, bytes(cam_meas.raw_data), int(cam_meas.width), int(cam_meas.height)))
                rec.write_csv_row(
                    f_cam_ts,
                    (f"{seq_camera:06d}", rec.format_float(float(cam_meas.timestamp)), int(cam_meas.frame)),
                )
                camera_stamps.append(float(cam_meas.timestamp))
                seq_camera += 1

        if lidar_packets:
            dropped_partial_sweeps += 1
            print(f"[WARN] dropping incomplete LiDAR sweep with {len(lidar_packets)} partial packets")

        end_tf = ego.get_transform()
        append_scenario_summary(
            meta_path,
            {
                "completed": orbit_completed,
                "ego_blueprint": ego_bp.id,
                "target_blueprint": target_bp.id,
                "target_pose_carla": target_tf,
                "ego_start_pose_carla": start_tf,
                "ego_end_pose_carla": end_tf,
                "orbit_half_length_m": float(args.orbit_half_length),
                "orbit_half_width_m": float(args.orbit_half_width),
                "corner_radius_m": float(args.corner_radius),
                "orbit_laps": float(args.orbit_laps),
                "target_speed_kmh": float(args.target_speed_kmh),
                "path_length_m": float(orbit_cumulative[-1]),
                "target_record_distance_m": target_record_distance,
                "recorded_distance_m": float(motion_state.get("distance_m", 0.0)),
                "target_clearance_m": float(target_clearance),
                "start_frame": start_frame,
                "start_timestamp": start_timestamp,
                "end_frame": last_world_frame,
                "end_timestamp": last_sim_timestamp,
            },
        )

        invalid_reasons = []
        if missing_lidar > 0:
            invalid_reasons.append("missing_lidar_partials")
        if missing_imu > 0:
            invalid_reasons.append("missing_imu")
        if dropped_partial_sweeps > 0:
            invalid_reasons.append("dropped_partial_sweeps")
        if not orbit_completed:
            invalid_reasons.append("orbit_not_completed")
        rec.append_dataset_status(
            meta_path,
            {
                "valid": not invalid_reasons,
                "invalid_reason": ",".join(invalid_reasons),
                "missing_lidar_partials": missing_lidar,
                "missing_imu": missing_imu,
                "dropped_partial_sweeps": dropped_partial_sweeps,
            },
        )

    finally:
        for sensor in actors[2:]:
            try:
                sensor.stop()
            except Exception:
                pass

        close_errors = []
        for writer in (camera_writer, lidar_writer, imu_writer):
            if writer is not None:
                try:
                    writer.close()
                except Exception as exc:
                    close_errors.append(str(exc))

        for f in (f_cam_ts, f_lidar_ts, f_imu_ts):
            try:
                if f is not None:
                    f.close()
            except Exception:
                pass

        for actor in reversed(actors):
            try:
                actor.destroy()
            except Exception:
                pass

        try:
            if world is not None and original_settings is not None:
                world.apply_settings(original_settings)
        except Exception:
            pass

        if close_errors:
            raise RuntimeError("\n".join(close_errors))

    print("[DONE] recording finalized")
    if args.no_camera:
        print("[CHECK] camera: disabled")
    else:
        print("[CHECK] " + rec.summarize_intervals("camera", camera_stamps, 1.0 / args.camera_hz))
    print("[CHECK] " + rec.summarize_intervals("lidar", lidar_stamps, 1.0 / args.lidar_hz))
    print("[CHECK] " + rec.summarize_intervals("imu", imu_stamps, 1.0 / args.imu_hz))
    print(f"[CHECK] missing_lidar_partials={missing_lidar}, missing_imu={missing_imu}, dropped_partial_sweeps={dropped_partial_sweeps}")
    print(f"[DONE] output: {out_dir}")


if __name__ == "__main__":
    main()
