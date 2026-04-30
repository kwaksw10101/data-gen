#!/usr/bin/env python3
"""
Synchronous CARLA recorder for two nearby free-moving vehicles.

This script is a data generator for downstream LiDAR target tracking work. It
spawns:
  A: ego vehicle with camera, LiDAR, and IMU
  B: target vehicle with ground-truth pose logging

Both vehicles are replayed kinematically in a configurable open area. The script
does not implement target detection or tracking.
"""

from __future__ import annotations

import argparse
import array
import json
import math
import os
import random
import time
from datetime import datetime
from typing import Any, Dict, List, Sequence, Tuple

import carla_rotation_around_object_recorder as orbit_rec
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
    ap.add_argument("--duration", type=float, default=20.0)

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

    ap.add_argument("--area-center-x", type=float, default=None)
    ap.add_argument("--area-center-y", type=float, default=None)
    ap.add_argument("--area-center-z", type=float, default=None)
    ap.add_argument("--area-yaw", type=float, default=None, help="Defaults to the selected spawn point yaw, or 0 with explicit coordinates")
    ap.add_argument("--area-spawn-index", type=int, default=0)
    ap.add_argument("--spawn-z-offset", type=float, default=0.0)
    ap.add_argument("--area-length", type=float, default=60.0)
    ap.add_argument("--area-width", type=float, default=40.0)
    ap.add_argument("--min-separation", type=float, default=6.0)
    ap.add_argument("--max-separation", type=float, default=40.0)
    ap.add_argument("--shared-area", action="store_true", help="Use one shared area instead of the default separated A/B zones")
    ap.add_argument("--zone-gap", type=float, default=0.0, help="Gap between separated zones; default=max(8, min_separation)")
    ap.add_argument("--collision-margin", type=float, default=1.0, help="Added to actor bounding radii for separation checks")
    ap.add_argument("--separation-check-samples", type=int, default=720)

    ap.add_argument("--ego-speed-kmh", type=float, default=15.0)
    ap.add_argument("--target-speed-kmh", type=float, default=10.0)
    ap.add_argument("--ego-trajectory", choices=["rounded-rectangle"], default="rounded-rectangle")
    ap.add_argument("--target-trajectory", choices=["figure-eight", "rounded-rectangle"], default="figure-eight")
    ap.add_argument("--ego-path-half-length", type=float, default=None)
    ap.add_argument("--ego-path-half-width", type=float, default=None)
    ap.add_argument("--target-path-half-length", type=float, default=None)
    ap.add_argument("--target-path-half-width", type=float, default=None)
    ap.add_argument("--corner-radius", type=float, default=4.0)
    ap.add_argument("--target-corner-radius", type=float, default=2.5)
    ap.add_argument("--path-step-m", type=float, default=0.5)
    ap.add_argument("--lookahead-m", type=float, default=2.0)
    ap.add_argument("--target-phase-m", type=float, default=0.0)
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


def resolve_area_transform(carla: Any, world: Any, args: argparse.Namespace) -> Any:
    world_map = world.get_map()
    if args.area_center_x is not None or args.area_center_y is not None:
        if args.area_center_x is None or args.area_center_y is None:
            raise RuntimeError("--area-center-x and --area-center-y must be provided together")
        if args.area_center_z is None:
            probe = carla.Location(x=float(args.area_center_x), y=float(args.area_center_y), z=0.0)
            wp = world_map.get_waypoint(probe, project_to_road=True)
            z = float(wp.transform.location.z) if wp is not None else 0.0
        else:
            z = float(args.area_center_z)
        return carla.Transform(
            carla.Location(
                x=float(args.area_center_x),
                y=float(args.area_center_y),
                z=z + float(args.spawn_z_offset),
            ),
            carla.Rotation(yaw=float(0.0 if args.area_yaw is None else args.area_yaw)),
        )

    spawn_points = orbit_rec.sorted_spawn_points(world_map)
    if not spawn_points:
        raise RuntimeError("current map has no spawn points; pass --area-center-x/--area-center-y/--area-center-z")
    tf = spawn_points[int(args.area_spawn_index) % len(spawn_points)]
    return carla.Transform(
        carla.Location(
            x=float(tf.location.x),
            y=float(tf.location.y),
            z=float(tf.location.z) + float(args.spawn_z_offset),
        ),
        carla.Rotation(yaw=float(tf.rotation.yaw if args.area_yaw is None else args.area_yaw)),
    )


def make_orbit_args(
    half_length: float,
    half_width: float,
    corner_radius: float,
    path_step_m: float,
    direction: str = "ccw",
) -> argparse.Namespace:
    return argparse.Namespace(
        orbit_half_length=float(half_length),
        orbit_half_width=float(half_width),
        corner_radius=float(corner_radius),
        path_step_m=float(path_step_m),
        orbit_direction=direction,
    )


def build_ego_path(carla: Any, center_tf: Any, args: argparse.Namespace) -> List[Any]:
    half_l = float(args.ego_path_half_length) if args.ego_path_half_length is not None else max(5.0, float(args.area_length) * 0.30)
    half_w = float(args.ego_path_half_width) if args.ego_path_half_width is not None else max(3.0, float(args.area_width) * 0.30)
    if half_l > float(args.area_length) * 0.5 or half_w > float(args.area_width) * 0.5:
        raise RuntimeError("ego path extents must fit inside --area-length/--area-width")
    orbit_args = make_orbit_args(half_l, half_w, float(args.corner_radius), float(args.path_step_m), "ccw")
    return orbit_rec.build_rounded_rectangle_path(carla, center_tf.location, float(center_tf.rotation.yaw), orbit_args)


def build_figure_eight_path(carla: Any, center_tf: Any, args: argparse.Namespace) -> List[Any]:
    amp_l = (
        float(args.target_path_half_length)
        if args.target_path_half_length is not None
        else max(3.0, float(args.area_length) * 0.12)
    )
    amp_w = (
        float(args.target_path_half_width)
        if args.target_path_half_width is not None
        else max(2.0, float(args.area_width) * 0.12)
    )
    if amp_l > float(args.area_length) * 0.5 or amp_w > float(args.area_width) * 0.5:
        raise RuntimeError("target path extents must fit inside --area-length/--area-width")

    step = max(0.05, float(args.path_step_m))
    approx_len = max(1.0, 2.0 * math.pi * math.sqrt((amp_l * amp_l + amp_w * amp_w) * 0.5))
    samples = max(48, int(math.ceil(approx_len / step)))
    nodes = []
    for i in range(samples + 1):
        theta = 2.0 * math.pi * (i / samples)
        x_local = amp_l * math.sin(theta)
        y_local = amp_w * math.sin(theta) * math.cos(theta)
        x_rot, y_rot = orbit_rec.rotate_xy(x_local, y_local, float(center_tf.rotation.yaw))
        nodes.append(
            carla.Location(
                x=float(center_tf.location.x) + x_rot,
                y=float(center_tf.location.y) + y_rot,
                z=float(center_tf.location.z),
            )
        )
    return nodes


def build_target_path(carla: Any, center_tf: Any, args: argparse.Namespace) -> List[Any]:
    if args.target_trajectory == "rounded-rectangle":
        half_l = (
            float(args.target_path_half_length)
            if args.target_path_half_length is not None
            else max(4.0, float(args.area_length) * 0.18)
        )
        half_w = (
            float(args.target_path_half_width)
            if args.target_path_half_width is not None
            else max(3.0, float(args.area_width) * 0.18)
        )
        orbit_args = make_orbit_args(half_l, half_w, float(args.target_corner_radius), float(args.path_step_m), "cw")
        return orbit_rec.build_rounded_rectangle_path(carla, center_tf.location, float(center_tf.rotation.yaw), orbit_args)
    return build_figure_eight_path(carla, center_tf, args)


def offset_transform(carla: Any, tf: Any, local_x: float, local_y: float) -> Any:
    dx, dy = orbit_rec.rotate_xy(local_x, local_y, float(tf.rotation.yaw))
    return carla.Transform(
        carla.Location(
            x=float(tf.location.x) + dx,
            y=float(tf.location.y) + dy,
            z=float(tf.location.z),
        ),
        carla.Rotation(yaw=float(tf.rotation.yaw)),
    )


def zone_transforms_and_args(carla: Any, area_tf: Any, args: argparse.Namespace) -> Tuple[Any, argparse.Namespace, Any, argparse.Namespace]:
    if args.shared_area:
        return area_tf, args, area_tf, args

    gap = float(args.zone_gap) if float(args.zone_gap) > 0.0 else max(8.0, float(args.min_separation))
    zone_width = (float(args.area_width) - gap) * 0.5
    if zone_width <= 4.0:
        raise RuntimeError(
            f"area width is too small for separated zones: area_width={args.area_width:.2f} gap={gap:.2f}"
        )
    zone_length = float(args.area_length)
    offset_y = gap * 0.5 + zone_width * 0.5
    ego_tf = offset_transform(carla, area_tf, 0.0, -offset_y)
    target_tf = offset_transform(carla, area_tf, 0.0, offset_y)

    ego_args = argparse.Namespace(**vars(args))
    target_args = argparse.Namespace(**vars(args))
    ego_args.area_length = zone_length
    ego_args.area_width = zone_width
    target_args.area_length = zone_length
    target_args.area_width = zone_width
    return ego_tf, ego_args, target_tf, target_args


def cumulative_lengths(nodes: Sequence[Any]) -> List[float]:
    cumulative = [0.0]
    for a, b in zip(nodes[:-1], nodes[1:]):
        cumulative.append(cumulative[-1] + float(b.distance(a)))
    return cumulative


def sample_closed_path(nodes: Sequence[Any], cumulative: Sequence[float], distance_m: float) -> Tuple[float, float, float]:
    total = float(cumulative[-1])
    if total <= 1e-9:
        loc = nodes[0]
        return float(loc.x), float(loc.y), float(loc.z)
    d = distance_m % total
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


def transform_closed_at_distance(
    carla: Any,
    nodes: Sequence[Any],
    cumulative: Sequence[float],
    distance_m: float,
    lookahead_m: float,
) -> Any:
    x, y, z = sample_closed_path(nodes, cumulative, distance_m)
    back_x, back_y, _ = sample_closed_path(nodes, cumulative, distance_m - lookahead_m)
    ahead_x, ahead_y, _ = sample_closed_path(nodes, cumulative, distance_m + lookahead_m)
    yaw = math.degrees(math.atan2(ahead_y - back_y, ahead_x - back_x))
    return carla.Transform(carla.Location(x=x, y=y, z=z), carla.Rotation(yaw=yaw))


def apply_kinematic_actor(
    carla: Any,
    actor: Any,
    nodes: Sequence[Any],
    cumulative: Sequence[float],
    state: Dict[str, Any],
    speed_kmh: float,
    lookahead_m: float,
    world_dt: float,
) -> Any:
    speed_mps = max(0.1, float(speed_kmh) / 3.6)
    distance_m = float(state.get("distance_m", 0.0)) + speed_mps * world_dt
    tf = transform_closed_at_distance(carla, nodes, cumulative, distance_m, max(0.1, lookahead_m))
    actor.set_transform(tf)
    try:
        actor.set_target_velocity(carla.Vector3D(0.0, 0.0, 0.0))
        actor.set_target_angular_velocity(carla.Vector3D(0.0, 0.0, 0.0))
    except Exception:
        pass
    state["distance_m"] = distance_m
    state["last_transform"] = tf
    return tf


def update_ego_synthetic_imu(state: Dict[str, Any], ego_tf: Any, ego_speed_kmh: float, world_dt: float) -> None:
    speed_mps = max(0.1, float(ego_speed_kmh) / 3.6)
    yaw = float(ego_tf.rotation.yaw)
    prev_yaw = float(state.get("prev_yaw", yaw))
    if bool(state.get("imu_initialized", False)):
        yaw_rate = math.radians(rec.wrap_deg(yaw - prev_yaw)) / world_dt
    else:
        yaw_rate = 0.0
        state["imu_initialized"] = True
    yaw_rate = rec.clamp(yaw_rate, -2.5, 2.5)
    state["synthetic_imu_base"] = (
        0.0,
        rec.clamp(speed_mps * yaw_rate, -20.0, 20.0),
        9.81,
        0.0,
        0.0,
        -yaw_rate,
        float((math.radians(90.0 - yaw)) % (2.0 * math.pi)),
    )
    state["prev_yaw"] = yaw


def synthetic_imu_values(meas: Any, state: Dict[str, Any]) -> array.array:
    base = state.get("synthetic_imu_base", (0.0, 0.0, 9.81, 0.0, 0.0, 0.0, float(meas.compass)))
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


def pose_in_area_bounds(tf: Any, area_tf: Any, args: argparse.Namespace) -> bool:
    dx = float(tf.location.x) - float(area_tf.location.x)
    dy = float(tf.location.y) - float(area_tf.location.y)
    local_x, local_y = orbit_rec.rotate_xy(dx, dy, -float(area_tf.rotation.yaw))
    return abs(local_x) <= float(args.area_length) * 0.5 + 1e-6 and abs(local_y) <= float(args.area_width) * 0.5 + 1e-6


def actor_planar_radius(actor: Any) -> float:
    extent = actor.bounding_box.extent
    return math.hypot(float(extent.x), float(extent.y))


def min_sampled_path_separation(
    ego_nodes: Sequence[Any],
    ego_cumulative: Sequence[float],
    target_nodes: Sequence[Any],
    target_cumulative: Sequence[float],
    ego_speed_kmh: float,
    target_speed_kmh: float,
    target_phase_m: float,
    duration_s: float,
    samples: int,
) -> float:
    n = max(2, int(samples))
    min_sep = float("inf")
    ego_speed_mps = max(0.1, float(ego_speed_kmh) / 3.6)
    target_speed_mps = max(0.1, float(target_speed_kmh) / 3.6)
    for i in range(n + 1):
        t = float(duration_s) * (i / n)
        ex, ey, ez = sample_closed_path(ego_nodes, ego_cumulative, ego_speed_mps * t)
        tx, ty, tz = sample_closed_path(target_nodes, target_cumulative, float(target_phase_m) + target_speed_mps * t)
        sep = math.sqrt((tx - ex) ** 2 + (ty - ey) ** 2 + (tz - ez) ** 2)
        min_sep = min(min_sep, sep)
    return min_sep


def relative_target_pose(ego_tf: Any, target_tf: Any) -> Dict[str, float]:
    dx = float(target_tf.location.x) - float(ego_tf.location.x)
    dy = float(target_tf.location.y) - float(ego_tf.location.y)
    dz = float(target_tf.location.z) - float(ego_tf.location.z)
    yaw = math.radians(float(ego_tf.rotation.yaw))
    c = math.cos(yaw)
    s = math.sin(yaw)
    x_forward = dx * c + dy * s
    y_right = -dx * s + dy * c
    y_left = -y_right
    distance = math.sqrt(dx * dx + dy * dy + dz * dz)
    bearing_ros_deg = math.degrees(math.atan2(y_left, x_forward))
    yaw_rel = rec.wrap_deg(float(target_tf.rotation.yaw) - float(ego_tf.rotation.yaw))
    return {
        "x_forward": x_forward,
        "y_left": y_left,
        "z_up": dz,
        "distance": distance,
        "bearing_ros_deg": bearing_ros_deg,
        "target_yaw_relative_deg": yaw_rel,
    }


def write_pose_header(f: Any) -> None:
    f.write(
        "seq,timestamp_seconds,carla_frame,"
        "ego_x,ego_y,ego_z,ego_roll,ego_pitch,ego_yaw,"
        "target_x,target_y,target_z,target_roll,target_pitch,target_yaw,"
        "rel_x_forward,rel_y_left,rel_z_up,distance,bearing_ros_deg,target_yaw_relative_deg\n"
    )


def write_pose_row(f: Any, seq: int, timestamp: float, frame: int, ego_tf: Any, target_tf: Any) -> None:
    rel = relative_target_pose(ego_tf, target_tf)
    f.write(
        f"{seq},{timestamp:.9f},{frame},"
        f"{ego_tf.location.x:.9f},{ego_tf.location.y:.9f},{ego_tf.location.z:.9f},"
        f"{ego_tf.rotation.roll:.9f},{ego_tf.rotation.pitch:.9f},{ego_tf.rotation.yaw:.9f},"
        f"{target_tf.location.x:.9f},{target_tf.location.y:.9f},{target_tf.location.z:.9f},"
        f"{target_tf.rotation.roll:.9f},{target_tf.rotation.pitch:.9f},{target_tf.rotation.yaw:.9f},"
        f"{rel['x_forward']:.9f},{rel['y_left']:.9f},{rel['z_up']:.9f},"
        f"{rel['distance']:.9f},{rel['bearing_ros_deg']:.9f},{rel['target_yaw_relative_deg']:.9f}\n"
    )


def save_scenario_paths(path: str, ego_nodes: Sequence[Any], target_nodes: Sequence[Any], args: argparse.Namespace) -> None:
    data = {
        "format": "carla_dual_vehicle_tracking_paths_v1",
        "area_length_m": float(args.area_length),
        "area_width_m": float(args.area_width),
        "ego_trajectory": args.ego_trajectory,
        "target_trajectory": args.target_trajectory,
        "ego_nodes": [rec.location_to_dict(loc) for loc in ego_nodes],
        "target_nodes": [rec.location_to_dict(loc) for loc in target_nodes],
    }
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(data, f, indent=2)
        f.write("\n")


def append_scenario_summary(path: str, summary: Dict[str, Any]) -> None:
    def pose(tf: Any) -> str:
        return (
            f"{{x: {tf.location.x:.9f}, y: {tf.location.y:.9f}, z: {tf.location.z:.9f}, "
            f"roll: {tf.rotation.roll:.9f}, pitch: {tf.rotation.pitch:.9f}, yaw: {tf.rotation.yaw:.9f}}}"
        )

    text = "\nscenario:\n"
    text += "  mode: \"dual-vehicle-free-motion-tracking\"\n"
    text += f"  completed: {str(bool(summary['completed'])).lower()}\n"
    text += f"  ego_blueprint: \"{summary['ego_blueprint']}\"\n"
    text += f"  target_blueprint: \"{summary['target_blueprint']}\"\n"
    text += f"  area_center_carla: {pose(summary['area_center_carla'])}\n"
    text += f"  area_length_m: {summary['area_length_m']:.9f}\n"
    text += f"  area_width_m: {summary['area_width_m']:.9f}\n"
    text += f"  min_separation_m: {summary['min_separation_m']:.9f}\n"
    text += f"  max_separation_m: {summary['max_separation_m']:.9f}\n"
    text += f"  min_observed_separation_m: {summary['min_observed_separation_m']:.9f}\n"
    text += f"  max_observed_separation_m: {summary['max_observed_separation_m']:.9f}\n"
    text += f"  required_center_separation_m: {summary['required_center_separation_m']:.9f}\n"
    text += f"  planned_min_center_separation_m: {summary['planned_min_center_separation_m']:.9f}\n"
    text += f"  separated_zones: {str(bool(summary['separated_zones'])).lower()}\n"
    text += f"  min_separation_violations: {summary['min_separation_violations']}\n"
    text += f"  max_separation_violations: {summary['max_separation_violations']}\n"
    text += f"  area_bound_violations: {summary['area_bound_violations']}\n"
    text += f"  ego_trajectory: \"{summary['ego_trajectory']}\"\n"
    text += f"  target_trajectory: \"{summary['target_trajectory']}\"\n"
    text += f"  ego_speed_kmh: {summary['ego_speed_kmh']:.9f}\n"
    text += f"  target_speed_kmh: {summary['target_speed_kmh']:.9f}\n"
    text += f"  duration_s: {summary['duration_s']:.9f}\n"
    text += f"  start_frame: {summary['start_frame']}\n"
    text += f"  start_timestamp: {summary['start_timestamp']:.9f}\n"
    text += f"  end_frame: {summary['end_frame']}\n"
    text += f"  end_timestamp: {summary['end_timestamp']:.9f}\n"
    text += "  target_pose_file: \"target_pose/data.csv\"\n"
    text += "  scenario_paths_file: \"scenario_paths.json\"\n"
    with open(path, "a", encoding="utf-8", newline="\n") as f:
        f.write(text)


def main() -> None:
    args = parse_args_with_config()
    if args.duration <= 0.0:
        raise ValueError("--duration must be positive for this recorder")
    if args.min_separation < 0.0 or args.max_separation <= args.min_separation:
        raise ValueError("--max-separation must be greater than --min-separation")

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

    run_name = args.run_name or datetime.now().strftime("dual_vehicle_tracking_%Y%m%d_%H%M%S")
    out_dir = os.path.abspath(os.path.join(args.out_root, run_name))
    lidar_dir = os.path.join(out_dir, "lidar")
    imu_dir = os.path.join(out_dir, "imu")
    target_pose_dir = os.path.join(out_dir, "target_pose")
    dirs = [os.path.join(lidar_dir, "data"), os.path.join(imu_dir, "data"), target_pose_dir]
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
    f_pose = None

    camera_stamps: List[float] = []
    lidar_stamps: List[float] = []
    imu_stamps: List[float] = []
    missing_lidar = 0
    missing_imu = 0
    dropped_partial_sweeps = 0
    min_sep_violations = 0
    max_sep_violations = 0
    area_bound_violations = 0
    min_observed_sep = float("inf")
    max_observed_sep = 0.0
    planned_min_sep = float("inf")
    required_center_sep = float(args.min_separation)
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
        ego_bp = orbit_rec.pick_exact_vehicle_blueprint(bp_lib, args.ego_blueprint, args.ego_filter, "ego", rng)
        target_bp = orbit_rec.pick_exact_vehicle_blueprint(bp_lib, args.target_blueprint, args.target_filter, "target", rng)

        area_tf = resolve_area_transform(carla, world, args)
        ego_zone_tf, ego_path_args, target_zone_tf, target_path_args = zone_transforms_and_args(carla, area_tf, args)
        ego_nodes = build_ego_path(carla, ego_zone_tf, ego_path_args)
        target_nodes = build_target_path(carla, target_zone_tf, target_path_args)
        ego_cumulative = cumulative_lengths(ego_nodes)
        target_cumulative = cumulative_lengths(target_nodes)
        if ego_cumulative[-1] <= 1e-6 or target_cumulative[-1] <= 1e-6:
            raise RuntimeError("generated trajectory path is empty")
        save_scenario_paths(os.path.join(out_dir, "scenario_paths.json"), ego_nodes, target_nodes, args)

        ego_start_tf = transform_closed_at_distance(carla, ego_nodes, ego_cumulative, 0.0, float(args.lookahead_m))
        target_start_tf = transform_closed_at_distance(
            carla, target_nodes, target_cumulative, float(args.target_phase_m), float(args.lookahead_m)
        )

        target = world.try_spawn_actor(target_bp, target_start_tf)
        if target is None:
            raise RuntimeError("failed to spawn target vehicle B at its trajectory start pose")
        actors.append(target)
        try:
            target.set_simulate_physics(False)
        except Exception:
            pass

        ego = world.try_spawn_actor(ego_bp, ego_start_tf)
        if ego is None:
            raise RuntimeError("failed to spawn ego vehicle A at its trajectory start pose")
        actors.append(ego)
        try:
            ego.set_simulate_physics(True)
        except Exception:
            pass

        required_center_sep = max(
            float(args.min_separation),
            actor_planar_radius(ego) + actor_planar_radius(target) + float(args.collision_margin),
        )
        planned_min_sep = min_sampled_path_separation(
            ego_nodes,
            ego_cumulative,
            target_nodes,
            target_cumulative,
            args.ego_speed_kmh,
            args.target_speed_kmh,
            args.target_phase_m,
            args.duration,
            args.separation_check_samples,
        )
        if planned_min_sep < required_center_sep:
            raise RuntimeError(
                f"planned A/B trajectories are too close: sampled_min={planned_min_sep:.2f}m "
                f"required_center_separation={required_center_sep:.2f}m. "
                "Increase --area-width/--zone-gap or lower path extents."
            )

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
        orbit_rec.write_meta(
            meta_path,
            args,
            client.get_server_version(),
            world.get_map().name,
            ego_bp.id,
            ego_start_tf,
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
        f_pose = open(os.path.join(target_pose_dir, "data.csv"), "w", encoding="ascii", buffering=1)
        write_pose_header(f_pose)

        print(f"[RUN] output: {out_dir}")
        print(
            f"[SCENARIO] ego={ego_bp.id} target={target_bp.id} "
            f"area={args.area_length:.1f}x{args.area_width:.1f}m duration={args.duration:.1f}s "
            f"zones={'shared' if args.shared_area else 'separated'} planned_min_sep={planned_min_sep:.1f}m"
        )
        print(
            f"[RUN] world={1/world_dt:.1f}Hz, camera={'off' if args.no_camera else f'{args.camera_hz:.1f}Hz'}, "
            f"lidar={args.lidar_hz:.1f}Hz ({lidar_sweep_ticks} partials/sweep), imu={args.imu_hz:.1f}Hz"
        )

        warmup_frame, sync_stats = orbit_rec.run_sensor_warmup_and_validation(
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
            target.set_simulate_physics(False)
        except Exception:
            pass
        ego.set_transform(ego_start_tf)
        target.set_transform(target_start_tf)
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
        seq_pose = 0
        tick_idx = 0
        raw_target_ticks = max(1, int(math.ceil(float(args.duration) / world_dt)))
        target_ticks = int(math.ceil(raw_target_ticks / lidar_sweep_ticks) * lidar_sweep_ticks)
        lidar_packets: List[Tuple[bytes, float, int]] = []
        last_progress_t = time.time()
        record_start_wall = time.time()
        ego_state: Dict[str, Any] = {"distance_m": 0.0}
        target_state: Dict[str, Any] = {"distance_m": float(args.target_phase_m)}

        while tick_idx < target_ticks:
            ego_tf = apply_kinematic_actor(
                carla, ego, ego_nodes, ego_cumulative, ego_state, args.ego_speed_kmh, args.lookahead_m, world_dt
            )
            target_tf = apply_kinematic_actor(
                carla,
                target,
                target_nodes,
                target_cumulative,
                target_state,
                args.target_speed_kmh,
                args.lookahead_m,
                world_dt,
            )
            update_ego_synthetic_imu(ego_state, ego_tf, args.ego_speed_kmh, world_dt)

            world_frame = int(world.tick())
            tick_idx += 1
            snapshot = world.get_snapshot()
            last_world_frame = world_frame
            last_sim_timestamp = float(snapshot.timestamp.elapsed_seconds)

            rel = relative_target_pose(ego_tf, target_tf)
            sep = float(rel["distance"])
            min_observed_sep = min(min_observed_sep, sep)
            max_observed_sep = max(max_observed_sep, sep)
            if sep < required_center_sep:
                min_sep_violations += 1
            if sep > float(args.max_separation):
                max_sep_violations += 1
            if not pose_in_area_bounds(ego_tf, area_tf, args) or not pose_in_area_bounds(target_tf, area_tf, args):
                area_bound_violations += 1
            write_pose_row(f_pose, seq_pose, last_sim_timestamp, world_frame, ego_tf, target_tf)
            seq_pose += 1

            imu_meas = imu_buf.wait(world_frame, args.wait_timeout)
            if imu_meas is None:
                missing_imu += 1
                print(f"[WARN] missing IMU frame={world_frame}")
            else:
                imu_writer.push((seq_imu, synthetic_imu_values(imu_meas, ego_state)))
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

            now = time.time()
            if now - last_progress_t >= 2.0:
                sim_done = tick_idx * world_dt
                wall_elapsed = max(1e-6, now - record_start_wall)
                print(
                    f"[REC] sim={sim_done:.2f}s wall={wall_elapsed:.2f}s "
                    f"camera={'off' if args.no_camera else seq_camera} lidar={seq_lidar} imu={seq_imu} pose={seq_pose} "
                    f"sep={sep:.1f}m"
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

        completed = tick_idx >= target_ticks
        append_scenario_summary(
            meta_path,
            {
                "completed": completed,
                "ego_blueprint": ego_bp.id,
                "target_blueprint": target_bp.id,
                "area_center_carla": area_tf,
                "area_length_m": float(args.area_length),
                "area_width_m": float(args.area_width),
                "min_separation_m": float(args.min_separation),
                "max_separation_m": float(args.max_separation),
                "min_observed_separation_m": float(min_observed_sep),
                "max_observed_separation_m": float(max_observed_sep),
                "required_center_separation_m": float(required_center_sep),
                "planned_min_center_separation_m": float(planned_min_sep),
                "separated_zones": not bool(args.shared_area),
                "min_separation_violations": min_sep_violations,
                "max_separation_violations": max_sep_violations,
                "area_bound_violations": area_bound_violations,
                "ego_trajectory": args.ego_trajectory,
                "target_trajectory": args.target_trajectory,
                "ego_speed_kmh": float(args.ego_speed_kmh),
                "target_speed_kmh": float(args.target_speed_kmh),
                "duration_s": tick_idx * world_dt,
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
        if min_sep_violations > 0:
            invalid_reasons.append("min_separation_violation")
        if max_sep_violations > 0:
            invalid_reasons.append("max_separation_violation")
        if area_bound_violations > 0:
            invalid_reasons.append("area_bound_violation")
        if not completed:
            invalid_reasons.append("duration_not_completed")
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

        for f in (f_cam_ts, f_lidar_ts, f_imu_ts, f_pose):
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
    print(
        f"[CHECK] missing_lidar_partials={missing_lidar}, missing_imu={missing_imu}, "
        f"dropped_partial_sweeps={dropped_partial_sweeps}"
    )
    print(
        f"[CHECK] separation min/max={min_observed_sep:.3f}/{max_observed_sep:.3f}m "
        f"violations min={min_sep_violations} max={max_sep_violations} bounds={area_bound_violations}"
    )
    print(f"[DONE] output: {out_dir}")


if __name__ == "__main__":
    main()
