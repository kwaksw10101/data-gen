#!/usr/bin/env python3
"""
Synchronous CARLA recorder for one camera, one rotating LiDAR, and one IMU.

Default timing:
  world:  100 Hz, fixed_delta_seconds=0.01
  camera: 20 Hz, sensor_tick=0.05
  LiDAR:  10 Hz full sweeps, built from 10 partial packets at 100 Hz
  IMU:    100 Hz, sensor_tick=0.01

Output coordinates use a ROS-style body frame:
  x forward, y left, z up.
CARLA/Unreal uses y right, so LiDAR points and IMU linear acceleration are
converted by flipping y. IMU angular velocity is treated as an axial vector
across the handedness change: [-gx, gy, -gz].
"""

from __future__ import annotations

import argparse
import array
import binascii
import glob
import json
import math
import os
import queue
import random
import shlex
import struct
import sys
import threading
import time
import traceback
import zlib
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple


def add_carla_paths(carla_root: Optional[str]) -> List[str]:
    """Add CARLA PythonAPI paths before importing carla."""
    candidates: List[str] = []
    if carla_root:
        candidates.append(os.path.abspath(carla_root))

    for env_name in ("CARLA_ROOT", "CARLA_HOME"):
        val = os.environ.get(env_name)
        if val:
            candidates.append(os.path.abspath(val))

    home = os.path.expanduser("~")
    candidates.extend(
        [
            os.path.join(home, "Downloads", "CARLA_0.9.16"),
            os.path.join(home, "CARLA_0.9.16"),
            r"C:\CARLA_0.9.16",
        ]
    )

    added: List[str] = []
    seen = set()
    for root in candidates:
        if not root or root in seen:
            continue
        seen.add(root)

        if os.path.isfile(root) and root.lower().endswith((".egg", ".whl")):
            paths = [root]
        else:
            dist = os.path.join(root, "PythonAPI", "carla", "dist")
            paths = sorted(glob.glob(os.path.join(dist, "carla-*.egg")))
            paths += sorted(glob.glob(os.path.join(dist, "carla-*.whl")))
            paths.append(os.path.join(root, "PythonAPI", "carla"))
            paths.append(os.path.join(root, "PythonAPI", "examples"))

        for path in paths:
            if os.path.exists(path) and path not in sys.path:
                sys.path.insert(0, path)
                added.append(path)
    return added


def import_carla_or_exit(carla_root: Optional[str]):
    if not carla_root:
        try:
            import carla  # type: ignore

            return carla
        except Exception:
            sys.modules.pop("carla", None)

    added = add_carla_paths(carla_root)
    try:
        sys.modules.pop("carla", None)
        import carla  # type: ignore

        return carla
    except Exception as exc:
        print("[ERR] failed to import CARLA PythonAPI.", file=sys.stderr)
        print(f"[ERR] current Python: {sys.version.split()[0]} ({sys.executable})", file=sys.stderr)
        if added:
            print("[ERR] paths tried:", file=sys.stderr)
            for path in added:
                print(f"      {path}", file=sys.stderr)
        else:
            print("[ERR] no CARLA PythonAPI path was found/tried.", file=sys.stderr)
        print(
            "[HINT] CARLA 0.9.16 Windows package commonly ships a cp312 wheel; "
            "run this script with Python 3.12 or pass --carla-root to a matching PythonAPI.",
            file=sys.stderr,
        )
        raise exc


def set_attr_if_exists(bp: Any, name: str, value: Any) -> bool:
    if bp.has_attribute(name):
        bp.set_attribute(name, str(value))
        return True
    return False


def configure_camera_blueprint(bp: Any, width: int, height: int, fov: float, sensor_tick: float) -> None:
    set_attr_if_exists(bp, "image_size_x", width)
    set_attr_if_exists(bp, "image_size_y", height)
    set_attr_if_exists(bp, "fov", fov)
    set_attr_if_exists(bp, "sensor_tick", f"{sensor_tick:.9f}")


def configure_lidar_blueprint(
    bp: Any,
    range_m: float,
    points_per_second: int,
    channels: int,
    upper_fov: float,
    lower_fov: float,
    horizontal_fov: float,
    rotation_frequency: float,
    sensor_tick: float,
) -> None:
    set_attr_if_exists(bp, "range", range_m)
    set_attr_if_exists(bp, "points_per_second", points_per_second)
    set_attr_if_exists(bp, "channels", channels)
    set_attr_if_exists(bp, "upper_fov", upper_fov)
    set_attr_if_exists(bp, "lower_fov", lower_fov)
    set_attr_if_exists(bp, "horizontal_fov", horizontal_fov)
    set_attr_if_exists(bp, "rotation_frequency", rotation_frequency)
    set_attr_if_exists(bp, "sensor_tick", f"{sensor_tick:.9f}")

    # Prefer deterministic data for calibration datasets.
    for name, value in (
        ("dropoff_general_rate", 0.0),
        ("dropoff_zero_intensity", 0.0),
        ("dropoff_intensity_limit", 1.0),
        ("noise_stddev", 0.0),
        ("atmosphere_attenuation_rate", 0.0),
    ):
        set_attr_if_exists(bp, name, value)


def configure_imu_blueprint(bp: Any, sensor_tick: float) -> None:
    set_attr_if_exists(bp, "sensor_tick", f"{sensor_tick:.9f}")
    for name in (
        "noise_accel_stddev_x",
        "noise_accel_stddev_y",
        "noise_accel_stddev_z",
        "noise_gyro_stddev_x",
        "noise_gyro_stddev_y",
        "noise_gyro_stddev_z",
        "noise_gyro_bias_x",
        "noise_gyro_bias_y",
        "noise_gyro_bias_z",
    ):
        set_attr_if_exists(bp, name, 0.0)


def ensure_integer_period_ticks(name: str, hz: float, world_dt: float) -> int:
    if hz <= 0.0:
        raise ValueError(f"{name} Hz must be positive")
    ticks_f = (1.0 / hz) / world_dt
    ticks = int(round(ticks_f))
    if ticks < 1:
        raise ValueError(f"{name} period is smaller than world fixed_dt")
    if abs(ticks - ticks_f) > 1e-6:
        raise ValueError(
            f"{name} period must be an integer number of world ticks for stable sync: "
            f"hz={hz}, fixed_dt={world_dt}, ticks={ticks_f}"
        )
    return ticks


def mkdir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def png_chunk(kind: bytes, data: bytes) -> bytes:
    crc = binascii.crc32(kind)
    crc = binascii.crc32(data, crc) & 0xFFFFFFFF
    return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", crc)


def save_bgra_png(path: str, raw_bgra: bytes, width: int, height: int, color_mode: str) -> None:
    """Write CARLA BGRA bytes as PNG using only the Python standard library."""
    if color_mode == "gray":
        color_type = 0
        row_bytes = width
        png_rows = bytearray((row_bytes + 1) * height)
        dst = 0
        src = 0
        for _ in range(height):
            png_rows[dst] = 0
            dst += 1
            row_end = src + width * 4
            while src < row_end:
                png_rows[dst] = raw_bgra[src + 1]
                dst += 1
                src += 4
        compressed = zlib.compress(bytes(png_rows), level=1)
    else:
        color_type = 2
        row_bytes = width * 3
        png_rows = bytearray((row_bytes + 1) * height)
        dst = 0
        src = 0
        for _ in range(height):
            png_rows[dst] = 0
            dst += 1
            row_end = src + width * 4
            while src < row_end:
                b = raw_bgra[src]
                g = raw_bgra[src + 1]
                r = raw_bgra[src + 2]
                png_rows[dst] = r
                png_rows[dst + 1] = g
                png_rows[dst + 2] = b
                dst += 3
                src += 4
        compressed = zlib.compress(bytes(png_rows), level=1)

    ihdr = struct.pack(">IIBBBBB", width, height, 8, color_type, 0, 0, 0)
    with open(path, "wb") as f:
        f.write(b"\x89PNG\r\n\x1a\n")
        f.write(png_chunk(b"IHDR", ihdr))
        f.write(png_chunk(b"IDAT", compressed))
        f.write(png_chunk(b"IEND", b""))


def array_from_lidar_raw(raw_bytes: bytes) -> array.array:
    vals = array.array("f")
    vals.frombytes(raw_bytes)
    if sys.byteorder != "little":
        vals.byteswap()
    return vals


def build_lidar_xyzir_timestamp(
    packets: Sequence[Tuple[bytes, float, int]],
    channels: int,
    lower_fov: float,
    upper_fov: float,
    point_frame: str = "lidar",
    sensor_xyz_ros: Sequence[float] = (0.0, 0.0, 0.0),
) -> Tuple[array.array, int]:
    """Build float32 [x, y, z, intensity, ring, timestamp] in ROS body axes."""
    out = array.array("f")
    fov_span = max(1e-6, upper_fov - lower_fov)
    max_ring = max(0, channels - 1)
    point_count = 0
    sensor_x = float(sensor_xyz_ros[0]) if len(sensor_xyz_ros) > 0 else 0.0
    sensor_y = float(sensor_xyz_ros[1]) if len(sensor_xyz_ros) > 1 else 0.0
    sensor_z = float(sensor_xyz_ros[2]) if len(sensor_xyz_ros) > 2 else 0.0
    write_base_link = point_frame == "base_link"

    for raw_bytes, packet_ts, _frame in packets:
        vals = array_from_lidar_raw(raw_bytes)
        usable = len(vals) - (len(vals) % 4)
        point_count += usable // 4
        ts_f = float(packet_ts)
        for i in range(0, usable, 4):
            x = vals[i]
            y_carla = vals[i + 1]
            z = vals[i + 2]
            intensity = vals[i + 3]
            xy = math.hypot(x, y_carla)
            elev_deg = math.degrees(math.atan2(z, xy)) if xy > 1e-9 else (upper_fov if z >= 0.0 else lower_fov)
            ring = int(round((elev_deg - lower_fov) / fov_span * max_ring))
            if ring < 0:
                ring = 0
            elif ring > max_ring:
                ring = max_ring

            x_ros = float(x)
            y_ros = float(-y_carla)
            z_ros = float(z)
            if write_base_link:
                x_ros += sensor_x
                y_ros += sensor_y
                z_ros += sensor_z

            out.append(x_ros)
            out.append(y_ros)
            out.append(z_ros)
            out.append(float(intensity))
            out.append(float(ring))
            out.append(ts_f)

    return out, point_count


def imu_values_ros(meas: Any) -> array.array:
    acc = meas.accelerometer
    gyro = meas.gyroscope
    return array.array(
        "d",
        [
            float(acc.x),
            float(-acc.y),
            float(acc.z),
            float(-gyro.x),
            float(gyro.y),
            float(-gyro.z),
            float(meas.compass),
            float(meas.timestamp),
        ],
    )


class SensorBuffer:
    def __init__(self, name: str, max_keep: int = 4096):
        self.name = name
        self.max_keep = int(max_keep)
        self._cv = threading.Condition()
        self._buf: Dict[int, Any] = {}

    def put(self, meas: Any) -> None:
        frame = int(meas.frame)
        with self._cv:
            self._buf[frame] = meas
            if len(self._buf) > self.max_keep * 2:
                oldest = frame - self.max_keep
                for key in [k for k in self._buf.keys() if k < oldest]:
                    self._buf.pop(key, None)
            self._cv.notify_all()

    def clear(self) -> None:
        with self._cv:
            self._buf.clear()

    def wait(self, frame: int, timeout_s: float) -> Optional[Any]:
        deadline = time.time() + timeout_s
        with self._cv:
            while True:
                meas = self._buf.pop(frame, None)
                if meas is not None:
                    return meas
                remain = deadline - time.time()
                if remain <= 0.0:
                    return None
                self._cv.wait(timeout=min(0.01, remain))

    def pop_all_ready(self, max_frame: int, min_frame_exclusive: int) -> List[Any]:
        with self._cv:
            keys = sorted(k for k in self._buf.keys() if min_frame_exclusive < k <= max_frame)
            out = [self._buf.pop(k) for k in keys]
            for key in [k for k in self._buf.keys() if k <= min_frame_exclusive]:
                self._buf.pop(key, None)
            return out


class BaseWriter:
    def __init__(self, out_dir: str, max_queue: int = 128):
        self.out_dir = out_dir
        mkdir(out_dir)
        self.q: "queue.Queue[Any]" = queue.Queue(maxsize=max_queue)
        self.written = 0
        self.errors: List[str] = []
        self._thread = threading.Thread(target=self._run_guarded, daemon=False)
        self._thread.start()

    def push(self, item: Any) -> None:
        if self.errors:
            raise RuntimeError("\n".join(self.errors))
        self.q.put(item)

    def close(self) -> None:
        if self._thread.is_alive():
            while self._thread.is_alive():
                try:
                    self.q.put(None, timeout=0.1)
                    break
                except queue.Full:
                    continue
            self._thread.join()
        if self.errors:
            raise RuntimeError("\n".join(self.errors))

    def _run_guarded(self) -> None:
        try:
            self._run()
        except Exception:
            self.errors.append(traceback.format_exc())

    def _run(self) -> None:
        raise NotImplementedError


class CameraWriter(BaseWriter):
    def __init__(self, out_dir: str, color_mode: str, max_queue: int = 128):
        self.color_mode = color_mode
        super().__init__(out_dir, max_queue=max_queue)

    def _run(self) -> None:
        while True:
            item = self.q.get()
            if item is None:
                return
            seq, raw_bgra, width, height = item
            path = os.path.join(self.out_dir, f"{seq:06d}.png")
            save_bgra_png(path, raw_bgra, width, height, self.color_mode)
            self.written += 1


class ImuWriter(BaseWriter):
    def _run(self) -> None:
        while True:
            item = self.q.get()
            if item is None:
                return
            seq, values = item
            path = os.path.join(self.out_dir, f"{seq:06d}.bin")
            with open(path, "wb") as f:
                values.tofile(f)
            self.written += 1


class LidarWriter(BaseWriter):
    def __init__(
        self,
        out_dir: str,
        channels: int,
        lower_fov: float,
        upper_fov: float,
        point_frame: str = "lidar",
        sensor_xyz_ros: Sequence[float] = (0.0, 0.0, 0.0),
        max_queue: int = 128,
    ):
        self.channels = channels
        self.lower_fov = lower_fov
        self.upper_fov = upper_fov
        if point_frame not in ("lidar", "base_link"):
            raise ValueError(f"unsupported LiDAR point frame: {point_frame}")
        self.point_frame = point_frame
        self.sensor_xyz_ros = tuple(float(v) for v in sensor_xyz_ros[:3])
        if len(self.sensor_xyz_ros) != 3:
            raise ValueError("sensor_xyz_ros must contain exactly three values")
        self.last_point_count = 0
        super().__init__(out_dir, max_queue=max_queue)

    def _run(self) -> None:
        while True:
            item = self.q.get()
            if item is None:
                return
            seq, packets = item
            cloud, point_count = build_lidar_xyzir_timestamp(
                packets,
                self.channels,
                self.lower_fov,
                self.upper_fov,
                self.point_frame,
                self.sensor_xyz_ros,
            )
            path = os.path.join(self.out_dir, f"{seq:06d}.bin")
            with open(path, "wb") as f:
                cloud.tofile(f)
            self.last_point_count = point_count
            self.written += 1


def format_float(x: float) -> str:
    return f"{x:.9f}"


def open_csv_with_header(path: str, columns: Sequence[str]):
    f = open(path, "w", encoding="ascii", buffering=1, newline="\n")
    f.write(",".join(columns) + "\n")
    return f


def write_csv_row(f: Any, values: Sequence[Any]) -> None:
    f.write(",".join(str(v) for v in values) + "\n")


def transform_to_dict(tf: Any) -> Dict[str, float]:
    return {
        "x": float(tf.location.x),
        "y": float(tf.location.y),
        "z": float(tf.location.z),
        "roll": float(tf.rotation.roll),
        "pitch": float(tf.rotation.pitch),
        "yaw": float(tf.rotation.yaw),
    }


def location_to_dict(loc: Any) -> Dict[str, float]:
    return {"x": float(loc.x), "y": float(loc.y), "z": float(loc.z)}


def ros_transform_from_carla_location(x: float, y: float, z: float) -> Dict[str, float]:
    return {"x": float(x), "y": float(-y), "z": float(z), "roll": 0.0, "pitch": 0.0, "yaw": 0.0}


def relative_ros_transform(parent_to_child: Dict[str, float], parent_to_other: Dict[str, float]) -> Dict[str, float]:
    return {
        "x": float(parent_to_other["x"] - parent_to_child["x"]),
        "y": float(parent_to_other["y"] - parent_to_child["y"]),
        "z": float(parent_to_other["z"] - parent_to_child["z"]),
        "roll": float(parent_to_other["roll"] - parent_to_child["roll"]),
        "pitch": float(parent_to_other["pitch"] - parent_to_child["pitch"]),
        "yaw": float(parent_to_other["yaw"] - parent_to_child["yaw"]),
    }


def requested_horizon_scan(points_per_sweep: int, channels: int) -> int:
    return max(1, int(round(float(points_per_sweep) / max(1, int(channels)))))


def yaml_quote(value: Any) -> str:
    return "'" + str(value).replace("'", "''") + "'"


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
    fx = (args.camera_width * 0.5) / math.tan(math.radians(args.camera_fov) * 0.5)
    fy = fx
    cx = args.camera_width * 0.5
    cy = args.camera_height * 0.5
    spawn = transform_to_dict(spawn_tf)
    camera_tf_ros = ros_transform_from_carla_location(args.camera_x, args.camera_y, args.camera_z)
    lidar_tf_ros = ros_transform_from_carla_location(args.lidar_x, args.lidar_y, args.lidar_z)
    imu_tf_ros = ros_transform_from_carla_location(args.imu_x, args.imu_y, args.imu_z)
    lidar_to_imu_tf_ros = relative_ros_transform(lidar_tf_ros, imu_tf_ros)
    imu_data_source = str(getattr(args, "imu_data_source", "carla_raw"))
    imu_noise_model = str(getattr(args, "imu_noise_model", "ideal_zero_noise_bias"))
    cmd = " ".join(shlex.quote(x) for x in sys.argv)

    text = f"""# Generated by carla_sync_camera_lidar_imu_recorder.py
generated_at_local: {yaml_quote(datetime.now().isoformat(timespec="seconds"))}
run_name: {yaml_quote(run_name)}
output_dir: {yaml_quote(out_dir.replace(os.sep, "/"))}
command: {yaml_quote(cmd)}

carla:
  server_version: {yaml_quote(server_version)}
  map: {yaml_quote(map_name)}
  vehicle_blueprint: {yaml_quote(vehicle_id)}
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
  output_frame: {yaml_quote("ROS body frame: x forward, y left, z up")}
  carla_to_output_position_vector: {yaml_quote("[x, -y, z]")}
  carla_to_output_imu_angular_velocity: {yaml_quote("[-x, y, -z]")}
  note: {yaml_quote("LiDAR packets are merged without recorder-side deskew; each point keeps its partial packet timestamp.")}

camera:
  name: {yaml_quote("camera")}
  directory: {yaml_quote("camera")}
  data_directory: {yaml_quote("camera/data")}
  timestamps_file: {yaml_quote("camera/timestamps.csv")}
  data_format: {yaml_quote("png")}
  color_mode: {yaml_quote(args.camera_color)}
  rate_hz: {args.camera_hz:.9f}
  sensor_tick: {camera_tick:.9f}
  width: {args.camera_width}
  height: {args.camera_height}
  horizontal_fov_deg: {args.camera_fov:.9f}
  intrinsics:
    fx: {fx:.9f}
    fy: {fy:.9f}
    cx: {cx:.9f}
    cy: {cy:.9f}
    k1: 0.0
    k2: 0.0
    p1: 0.0
    p2: 0.0
  base_link_to_sensor_ros:
    x: {camera_tf_ros["x"]:.9f}
    y: {camera_tf_ros["y"]:.9f}
    z: {camera_tf_ros["z"]:.9f}
    roll: {camera_tf_ros["roll"]:.9f}
    pitch: {camera_tf_ros["pitch"]:.9f}
    yaw: {camera_tf_ros["yaw"]:.9f}
  timestamps_columns: ["seq", "timestamp_seconds", "carla_frame"]

lidar:
  name: {yaml_quote("lidar")}
  blueprint: {yaml_quote("sensor.lidar.ray_cast")}
  directory: {yaml_quote("lidar")}
  data_directory: {yaml_quote("lidar/data")}
  timestamps_file: {yaml_quote("lidar/timestamps.csv")}
  data_format: {yaml_quote("raw float32 row-major Nx6")}
  point_stride_bytes: 24
  fields: ["x", "y", "z", "intensity", "ring", "timestamp"]
  point_frame_ros: {yaml_quote(str(getattr(args, "lidar_point_frame", "lidar")))}
  timestamp_field: {yaml_quote("absolute CARLA elapsed_seconds of the 100Hz partial packet; constant for points in the same partial")}
  rate_hz: {args.lidar_hz:.9f}
  partial_packet_rate_hz: {1.0 / lidar_tick:.9f}
  sensor_tick: {lidar_tick:.9f}
  packets_per_sweep: {packets_per_sweep}
  rotation_frequency_hz: {args.lidar_hz:.9f}
  points_per_second: {points_per_second}
  carla_requested_rays_per_sweep: {args.lidar_sweep_points}
  horizon_scan_requested: {requested_horizon_scan(int(args.lidar_sweep_points), int(args.lidar_channels))}
  target_returned_points_per_sweep_approx: 50000
  point_count_note: {yaml_quote("CARLA ray_cast returns hit points only; requested rays/sweep is tuned to produce about 50k returned points in typical maps.")}
  channels: {args.lidar_channels}
  lower_fov_deg: {args.lidar_lower_fov:.9f}
  upper_fov_deg: {args.lidar_upper_fov:.9f}
  horizontal_fov_deg: {args.lidar_horizontal_fov:.9f}
  max_range_m: {args.lidar_range:.9f}
  ring_calculation: {yaml_quote("round((atan2(z, hypot(x, y))_deg - lower_fov) / (upper_fov - lower_fov) * (channels - 1))")}
  base_link_to_sensor_ros:
    x: {lidar_tf_ros["x"]:.9f}
    y: {lidar_tf_ros["y"]:.9f}
    z: {lidar_tf_ros["z"]:.9f}
    roll: {lidar_tf_ros["roll"]:.9f}
    pitch: {lidar_tf_ros["pitch"]:.9f}
    yaw: {lidar_tf_ros["yaw"]:.9f}
  timestamps_columns: ["seq", "sweep_end_timestamp_seconds", "sweep_end_carla_frame", "sweep_start_timestamp_seconds", "sweep_start_carla_frame"]

imu:
  name: {yaml_quote("imu")}
  blueprint: {yaml_quote("sensor.other.imu")}
  directory: {yaml_quote("imu")}
  data_directory: {yaml_quote("imu/data")}
  timestamps_file: {yaml_quote("imu/timestamps.csv")}
  data_format: {yaml_quote("raw float64 row-major 1x8")}
  sample_stride_bytes: 64
  fields: ["accel_x", "accel_y", "accel_z", "gyro_x", "gyro_y", "gyro_z", "compass", "timestamp"]
  accel_units: {yaml_quote("m/s^2")}
  gyro_units: {yaml_quote("rad/s")}
  compass_units: {yaml_quote("rad, CARLA-provided compass")}
  timestamp_field: {yaml_quote("absolute CARLA elapsed_seconds")}
  data_source: {yaml_quote(imu_data_source)}
  noise_model: {yaml_quote(imu_noise_model)}
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


def make_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=2000)
    ap.add_argument("--timeout", type=float, default=10.0)
    ap.add_argument("--carla-root", default=None, help="CARLA root, e.g. C:/Users/.../CARLA_0.9.16")
    ap.add_argument("--map", default=None, help="Optional map name to load before recording")
    ap.add_argument("--out-root", default=os.path.join("data-gen", "carla", "output"))
    ap.add_argument("--run-name", default=None)
    ap.add_argument("--duration", type=float, default=10.0, help="Recording duration in simulation seconds; <=0 means until Ctrl+C")
    ap.add_argument("--warmup-ticks", type=int, default=20)
    ap.add_argument("--warmup-min-ticks", type=int, default=50)
    ap.add_argument("--warmup-timeout", type=float, default=10.0, help="Seconds to retry sensor sync validation; <=0 means forever")
    ap.add_argument("--sync-validation-sweeps", type=int, default=3)
    ap.add_argument("--sync-max-missing", type=int, default=0)
    ap.add_argument("--fixed-dt", type=float, default=0.01)
    ap.add_argument("--wait-timeout", type=float, default=2.0)
    ap.add_argument("--max-writer-queue", type=int, default=128)

    ap.add_argument("--camera-hz", type=float, default=20.0)
    ap.add_argument("--camera-width", type=int, default=1280)
    ap.add_argument("--camera-height", type=int, default=360)
    ap.add_argument("--camera-fov", type=float, default=90.0)
    ap.add_argument("--camera-color", choices=["rgb", "gray"], default="rgb")
    ap.add_argument("--camera-x", type=float, default=1.8)
    ap.add_argument("--camera-y", type=float, default=0.0)
    ap.add_argument("--camera-z", type=float, default=1.6)

    ap.add_argument("--lidar-hz", type=float, default=10.0)
    ap.add_argument(
        "--lidar-sweep-points",
        type=int,
        default=65000,
        help="CARLA rays per full sweep. CARLA returns hit points only; 65000 is tuned to yield roughly 50000 saved points/sweep.",
    )
    ap.add_argument("--lidar-pps", type=int, default=0, help="Override points_per_second; default=sweep_points*lidar_hz")
    ap.add_argument("--lidar-channels", type=int, default=64)
    ap.add_argument("--lidar-lower-fov", type=float, default=-5.0)
    ap.add_argument("--lidar-upper-fov", type=float, default=15.0)
    ap.add_argument("--lidar-horizontal-fov", type=float, default=360.0)
    ap.add_argument("--lidar-range", type=float, default=100.0)
    ap.add_argument("--lidar-x", type=float, default=0.0)
    ap.add_argument("--lidar-y", type=float, default=0.0)
    ap.add_argument("--lidar-z", type=float, default=1.9)
    ap.add_argument(
        "--lidar-point-frame",
        choices=["lidar", "base_link"],
        default="lidar",
        help="Coordinate frame used for saved LiDAR XYZ points. Default preserves raw sensor-frame output.",
    )

    ap.add_argument("--imu-hz", type=float, default=100.0)
    ap.add_argument("--imu-x", type=float, default=0.0)
    ap.add_argument("--imu-y", type=float, default=0.0)
    ap.add_argument("--imu-z", type=float, default=0.0)

    ap.add_argument("--ego-filter", default="vehicle.tesla.model3")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--spawn-x", type=float, default=None)
    ap.add_argument("--spawn-y", type=float, default=None)
    ap.add_argument("--spawn-z", type=float, default=None)
    ap.add_argument("--spawn-roll", type=float, default=0.0)
    ap.add_argument("--spawn-pitch", type=float, default=0.0)
    ap.add_argument("--spawn-yaw", type=float, default=0.0)
    ap.add_argument("--spawn-strict", action="store_true")

    ap.add_argument("--motion-mode", choices=["static", "sine", "traffic-manager", "route-loop"], default="static")
    ap.add_argument("--tm-port", type=int, default=8000)
    ap.add_argument("--sine-throttle", type=float, default=0.25)
    ap.add_argument("--sine-steer-amp", type=float, default=0.18)
    ap.add_argument("--sine-steer-hz", type=float, default=0.15)
    ap.add_argument("--target-speed-kmh", type=float, default=25.0)
    ap.add_argument("--loop-target-distance", type=float, default=800.0)
    ap.add_argument("--loop-min-distance", type=float, default=50.0)
    ap.add_argument("--loop-closure-radius", type=float, default=2.0)
    ap.add_argument("--loop-closure-yaw-deg", type=float, default=45.0)
    ap.add_argument("--loop-timeout", type=float, default=0.0, help="Route-loop timeout in simulation seconds; <=0 means no timeout")
    ap.add_argument("--loop-waypoints", type=int, default=8, help="Deprecated; route-loop uses --route-step-m dense waypoints")
    ap.add_argument("--route-direction", choices=["ccw", "cw"], default="ccw")
    ap.add_argument("--route-step-m", type=float, default=8.0, help="Waypoint spacing used for route-loop planning")
    ap.add_argument("--route-search-beam", type=int, default=64, help="Route-loop candidate paths kept per search step")
    ap.add_argument("--route-search-max-factor", type=float, default=2.0, help="Maximum planned route length as target_distance*factor")
    ap.add_argument("--route-file", default=None, help="Load a fixed route-loop JSON route instead of planning a new route")
    ap.add_argument("--save-route-file", default=None, help="Also save the planned route-loop JSON route to this path")
    ap.add_argument("--route-laps", type=float, default=1.05, help="Route-loop laps to record before finalizing")
    ap.add_argument("--route-extra-distance", type=float, default=0.0, help="Extra meters to record after route-laps")
    ap.add_argument("--route-imu-mode", choices=["synthetic", "carla"], default="synthetic", help="IMU source for kinematic route-loop")
    ap.add_argument("--route-ignore-lights", action="store_true", default=True)
    ap.add_argument("--respect-traffic-lights", action="store_true", help="Disable the default route-loop traffic light ignoring")
    return ap


def pick_vehicle_blueprint(bp_lib: Any, ego_filter: str, rng: random.Random) -> Any:
    candidates = list(bp_lib.filter(ego_filter))
    if not candidates:
        candidates = list(bp_lib.filter("vehicle.*"))

    four_wheel = []
    for bp in candidates:
        if bp.has_attribute("number_of_wheels"):
            try:
                if bp.get_attribute("number_of_wheels").as_int() == 4:
                    four_wheel.append(bp)
            except Exception:
                pass
    if four_wheel:
        candidates = four_wheel

    bp = rng.choice(candidates)
    set_attr_if_exists(bp, "role_name", "ego")
    if bp.has_attribute("color"):
        colors = bp.get_attribute("color").recommended_values
        if colors:
            bp.set_attribute("color", rng.choice(colors))
    return bp


def spawn_ego(carla: Any, world: Any, bp: Any, args: argparse.Namespace, rng: random.Random) -> Tuple[Any, Any]:
    override_given = args.spawn_x is not None and args.spawn_y is not None and args.spawn_z is not None
    if override_given:
        spawn_tf = carla.Transform(
            carla.Location(x=args.spawn_x, y=args.spawn_y, z=args.spawn_z),
            carla.Rotation(roll=args.spawn_roll, pitch=args.spawn_pitch, yaw=args.spawn_yaw),
        )
        ego = world.try_spawn_actor(bp, spawn_tf)
        if ego is not None:
            return ego, spawn_tf
        if args.spawn_strict:
            raise RuntimeError("spawn override failed and --spawn-strict is set")
        print("[WARN] spawn override failed; falling back to random map spawn point")

    spawn_points = world.get_map().get_spawn_points()
    if not spawn_points:
        raise RuntimeError("current map has no spawn points")
    rng.shuffle(spawn_points)
    last_route_error = None
    for spawn_tf in spawn_points:
        if args.motion_mode == "route-loop":
            try:
                build_tm_loop_path(carla, world.get_map(), spawn_tf.location, args)
            except Exception as exc:
                last_route_error = exc
                continue
        ego = world.try_spawn_actor(bp, spawn_tf)
        if ego is not None:
            return ego, spawn_tf
    if args.motion_mode == "route-loop" and last_route_error is not None:
        raise RuntimeError(f"failed to find a spawn point with a closed route-loop: {last_route_error}")
    raise RuntimeError("failed to spawn ego vehicle")


def get_or_load_world(client: Any, args: argparse.Namespace) -> Any:
    try:
        if args.map:
            print(f"[CARLA] loading map {args.map}")
            return client.load_world(args.map)
        return client.get_world()
    except UnicodeDecodeError as exc:
        decoded = ""
        if isinstance(exc.object, (bytes, bytearray)):
            decoded = bytes(exc.object).decode("cp949", errors="replace")
            decoded = f" Decoded Windows message: {decoded}"
        raise RuntimeError(
            "CARLA PythonAPI failed while decoding world metadata as UTF-8. "
            "A common cause on this setup is that CARLA cannot write its cache under "
            "%USERPROFILE%\\carlaCache, then the Korean Windows error message fails UTF-8 decoding. "
            "Run the CARLA client once with permissions to create the cache, or fix that directory ACL."
            + decoded
        ) from exc


def wrap_deg(angle: float) -> float:
    return (angle + 180.0) % 360.0 - 180.0


def hold_vehicle_stopped(carla: Any, ego: Any) -> None:
    control = carla.VehicleControl()
    control.throttle = 0.0
    control.brake = 1.0
    control.hand_brake = True
    ego.apply_control(control)


def run_sensor_warmup_and_validation(
    carla: Any,
    world: Any,
    ego: Any,
    args: argparse.Namespace,
    camera_buf: SensorBuffer,
    lidar_buf: SensorBuffer,
    imu_buf: SensorBuffer,
    camera_ticks: int,
    lidar_sweep_ticks: int,
) -> Tuple[int, Dict[str, Any]]:
    min_warmup_ticks = max(int(args.warmup_ticks), int(args.warmup_min_ticks))
    validation_ticks = max(
        lidar_sweep_ticks * int(args.sync_validation_sweeps),
        camera_ticks * int(args.sync_validation_sweeps),
        10,
    )
    max_missing = int(args.sync_max_missing)
    deadline = None if args.warmup_timeout <= 0.0 else time.time() + float(args.warmup_timeout)
    attempt = 0

    while True:
        attempt += 1
        print(
            f"[WARMUP] attempt={attempt} warmup_ticks={min_warmup_ticks} "
            f"validation_ticks={validation_ticks}"
        )

        for _ in range(min_warmup_ticks):
            hold_vehicle_stopped(carla, ego)
            world.tick()

        warmup_frame = int(world.get_snapshot().frame)
        camera_buf.clear()
        lidar_buf.clear()
        imu_buf.clear()

        missing_imu = 0
        missing_lidar = 0
        camera_frames: List[int] = []
        first_validation_frame = None
        last_validation_frame = None

        for _ in range(validation_ticks):
            hold_vehicle_stopped(carla, ego)
            world_frame = int(world.tick())
            if first_validation_frame is None:
                first_validation_frame = world_frame
            last_validation_frame = world_frame

            if imu_buf.wait(world_frame, args.wait_timeout) is None:
                missing_imu += 1
            if lidar_buf.wait(world_frame, args.wait_timeout) is None:
                missing_lidar += 1

            for cam_meas in camera_buf.pop_all_ready(world_frame, warmup_frame):
                camera_frames.append(int(cam_meas.frame))

        camera_diffs = [b - a for a, b in zip(camera_frames[:-1], camera_frames[1:])]
        camera_ok = len(camera_frames) >= 2 and all(d == camera_ticks for d in camera_diffs)
        missing_ok = missing_imu <= max_missing and missing_lidar <= max_missing

        stats: Dict[str, Any] = {
            "attempt": attempt,
            "warmup_frame": warmup_frame,
            "first_validation_frame": first_validation_frame,
            "last_validation_frame": last_validation_frame,
            "validation_ticks": validation_ticks,
            "missing_imu": missing_imu,
            "missing_lidar": missing_lidar,
            "camera_frames": len(camera_frames),
            "camera_diffs": camera_diffs,
            "camera_ok": camera_ok,
            "missing_ok": missing_ok,
        }

        if camera_ok and missing_ok:
            boundary_period = math.lcm(camera_ticks, lidar_sweep_ticks)
            for _ in range(boundary_period * 2 + 1):
                hold_vehicle_stopped(carla, ego)
                frame = int(world.tick())
                if (frame % boundary_period) == 0:
                    camera_buf.clear()
                    lidar_buf.clear()
                    imu_buf.clear()
                    stats["record_start_boundary_frame"] = frame
                    print(
                        f"[WARMUP] stable. camera_frames={len(camera_frames)} "
                        f"missing_lidar={missing_lidar} missing_imu={missing_imu} boundary_frame={frame}"
                    )
                    return frame, stats

            stats["alignment_failed"] = True

        print(
            f"[WARMUP][RETRY] camera_ok={camera_ok} camera_frames={len(camera_frames)} "
            f"camera_diffs={camera_diffs[:8]} missing_lidar={missing_lidar} missing_imu={missing_imu}"
        )
        camera_buf.clear()
        lidar_buf.clear()
        imu_buf.clear()

        if deadline is not None and time.time() > deadline:
            raise RuntimeError(f"sensor warmup/sync validation failed before timeout: {stats}")


def waypoint_key(wp: Any, route_step_m: float) -> Tuple[Any, ...]:
    try:
        return (
            int(wp.road_id),
            int(wp.section_id),
            int(wp.lane_id),
            int(round(float(wp.s) / max(1.0, route_step_m))),
        )
    except Exception:
        loc = wp.transform.location
        return (round(float(loc.x), 1), round(float(loc.y), 1), round(float(loc.z), 1))


def waypoint_location(carla: Any, wp: Any) -> Any:
    loc = wp.transform.location
    return carla.Location(x=float(loc.x), y=float(loc.y), z=float(loc.z))


def sort_route_candidates(current_wp: Any, candidates: Sequence[Any], args: argparse.Namespace) -> List[Any]:
    current_yaw = float(current_wp.transform.rotation.yaw)
    route_step_m = float(args.route_step_m)
    unique: Dict[Tuple[Any, ...], Any] = {}
    for wp in candidates:
        unique.setdefault(waypoint_key(wp, route_step_m), wp)

    def score(wp: Any) -> Tuple[int, float, Tuple[Any, ...]]:
        delta = wrap_deg(float(wp.transform.rotation.yaw) - current_yaw)
        if abs(delta) <= 10.0:
            turn_bucket = 1
        elif args.route_direction == "cw":
            turn_bucket = 0 if delta < 0.0 else 2
        else:
            turn_bucket = 0 if delta > 0.0 else 2
        return (turn_bucket, abs(delta), waypoint_key(wp, route_step_m))

    return sorted(unique.values(), key=score)


def build_tm_loop_path(carla: Any, world_map: Any, start_location: Any, args: argparse.Namespace) -> Tuple[List[Any], Dict[str, Any]]:
    start_wp = world_map.get_waypoint(start_location, project_to_road=True)
    if start_wp is None:
        raise RuntimeError("cannot build route-loop path because the start location is not on a drivable waypoint")

    route_step_m = max(2.0, float(args.route_step_m))
    target_distance = max(float(args.loop_target_distance), float(args.loop_min_distance))
    min_distance = max(0.0, float(args.loop_min_distance))
    max_distance = max(target_distance + 100.0, target_distance * max(1.1, float(args.route_search_max_factor)))
    beam_width = max(8, int(args.route_search_beam))
    max_steps = max(4, int(math.ceil(max_distance / route_step_m)) + 2)
    closure_radius = float(args.loop_closure_radius)
    closure_yaw_deg = float(args.loop_closure_yaw_deg)
    start_key = waypoint_key(start_wp, route_step_m)

    states: List[Tuple[Any, List[Any], float, Dict[Tuple[Any, ...], int]]] = [
        (start_wp, [start_wp], 0.0, {start_key: 1})
    ]
    solutions: List[Tuple[float, List[Any], float, float]] = []
    explored = 0

    for _ in range(max_steps):
        next_states: List[Tuple[Any, List[Any], float, Dict[Tuple[Any, ...], int]]] = []
        for current_wp, path, distance_m, visits in states:
            current_loc = current_wp.transform.location
            candidates = sort_route_candidates(current_wp, current_wp.next(route_step_m), args)
            explored += len(candidates)
            for next_wp in candidates:
                next_loc = next_wp.transform.location
                step_dist = float(next_loc.distance(current_loc))
                if step_dist <= 0.01:
                    step_dist = route_step_m
                next_distance = distance_m + step_dist
                if next_distance > max_distance:
                    continue

                closure_distance = float(next_loc.distance(start_wp.transform.location))
                closure_yaw = abs(wrap_deg(float(next_wp.transform.rotation.yaw) - float(start_wp.transform.rotation.yaw)))
                next_path = path + [next_wp]
                if (
                    next_distance >= min_distance
                    and closure_distance <= closure_radius
                    and closure_yaw <= closure_yaw_deg
                ):
                    solutions.append((next_distance, next_path, closure_distance, closure_yaw))
                    continue

                key = waypoint_key(next_wp, route_step_m)
                if key == start_key and next_distance < min_distance:
                    continue
                if visits.get(key, 0) >= 1:
                    continue

                next_visits = dict(visits)
                next_visits[key] = next_visits.get(key, 0) + 1
                next_states.append((next_wp, next_path, next_distance, next_visits))

        if solutions:
            break
        if not next_states:
            break

        def state_score(state: Tuple[Any, List[Any], float, Dict[Tuple[Any, ...], int]]) -> Tuple[float, int]:
            wp = state[0]
            distance_m = state[2]
            loc = wp.transform.location
            closure_distance = float(loc.distance(start_wp.transform.location))
            target_err = abs(distance_m - target_distance)
            return (target_err + closure_distance * 0.25, len(state[1]))

        next_states.sort(key=state_score)
        states = next_states[:beam_width]

    if not solutions:
        map_name = getattr(world_map, "name", "<unknown>")
        start = start_wp.transform
        raise RuntimeError(
            "route-loop waypoint planner failed to find a closed route. "
            f"map={map_name} start=({start.location.x:.3f}, {start.location.y:.3f}, {start.location.z:.3f}, "
            f"yaw={start.rotation.yaw:.3f}) explored_edges={explored} "
            f"target_distance={target_distance:.1f} max_distance={max_distance:.1f}"
        )

    def solution_score(item: Tuple[float, List[Any], float, float]) -> Tuple[float, float, float]:
        distance_m, _path, closure_distance, closure_yaw = item
        return (abs(distance_m - target_distance), closure_distance, closure_yaw)

    planned_length, planned_wps, end_distance, end_yaw = min(solutions, key=solution_score)
    locations = [waypoint_location(carla, wp) for wp in planned_wps[1:]]
    plan = {
        "planned_length_m": planned_length,
        "planned_start_end_distance_m": end_distance,
        "planned_start_end_yaw_error_deg": end_yaw,
        "path_waypoints": len(locations),
        "explored_edges": explored,
        "route_step_m": route_step_m,
    }
    return locations, plan


def route_nodes_and_length(carla: Any, start_location: Any, route_path: Sequence[Any]) -> Tuple[List[Any], float]:
    nodes = [carla.Location(x=float(start_location.x), y=float(start_location.y), z=float(start_location.z))]
    nodes.extend(carla.Location(x=float(loc.x), y=float(loc.y), z=float(loc.z)) for loc in route_path)
    if len(nodes) > 1 and float(nodes[-1].distance(nodes[0])) > 0.05:
        nodes.append(carla.Location(x=float(nodes[0].x), y=float(nodes[0].y), z=float(nodes[0].z)))

    length_m = 0.0
    for a, b in zip(nodes[:-1], nodes[1:]):
        length_m += float(b.distance(a))
    return nodes, length_m


def save_route_json(path: str, world_map: Any, spawn_tf: Any, route_path: Sequence[Any], route_plan: Dict[str, Any], args: argparse.Namespace) -> None:
    route_nodes, playback_length_m = route_nodes_and_length_for_json(spawn_tf.location, route_path)
    data = {
        "format": "carla_route_loop_v1",
        "map": getattr(world_map, "name", "<unknown>"),
        "spawn_carla": transform_to_dict(spawn_tf),
        "route": {
            "direction": args.route_direction,
            "target_speed_kmh": float(args.target_speed_kmh),
            "route_step_m": float(route_plan.get("route_step_m", args.route_step_m)),
            "planned_length_m": float(route_plan["planned_length_m"]),
            "playback_length_m": float(playback_length_m),
            "planned_start_end_distance_m": float(route_plan["planned_start_end_distance_m"]),
            "planned_start_end_yaw_error_deg": float(route_plan["planned_start_end_yaw_error_deg"]),
            "path_waypoints": int(route_plan["path_waypoints"]),
        },
        "waypoints": [location_to_dict(loc) for loc in route_path],
        "playback_nodes": route_nodes,
    }
    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(data, f, indent=2)
        f.write("\n")


def route_nodes_and_length_for_json(start_location: Any, route_path: Sequence[Any]) -> Tuple[List[Dict[str, float]], float]:
    nodes = [location_to_dict(start_location)]
    nodes.extend(location_to_dict(loc) for loc in route_path)
    if len(nodes) > 1:
        dx = nodes[-1]["x"] - nodes[0]["x"]
        dy = nodes[-1]["y"] - nodes[0]["y"]
        dz = nodes[-1]["z"] - nodes[0]["z"]
        if math.sqrt(dx * dx + dy * dy + dz * dz) > 0.05:
            nodes.append(dict(nodes[0]))

    length_m = 0.0
    for a, b in zip(nodes[:-1], nodes[1:]):
        dx = b["x"] - a["x"]
        dy = b["y"] - a["y"]
        dz = b["z"] - a["z"]
        length_m += math.sqrt(dx * dx + dy * dy + dz * dz)
    return nodes, length_m


def load_route_json(carla: Any, world_map: Any, path: str) -> Tuple[List[Any], Dict[str, Any], Any]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if data.get("format") != "carla_route_loop_v1":
        raise RuntimeError(f"unsupported route file format in {path}")
    route_map = data.get("map")
    current_map = getattr(world_map, "name", "<unknown>")
    if route_map and route_map != current_map:
        raise RuntimeError(f"route file map mismatch: route={route_map}, current={current_map}")

    spawn = data["spawn_carla"]
    spawn_tf = carla.Transform(
        carla.Location(x=float(spawn["x"]), y=float(spawn["y"]), z=float(spawn["z"])),
        carla.Rotation(
            roll=float(spawn.get("roll", 0.0)),
            pitch=float(spawn.get("pitch", 0.0)),
            yaw=float(spawn.get("yaw", 0.0)),
        ),
    )
    route_path = [
        carla.Location(x=float(item["x"]), y=float(item["y"]), z=float(item["z"]))
        for item in data.get("waypoints", [])
    ]
    if not route_path:
        raise RuntimeError(f"route file has no waypoints: {path}")

    _, playback_length_m = route_nodes_and_length(carla, spawn_tf.location, route_path)
    route = data.get("route", {})
    plan = {
        "planned_length_m": float(route.get("planned_length_m", playback_length_m)),
        "playback_length_m": float(route.get("playback_length_m", playback_length_m)),
        "planned_start_end_distance_m": float(route.get("planned_start_end_distance_m", route_path[-1].distance(spawn_tf.location))),
        "planned_start_end_yaw_error_deg": float(route.get("planned_start_end_yaw_error_deg", 0.0)),
        "path_waypoints": len(route_path),
        "explored_edges": 0,
        "route_step_m": float(route.get("route_step_m", 0.0)),
        "source": "file",
    }
    return route_path, plan, spawn_tf


def append_route_summary(path: str, summary: Dict[str, Any]) -> None:
    def pose_yaml(prefix: str, tf: Any) -> str:
        return (
            f"  {prefix}: "
            f"{{x: {tf.location.x:.9f}, y: {tf.location.y:.9f}, z: {tf.location.z:.9f}, "
            f"roll: {tf.rotation.roll:.9f}, pitch: {tf.rotation.pitch:.9f}, yaw: {tf.rotation.yaw:.9f}}}\n"
        )

    text = "\nroute:\n"
    text += f"  mode: {yaml_quote(summary['mode'])}\n"
    text += f"  closed: {str(bool(summary['closed'])).lower()}\n"
    text += f"  target_speed_kmh: {summary['target_speed_kmh']:.9f}\n"
    text += f"  planned_length_m: {summary['planned_length_m']:.9f}\n"
    text += f"  planned_chord_length_m: {summary['planned_length_m']:.9f}\n"
    text += f"  playback_length_m: {summary['playback_length_m']:.9f}\n"
    text += f"  target_record_distance_m: {summary['target_record_distance_m']:.9f}\n"
    text += f"  route_laps: {summary['route_laps']:.9f}\n"
    text += f"  route_extra_distance_m: {summary['route_extra_distance_m']:.9f}\n"
    text += f"  route_imu_mode: {yaml_quote(summary['route_imu_mode'])}\n"
    text += f"  source: {yaml_quote(summary['source'])}\n"
    text += f"  planned_start_end_distance_m: {summary['planned_start_end_distance_m']:.9f}\n"
    text += f"  planned_start_end_yaw_error_deg: {summary['planned_start_end_yaw_error_deg']:.9f}\n"
    text += f"  route_step_m: {summary['route_step_m']:.9f}\n"
    text += f"  planner_explored_edges: {summary['planner_explored_edges']}\n"
    text += f"  recorded_distance_m: {summary['recorded_distance_m']:.9f}\n"
    text += f"  closure_distance_m: {summary['closure_distance_m']:.9f}\n"
    text += f"  closure_yaw_error_deg: {summary['closure_yaw_error_deg']:.9f}\n"
    text += f"  start_frame: {summary['start_frame']}\n"
    text += f"  start_timestamp: {summary['start_timestamp']:.9f}\n"
    text += f"  end_frame: {summary['end_frame']}\n"
    text += f"  end_timestamp: {summary['end_timestamp']:.9f}\n"
    text += f"  path_waypoints: {summary['path_waypoints']}\n"
    text += pose_yaml("start_pose_carla", summary["start_pose_carla"])
    text += pose_yaml("end_pose_carla", summary["end_pose_carla"])
    with open(path, "a", encoding="utf-8", newline="\n") as f:
        f.write(text)


def yaml_scalar(value: Any) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    if value is None:
        return "null"
    if isinstance(value, float):
        return f"{value:.9f}"
    if isinstance(value, int):
        return str(value)
    return yaml_quote(value)


def append_sync_summary(path: str, stats: Dict[str, Any]) -> None:
    text = "\nsync_validation:\n"
    for key in (
        "attempt",
        "warmup_frame",
        "first_validation_frame",
        "last_validation_frame",
        "validation_ticks",
        "missing_imu",
        "missing_lidar",
        "camera_frames",
        "camera_ok",
        "missing_ok",
        "record_start_boundary_frame",
    ):
        if key in stats:
            text += f"  {key}: {yaml_scalar(stats[key])}\n"
    camera_diffs = stats.get("camera_diffs", [])
    text += "  camera_diffs: [" + ", ".join(str(int(x)) for x in camera_diffs) + "]\n"
    with open(path, "a", encoding="utf-8", newline="\n") as f:
        f.write(text)


def append_dataset_status(path: str, status: Dict[str, Any]) -> None:
    text = "\ndataset_status:\n"
    for key in (
        "valid",
        "invalid_reason",
        "missing_lidar_partials",
        "missing_imu",
        "dropped_partial_sweeps",
        "route_closed",
    ):
        if key in status:
            text += f"  {key}: {yaml_scalar(status[key])}\n"
    with open(path, "a", encoding="utf-8", newline="\n") as f:
        f.write(text)


def apply_motion_control(carla: Any, ego: Any, args: argparse.Namespace, elapsed_record_s: float) -> None:
    if args.motion_mode == "static":
        control = carla.VehicleControl()
        control.throttle = 0.0
        control.brake = 1.0
        control.hand_brake = True
        ego.apply_control(control)
        return

    if args.motion_mode == "sine":
        control = carla.VehicleControl()
        ramp = min(1.0, max(0.0, elapsed_record_s / 2.0))
        control.throttle = max(0.0, min(1.0, args.sine_throttle * ramp))
        control.brake = 0.0
        control.steer = max(
            -1.0,
            min(1.0, args.sine_steer_amp * math.sin(2.0 * math.pi * args.sine_steer_hz * elapsed_record_s)),
        )
        control.hand_brake = False
        ego.apply_control(control)


def clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def apply_route_loop_kinematic(
    carla: Any,
    ego: Any,
    route_path: Sequence[Any],
    route_state: Dict[str, Any],
    args: argparse.Namespace,
    world_dt: float,
) -> None:
    if not route_path:
        hold_vehicle_stopped(carla, ego)
        return

    if "nodes" not in route_state:
        start_loc = ego.get_transform().location
        nodes, _length = route_nodes_and_length(carla, start_loc, route_path)
        cumulative = [0.0]
        for a, b in zip(nodes[:-1], nodes[1:]):
            cumulative.append(cumulative[-1] + float(b.distance(a)))
        total_length = float(cumulative[-1])
        target_distance = max(
            float(args.loop_min_distance),
            total_length * max(1.0, float(args.route_laps)) + max(0.0, float(args.route_extra_distance)),
        )
        route_state["nodes"] = nodes
        route_state["cumulative"] = cumulative
        route_state["distance_m"] = 0.0
        route_state["segment_index"] = 0
        route_state["total_length_m"] = total_length
        route_state["target_distance_m"] = target_distance
        route_state["completed"] = False
        route_state["prev_x"] = float(start_loc.x)
        route_state["prev_y"] = float(start_loc.y)
        route_state["prev_vx"] = 0.0
        route_state["prev_vy"] = 0.0
        route_state["prev_yaw"] = float(ego.get_transform().rotation.yaw)
        route_state["imu_initialized"] = False

    nodes = route_state["nodes"]
    cumulative = route_state["cumulative"]
    total_length = float(cumulative[-1])
    target_distance = float(route_state["target_distance_m"])
    speed_mps = max(0.1, float(args.target_speed_kmh) / 3.6)
    distance_m = min(target_distance, float(route_state.get("distance_m", 0.0)) + speed_mps * world_dt)
    route_state["completed"] = distance_m >= target_distance - 1e-6

    playback_distance = distance_m % total_length if total_length > 1e-6 else 0.0
    if route_state["completed"] and abs(target_distance % total_length) <= speed_mps * world_dt + 1e-6:
        playback_distance = total_length

    segment_index = int(route_state.get("segment_index", 0))
    if playback_distance < cumulative[min(segment_index, len(cumulative) - 1)]:
        segment_index = 0
    while segment_index + 1 < len(cumulative) - 1 and cumulative[segment_index + 1] < playback_distance:
        segment_index += 1
    route_state["distance_m"] = distance_m
    route_state["segment_index"] = segment_index

    start = nodes[segment_index]
    end = nodes[min(segment_index + 1, len(nodes) - 1)]
    seg_len = max(1e-6, float(end.distance(start)))
    alpha = clamp((playback_distance - cumulative[segment_index]) / seg_len, 0.0, 1.0)
    x = float(start.x) + (float(end.x) - float(start.x)) * alpha
    y = float(start.y) + (float(end.y) - float(start.y)) * alpha
    z = float(start.z) + (float(end.z) - float(start.z)) * alpha
    tangent_lookahead = min(8.0, max(2.0, total_length * 0.02))
    back_x, back_y, _back_z = sample_route_location(nodes, cumulative, playback_distance - tangent_lookahead)
    ahead_x, ahead_y, _ahead_z = sample_route_location(nodes, cumulative, playback_distance + tangent_lookahead)
    yaw = math.degrees(math.atan2(ahead_y - back_y, ahead_x - back_x))

    ego.set_transform(carla.Transform(carla.Location(x=x, y=y, z=z), carla.Rotation(yaw=yaw)))
    prev_yaw = float(route_state.get("prev_yaw", yaw))
    if bool(route_state.get("imu_initialized", False)):
        yaw_rate = math.radians(wrap_deg(yaw - prev_yaw)) / world_dt
    else:
        yaw_rate = 0.0
        route_state["imu_initialized"] = True
    yaw_rate = clamp(yaw_rate, -2.5, 2.5)
    accel_y_ros = clamp(speed_mps * yaw_rate, -20.0, 20.0)
    route_state["synthetic_imu_base"] = (
        0.0,
        float(accel_y_ros),
        9.81,
        0.0,
        0.0,
        float(-yaw_rate),
        float((math.radians(90.0 - yaw)) % (2.0 * math.pi)),
    )
    route_state["prev_x"] = x
    route_state["prev_y"] = y
    route_state["prev_yaw"] = yaw


def sample_route_location(nodes: Sequence[Any], cumulative: Sequence[float], distance_m: float) -> Tuple[float, float, float]:
    total_length = float(cumulative[-1]) if cumulative else 0.0
    if total_length <= 1e-6:
        loc = nodes[0]
        return float(loc.x), float(loc.y), float(loc.z)
    d = distance_m % total_length
    segment_index = 0
    while segment_index + 1 < len(cumulative) - 1 and cumulative[segment_index + 1] < d:
        segment_index += 1
    start = nodes[segment_index]
    end = nodes[min(segment_index + 1, len(nodes) - 1)]
    seg_len = max(1e-6, float(end.distance(start)))
    alpha = clamp((d - cumulative[segment_index]) / seg_len, 0.0, 1.0)
    return (
        float(start.x) + (float(end.x) - float(start.x)) * alpha,
        float(start.y) + (float(end.y) - float(start.y)) * alpha,
        float(start.z) + (float(end.z) - float(start.z)) * alpha,
    )


def synthetic_imu_values_from_route(meas: Any, route_state: Dict[str, Any]) -> array.array:
    base = route_state.get("synthetic_imu_base", (0.0, 0.0, 9.81, 0.0, 0.0, 0.0, float(meas.compass)))
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

def summarize_intervals(name: str, stamps: Sequence[float], expected_dt: float) -> str:
    if len(stamps) < 2:
        return f"{name}: count={len(stamps)}, not enough data for interval stats"
    diffs = [b - a for a, b in zip(stamps[:-1], stamps[1:])]
    avg = sum(diffs) / len(diffs)
    min_dt = min(diffs)
    max_dt = max(diffs)
    max_err = max(abs(d - expected_dt) for d in diffs)
    return (
        f"{name}: count={len(stamps)}, dt_avg={avg:.9f}, "
        f"dt_min={min_dt:.9f}, dt_max={max_dt:.9f}, max_abs_err={max_err:.9f}"
    )


def main() -> None:
    args = make_arg_parser().parse_args()
    if args.respect_traffic_lights:
        args.route_ignore_lights = False
    rng = random.Random(args.seed)
    carla = import_carla_or_exit(args.carla_root)

    world_dt = float(args.fixed_dt)
    camera_ticks = ensure_integer_period_ticks("camera", args.camera_hz, world_dt)
    lidar_sweep_ticks = ensure_integer_period_ticks("lidar", args.lidar_hz, world_dt)
    imu_ticks = ensure_integer_period_ticks("imu", args.imu_hz, world_dt)
    if imu_ticks != 1:
        raise ValueError("For this recorder IMU must run every world tick; use --imu-hz 100 with --fixed-dt 0.01")

    camera_tick = camera_ticks * world_dt
    lidar_tick = world_dt
    imu_tick = imu_ticks * world_dt
    points_per_second = int(args.lidar_pps) if args.lidar_pps > 0 else int(round(args.lidar_sweep_points * args.lidar_hz))

    run_name = args.run_name or datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = os.path.abspath(os.path.join(args.out_root, run_name))
    camera_dir = os.path.join(out_dir, "camera")
    lidar_dir = os.path.join(out_dir, "lidar")
    imu_dir = os.path.join(out_dir, "imu")
    for d in (
        os.path.join(camera_dir, "data"),
        os.path.join(lidar_dir, "data"),
        os.path.join(imu_dir, "data"),
    ):
        mkdir(d)

    client = carla.Client(args.host, args.port)
    client.set_timeout(args.timeout)

    world = None
    original_settings = None
    traffic_manager = None
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
    route_path: List[Any] = []
    route_plan: Dict[str, Any] = {
        "planned_length_m": 0.0,
        "playback_length_m": 0.0,
        "planned_start_end_distance_m": float("inf"),
        "planned_start_end_yaw_error_deg": float("inf"),
        "path_waypoints": 0,
    }
    preloaded_route_path: List[Any] = []
    preloaded_route_plan: Dict[str, Any] = {}
    preloaded_spawn_tf = None
    route_closed = False
    route_start_tf = None
    route_start_frame = 0
    route_start_timestamp = 0.0
    route_recorded_distance = 0.0
    route_closure_distance = float("inf")
    route_closure_yaw = float("inf")
    route_current_closure_distance = float("inf")
    route_current_closure_yaw = float("inf")
    route_target_distance = 0.0
    last_world_frame = 0
    last_sim_timestamp = 0.0

    try:
        world = get_or_load_world(client, args)

        original_settings = world.get_settings()
        settings = world.get_settings()
        settings.synchronous_mode = True
        settings.fixed_delta_seconds = world_dt
        world.apply_settings(settings)

        if args.motion_mode == "traffic-manager":
            traffic_manager = client.get_trafficmanager(args.tm_port)
            traffic_manager.set_synchronous_mode(True)
            try:
                traffic_manager.set_random_device_seed(int(args.seed))
            except Exception:
                pass

        if args.motion_mode == "route-loop" and args.route_file:
            preloaded_route_path, preloaded_route_plan, preloaded_spawn_tf = load_route_json(
                carla, world.get_map(), args.route_file
            )
            print(
                f"[ROUTE] loaded fixed route file={args.route_file} "
                f"path_waypoints={len(preloaded_route_path)} "
                f"playback_length={preloaded_route_plan['playback_length_m']:.1f}m"
            )

        bp_lib = world.get_blueprint_library()
        vehicle_bp = pick_vehicle_blueprint(bp_lib, args.ego_filter, rng)
        if preloaded_spawn_tf is not None:
            ego = world.try_spawn_actor(vehicle_bp, preloaded_spawn_tf)
            if ego is None:
                raise RuntimeError(f"failed to spawn ego at route-file spawn pose: {args.route_file}")
            spawn_tf = preloaded_spawn_tf
        else:
            ego, spawn_tf = spawn_ego(carla, world, vehicle_bp, args, rng)
        actors.append(ego)
        try:
            ego.set_simulate_physics(True)
        except Exception:
            pass

        camera_bp = bp_lib.find("sensor.camera.rgb")
        configure_camera_blueprint(camera_bp, args.camera_width, args.camera_height, args.camera_fov, camera_tick)
        set_attr_if_exists(camera_bp, "role_name", "camera")
        camera_tf = carla.Transform(
            carla.Location(x=args.camera_x, y=args.camera_y, z=args.camera_z),
            carla.Rotation(roll=0.0, pitch=0.0, yaw=0.0),
        )
        camera = world.spawn_actor(camera_bp, camera_tf, attach_to=ego)
        actors.append(camera)

        lidar_bp = bp_lib.find("sensor.lidar.ray_cast")
        configure_lidar_blueprint(
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
        set_attr_if_exists(lidar_bp, "role_name", "lidar")
        lidar_tf = carla.Transform(
            carla.Location(x=args.lidar_x, y=args.lidar_y, z=args.lidar_z),
            carla.Rotation(roll=0.0, pitch=0.0, yaw=0.0),
        )
        lidar = world.spawn_actor(lidar_bp, lidar_tf, attach_to=ego)
        actors.append(lidar)

        imu_bp = bp_lib.find("sensor.other.imu")
        configure_imu_blueprint(imu_bp, imu_tick)
        set_attr_if_exists(imu_bp, "role_name", "imu")
        imu_tf = carla.Transform(
            carla.Location(x=args.imu_x, y=args.imu_y, z=args.imu_z),
            carla.Rotation(roll=0.0, pitch=0.0, yaw=0.0),
        )
        imu = world.spawn_actor(imu_bp, imu_tf, attach_to=ego)
        actors.append(imu)

        camera_buf = SensorBuffer("camera", max_keep=8192)
        lidar_buf = SensorBuffer("lidar", max_keep=8192)
        imu_buf = SensorBuffer("imu", max_keep=8192)
        camera.listen(lambda m: camera_buf.put(m))
        lidar.listen(lambda m: lidar_buf.put(m))
        imu.listen(lambda m: imu_buf.put(m))

        if args.motion_mode == "route-loop":
            if preloaded_route_path:
                route_path = preloaded_route_path
                route_plan = preloaded_route_plan
            else:
                route_path, route_plan = build_tm_loop_path(
                    carla, world.get_map(), spawn_tf.location, args
                )
            _route_nodes, route_playback_length = route_nodes_and_length(carla, spawn_tf.location, route_path)
            route_plan["playback_length_m"] = route_playback_length
            route_target_distance = max(
                float(args.loop_min_distance),
                route_playback_length * max(1.0, float(args.route_laps)) + max(0.0, float(args.route_extra_distance)),
            )
            print(
                f"[ROUTE] route-loop path_waypoints={len(route_path)} "
                f"planned_length={route_plan['planned_length_m']:.1f}m "
                f"playback_length={route_plan['playback_length_m']:.1f}m "
                f"target_record_distance={route_target_distance:.1f}m "
                f"planned_closure={route_plan['planned_start_end_distance_m']:.2f}m/"
                f"{route_plan['planned_start_end_yaw_error_deg']:.2f}deg"
            )
            default_route_file = os.path.join(out_dir, "route.json")
            save_route_json(default_route_file, world.get_map(), spawn_tf, route_path, route_plan, args)
            if args.save_route_file:
                save_route_json(args.save_route_file, world.get_map(), spawn_tf, route_path, route_plan, args)
            print(f"[ROUTE] saved fixed route: {default_route_file}")

        write_meta(
            os.path.join(out_dir, "meta.yaml"),
            args,
            client.get_server_version(),
            world.get_map().name,
            vehicle_bp.id,
            spawn_tf,
            out_dir,
            run_name,
            camera_tick,
            lidar_tick,
            imu_tick,
            lidar_sweep_ticks,
            points_per_second,
        )

        camera_writer = CameraWriter(
            os.path.join(camera_dir, "data"), args.camera_color, max_queue=args.max_writer_queue
        )
        lidar_writer = LidarWriter(
            os.path.join(lidar_dir, "data"),
            args.lidar_channels,
            args.lidar_lower_fov,
            args.lidar_upper_fov,
            point_frame=args.lidar_point_frame,
            sensor_xyz_ros=(args.lidar_x, -args.lidar_y, args.lidar_z),
            max_queue=args.max_writer_queue,
        )
        imu_writer = ImuWriter(os.path.join(imu_dir, "data"), max_queue=args.max_writer_queue)

        f_cam_ts = open_csv_with_header(
            os.path.join(camera_dir, "timestamps.csv"),
            ("seq", "timestamp_seconds", "carla_frame"),
        )
        f_lidar_ts = open_csv_with_header(
            os.path.join(lidar_dir, "timestamps.csv"),
            (
                "seq",
                "sweep_end_timestamp_seconds",
                "sweep_end_carla_frame",
                "sweep_start_timestamp_seconds",
                "sweep_start_carla_frame",
            ),
        )
        f_imu_ts = open_csv_with_header(
            os.path.join(imu_dir, "timestamps.csv"),
            ("seq", "timestamp_seconds", "carla_frame"),
        )

        print(f"[RUN] output: {out_dir}")
        print(
            f"[RUN] world={1/world_dt:.1f}Hz, camera={args.camera_hz:.1f}Hz, "
            f"lidar={args.lidar_hz:.1f}Hz ({lidar_sweep_ticks} partials/sweep), imu={args.imu_hz:.1f}Hz"
        )
        print(
            f"[RUN] lidar pps={points_per_second} (requested rays/sweep={args.lidar_sweep_points}), channels={args.lidar_channels}, "
            f"vertical_fov=[{args.lidar_lower_fov}, {args.lidar_upper_fov}] deg"
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
        append_sync_summary(os.path.join(out_dir, "meta.yaml"), sync_stats)

        if args.motion_mode == "traffic-manager":
            if traffic_manager is None:
                raise RuntimeError("traffic manager is not initialized")
            ego.set_autopilot(True, args.tm_port)
        elif args.motion_mode == "route-loop":
            if not route_path:
                raise RuntimeError("route-loop path is empty")
            try:
                ego.set_simulate_physics(False)
            except Exception:
                pass
            print(
                f"[ROUTE] armed deterministic kinematic route player target_speed={args.target_speed_kmh:.1f}km/h "
                f"closure_radius={args.loop_closure_radius:.1f}m"
            )

        start_snapshot = world.get_snapshot()
        route_start_frame = int(start_snapshot.frame)
        route_start_timestamp = float(start_snapshot.timestamp.elapsed_seconds)
        route_start_tf = ego.get_transform()

        if args.motion_mode == "route-loop":
            target_ticks = None if args.loop_timeout <= 0.0 else max(1, int(round(args.loop_timeout / world_dt)))
        else:
            target_ticks = None if args.duration <= 0.0 else max(1, int(round(args.duration / world_dt)))
        seq_camera = 0
        seq_lidar = 0
        seq_imu = 0
        tick_idx = 0
        lidar_packets: List[Tuple[bytes, float, int]] = []
        last_progress_t = time.time()
        record_start_wall = time.time()
        prev_route_location = ego.get_transform().location
        closure_pending = False
        route_state: Dict[str, Any] = {"target_index": 0}

        while target_ticks is None or tick_idx < target_ticks:
            elapsed_record_s = tick_idx * world_dt
            if args.motion_mode == "route-loop":
                apply_route_loop_kinematic(carla, ego, route_path, route_state, args, world_dt)
            elif args.motion_mode != "traffic-manager":
                apply_motion_control(carla, ego, args, elapsed_record_s)

            world_frame = int(world.tick())
            tick_idx += 1
            last_world_frame = world_frame
            last_sim_timestamp = float(world.get_snapshot().timestamp.elapsed_seconds)

            imu_meas = imu_buf.wait(world_frame, args.wait_timeout)
            if imu_meas is None:
                missing_imu += 1
                print(f"[WARN] missing IMU frame={world_frame}")
            else:
                if args.motion_mode == "route-loop" and args.route_imu_mode == "synthetic":
                    values = synthetic_imu_values_from_route(imu_meas, route_state)
                else:
                    values = imu_values_ros(imu_meas)
                imu_writer.push((seq_imu, values))
                write_csv_row(f_imu_ts, (f"{seq_imu:06d}", format_float(float(imu_meas.timestamp)), int(imu_meas.frame)))
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
                    start_frame = lidar_packets[0][2]
                    end_ts = lidar_packets[-1][1]
                    end_frame = lidar_packets[-1][2]
                    lidar_writer.push((seq_lidar, lidar_packets))
                    write_csv_row(
                        f_lidar_ts,
                        (
                            f"{seq_lidar:06d}",
                            format_float(end_ts),
                            end_frame,
                            format_float(start_ts),
                            start_frame,
                        ),
                    )
                    lidar_stamps.append(end_ts)
                    seq_lidar += 1
                    lidar_packets = []

            for cam_meas in camera_buf.pop_all_ready(world_frame, warmup_frame):
                camera_writer.push((seq_camera, bytes(cam_meas.raw_data), int(cam_meas.width), int(cam_meas.height)))
                write_csv_row(
                    f_cam_ts,
                    (f"{seq_camera:06d}", format_float(float(cam_meas.timestamp)), int(cam_meas.frame)),
                )
                camera_stamps.append(float(cam_meas.timestamp))
                seq_camera += 1

            now = time.time()
            if args.motion_mode == "route-loop":
                cur_tf = ego.get_transform()
                cur_loc = cur_tf.location
                route_recorded_distance += float(cur_loc.distance(prev_route_location))
                prev_route_location = cur_loc
                if route_start_tf is not None:
                    route_current_closure_distance = float(cur_loc.distance(route_start_tf.location))
                    route_current_closure_yaw = abs(wrap_deg(float(cur_tf.rotation.yaw) - float(route_start_tf.rotation.yaw)))
                    if route_recorded_distance >= float(args.loop_min_distance) and route_current_closure_distance < route_closure_distance:
                        route_closure_distance = route_current_closure_distance
                        route_closure_yaw = route_current_closure_yaw
                    if (
                        not route_closed
                        and route_recorded_distance >= float(args.loop_min_distance)
                        and route_current_closure_distance <= float(args.loop_closure_radius)
                        and route_current_closure_yaw <= float(args.loop_closure_yaw_deg)
                        and seq_lidar > 0
                    ):
                        route_closed = True
                        route_closure_distance = route_current_closure_distance
                        route_closure_yaw = route_current_closure_yaw
                        print(
                            f"[ROUTE] closure detected distance={route_recorded_distance:.1f}m "
                            f"closure_dist={route_closure_distance:.2f}m yaw_err={route_closure_yaw:.2f}deg"
                        )
                    if bool(route_state.get("completed", False)) and not lidar_packets:
                        break

            if now - last_progress_t >= 2.0:
                sim_done = tick_idx * world_dt
                wall_elapsed = max(1e-6, now - record_start_wall)
                route_msg = ""
                if args.motion_mode == "route-loop":
                    route_msg = (
                        f" route_dist={route_recorded_distance:.1f}m "
                        f"closure={route_current_closure_distance:.1f}m/{route_current_closure_yaw:.1f}deg"
                    )
                print(
                    f"[REC] sim={sim_done:.2f}s wall={wall_elapsed:.2f}s "
                    f"camera={seq_camera} lidar={seq_lidar} imu={seq_imu}{route_msg}"
                )
                last_progress_t = now

        # Drain camera callbacks that arrived during the last tick.
        time.sleep(0.05)
        for cam_meas in camera_buf.pop_all_ready(10**12, warmup_frame):
            camera_writer.push((seq_camera, bytes(cam_meas.raw_data), int(cam_meas.width), int(cam_meas.height)))
            write_csv_row(
                f_cam_ts,
                (f"{seq_camera:06d}", format_float(float(cam_meas.timestamp)), int(cam_meas.frame)),
            )
            camera_stamps.append(float(cam_meas.timestamp))
            seq_camera += 1

        if lidar_packets:
            dropped_partial_sweeps += 1
            print(f"[WARN] dropping incomplete LiDAR sweep with {len(lidar_packets)} partial packets")

        if args.motion_mode == "route-loop":
            end_tf = ego.get_transform()
            if route_start_tf is None:
                route_start_tf = end_tf
            if last_world_frame == 0:
                snap = world.get_snapshot()
                last_world_frame = int(snap.frame)
                last_sim_timestamp = float(snap.timestamp.elapsed_seconds)
            append_route_summary(
                os.path.join(out_dir, "meta.yaml"),
                {
                    "mode": "route-loop",
                    "closed": route_closed,
                    "target_speed_kmh": float(args.target_speed_kmh),
                    "planned_length_m": float(route_plan["planned_length_m"]),
                    "playback_length_m": float(route_plan["playback_length_m"]),
                    "target_record_distance_m": route_target_distance,
                    "route_laps": float(args.route_laps),
                    "route_extra_distance_m": float(args.route_extra_distance),
                    "route_imu_mode": args.route_imu_mode,
                    "source": str(route_plan.get("source", "planned")),
                    "planned_start_end_distance_m": float(route_plan["planned_start_end_distance_m"]),
                    "planned_start_end_yaw_error_deg": float(route_plan["planned_start_end_yaw_error_deg"]),
                    "route_step_m": float(route_plan["route_step_m"]),
                    "planner_explored_edges": int(route_plan["explored_edges"]),
                    "recorded_distance_m": route_recorded_distance,
                    "closure_distance_m": route_closure_distance,
                    "closure_yaw_error_deg": route_closure_yaw,
                    "start_frame": route_start_frame,
                    "start_timestamp": route_start_timestamp,
                    "end_frame": last_world_frame,
                    "end_timestamp": last_sim_timestamp,
                    "path_waypoints": int(route_plan["path_waypoints"]),
                    "start_pose_carla": route_start_tf,
                    "end_pose_carla": end_tf,
                },
            )
            if not route_closed:
                print(
                    f"[ROUTE][WARN] loop closure not reached before timeout. "
                    f"distance={route_recorded_distance:.1f}m "
                    f"closure={route_closure_distance:.1f}m yaw_err={route_closure_yaw:.1f}deg"
                )

        invalid_reasons = []
        if missing_lidar > 0:
            invalid_reasons.append("missing_lidar_partials")
        if missing_imu > 0:
            invalid_reasons.append("missing_imu")
        if dropped_partial_sweeps > 0:
            invalid_reasons.append("dropped_partial_sweeps")
        if args.motion_mode == "route-loop" and not route_closed:
            invalid_reasons.append("route_not_closed")
        append_dataset_status(
            os.path.join(out_dir, "meta.yaml"),
            {
                "valid": not invalid_reasons,
                "invalid_reason": ",".join(invalid_reasons),
                "missing_lidar_partials": missing_lidar,
                "missing_imu": missing_imu,
                "dropped_partial_sweeps": dropped_partial_sweeps,
                "route_closed": route_closed if args.motion_mode == "route-loop" else None,
            },
        )

    finally:
        for sensor in actors[1:]:
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

        if args.motion_mode in ("traffic-manager", "route-loop"):
            try:
                if actors:
                    actors[0].set_autopilot(False)
            except Exception:
                pass
            try:
                if traffic_manager is not None:
                    traffic_manager.set_synchronous_mode(False)
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
    print("[CHECK] " + summarize_intervals("camera", camera_stamps, 1.0 / args.camera_hz))
    print("[CHECK] " + summarize_intervals("lidar", lidar_stamps, 1.0 / args.lidar_hz))
    print("[CHECK] " + summarize_intervals("imu", imu_stamps, 1.0 / args.imu_hz))
    print(f"[CHECK] missing_lidar_partials={missing_lidar}, missing_imu={missing_imu}, dropped_partial_sweeps={dropped_partial_sweeps}")
    print(f"[DONE] output: {out_dir}")


if __name__ == "__main__":
    main()
