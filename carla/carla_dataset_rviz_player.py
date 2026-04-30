#!/usr/bin/env python3
"""Replay CARLA recorder camera and LiDAR files as ROS 2 topics for RViz2.

Expected dataset layout:

  dataset/
    meta.yaml
    camera/timestamps.csv
    camera/data/000000.png
    lidar/timestamps.csv
    lidar/data/000000.bin

LiDAR BIN files are the recorder format: float32 row-major Nx6
``x, y, z, intensity, ring, timestamp`` in ROS body axes.
"""

from __future__ import annotations

import argparse
import csv
import os
import struct
import sys
import time
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


DEFAULT_DATASET = "src/windows-data/route_loop_fixed_load_laps_001"


@dataclass(frozen=True)
class TimedFile:
    seq: int
    stamp: float
    frame: int
    path: Path


@dataclass(frozen=True)
class ImageData:
    width: int
    height: int
    encoding: str
    data: bytes


def resolve_dataset(path_text: str) -> Path:
    path = Path(path_text).expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path
    return path


def parse_timestamp_file(path: Path, data_dir: Path, suffix: str, default_rate_hz: float) -> List[TimedFile]:
    rows: List[TimedFile] = []
    if path.exists():
        if path.suffix.lower() == ".csv":
            with path.open("r", encoding="ascii", newline="") as f:
                reader = csv.DictReader(f)
                if reader.fieldnames is None:
                    raise ValueError(f"{path}: missing CSV header")
                for line_no, row in enumerate(reader, 2):
                    seq_text = row.get("seq")
                    if not seq_text:
                        continue
                    stamp_text = row.get("timestamp_seconds", row.get("sweep_end_timestamp_seconds"))
                    frame_text = row.get("carla_frame", row.get("sweep_end_carla_frame"))
                    if stamp_text is None or frame_text is None:
                        raise ValueError(f"{path}:{line_no}: missing timestamp/frame columns")
                    seq = int(seq_text)
                    stamp = float(stamp_text)
                    frame = int(frame_text)
                    rows.append(TimedFile(seq, stamp, frame, data_dir / f"{seq:06d}{suffix}"))
        else:
            with path.open("r", encoding="ascii") as f:
                for line_no, line in enumerate(f, 1):
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    parts = line.split()
                    if len(parts) < 3:
                        raise ValueError(f"{path}:{line_no}: expected at least 3 columns")
                    seq = int(parts[0])
                    stamp = float(parts[1])
                    frame = int(parts[2])
                    rows.append(TimedFile(seq, stamp, frame, data_dir / f"{seq:06d}{suffix}"))
    else:
        period = 1.0 / default_rate_hz
        for i, file_path in enumerate(sorted(data_dir.glob(f"*{suffix}"))):
            rows.append(TimedFile(i, i * period, i, file_path))

    return [row for row in rows if row.path.exists()]


def timestamp_path(dataset: Path, sensor_name: str) -> Path:
    csv_path = dataset / sensor_name / "timestamps.csv"
    if csv_path.exists():
        return csv_path
    return dataset / sensor_name / "timestamps.txt"


def load_meta(dataset: Path) -> Dict[str, Any]:
    meta_path = dataset / "meta.yaml"
    if not meta_path.exists():
        return {}
    try:
        import yaml  # type: ignore

        with meta_path.open("r", encoding="utf-8") as f:
            loaded = yaml.safe_load(f) or {}
        return loaded if isinstance(loaded, dict) else {}
    except Exception:
        return {}


def nested(meta: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    value: Any = meta
    for key in keys:
        if not isinstance(value, dict) or key not in value:
            return default
        value = value[key]
    return value


def read_png_from_recorder(path: Path) -> ImageData:
    """Decode PNGs written by the recorder."""
    fast_image = try_read_png_fast(path)
    if fast_image is not None:
        return fast_image
    return read_png_stdlib(path)


def try_read_png_fast(path: Path) -> Optional[ImageData]:
    try:
        import cv2  # type: ignore
        import numpy as np  # type: ignore

        blob = np.frombuffer(path.read_bytes(), dtype=np.uint8)
        image = cv2.imdecode(blob, cv2.IMREAD_UNCHANGED)
        if image is None:
            return None
        if len(image.shape) == 2:
            return ImageData(int(image.shape[1]), int(image.shape[0]), "mono8", image.tobytes())
        if image.shape[2] >= 3:
            rgb = np.ascontiguousarray(image[:, :, 2::-1])
            return ImageData(int(rgb.shape[1]), int(rgb.shape[0]), "rgb8", rgb.tobytes())
    except Exception:
        pass

    try:
        from PIL import Image  # type: ignore

        with Image.open(path) as image:
            if image.mode == "L":
                decoded = image
                encoding = "mono8"
            else:
                decoded = image.convert("RGB")
                encoding = "rgb8"
            return ImageData(decoded.width, decoded.height, encoding, decoded.tobytes())
    except Exception:
        return None

    return None


def read_png_stdlib(path: Path) -> ImageData:
    """Decode PNGs using only the standard library."""
    blob = path.read_bytes()
    if not blob.startswith(b"\x89PNG\r\n\x1a\n"):
        raise ValueError(f"{path} is not a PNG")

    offset = 8
    width = height = 0
    bit_depth = color_type = None
    compressed = bytearray()

    while offset + 12 <= len(blob):
        length = struct.unpack(">I", blob[offset : offset + 4])[0]
        chunk_type = blob[offset + 4 : offset + 8]
        data = blob[offset + 8 : offset + 8 + length]
        offset += 12 + length
        if chunk_type == b"IHDR":
            width, height, bit_depth, color_type, compression, filter_method, interlace = struct.unpack(
                ">IIBBBBB", data
            )
            if bit_depth != 8 or compression != 0 or filter_method != 0 or interlace != 0:
                raise ValueError(f"{path}: unsupported PNG parameters")
            if color_type not in (0, 2):
                raise ValueError(f"{path}: unsupported PNG color_type={color_type}")
        elif chunk_type == b"IDAT":
            compressed.extend(data)
        elif chunk_type == b"IEND":
            break

    if width <= 0 or height <= 0 or bit_depth != 8 or color_type is None:
        raise ValueError(f"{path}: missing PNG IHDR")

    channels = 1 if color_type == 0 else 3
    row_bytes = width * channels
    raw = zlib.decompress(bytes(compressed))
    expected_min = height * (row_bytes + 1)
    if len(raw) < expected_min:
        raise ValueError(f"{path}: truncated PNG payload")

    out = bytearray(height * row_bytes)
    prev = bytearray(row_bytes)
    src = 0
    dst = 0

    for _ in range(height):
        filter_type = raw[src]
        src += 1
        row = bytearray(raw[src : src + row_bytes])
        src += row_bytes

        for i in range(row_bytes):
            left = row[i - channels] if i >= channels else 0
            up = prev[i]
            up_left = prev[i - channels] if i >= channels else 0
            if filter_type == 0:
                val = row[i]
            elif filter_type == 1:
                val = row[i] + left
            elif filter_type == 2:
                val = row[i] + up
            elif filter_type == 3:
                val = row[i] + ((left + up) >> 1)
            elif filter_type == 4:
                val = row[i] + paeth_predictor(left, up, up_left)
            else:
                raise ValueError(f"{path}: unsupported PNG filter={filter_type}")
            row[i] = val & 0xFF

        out[dst : dst + row_bytes] = row
        dst += row_bytes
        prev = row

    return ImageData(width, height, "mono8" if channels == 1 else "rgb8", bytes(out))


def paeth_predictor(a: int, b: int, c: int) -> int:
    p = a + b - c
    pa = abs(p - a)
    pb = abs(p - b)
    pc = abs(p - c)
    if pa <= pb and pa <= pc:
        return a
    if pb <= pc:
        return b
    return c


def validate_dataset(
    dataset: Path,
    enable_camera: bool = True,
    enable_lidar: bool = True,
) -> Tuple[List[TimedFile], List[TimedFile], Dict[str, Any]]:
    if not enable_camera and not enable_lidar:
        raise ValueError("at least one of camera or lidar must be enabled")
    if not dataset.exists():
        raise FileNotFoundError(f"dataset does not exist: {dataset}")

    meta = load_meta(dataset)

    camera: List[TimedFile] = []
    lidar: List[TimedFile] = []
    status_lines = [f"[OK] dataset={dataset}"]

    if enable_camera:
        camera = parse_timestamp_file(
            timestamp_path(dataset, "camera"),
            dataset / "camera" / "data",
            ".png",
            float(nested(meta, "camera", "rate_hz", default=20.0)),
        )

    if enable_lidar:
        lidar = parse_timestamp_file(
            timestamp_path(dataset, "lidar"),
            dataset / "lidar" / "data",
            ".bin",
            float(nested(meta, "lidar", "rate_hz", default=10.0)),
        )

    if enable_camera and camera:
        first_image = read_png_from_recorder(camera[0].path)
        status_lines.append(
            f"[OK] camera frames={len(camera)} first={camera[0].path.name} "
            f"{first_image.width}x{first_image.height} {first_image.encoding}"
        )
    elif enable_camera:
        status_lines.append(f"[SKIP] camera missing under {dataset / 'camera' / 'data'}")
    else:
        status_lines.append("[SKIP] camera disabled")

    if enable_lidar and lidar:
        bad_lidar = [row.path for row in lidar[:20] if row.path.stat().st_size % 24 != 0]
        if bad_lidar:
            raise ValueError(f"LiDAR file size is not divisible by 24 bytes: {bad_lidar[0]}")
        status_lines.append(
            f"[OK] lidar sweeps={len(lidar)} first={lidar[0].path.name} "
            f"points={lidar[0].path.stat().st_size // 24}"
        )
    elif enable_lidar:
        status_lines.append(f"[SKIP] lidar missing under {dataset / 'lidar' / 'data'}")
    else:
        status_lines.append("[SKIP] lidar disabled")

    if not camera and not lidar:
        requested = []
        if enable_camera:
            requested.append("camera")
        if enable_lidar:
            requested.append("lidar")
        requested_text = ",".join(requested) if requested else "camera,lidar"
        raise FileNotFoundError(f"no playable data found for requested sensors: {requested_text}")

    print("\n".join(status_lines))
    return camera, lidar, meta


class CarlaDatasetRvizPlayer:
    def __init__(self, args: argparse.Namespace, camera: List[TimedFile], lidar: List[TimedFile], meta: Dict[str, Any]):
        import rclpy
        from rclpy.node import Node

        class PlayerNode(Node):
            pass

        self.rclpy = rclpy
        self.node = PlayerNode("carla_dataset_rviz_player")
        self.args = args
        self.camera = camera
        self.lidar = lidar
        self.meta = meta
        self.camera_index = 0
        self.lidar_index = 0
        self.loop_start_wall = time.monotonic()
        self.loop_start_stamp = self.start_stamp
        self.last_log_wall = 0.0

        from rclpy.qos import QoSProfile, ReliabilityPolicy
        from sensor_msgs.msg import CameraInfo, Image, PointCloud2

        qos = QoSProfile(depth=5, reliability=ReliabilityPolicy.RELIABLE)
        self.image_msg_type = Image if self.camera else None
        self.camera_info_msg_type = CameraInfo if self.camera else None
        self.cloud_msg_type = PointCloud2 if self.lidar else None
        self.image_pub = self.node.create_publisher(Image, args.image_topic, qos) if self.camera else None
        self.camera_info_pub = (
            self.node.create_publisher(CameraInfo, args.camera_info_topic, qos) if self.camera else None
        )
        self.cloud_pub = self.node.create_publisher(PointCloud2, args.lidar_topic, qos) if self.lidar else None
        self.publish_static_transforms()
        self.timer = self.node.create_timer(0.005, self.on_timer)

    @property
    def start_stamp(self) -> float:
        stamps = []
        if self.camera:
            stamps.append(self.camera[0].stamp)
        if self.lidar:
            stamps.append(self.lidar[0].stamp)
        return min(stamps)

    @property
    def end_stamp(self) -> float:
        stamps = []
        if self.camera:
            stamps.append(self.camera[-1].stamp)
        if self.lidar:
            stamps.append(self.lidar[-1].stamp)
        return max(stamps)

    def now_dataset_stamp(self) -> float:
        elapsed = (time.monotonic() - self.loop_start_wall) * float(self.args.speed)
        return self.loop_start_stamp + elapsed

    def reset_loop(self) -> None:
        self.camera_index = 0
        self.lidar_index = 0
        self.loop_start_wall = time.monotonic()
        self.loop_start_stamp = self.start_stamp
        self.node.get_logger().info("looping dataset")

    def on_timer(self) -> None:
        now_stamp = self.now_dataset_stamp()

        while self.camera_index < len(self.camera) and self.camera[self.camera_index].stamp <= now_stamp:
            self.publish_image(self.camera[self.camera_index])
            self.camera_index += 1

        while self.lidar_index < len(self.lidar) and self.lidar[self.lidar_index].stamp <= now_stamp:
            self.publish_cloud(self.lidar[self.lidar_index])
            self.lidar_index += 1

        if self.camera_index >= len(self.camera) and self.lidar_index >= len(self.lidar):
            if self.args.loop:
                self.reset_loop()
            else:
                self.node.get_logger().info("finished dataset")
                self.rclpy.shutdown()
                return

        wall = time.monotonic()
        if wall - self.last_log_wall > 5.0:
            self.last_log_wall = wall
            self.node.get_logger().info(
                f"published camera={self.camera_index}/{len(self.camera)} "
                f"lidar={self.lidar_index}/{len(self.lidar)}"
            )

    def ros_stamp(self, stamp_seconds: float) -> Any:
        from builtin_interfaces.msg import Time

        sec = int(stamp_seconds)
        nanosec = int(round((stamp_seconds - sec) * 1_000_000_000))
        if nanosec >= 1_000_000_000:
            sec += 1
            nanosec -= 1_000_000_000
        return Time(sec=sec, nanosec=nanosec)

    def publish_image(self, row: TimedFile) -> None:
        if self.image_msg_type is None or self.image_pub is None or self.camera_info_pub is None:
            return
        image = read_png_from_recorder(row.path)
        msg = self.image_msg_type()
        msg.header.stamp = self.ros_stamp(row.stamp)
        msg.header.frame_id = self.args.camera_frame
        msg.height = image.height
        msg.width = image.width
        msg.encoding = image.encoding
        msg.is_bigendian = False
        msg.step = image.width * (1 if image.encoding == "mono8" else 3)
        msg.data = image.data
        self.image_pub.publish(msg)
        self.camera_info_pub.publish(self.make_camera_info(row.stamp, image.width, image.height))

    def make_camera_info(self, stamp: float, width: int, height: int) -> Any:
        if self.camera_info_msg_type is None:
            raise RuntimeError("camera is disabled")
        msg = self.camera_info_msg_type()
        msg.header.stamp = self.ros_stamp(stamp)
        msg.header.frame_id = self.args.camera_frame
        msg.width = int(nested(self.meta, "camera", "width", default=width))
        msg.height = int(nested(self.meta, "camera", "height", default=height))
        fx = float(nested(self.meta, "camera", "intrinsics", "fx", default=width * 0.5))
        fy = float(nested(self.meta, "camera", "intrinsics", "fy", default=fx))
        cx = float(nested(self.meta, "camera", "intrinsics", "cx", default=width * 0.5))
        cy = float(nested(self.meta, "camera", "intrinsics", "cy", default=height * 0.5))
        msg.k = [fx, 0.0, cx, 0.0, fy, cy, 0.0, 0.0, 1.0]
        msg.p = [fx, 0.0, cx, 0.0, 0.0, fy, cy, 0.0, 0.0, 0.0, 1.0, 0.0]
        msg.r = [1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0]
        msg.d = [
            float(nested(self.meta, "camera", "intrinsics", "k1", default=0.0)),
            float(nested(self.meta, "camera", "intrinsics", "k2", default=0.0)),
            float(nested(self.meta, "camera", "intrinsics", "p1", default=0.0)),
            float(nested(self.meta, "camera", "intrinsics", "p2", default=0.0)),
            0.0,
        ]
        msg.distortion_model = "plumb_bob"
        return msg

    def publish_cloud(self, row: TimedFile) -> None:
        if self.cloud_msg_type is None or self.cloud_pub is None:
            return
        from sensor_msgs.msg import PointField

        data = row.path.read_bytes()
        point_step = 24
        point_count = len(data) // point_step

        msg = self.cloud_msg_type()
        msg.header.stamp = self.ros_stamp(row.stamp)
        msg.header.frame_id = self.args.lidar_frame
        msg.height = 1
        msg.width = point_count
        msg.fields = [
            PointField(name="x", offset=0, datatype=PointField.FLOAT32, count=1),
            PointField(name="y", offset=4, datatype=PointField.FLOAT32, count=1),
            PointField(name="z", offset=8, datatype=PointField.FLOAT32, count=1),
            PointField(name="intensity", offset=12, datatype=PointField.FLOAT32, count=1),
            PointField(name="ring", offset=16, datatype=PointField.FLOAT32, count=1),
            PointField(name="timestamp", offset=20, datatype=PointField.FLOAT32, count=1),
        ]
        msg.is_bigendian = False
        msg.point_step = point_step
        msg.row_step = point_step * point_count
        msg.data = data[: msg.row_step]
        msg.is_dense = False
        self.cloud_pub.publish(msg)

    def publish_static_transforms(self) -> None:
        from geometry_msgs.msg import TransformStamped
        from tf2_ros import StaticTransformBroadcaster

        self.static_broadcaster = StaticTransformBroadcaster(self.node)
        transforms = []
        if self.camera:
            transforms.append(
                self.make_static_transform(
                    self.args.fixed_frame,
                    self.args.camera_frame,
                    nested(self.meta, "camera", "base_link_to_sensor_ros", default={}),
                )
            )
        if self.lidar:
            transforms.append(
                self.make_static_transform(
                    self.args.fixed_frame,
                    self.args.lidar_frame,
                    nested(self.meta, "lidar", "base_link_to_sensor_ros", default={}),
                )
            )
        self.static_broadcaster.sendTransform([tf for tf in transforms if isinstance(tf, TransformStamped)])

    def make_static_transform(self, parent: str, child: str, tf_meta: Dict[str, Any]) -> Any:
        from geometry_msgs.msg import TransformStamped

        msg = TransformStamped()
        msg.header.stamp = self.ros_stamp(0.0)
        msg.header.frame_id = parent
        msg.child_frame_id = child
        msg.transform.translation.x = float(tf_meta.get("x", 0.0))
        msg.transform.translation.y = float(tf_meta.get("y", 0.0))
        msg.transform.translation.z = float(tf_meta.get("z", 0.0))
        qx, qy, qz, qw = quaternion_from_rpy(
            float(tf_meta.get("roll", 0.0)),
            float(tf_meta.get("pitch", 0.0)),
            float(tf_meta.get("yaw", 0.0)),
        )
        msg.transform.rotation.x = qx
        msg.transform.rotation.y = qy
        msg.transform.rotation.z = qz
        msg.transform.rotation.w = qw
        return msg

    def spin(self) -> None:
        self.node.get_logger().info(
            f"playing camera={len(self.camera)} lidar={len(self.lidar)} "
            f"stamp_range=[{self.start_stamp:.3f}, {self.end_stamp:.3f}] speed={self.args.speed}"
        )
        self.rclpy.spin(self.node)


def quaternion_from_rpy(roll: float, pitch: float, yaw: float) -> Tuple[float, float, float, float]:
    import math

    if max(abs(roll), abs(pitch), abs(yaw)) > math.tau:
        roll = math.radians(roll)
        pitch = math.radians(pitch)
        yaw = math.radians(yaw)

    cr = math.cos(roll * 0.5)
    sr = math.sin(roll * 0.5)
    cp = math.cos(pitch * 0.5)
    sp = math.sin(pitch * 0.5)
    cy = math.cos(yaw * 0.5)
    sy = math.sin(yaw * 0.5)
    return (
        sr * cp * cy - cr * sp * sy,
        cr * sp * cy + sr * cp * sy,
        cr * cp * sy - sr * sp * cy,
        cr * cp * cy + sr * sp * sy,
    )


def positive_float(text: str) -> float:
    value = float(text)
    if value <= 0.0:
        raise argparse.ArgumentTypeError("must be positive")
    return value


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dataset", default=DEFAULT_DATASET, help="CARLA recorder dataset directory")
    ap.add_argument("--validate-only", action="store_true", help="parse files and exit without ROS")
    ap.add_argument("--loop", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument("--speed", type=positive_float, default=1.0, help="playback speed multiplier")
    ap.add_argument("--camera", action=argparse.BooleanOptionalAction, default=True, help="enable camera playback")
    ap.add_argument("--lidar", action=argparse.BooleanOptionalAction, default=True, help="enable LiDAR playback")
    ap.add_argument("--fixed-frame", default="base_link")
    ap.add_argument("--camera-frame", default="camera")
    ap.add_argument("--lidar-frame", default="lidar")
    ap.add_argument("--image-topic", default="/camera/image")
    ap.add_argument("--camera-info-topic", default="/camera/camera_info")
    ap.add_argument("--lidar-topic", default="/lidar/points")
    return ap.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    dataset = resolve_dataset(args.dataset)
    try:
        camera, lidar, meta = validate_dataset(dataset, enable_camera=args.camera, enable_lidar=args.lidar)
    except Exception as exc:
        print(f"[ERR] {exc}", file=sys.stderr)
        return 2

    if args.validate_only:
        return 0

    try:
        import rclpy
    except Exception as exc:
        print("[ERR] failed to import rclpy. Source your ROS 2 environment first.", file=sys.stderr)
        print(f"[ERR] {exc}", file=sys.stderr)
        return 2

    rclpy.init(args=None)
    player = CarlaDatasetRvizPlayer(args, camera, lidar, meta)
    try:
        player.spin()
    except KeyboardInterrupt:
        pass
    finally:
        if rclpy.ok():
            rclpy.shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
