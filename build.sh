#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_NAME="atb"
PACKAGE="./cmd/atb"
OUTPUT_DIR="${ROOT_DIR}/dist"

SUPPORTED_OSES=("darwin" "linux" "windows")
SUPPORTED_ARCHES=("amd64" "arm64")

usage() {
  cat <<'EOF'
Build the atb CLI for the current platform or a selected target.

Usage:
  ./build.sh
  ./build.sh --all
  ./build.sh --os linux --arch arm64
  ./build.sh --os windows
  ./build.sh --arch amd64
  ./build.sh --output-dir ./build

Options:
  --all              Build the full supported matrix: darwin/linux/windows x amd64/arm64.
  --os VALUE         Target OS: darwin, linux, or windows.
  --arch VALUE       Target architecture: amd64 or arm64.
  --output-dir PATH  Output directory. Default: ./dist
  --help             Show this help text.

Notes:
  - With no target flags, the script builds for the current Go host target.
  - x86 builds map to Go's amd64 target.
  - Binaries are written as:
      dist/<os>-<arch>/atb
      dist/<os>-<arch>/atb.exe   (windows)
EOF
}

contains() {
  local needle="$1"
  shift
  local item
  for item in "$@"; do
    if [[ "$item" == "$needle" ]]; then
      return 0
    fi
  done
  return 1
}

normalize_arch() {
  case "$1" in
    x86_64|x86|amd64)
      printf 'amd64\n'
      ;;
    arm64|aarch64)
      printf 'arm64\n'
      ;;
    *)
      printf '%s\n' "$1"
      ;;
  esac
}

build_one() {
  local target_os="$1"
  local target_arch="$2"

  local ext=""
  if [[ "$target_os" == "windows" ]]; then
    ext=".exe"
  fi

  local target_dir="${OUTPUT_DIR}/${target_os}-${target_arch}"
  local output_path="${target_dir}/${APP_NAME}${ext}"

  mkdir -p "${target_dir}"
  printf 'Building %s/%s -> %s\n' "${target_os}" "${target_arch}" "${output_path}"

  GOOS="${target_os}" \
  GOARCH="${target_arch}" \
  CGO_ENABLED=0 \
  go build -trimpath -o "${output_path}" "${PACKAGE}"
}

build_all=false
target_os=""
target_arch=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --all)
      build_all=true
      shift
      ;;
    --os)
      [[ $# -ge 2 ]] || { printf 'Missing value for --os\n' >&2; exit 1; }
      target_os="$2"
      shift 2
      ;;
    --arch)
      [[ $# -ge 2 ]] || { printf 'Missing value for --arch\n' >&2; exit 1; }
      target_arch="$(normalize_arch "$2")"
      shift 2
      ;;
    --output-dir)
      [[ $# -ge 2 ]] || { printf 'Missing value for --output-dir\n' >&2; exit 1; }
      if [[ "$2" = /* ]]; then
        OUTPUT_DIR="$2"
      else
        OUTPUT_DIR="${ROOT_DIR}/$2"
      fi
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      printf 'Unknown option: %s\n\n' "$1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ "${build_all}" == true && ( -n "${target_os}" || -n "${target_arch}" ) ]]; then
  printf 'Do not combine --all with --os or --arch.\n' >&2
  exit 1
fi

if [[ "${build_all}" == true ]]; then
  rm -rf "${OUTPUT_DIR}"
  for os_name in "${SUPPORTED_OSES[@]}"; do
    for arch_name in "${SUPPORTED_ARCHES[@]}"; do
      build_one "${os_name}" "${arch_name}"
    done
  done
  exit 0
fi

if [[ -z "${target_os}" ]]; then
  target_os="$(go env GOOS)"
fi
if [[ -z "${target_arch}" ]]; then
  target_arch="$(go env GOARCH)"
fi

if ! contains "${target_os}" "${SUPPORTED_OSES[@]}"; then
  printf 'Unsupported OS %q. Valid values: %s\n' "${target_os}" "$(IFS=', '; echo "${SUPPORTED_OSES[*]}")" >&2
  exit 1
fi
if ! contains "${target_arch}" "${SUPPORTED_ARCHES[@]}"; then
  printf 'Unsupported architecture %q. Valid values: %s\n' "${target_arch}" "$(IFS=', '; echo "${SUPPORTED_ARCHES[*]}")" >&2
  exit 1
fi

build_one "${target_os}" "${target_arch}"
