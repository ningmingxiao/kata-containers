export distro="ubuntu"
export ROOTFS_DIR="$(realpath kata-containers/tools/osbuilder/rootfs-builder/rootfs)"
sudo rm -rf "${ROOTFS_DIR}"
pushd kata-containers/tools/osbuilder/rootfs-builder
script -fec 'sudo -E USE_DOCKER=true OS_VERSION=noble  EXTRA_PKGS="criu" ./rootfs.sh "${distro}"'
popd
