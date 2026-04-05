#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
IMAGES_DIR="$SCRIPT_DIR/images"
WORK_DIR="$SCRIPT_DIR/.work"
BASE_IMAGE="$IMAGES_DIR/ubuntu-noble.img"
NODE_COUNT="${2:-5}"

SSH_PORT_BASE=2200
BLOCKYARD_PORT_BASE=7400
MONITOR_PORT_BASE=4440

mkdir -p "$WORK_DIR"

if [ ! -f "$BASE_IMAGE" ]; then
    echo "ERROR: Base image not found at $BASE_IMAGE"
    echo "Download it: wget -O $BASE_IMAGE https://cloud-images.ubuntu.com/noble/current/noble-server-cloudimg-amd64.img"
    exit 1
fi

generate_ssh_key() {
    if [ ! -f "$WORK_DIR/id_test" ]; then
        ssh-keygen -t ed25519 -f "$WORK_DIR/id_test" -N "" -q
        echo "Generated SSH key: $WORK_DIR/id_test"
    fi
}

create_seed_iso() {
    local node_id=$1
    local node_name="node-${node_id}"
    local seed_dir="$WORK_DIR/${node_name}-seed"
    local ssh_pub_key
    ssh_pub_key=$(cat "$WORK_DIR/id_test.pub")

    mkdir -p "$seed_dir"

    cat > "$seed_dir/meta-data" << EOF
instance-id: ${node_name}
local-hostname: ${node_name}
EOF

    cat > "$seed_dir/user-data" << EOF
#cloud-config
users:
  - name: root
    ssh_authorized_keys:
      - ${ssh_pub_key}
    lock_passwd: false

ssh_pwauth: false

package_update: false
package_upgrade: false

write_files:
  - path: /etc/sysctl.d/99-blockyard.conf
    content: |
      net.ipv4.ip_forward=1
    permissions: '0644'

runcmd:
  - sysctl -p /etc/sysctl.d/99-blockyard.conf
  - echo "node ${node_name} ready" > /tmp/blockyard-ready
EOF

    mkisofs -output "$WORK_DIR/${node_name}-seed.iso" \
        -volid cidata -joliet -rock \
        "$seed_dir/user-data" "$seed_dir/meta-data" 2>/dev/null

    echo "Created seed ISO: $WORK_DIR/${node_name}-seed.iso"
}

create_node_disk() {
    local node_id=$1
    local node_name="node-${node_id}"
    local disk="$WORK_DIR/${node_name}-disk.qcow2"
    local zfs_disk="$WORK_DIR/${node_name}-zfs.qcow2"

    if [ ! -f "$disk" ]; then
        qemu-img create -f qcow2 -b "$BASE_IMAGE" -F qcow2 "$disk" 10G
        echo "Created disk: $disk"
    fi

    if [ ! -f "$zfs_disk" ]; then
        qemu-img create -f qcow2 "$zfs_disk" 5G
        echo "Created ZFS disk: $zfs_disk"
    fi
}

start_node() {
    local node_id=$1
    local node_name="node-${node_id}"
    local ssh_port=$((SSH_PORT_BASE + node_id))
    local by_port=$((BLOCKYARD_PORT_BASE + node_id * 10))
    local data_port=$((by_port + 1))
    local grpc_port=$((by_port + 2))
    local monitor_port=$((MONITOR_PORT_BASE + node_id))

    local pidfile="$WORK_DIR/${node_name}.pid"
    if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
        echo "Node $node_name already running (pid $(cat "$pidfile"))"
        return 0
    fi

    echo "Starting $node_name (ssh=$ssh_port, blockyard=$by_port, grpc=$grpc_port)..."

    qemu-system-x86_64 \
        -m 1024M \
        -smp 2 \
        -cpu host \
        -enable-kvm \
        -drive "file=$WORK_DIR/${node_name}-disk.qcow2,format=qcow2,if=virtio" \
        -drive "file=$WORK_DIR/${node_name}-zfs.qcow2,format=qcow2,if=virtio" \
        -cdrom "$WORK_DIR/${node_name}-seed.iso" \
        -netdev "user,id=net0,hostfwd=tcp::${ssh_port}-:22,hostfwd=tcp::${by_port}-:7400,hostfwd=tcp::${data_port}-:7401,hostfwd=tcp::${grpc_port}-:7402" \
        -device "virtio-net-pci,netdev=net0" \
        -monitor "tcp:127.0.0.1:${monitor_port},server,nowait" \
        -display none \
        -serial "file:$WORK_DIR/${node_name}-serial.log" \
        -daemonize \
        -pidfile "$pidfile"

    echo "Started $node_name (pid $(cat "$pidfile"))"
}

stop_node() {
    local node_id=$1
    local node_name="node-${node_id}"
    local pidfile="$WORK_DIR/${node_name}.pid"

    if [ -f "$pidfile" ]; then
        local pid
        pid=$(cat "$pidfile")
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid"
            echo "Stopped $node_name (pid $pid)"
        fi
        rm -f "$pidfile"
    fi
}

wait_for_ssh() {
    local node_id=$1
    local ssh_port=$((SSH_PORT_BASE + node_id))
    local timeout=${2:-120}
    local node_name="node-${node_id}"

    echo -n "Waiting for SSH on $node_name (port $ssh_port)..."
    local deadline=$((SECONDS + timeout))
    while [ $SECONDS -lt $deadline ]; do
        if ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
               -o ConnectTimeout=2 -i "$WORK_DIR/id_test" \
               -p "$ssh_port" root@127.0.0.1 "echo ready" 2>/dev/null | grep -q ready; then
            echo " ready"
            return 0
        fi
        sleep 2
        echo -n "."
    done
    echo " TIMEOUT"
    return 1
}

ssh_node() {
    local node_id=$1
    shift
    local ssh_port=$((SSH_PORT_BASE + node_id))
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
        -o ConnectTimeout=5 -i "$WORK_DIR/id_test" \
        -p "$ssh_port" root@127.0.0.1 "$@"
}

deploy_blockyard() {
    local node_id=$1
    local ssh_port=$((SSH_PORT_BASE + node_id))
    local binary="$PROJECT_ROOT/target/release/blockyard"

    if [ ! -f "$binary" ]; then
        echo "ERROR: Build blockyard first: cargo build --release"
        return 1
    fi

    scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
        -i "$WORK_DIR/id_test" \
        -P "$ssh_port" "$binary" root@127.0.0.1:/usr/local/bin/blockyard

    echo "Deployed blockyard to node-${node_id}"
}

case "${1:-}" in
    provision)
        generate_ssh_key
        for i in $(seq 0 $((NODE_COUNT - 1))); do
            create_seed_iso "$i"
            create_node_disk "$i"
        done
        echo "Provisioned $NODE_COUNT nodes"
        ;;
    start)
        for i in $(seq 0 $((NODE_COUNT - 1))); do
            start_node "$i"
        done
        echo "Started $NODE_COUNT nodes"
        ;;
    stop)
        for i in $(seq 0 $((NODE_COUNT - 1))); do
            stop_node "$i"
        done
        echo "Stopped all nodes"
        ;;
    wait-ssh)
        for i in $(seq 0 $((NODE_COUNT - 1))); do
            wait_for_ssh "$i" 120
        done
        ;;
    deploy)
        for i in $(seq 0 $((NODE_COUNT - 1))); do
            deploy_blockyard "$i"
        done
        ;;
    ssh)
        node_id="${2:-0}"
        shift 2 || true
        ssh_node "$node_id" "$@"
        ;;
    status)
        for i in $(seq 0 $((NODE_COUNT - 1))); do
            local_name="node-${i}"
            pidfile="$WORK_DIR/${local_name}.pid"
            if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
                echo "$local_name: running (pid $(cat "$pidfile"))"
            else
                echo "$local_name: stopped"
            fi
        done
        ;;
    *)
        echo "Usage: $0 {provision|start|stop|wait-ssh|deploy|ssh|status} [node_count]"
        echo ""
        echo "Commands:"
        echo "  provision   Create disk images and cloud-init ISOs"
        echo "  start       Boot all QEMU VMs"
        echo "  stop        Shutdown all VMs"
        echo "  wait-ssh    Wait until SSH is available on all nodes"
        echo "  deploy      SCP blockyard binary to all nodes"
        echo "  ssh N CMD   SSH into node N and run CMD"
        echo "  status      Show running status of all nodes"
        exit 1
        ;;
esac
