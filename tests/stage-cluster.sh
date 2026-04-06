#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
IMAGES_DIR="$SCRIPT_DIR/images"
WORK_DIR="$SCRIPT_DIR/.work"
BASE_IMAGE="$IMAGES_DIR/fedora-42-cloud.qcow2"
SSH_KEY="$WORK_DIR/id_stage"
PREFIX="stage"
NODE_COUNT=3

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=5 -i $SSH_KEY"

# Port assignments: stage-0 on 2210/7500/7501, stage-1 on 2211/7510/7511, etc.
ssh_port()  { echo $((2210 + $1)); }
by_port()   { echo $((7500 + $1 * 10)); }
data_port() { echo $((7501 + $1 * 10)); }
mon_port()  { echo $((4450 + $1)); }

mkdir -p "$WORK_DIR"

generate_ssh_key() {
    if [ ! -f "$SSH_KEY" ]; then
        ssh-keygen -t ed25519 -f "$SSH_KEY" -N "" -q
    fi
}

create_node() {
    local i=$1
    local name="${PREFIX}-${i}"
    local seed_dir="$WORK_DIR/${name}-seed"
    local ssh_pub_key
    ssh_pub_key=$(cat "${SSH_KEY}.pub")

    mkdir -p "$seed_dir"
    cat > "$seed_dir/meta-data" << EOF
instance-id: ${name}
local-hostname: ${name}
EOF
    cat > "$seed_dir/user-data" << EOF
#cloud-config
users:
  - name: root
    ssh_authorized_keys:
      - ${ssh_pub_key}
    lock_passwd: false
ssh_pwauth: false
runcmd:
  - modprobe ublk_drv || true
  - modprobe nbd || true
  - echo "${name} ready" > /tmp/ready
EOF
    mkisofs -output "$WORK_DIR/${name}-seed.iso" -volid cidata -joliet -rock \
        "$seed_dir/user-data" "$seed_dir/meta-data" 2>/dev/null

    if [ ! -f "$WORK_DIR/${name}-disk.qcow2" ]; then
        qemu-img create -f qcow2 -b "$BASE_IMAGE" -F qcow2 "$WORK_DIR/${name}-disk.qcow2" 10G 2>/dev/null
    fi
    if [ ! -f "$WORK_DIR/${name}-zfs.qcow2" ]; then
        qemu-img create -f qcow2 "$WORK_DIR/${name}-zfs.qcow2" 5G 2>/dev/null
    fi
}

start_node() {
    local i=$1
    local name="${PREFIX}-${i}"
    local pidfile="$WORK_DIR/${name}.pid"
    if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
        echo "$name: already running"
        return
    fi
    qemu-system-x86_64 \
        -m 1024M -smp 2 -cpu host -enable-kvm \
        -drive "file=$WORK_DIR/${name}-disk.qcow2,format=qcow2,if=virtio" \
        -drive "file=$WORK_DIR/${name}-zfs.qcow2,format=qcow2,if=virtio" \
        -cdrom "$WORK_DIR/${name}-seed.iso" \
        -netdev "user,id=net0,hostfwd=tcp::$(ssh_port $i)-:22,hostfwd=tcp::$(by_port $i)-:7400,hostfwd=tcp::$(data_port $i)-:7401" \
        -device "virtio-net-pci,netdev=net0" \
        -monitor "tcp:127.0.0.1:$(mon_port $i),server,nowait" \
        -display none -serial "file:$WORK_DIR/${name}-serial.log" \
        -daemonize -pidfile "$pidfile"
    echo "$name: started (pid $(cat "$pidfile"), ssh=$(ssh_port $i), blockyard=$(by_port $i))"
}

stop_node() {
    local i=$1
    local name="${PREFIX}-${i}"
    local pidfile="$WORK_DIR/${name}.pid"
    if [ -f "$pidfile" ]; then
        kill "$(cat "$pidfile")" 2>/dev/null && echo "$name: stopped" || echo "$name: already stopped"
        rm -f "$pidfile"
    fi
}

ssh_node() {
    local i=$1; shift
    ssh $SSH_OPTS -p "$(ssh_port $i)" root@127.0.0.1 "$@"
}

wait_ssh() {
    local i=$1
    local deadline=$((SECONDS + 120))
    echo -n "${PREFIX}-${i}: waiting for ssh..."
    while [ $SECONDS -lt $deadline ]; do
        if ssh $SSH_OPTS -p "$(ssh_port $i)" root@127.0.0.1 "echo ready" 2>/dev/null | grep -q ready; then
            echo " ok"
            return 0
        fi
        sleep 2; echo -n "."
    done
    echo " TIMEOUT"
    return 1
}

deploy() {
    local binary="$PROJECT_ROOT/target/release/blockyard"
    [ -f "$binary" ] || { echo "ERROR: build first: cargo build --release --features blockyard-ublk/libublk"; exit 1; }
    for i in $(seq 0 $((NODE_COUNT - 1))); do
        scp $SSH_OPTS -P "$(ssh_port $i)" "$binary" root@127.0.0.1:/usr/local/bin/blockyard
        echo "${PREFIX}-${i}: deployed"
    done
}

configure_and_start() {
    local seeds=""
    for i in $(seq 0 $((NODE_COUNT - 1))); do
        [ -n "$seeds" ] && seeds="${seeds}, "
        seeds="${seeds}\"10.0.2.2:$(by_port $i)\""
    done

    for i in $(seq 0 $((NODE_COUNT - 1))); do
        ssh_node $i "cat > /etc/blockyard.toml << CFGEOF
[node]
name = \"${PREFIX}-${i}\"
listen = \"0.0.0.0:7400\"
data_listen = \"0.0.0.0:7401\"
metrics_listen = \"0.0.0.0:7402\"

[cluster]
seeds = [${seeds}]

[storage]
zfs_pool = \"blockyard\"
CFGEOF
modprobe ublk_drv 2>/dev/null || true
pkill -9 blockyard 2>/dev/null || true
sleep 1
RUST_LOG=info nohup /usr/local/bin/blockyard start --config /etc/blockyard.toml > /var/log/blockyard.log 2>&1 &
echo ${PREFIX}-${i}: blockyard started"
    done
}

status() {
    for i in $(seq 0 $((NODE_COUNT - 1))); do
        local name="${PREFIX}-${i}"
        local pidfile="$WORK_DIR/${name}.pid"
        if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
            local by_running
            by_running=$(ssh_node $i "pgrep -c blockyard 2>/dev/null" 2>/dev/null || echo "?")
            echo "$name: vm=running blockyard=${by_running} ssh=$(ssh_port $i) port=$(by_port $i)"
        else
            echo "$name: vm=stopped"
        fi
    done
}

case "${1:-help}" in
    up)
        generate_ssh_key
        for i in $(seq 0 $((NODE_COUNT - 1))); do create_node $i; done
        for i in $(seq 0 $((NODE_COUNT - 1))); do start_node $i; done
        for i in $(seq 0 $((NODE_COUNT - 1))); do wait_ssh $i; done
        deploy
        configure_and_start
        sleep 2
        echo "=== Cluster ready ==="
        status
        echo ""
        echo "CLI endpoint: http://127.0.0.1:$(data_port 0)"
        ;;
    down)
        for i in $(seq 0 $((NODE_COUNT - 1))); do
            ssh_node $i "pkill -9 blockyard 2>/dev/null" 2>/dev/null || true
            stop_node $i
        done
        echo "Cluster stopped"
        ;;
    destroy)
        for i in $(seq 0 $((NODE_COUNT - 1))); do
            ssh_node $i "pkill -9 blockyard 2>/dev/null" 2>/dev/null || true
            stop_node $i
        done
        rm -rf "$WORK_DIR"/${PREFIX}-*
        echo "Cluster destroyed"
        ;;
    status)
        status
        ;;
    ssh)
        shift; node=${1:-0}; shift || true
        ssh_node "$node" "$@"
        ;;
    *)
        echo "Usage: $0 {up|down|destroy|status|ssh N [cmd]}"
        echo ""
        echo "  up       Provision, start, deploy, configure 3-node stage cluster"
        echo "  down     Stop all VMs and blockyard processes"
        echo "  destroy  Stop and delete all VM images"
        echo "  status   Show cluster status"
        echo "  ssh N    SSH into stage-N (0-2)"
        ;;
esac
