#!/bin/sh
# List the host:port pairs that published to and resolved from the indexer.
#
# Reads the debug log of udp-addr-index: "stored mapping" lines for publishes
# and "read mapping" lines for resolves. The systemd unit in this directory
# enables them with RUST_LOG=info,udp_addr_index=debug.
#
# By default the log is read from the journal of udp-addr-index.service. Pass a
# file, or - for stdin, to read a saved log instead. --by-ip groups by address
# alone, since clients often use a new source port for each socket.
#
#     sudo ./peers-from-log.sh
#     sudo ./peers-from-log.sh --since '1 hour ago'
#     sudo ./peers-from-log.sh --by-ip
#     journalctl -u udp-addr-index.service -o cat | ./peers-from-log.sh -
set -eu

unit=udp-addr-index.service
since=
until=
file=
by_ip=0

usage() {
    echo "usage: $0 [--unit UNIT] [--since TIME] [--until TIME] [--by-ip] [FILE | -]" >&2
    exit 2
}

while [ $# -gt 0 ]; do
    case $1 in
        --unit) [ $# -ge 2 ] || usage; unit=$2; shift 2 ;;
        --since) [ $# -ge 2 ] || usage; since=$2; shift 2 ;;
        --until) [ $# -ge 2 ] || usage; until=$2; shift 2 ;;
        --by-ip) by_ip=1; shift ;;
        -h|--help) usage ;;
        -) file=-; shift ;;
        -*) usage ;;
        *) file=$1; shift ;;
    esac
done

read_log() {
    if [ "$file" = - ]; then
        cat
    elif [ -n "$file" ]; then
        cat -- "$file"
    else
        set -- journalctl -u "$unit" -o cat --no-pager
        [ -z "$since" ] || set -- "$@" --since "$since"
        [ -z "$until" ] || set -- "$@" --until "$until"
        "$@"
    fi
}

# Strip ANSI colors, then count per host:port. Only POSIX awk features, so it
# runs with mawk (Debian/Ubuntu default) as well as gawk.
esc=$(printf '\033')
read_log | sed "s/${esc}\[[0-9;]*m//g" | awk -v by_ip="$by_ip" '
    function field(name,    s) {
        if (!match($0, "(^|[ \t])" name "=[^ \t]+")) return ""
        s = substr($0, RSTART, RLENGTH)
        sub(/^[ \t]/, "", s)
        return substr(s, length(name) + 2)
    }
    # host:port, or just the host with --by-ip ("[v6]:port" keeps its brackets)
    function peer_of(    p) {
        p = field("from")
        if (by_ip) sub(/:[0-9]+$/, "", p)
        return p
    }
    function what(n) { return by_ip ? (n == 1 ? "address" : "addresses") : "host:port" }
    /stored mapping/ {
        peer = peer_of()
        if (peer != "") pub[peer]++
        next
    }
    /read mapping/ {
        peer = peer_of()
        if (peer == "") next
        res[peer]++
        if (field("found") == "true") hit[peer]++
    }
    END {
        np = 0; for (p in pub) np++
        nr = 0; for (p in res) nr++
        printf "published from %d %s\n", np, what(np)
        for (p in pub) printf "  %-24s %8d\n", p, pub[p] | "sort -k2,2nr -k1,1"
        close("sort -k2,2nr -k1,1")
        printf "\nresolved from %d %s (requests, hits)\n", nr, what(nr)
        for (p in res) printf "  %-24s %8d %8d\n", p, res[p], hit[p] + 0 | "sort -k2,2nr -k1,1"
        close("sort -k2,2nr -k1,1")
        if (np == 0 && nr == 0)
            print "\nno publish or resolve lines found; is udp_addr_index=debug enabled?" > "/dev/stderr"
    }
'
