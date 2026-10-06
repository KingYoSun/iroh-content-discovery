#!/bin/sh
# List the host:port pairs that published to and resolved from the indexer.
#
# Reads the debug log of udp-addr-index: "stored mapping" lines for publishes
# and "read mapping" lines for resolves. The systemd unit in this directory
# enables them with RUST_LOG=info,udp_addr_index=debug.
#
# By default the log is read from the journal of udp-addr-index.service. Pass a
# file, or - for stdin, to read a saved log instead.
#
#     sudo ./peers-from-log.sh
#     sudo ./peers-from-log.sh --since '1 hour ago'
#     journalctl -u udp-addr-index.service -o cat | ./peers-from-log.sh -
set -eu

unit=udp-addr-index.service
since=
until=
file=

usage() {
    echo "usage: $0 [--unit UNIT] [--since TIME] [--until TIME] [FILE | -]" >&2
    exit 2
}

while [ $# -gt 0 ]; do
    case $1 in
        --unit) [ $# -ge 2 ] || usage; unit=$2; shift 2 ;;
        --since) [ $# -ge 2 ] || usage; since=$2; shift 2 ;;
        --until) [ $# -ge 2 ] || usage; until=$2; shift 2 ;;
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
read_log | sed "s/${esc}\[[0-9;]*m//g" | awk '
    function field(name,    s) {
        if (!match($0, "(^|[ \t])" name "=[^ \t]+")) return ""
        s = substr($0, RSTART, RLENGTH)
        sub(/^[ \t]/, "", s)
        return substr(s, length(name) + 2)
    }
    /stored mapping/ {
        peer = field("from")
        if (peer != "") pub[peer]++
        next
    }
    /read mapping/ {
        peer = field("from")
        if (peer == "") next
        res[peer]++
        if (field("found") == "true") hit[peer]++
    }
    END {
        np = 0; for (p in pub) np++
        nr = 0; for (p in res) nr++
        printf "published from %d host:port\n", np
        for (p in pub) printf "  %-24s %8d\n", p, pub[p] | "sort -k2,2nr -k1,1"
        close("sort -k2,2nr -k1,1")
        printf "\nresolved from %d host:port (requests, hits)\n", nr
        for (p in res) printf "  %-24s %8d %8d\n", p, res[p], hit[p] + 0 | "sort -k2,2nr -k1,1"
        close("sort -k2,2nr -k1,1")
        if (np == 0 && nr == 0)
            print "\nno publish or resolve lines found; is udp_addr_index=debug enabled?" > "/dev/stderr"
    }
'
