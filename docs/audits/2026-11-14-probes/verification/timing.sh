#!/bin/sh
cd "$1"
c() { # label, regex
  n=$(grep -rnE "$2" tests --include='*.py' | wc -l | tr -d ' ')
  f=$(grep -rlE "$2" tests --include='*.py' | wc -l | tr -d ' ')
  echo "$1: $n sites in $f files"
  grep -rcE "$2" tests --include='*.py' | sort -t: -k2 -rn | head -${3:-6} | sed 's/^/    /'
}
c 'time.sleep(' 'time\.sleep\('
c 'timeout=0.x' 'timeout=0\.[0-9]'
c 'monotonic()' 'monotonic\(\)'
c 'perf_counter()' 'perf_counter\(\)'
c 'assert on elapsed/took/waited' 'assert .*(elapsed|took|waited|duration)[^=]*[<>]|assert [0-9.]+ *<=? *(elapsed|took|waited)'
c 'cpu_window(' 'cpu_window\('
c 'release_after_clock(' 'release_after_clock\('
c 'setswitchinterval' 'setswitchinterval'
c 'threading.Event()' 'threading\.Event\(\)'
c 'threading.Barrier(' 'Barrier\('
c 'run_concurrently(' 'run_concurrently\('
c 'run_in_child(' 'run_in_child\('
c 'os.fork()' 'os\.fork\(\)'
c 'subprocess.run' 'subprocess\.(run|Popen|check)'
c '.wait(timeout / .wait(N' '\.wait\((timeout=)?[0-9.]+\)'
c '.join(timeout' '\.join\((timeout=)?[0-9.]+\)'
